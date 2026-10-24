use bytes::Bytes;
use liazo_core::{
    control::{Failure, FailureCode},
    protocol::SourceKey,
    wire::{Frame, Header, ImageMeta},
};
use std::{collections::HashMap, sync::Mutex};
use tokio::sync::Notify;
use uuid::Uuid;

struct Slot {
    meta: ImageMeta,
    bytes: Bytes,
}
struct Selected {
    epoch: Uuid,
    minimum_revision: u64,
    image: Option<Slot>,
}
#[derive(Default)]
struct State {
    selected: HashMap<SourceKey, Selected>,
    bytes: usize,
}

pub struct LatestImages {
    state: Mutex<State>,
    changed: Notify,
    max_bytes: usize,
}

struct Sent {
    selection: Uuid,
    meta: ImageMeta,
}
#[derive(Default)]
pub struct SentImages(HashMap<SourceKey, Sent>);

fn invalid(message: &str) -> Failure {
    Failure::new(FailureCode::InvalidRequest, message)
}

impl LatestImages {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            max_bytes,
        }
    }

    pub fn select(&self, keys: &[SourceKey]) -> Result<(), Failure> {
        self.configure(keys, 0)
    }

    pub fn configure(&self, keys: &[SourceKey], revision: u64) -> Result<(), Failure> {
        if keys.len() > 8 {
            return Err(invalid("too many image sources"));
        }
        for key in keys {
            key.validate()
                .map_err(|_| invalid("invalid image source"))?;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.selected.retain(|key, _| keys.contains(key));
        for key in keys {
            state
                .selected
                .entry(key.clone())
                .or_insert_with(|| Selected {
                    epoch: Uuid::new_v4(),
                    minimum_revision: revision,
                    image: None,
                });
            state.selected.get_mut(key).unwrap().minimum_revision = revision;
        }
        state.bytes = state
            .selected
            .values()
            .filter_map(|selected| selected.image.as_ref())
            .map(|slot| slot.bytes.len())
            .sum();
        self.changed.notify_one();
        Ok(())
    }

    pub fn bytes_held(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).bytes
    }

    pub fn publish(&self, frame: Frame) -> Result<(), Failure> {
        let frame = Frame::new(frame.header, frame.payload)
            .map_err(|_| invalid("invalid captured image"))?;
        let (meta, unchanged) = match frame.header {
            Header::Image(meta) => (meta, false),
            Header::Unchanged(meta) => (meta, true),
            _ => return Err(invalid("expected image data")),
        };
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(selected) = state.selected.get(&meta.source) else {
            return Ok(());
        };
        if meta.configuration_revision < selected.minimum_revision {
            return Ok(());
        }
        let previous = selected.image.as_ref();
        if previous.is_some_and(|old| {
            (old.meta.configuration_revision, old.meta.sequence)
                >= (meta.configuration_revision, meta.sequence)
        }) {
            return Ok(());
        }
        let bytes = if unchanged {
            let old = previous
                .filter(|old| compatible(&old.meta, &meta))
                .ok_or_else(|| invalid("unchanged image has no matching payload"))?;
            old.bytes.clone()
        } else {
            frame.payload
        };
        let total = state.bytes - previous.map_or(0, |old| old.bytes.len()) + bytes.len();
        if total > self.max_bytes {
            return Err(Failure::new(
                FailureCode::Unavailable,
                "live image memory limit exceeded",
            ));
        }
        let selected = state.selected.get_mut(&meta.source).unwrap();
        selected.image = Some(Slot { meta, bytes });
        state.bytes = total;
        self.changed.notify_one();
        Ok(())
    }

    pub async fn next(&self, sent: &mut SentImages) -> Frame {
        loop {
            let changed = self.changed.notified();
            {
                let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                sent.0
                    .retain(|source, _| state.selected.contains_key(source));
                let ready = state
                    .selected
                    .iter()
                    .filter_map(|(source, selected)| {
                        let image = selected.image.as_ref()?;
                        if image.meta.configuration_revision < selected.minimum_revision {
                            return None;
                        }
                        let previous = sent.0.get(source);
                        let pending = previous.is_none_or(|old| {
                            old.selection != selected.epoch
                                || (old.meta.configuration_revision, old.meta.sequence)
                                    < (image.meta.configuration_revision, image.meta.sequence)
                        });
                        pending.then_some((source, selected, image))
                    })
                    .min_by_key(|(_, _, image)| image.meta.captured_ns);
                if let Some((source, selected, image)) = ready {
                    let duplicate = sent.0.get(source).is_some_and(|old| {
                        old.selection == selected.epoch
                            && old.meta.configuration_revision == image.meta.configuration_revision
                            && compatible(&old.meta, &image.meta)
                    });
                    let header = if duplicate {
                        Header::Unchanged(image.meta.clone())
                    } else {
                        Header::Image(image.meta.clone())
                    };
                    let payload = if duplicate {
                        Bytes::new()
                    } else {
                        image.bytes.clone()
                    };
                    sent.0.insert(
                        source.clone(),
                        Sent {
                            selection: selected.epoch,
                            meta: image.meta.clone(),
                        },
                    );
                    return Frame::new(header, payload).expect("validated mailbox image");
                }
            }
            changed.await;
        }
    }
}

fn compatible(a: &ImageMeta, b: &ImageMeta) -> bool {
    a.fingerprint == b.fingerprint
        && a.width == b.width
        && a.height == b.height
        && a.format == b.format
}
