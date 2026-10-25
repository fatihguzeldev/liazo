use crate::transcripts::{TranscriptBuffer, TranscriptionStatus};
use bytes::Bytes;
use liazo_core::transcript::Transcript;
use liazo_core::{
    catalog::SourceSnapshot,
    control::{Event, Failure},
    protocol::{AudioMeta, CaptureSettings, SourceAvailability, SourceKey},
    wire::{Frame, Header, ImageMeta},
};
use liazo_transport::SessionBinding;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Connecting,
    Connected,
    Stopped,
    Disconnected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Freshness {
    Live,
    Waiting,
    Stale,
    Unavailable,
    PermissionRequired,
    Disconnected,
    Stopped,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct AgeRange {
    pub minimum_ms: u64,
    pub maximum_ms: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct ClockMap {
    lower_offset: i128,
    upper_offset: i128,
}

impl ClockMap {
    pub fn new(
        sent: u64,
        received: u64,
        provider_received: u64,
        provider_sent: u64,
    ) -> Option<Self> {
        if received < sent || provider_sent < provider_received {
            return None;
        }
        let lower_offset = i128::from(sent) - i128::from(provider_received);
        let upper_offset = i128::from(received) - i128::from(provider_sent);
        (lower_offset <= upper_offset).then_some(Self {
            lower_offset,
            upper_offset,
        })
    }
    fn age(self, start: u64, end: u64, now: u64) -> AgeRange {
        let minimum = (i128::from(now) - i128::from(end) - self.upper_offset).max(0);
        let maximum = (i128::from(now) - i128::from(start) - self.lower_offset).max(0);
        AgeRange {
            minimum_ms: (minimum / 1_000_000).min(i128::from(u64::MAX)) as u64,
            maximum_ms: ((maximum + 999_999) / 1_000_000).min(i128::from(u64::MAX)) as u64,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct ObservedImage {
    pub metadata: ImageMeta,
    pub freshness: Freshness,
    pub age: Option<AgeRange>,
    #[serde(skip)]
    pub payload: Option<Bytes>,
}

#[derive(Clone, Serialize)]
pub struct Observation {
    pub binding: SessionBinding,
    pub cursor: u64,
    pub connection: ConnectionState,
    pub configuration_revision: u64,
    pub source_revision: u64,
    pub images: Vec<ObservedImage>,
    pub audio: Vec<AudioMeta>,
    pub transcripts: Vec<Transcript>,
    pub transcription: TranscriptionStatus,
    pub transcript_deltas_lost: bool,
    pub failures: Vec<(SourceKey, Failure)>,
    pub gaps: u64,
}

struct Image {
    metadata: ImageMeta,
    payload: Bytes,
    changed_at: u64,
}
struct Health {
    observed_ns: u64,
    failure: Failure,
}

pub(crate) struct LiveState {
    pub transcripts: TranscriptBuffer,
    pub binding: SessionBinding,
    pub connection: ConnectionState,
    pub cursor: u64,
    pub revision: u64,
    pub settings: CaptureSettings,
    pub sources: SourceSnapshot,
    pub clock: Option<ClockMap>,
    expected: Vec<SourceKey>,
    expected_audio: Vec<SourceKey>,
    images: HashMap<SourceKey, Image>,
    audio: HashMap<SourceKey, AudioMeta>,
    failures: HashMap<SourceKey, Health>,
    pub gaps: u64,
}

impl LiveState {
    pub fn new(binding: SessionBinding) -> Self {
        Self {
            transcripts: TranscriptBuffer::default(),
            binding,
            connection: ConnectionState::Connecting,
            cursor: 0,
            revision: 0,
            settings: CaptureSettings::default(),
            sources: SourceSnapshot {
                revision: 0,
                sources: vec![],
                issues: vec![],
            },
            clock: None,
            expected: vec![],
            expected_audio: vec![],
            images: HashMap::new(),
            audio: HashMap::new(),
            failures: HashMap::new(),
            gaps: 0,
        }
    }
    pub fn changed(&mut self) {
        self.cursor = self.cursor.saturating_add(1);
    }
    pub fn preparing(&mut self, settings: &CaptureSettings) {
        self.expected = settings
            .images
            .iter()
            .map(|image| image.source.clone())
            .collect();
        self.expected_audio = settings.audio.clone();
    }
    pub fn applied(&mut self, revision: u64, settings: CaptureSettings) {
        self.revision = revision;
        self.settings = settings;
        self.expected.clear();
        self.expected_audio.clear();
        self.prune();
        self.changed();
    }
    pub fn config_failed(&mut self) {
        self.expected.clear();
        self.expected_audio.clear();
        self.prune();
    }
    fn prune(&mut self) {
        self.images.retain(|source, _| {
            self.settings
                .images
                .iter()
                .any(|image| image.source == *source)
                || self.expected.contains(source)
        });
        self.audio.retain(|source, _| {
            self.settings.audio.contains(source) || self.expected_audio.contains(source)
        });
        self.failures.retain(|source, _| {
            self.settings
                .images
                .iter()
                .any(|image| image.source == *source)
                || self.settings.audio.contains(source)
        });
    }

    pub fn image(&mut self, frame: Frame) -> Result<(), ()> {
        let (meta, unchanged) = match frame.header {
            Header::Image(meta) => (meta, false),
            Header::Unchanged(meta) => (meta, true),
            _ => return Err(()),
        };
        if meta.configuration_revision < self.revision
            || !(self.expected.contains(&meta.source)
                || self
                    .settings
                    .images
                    .iter()
                    .any(|image| image.source == meta.source))
        {
            return Ok(());
        }
        let old = self.images.get(&meta.source);
        if old.is_some_and(|old| {
            (old.metadata.configuration_revision, old.metadata.sequence)
                >= (meta.configuration_revision, meta.sequence)
        }) {
            return Ok(());
        }
        let payload = if unchanged {
            old.filter(|old| {
                old.metadata.fingerprint == meta.fingerprint
                    && old.metadata.format == meta.format
                    && old.metadata.width == meta.width
                    && old.metadata.height == meta.height
            })
            .ok_or(())?
            .payload
            .clone()
        } else {
            frame.payload
        };
        let bytes: usize = self.images.values().map(|image| image.payload.len()).sum();
        if bytes - old.map_or(0, |image| image.payload.len()) + payload.len() > 512 * 1024 * 1024 {
            return Err(());
        }
        let skipped = old.is_some_and(|old| {
            old.metadata.configuration_revision == meta.configuration_revision
                && meta.sequence != old.metadata.sequence.saturating_add(1)
        });
        if skipped {
            self.gaps += 1;
        }
        let changed_at = if unchanged {
            old.unwrap().changed_at
        } else {
            self.cursor.saturating_add(1)
        };
        if self
            .failures
            .get(&meta.source)
            .is_some_and(|failure| failure.observed_ns <= meta.captured_ns)
        {
            self.failures.remove(&meta.source);
        }
        self.images.insert(
            meta.source.clone(),
            Image {
                metadata: meta,
                payload,
                changed_at,
            },
        );
        self.changed();
        Ok(())
    }

    pub fn audio(&mut self, frame: &mut Frame) -> Result<bool, ()> {
        let Header::Audio(meta) = &mut frame.header else {
            return Err(());
        };
        if !self.settings.audio.contains(&meta.source)
            && !self.expected_audio.contains(&meta.source)
        {
            return Ok(false);
        }
        if let Some(old) = self.audio.get(&meta.source) {
            if old.stream_id == meta.stream_id && meta.sequence <= old.sequence {
                return Ok(false);
            }
            if old.stream_id != meta.stream_id
                || old.sequence.saturating_add(1) != meta.sequence
                || old.sample_position + u64::from(old.frames) != meta.sample_position
            {
                meta.discontinuity = true;
                self.gaps += 1;
            }
        } else {
            meta.discontinuity = true;
        }
        self.failures.remove(&meta.source);
        self.audio.insert(meta.source.clone(), meta.clone());
        self.changed();
        Ok(true)
    }

    pub fn event(&mut self, event: Event) {
        if let Event::SourceStatus {
            source,
            revision,
            observed_ns,
            failure,
        } = event
        {
            let selected_image = self
                .settings
                .images
                .iter()
                .any(|image| image.source == source);
            if (selected_image && revision < self.revision)
                || (!selected_image && !self.settings.audio.contains(&source))
            {
                return;
            }
            if self
                .images
                .get(&source)
                .is_some_and(|image| image.metadata.captured_ns > observed_ns)
            {
                return;
            }
            if let Some(failure) = failure {
                self.failures.insert(
                    source,
                    Health {
                        observed_ns,
                        failure,
                    },
                );
            } else {
                self.failures.remove(&source);
            }
            self.changed();
        }
    }

    pub fn fresh_after(&self, sources: &[SourceKey], revision: u64, not_before_ns: u64) -> bool {
        sources.iter().all(|source| {
            self.images.get(source).is_some_and(|image| {
                image.metadata.configuration_revision == revision
                    && image.metadata.capture_started_ns >= not_before_ns
            })
        })
    }

    pub fn observe(
        &self,
        after: u64,
        include_current: bool,
        selected: &[SourceKey],
        now_ns: u64,
    ) -> Observation {
        let images = self
            .images
            .values()
            .filter(|image| selected.is_empty() || selected.contains(&image.metadata.source))
            .map(|image| {
                let age = self.clock.map(|clock| {
                    clock.age(
                        image.metadata.capture_started_ns,
                        image.metadata.captured_ns,
                        now_ns,
                    )
                });
                let source = self
                    .sources
                    .sources
                    .iter()
                    .find(|source| source.key == image.metadata.source);
                let limit = self
                    .settings
                    .images
                    .iter()
                    .find(|setting| setting.source == image.metadata.source)
                    .map_or(2000, |setting| u64::from(setting.interval_ms) * 3 + 1000);
                let freshness = match self.connection {
                    ConnectionState::Stopped => Freshness::Stopped,
                    ConnectionState::Disconnected => Freshness::Disconnected,
                    ConnectionState::Connecting => Freshness::Waiting,
                    ConnectionState::Connected => {
                        if image.metadata.configuration_revision != self.revision {
                            Freshness::Waiting
                        } else if self.failures.contains_key(&image.metadata.source)
                            || source.is_none_or(|source| {
                                source.availability == SourceAvailability::Unavailable
                            })
                        {
                            Freshness::Unavailable
                        } else if source.is_some_and(|source| {
                            source.availability == SourceAvailability::PermissionRequired
                        }) {
                            Freshness::PermissionRequired
                        } else if age.is_none() {
                            Freshness::Waiting
                        } else if age.is_some_and(|age| age.maximum_ms > limit) {
                            Freshness::Stale
                        } else {
                            Freshness::Live
                        }
                    }
                };
                ObservedImage {
                    metadata: image.metadata.clone(),
                    freshness,
                    age,
                    payload: (include_current || image.changed_at > after)
                        .then(|| image.payload.clone()),
                }
            })
            .collect();
        let (transcripts, transcript_deltas_lost) =
            self.transcripts.snapshot(after, selected, now_ns);
        Observation {
            binding: self.binding,
            cursor: self.cursor,
            connection: self.connection.clone(),
            configuration_revision: self.revision,
            source_revision: self.sources.revision,
            images,
            audio: self.audio.values().cloned().collect(),
            transcripts,
            transcription: self.transcripts.status.clone(),
            transcript_deltas_lost,
            failures: self
                .failures
                .iter()
                .map(|(source, health)| (source.clone(), health.failure.clone()))
                .collect(),
            gaps: self.gaps,
        }
    }
}
