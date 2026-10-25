use crate::history::{GapReason, HistoryLimits, RamHistory};
use crate::transcripts::{TranscriptBuffer, TranscriptionStatus};
use bytes::Bytes;
use liazo_core::transcript::Transcript;
use liazo_core::{
    catalog::SourceSnapshot,
    control::{Event, Failure},
    protocol::{AudioMeta, CaptureSettings, ImageSubscription, SourceAvailability, SourceKey},
    wire::{Frame, Header, ImageMeta},
};
use liazo_transport::SessionBinding;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

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
    payload_id: Uuid,
}
struct Health {
    observed_ns: u64,
    failure: Failure,
}

pub(crate) struct LiveState {
    pub history: RamHistory,
    pub transcripts: TranscriptBuffer,
    pub binding: SessionBinding,
    pub connection: ConnectionState,
    pub cursor: u64,
    pub revision: u64,
    pub settings: CaptureSettings,
    pub sources: SourceSnapshot,
    pub clock: Option<ClockMap>,
    expected: Vec<ImageSubscription>,
    expected_audio: Vec<SourceKey>,
    images: HashMap<SourceKey, Image>,
    audio: HashMap<SourceKey, AudioMeta>,
    failures: HashMap<SourceKey, Health>,
    pub gaps: u64,
}

impl LiveState {
    pub fn new(binding: SessionBinding) -> Self {
        Self {
            history: RamHistory::new(HistoryLimits::default()).unwrap(),
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
        self.expected = settings.images.clone();
        self.expected_audio = settings.audio.clone();
    }
    pub fn applied(&mut self, revision: u64, settings: CaptureSettings, now: u64) {
        for old in &self.settings.images {
            if !settings
                .images
                .iter()
                .any(|image| image.source == old.source)
            {
                self.history
                    .gap(old.source.clone(), now, GapReason::Stopped);
            }
        }
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

    pub fn sources_changed(&mut self, sources: SourceSnapshot, now: u64) {
        for image in self.settings.images.iter().chain(&self.expected) {
            let availability = sources
                .sources
                .iter()
                .find(|source| source.key == image.source)
                .map(|source| source.availability);
            let reason = match availability {
                Some(SourceAvailability::Available) => continue,
                Some(SourceAvailability::PermissionRequired) => GapReason::PermissionRequired,
                _ => GapReason::Unavailable,
            };
            self.history.gap(image.source.clone(), now, reason);
        }
        self.sources = sources;
        self.changed();
    }

    pub fn capture_failure(&self, sources: &[SourceKey], not_before: u64) -> Option<Failure> {
        sources
            .iter()
            .filter_map(|source| self.failures.get(source))
            .find(|health| health.observed_ns >= not_before)
            .map(|health| health.failure.clone())
    }
    fn prune(&mut self) {
        self.images.retain(|source, _| {
            self.settings
                .images
                .iter()
                .any(|image| image.source == *source)
                || self.expected.iter().any(|image| image.source == *source)
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

    pub fn image(&mut self, frame: Frame, now: u64) -> Result<(), ()> {
        let (meta, unchanged) = match frame.header {
            Header::Image(meta) => (meta, false),
            Header::Unchanged(meta) => (meta, true),
            _ => return Err(()),
        };
        if meta.configuration_revision < self.revision
            || !(self
                .expected
                .iter()
                .any(|image| image.source == meta.source)
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
        let payload_id = if unchanged {
            old.unwrap().payload_id
        } else {
            Uuid::new_v4()
        };
        let interval_ms = self
            .expected
            .iter()
            .chain(&self.settings.images)
            .find(|image| image.source == meta.source)
            .map_or(500, |image| image.interval_ms);
        self.history.image(
            meta.clone(),
            payload_id,
            payload.clone(),
            now,
            Duration::from_millis(u64::from(interval_ms) * 3 + 1000),
        );
        let recovered = self
            .failures
            .get(&meta.source)
            .is_some_and(|failure| failure.observed_ns <= meta.captured_ns);
        if recovered {
            self.failures.remove(&meta.source);
        }
        self.images.insert(
            meta.source.clone(),
            Image {
                metadata: meta,
                payload,
                changed_at,
                payload_id,
            },
        );
        if !unchanged || skipped || recovered {
            self.changed();
        }
        Ok(())
    }

    pub fn audio(&mut self, frame: &mut Frame) -> Result<(bool, bool), ()> {
        let Header::Audio(meta) = &mut frame.header else {
            return Err(());
        };
        if !self.settings.audio.contains(&meta.source)
            && !self.expected_audio.contains(&meta.source)
        {
            return Ok((false, false));
        }
        let mut significant = false;
        if let Some(old) = self.audio.get(&meta.source) {
            if old.stream_id == meta.stream_id && meta.sequence <= old.sequence {
                return Ok((false, false));
            }
            if meta.discontinuity
                || old.stream_id != meta.stream_id
                || old.sample_rate != meta.sample_rate
                || old.channels != meta.channels
                || old.sequence.saturating_add(1) != meta.sequence
                || old.sample_position + u64::from(old.frames) != meta.sample_position
            {
                meta.discontinuity = true;
                self.gaps += 1;
                significant = true;
            }
        } else {
            meta.discontinuity = true;
            significant = true;
        }
        if self
            .failures
            .get(&meta.source)
            .is_some_and(|health| health.observed_ns <= meta.captured_ns)
        {
            self.failures.remove(&meta.source);
            significant = true;
        }
        self.audio.insert(meta.source.clone(), meta.clone());
        if significant {
            self.changed();
        }
        Ok((true, significant))
    }

    pub fn event(&mut self, event: Event, now: u64) {
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
            let significant = match (self.failures.get(&source), failure.as_ref()) {
                (None, None) => false,
                (Some(old), Some(new)) => {
                    old.failure.code != new.code || old.failure.message != new.message
                }
                _ => true,
            };
            if let Some(failure) = failure {
                let reason = if failure.code == liazo_core::control::FailureCode::PermissionRequired
                {
                    GapReason::PermissionRequired
                } else {
                    GapReason::Unavailable
                };
                self.history.gap(source.clone(), now, reason);
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
            if significant {
                self.changed();
            }
        }
    }

    pub fn connection_ended(&mut self, now: u64) {
        let reason = if self.connection == ConnectionState::Stopped {
            GapReason::Stopped
        } else {
            GapReason::Disconnected
        };
        for source in self.images.keys() {
            self.history.gap(source.clone(), now, reason);
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
        let images =
            self.images
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
                            } else if self.failures.get(&image.metadata.source).is_some_and(
                                |health| {
                                    health.failure.code
                                        == liazo_core::control::FailureCode::PermissionRequired
                                },
                            ) || source.is_some_and(|source| {
                                source.availability == SourceAvailability::PermissionRequired
                            }) {
                                Freshness::PermissionRequired
                            } else if self.failures.contains_key(&image.metadata.source)
                                || source.is_none_or(|source| {
                                    source.availability == SourceAvailability::Unavailable
                                })
                            {
                                Freshness::Unavailable
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn audio_progress_is_current_but_only_lifecycle_and_gaps_advance_the_observation_cursor() {
        let key = SourceKey {
            id: "audio:fixture".into(),
            generation: Uuid::new_v4(),
        };
        let mut state = LiveState::new(SessionBinding {
            session: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        });
        state.connection = ConnectionState::Connected;
        state.applied(
            1,
            CaptureSettings {
                images: vec![],
                audio: vec![key.clone()],
            },
            0,
        );
        let mut meta = AudioMeta {
            source: key,
            stream_id: Uuid::new_v4(),
            sequence: 1,
            captured_ns: 0,
            sample_position: 0,
            sample_rate: 16000,
            channels: 1,
            frames: 160,
            discontinuity: false,
        };
        let mut frame = Frame::new(Header::Audio(meta.clone()), Bytes::from(vec![0; 640])).unwrap();
        assert_eq!(state.audio(&mut frame), Ok((true, true)));
        let cursor = state.cursor;
        for sequence in 2..=100 {
            meta.sequence = sequence;
            meta.sample_position = (sequence - 1) * 160;
            meta.captured_ns = sequence * 10_000_000;
            let mut frame =
                Frame::new(Header::Audio(meta.clone()), Bytes::from(vec![0; 640])).unwrap();
            assert_eq!(state.audio(&mut frame), Ok((true, false)));
        }
        assert_eq!(state.cursor, cursor);
        assert_eq!(
            state.observe(cursor, false, &[], 2_000_000_000).audio[0].sequence,
            100
        );
        meta.sequence = 102;
        meta.sample_position += 320;
        let mut frame = Frame::new(Header::Audio(meta), Bytes::from(vec![0; 640])).unwrap();
        assert_eq!(state.audio(&mut frame), Ok((true, true)));
        assert!(state.cursor > cursor);
        assert_eq!(state.gaps, 1);
    }
}
