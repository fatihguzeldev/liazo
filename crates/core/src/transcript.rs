use crate::protocol::{ProtocolError, SourceKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    Silence,
    Limit,
    Stopped,
    Gap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptionGapReason {
    AudioDiscontinuity,
    InvalidAudio,
    Backpressure,
    EngineFailure,
    Interrupted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TranscriptionGap {
    pub source: SourceKey,
    pub utterance: Option<Uuid>,
    pub reason: TranscriptionGapReason,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transcript {
    pub id: Uuid,
    pub source: SourceKey,
    pub stream_id: Uuid,
    pub revision: u32,
    // The covered audio range, in the provider's monotonic clock, not word timestamps.
    pub start_ns: u64,
    pub end_ns: u64,
    pub text: String,
    // A final revision is immutable. Gap means the utterance was interrupted.
    pub ending: Option<EndReason>,
}

impl Transcript {
    pub fn supersedes(&self, previous: &Self) -> bool {
        self.id == previous.id
            && previous.ending.is_none()
            && self.revision > previous.revision
            && self.source == previous.source
            && self.stream_id == previous.stream_id
            && self.start_ns == previous.start_ns
            && self.end_ns >= previous.end_ns
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.source.validate()?;
        if self.id.is_nil()
            || self.stream_id.is_nil()
            || self.revision == 0
            || self.end_ns < self.start_ns
            || self.end_ns - self.start_ns > 30_000_000_000
            || self.text.len() > 16_384
            || self
                .text
                .chars()
                .any(|ch| ch.is_control() && ch != '\n' && ch != '\t')
        {
            return Err(ProtocolError("invalid transcript revision"));
        }
        Ok(())
    }
}
