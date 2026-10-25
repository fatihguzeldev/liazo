pub mod archive;
pub mod backend;
mod capture;
pub mod history;
pub mod images;
mod live;
pub mod provider;
pub mod receiver;
mod speech;
mod transcripts;
pub use live::{AgeRange, AudioAge, ConnectionState, Freshness, Observation, ObservedImage};
pub use transcripts::TranscriptionStatus;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Archive(#[from] archive::ArchiveError),
    #[error(transparent)]
    Transcription(#[from] liazo_transcription::TranscriptionError),
    #[error(transparent)]
    Wire(#[from] liazo_core::wire::WireError),
    #[error(transparent)]
    Transport(#[from] liazo_transport::TransportError),
    #[error(transparent)]
    Failure(#[from] liazo_core::control::Failure),
    #[error("session closed")]
    Closed,
    #[error("invalid session message")]
    Protocol,
}
