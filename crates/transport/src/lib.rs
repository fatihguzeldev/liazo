mod identity;
mod peer;

pub use identity::{Fingerprint, Identity, fingerprint};
pub use peer::{PeerClient, PeerConnection, PeerServer};

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error(transparent)]
    Tls(#[from] rustls::Error),
    #[error(transparent)]
    Certificate(#[from] rcgen::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("peer connection timed out")]
    Timeout,
    #[error("peer certificate is not paired")]
    UntrustedPeer,
    #[error("unsupported peer protocol")]
    Protocol,
    #[error("invalid peer identity")]
    InvalidIdentity,
    #[error("client verification setup failed: {0}")]
    Verifier(String),
}
