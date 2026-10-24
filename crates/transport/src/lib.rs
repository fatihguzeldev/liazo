mod identity;
mod pairing;
mod peer;
mod session;

pub use identity::{Fingerprint, Identity, fingerprint};
pub use pairing::{
    ApprovedPeer, PairingInvitation, PairingServer, PendingPairing, request_pairing,
};
pub use peer::{PeerClient, PeerConnection, PeerServer};
pub use session::{AcceptedLane, LaneKind, PeerSession, SessionBinding, SessionServer};

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
    #[error("pairing is closed or the invitation is invalid")]
    PairingClosed,
    #[error("pairing was denied")]
    PairingDenied,
}
