use liazo_transport::{CredentialError, Fingerprint, StoredPeer};

pub trait TrustStore: Send + Sync + 'static {
    fn peers(&self) -> Result<Vec<StoredPeer>, CredentialError>;
    fn save(&self, peer: StoredPeer) -> Result<(), CredentialError>;
    fn remove(&self, peer: Fingerprint) -> Result<bool, CredentialError>;
}
