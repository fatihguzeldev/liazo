use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use keyring_core::{Entry, api::CredentialStore};
use rustls::pki_types::CertificateDer;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{Fingerprint, Identity, fingerprint};

// Windows Credential Manager's per-entry limit also bounds records on the Mac.
const MAX_RECORD: usize = 2560;
const MAX_PEERS: usize = 32;

#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("credential store access was denied")]
    AccessDenied,
    #[error("credential store operation failed")]
    Storage,
    #[error("stored device identity or peer trust is invalid")]
    InvalidRecord,
    #[error("paired device limit reached")]
    TooManyPeers,
    #[error("native credential storage is unavailable on this platform")]
    Unsupported,
}

impl From<keyring_core::Error> for CredentialError {
    fn from(error: keyring_core::Error) -> Self {
        match error {
            keyring_core::Error::NoStorageAccess(_) => Self::AccessDenied,
            _ => Self::Storage,
        }
    }
}

#[derive(Clone)]
pub struct StoredPeer {
    pub label: String,
    pub certificate: CertificateDer<'static>,
}

impl StoredPeer {
    pub fn fingerprint(&self) -> Fingerprint {
        fingerprint(&self.certificate)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerRecord {
    version: u8,
    label: String,
    certificate: String,
}

pub struct DeviceCredentials {
    backend: Arc<CredentialStore>,
    namespace: String,
    mutation: Mutex<()>,
}

impl DeviceCredentials {
    pub fn open() -> Result<Self, CredentialError> {
        Self::open_profile("default")
    }

    pub fn open_profile(profile: &str) -> Result<Self, CredentialError> {
        if profile.is_empty()
            || profile.len() > 64
            || !profile
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(CredentialError::InvalidRecord);
        }
        #[cfg(target_os = "macos")]
        let backend = apple_native_keyring_store::keychain::Store::new()?;
        #[cfg(target_os = "windows")]
        let backend = windows_native_keyring_store::Store::new()?;
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        return Ok(Self::with_backend(
            backend,
            &if profile == "default" {
                "app.liazo.credentials".into()
            } else {
                format!("app.liazo.credentials.{profile}")
            },
        ));
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        Err(CredentialError::Unsupported)
    }

    #[cfg(any(target_os = "macos", target_os = "windows", test))]
    fn with_backend(backend: Arc<CredentialStore>, namespace: &str) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
            mutation: Mutex::new(()),
        }
    }

    fn entry(&self, account: &str) -> Result<Entry, CredentialError> {
        Ok(self.backend.build(&self.namespace, account, None)?)
    }

    // The per-user runtime must hold its process/startup lock before creating an identity.
    pub fn load_or_create_identity(&self) -> Result<Identity, CredentialError> {
        let _lock = self.mutation.lock().unwrap_or_else(|e| e.into_inner());
        let entry = self.entry("identity-v1")?;
        match entry.get_secret() {
            Ok(bytes) => decode_identity(Zeroizing::new(bytes)),
            Err(keyring_core::Error::NoEntry) => {
                let identity = Identity::generate().map_err(|_| CredentialError::InvalidRecord)?;
                entry.set_secret(&encode_identity(&identity)?)?;
                Ok(identity)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn peers(&self) -> Result<Vec<StoredPeer>, CredentialError> {
        let _lock = self.mutation.lock().unwrap_or_else(|e| e.into_inner());
        self.read_index()?
            .into_iter()
            .map(|pin| {
                let bytes = self.entry(&pin.to_string())?.get_secret()?;
                if bytes.len() > MAX_RECORD {
                    return Err(CredentialError::InvalidRecord);
                }
                let record: PeerRecord =
                    serde_json::from_slice(&bytes).map_err(|_| CredentialError::InvalidRecord)?;
                let certificate = CertificateDer::from(
                    STANDARD
                        .decode(record.certificate)
                        .map_err(|_| CredentialError::InvalidRecord)?,
                );
                validate_peer(&certificate, &record.label)?;
                if record.version != 1 || fingerprint(&certificate) != pin {
                    return Err(CredentialError::InvalidRecord);
                }
                Ok(StoredPeer {
                    label: record.label,
                    certificate,
                })
            })
            .collect()
    }

    pub fn save_peer(
        &self,
        certificate: &CertificateDer<'static>,
        label: &str,
    ) -> Result<(), CredentialError> {
        let _lock = self.mutation.lock().unwrap_or_else(|e| e.into_inner());
        validate_peer(certificate, label)?;
        let pin = fingerprint(certificate);
        let mut index = self.read_index()?;
        if !index.contains(&pin) {
            if index.len() >= MAX_PEERS {
                return Err(CredentialError::TooManyPeers);
            }
            index.push(pin);
        }
        let record = serde_json::to_vec(&PeerRecord {
            version: 1,
            label: label.into(),
            certificate: STANDARD.encode(certificate.as_ref()),
        })
        .map_err(|_| CredentialError::InvalidRecord)?;
        if record.len() > MAX_RECORD {
            return Err(CredentialError::InvalidRecord);
        }
        // Publish trust only after the complete record is durable. Orphan records grant no trust.
        self.entry(&pin.to_string())?.set_secret(&record)?;
        self.write_index(&index)
    }

    pub fn remove_peer(&self, pin: Fingerprint) -> Result<bool, CredentialError> {
        let _lock = self.mutation.lock().unwrap_or_else(|e| e.into_inner());
        let mut index = self.read_index()?;
        let before = index.len();
        index.retain(|stored| *stored != pin);
        if index.len() == before {
            return Ok(false);
        }
        // Remove authority before deleting its now-unreferenced record.
        self.write_index(&index)?;
        match self.entry(&pin.to_string())?.delete_credential() {
            Ok(()) | Err(keyring_core::Error::NoEntry) => Ok(true),
            Err(error) => Err(error.into()),
        }
    }

    fn read_index(&self) -> Result<Vec<Fingerprint>, CredentialError> {
        let bytes = match self.entry("peer-index-v1")?.get_secret() {
            Ok(bytes) => bytes,
            Err(keyring_core::Error::NoEntry) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        if !bytes.starts_with(b"LP01")
            || (bytes.len() - 4) % 32 != 0
            || bytes.len() > 4 + MAX_PEERS * 32
        {
            return Err(CredentialError::InvalidRecord);
        }
        let pins: Vec<_> = bytes[4..]
            .as_chunks::<32>()
            .0
            .iter()
            .copied()
            .map(Fingerprint)
            .collect();
        if pins.iter().collect::<HashSet<_>>().len() != pins.len() {
            return Err(CredentialError::InvalidRecord);
        }
        Ok(pins)
    }

    fn write_index(&self, pins: &[Fingerprint]) -> Result<(), CredentialError> {
        let mut bytes = b"LP01".to_vec();
        for pin in pins {
            bytes.extend_from_slice(&pin.0);
        }
        Ok(self.entry("peer-index-v1")?.set_secret(&bytes)?)
    }
}

fn validate_peer(certificate: &CertificateDer<'_>, label: &str) -> Result<(), CredentialError> {
    if certificate.is_empty()
        || certificate.len() > 16 * 1024
        || label.trim().is_empty()
        || label.len() > 80
        || label.chars().any(char::is_control)
    {
        return Err(CredentialError::InvalidRecord);
    }
    rustls::RootCertStore::empty()
        .add(certificate.clone())
        .map_err(|_| CredentialError::InvalidRecord)
}

fn encode_identity(identity: &Identity) -> Result<Zeroizing<Vec<u8>>, CredentialError> {
    let cert = identity.certificate().as_ref();
    let key = identity.secret_key_der();
    if 8 + cert.len() + key.len() > MAX_RECORD {
        return Err(CredentialError::InvalidRecord);
    }
    let mut record = Zeroizing::new(b"LI01".to_vec());
    record.extend_from_slice(&(cert.len() as u16).to_be_bytes());
    record.extend_from_slice(&(key.len() as u16).to_be_bytes());
    record.extend_from_slice(cert);
    record.extend_from_slice(key);
    Ok(record)
}

fn decode_identity(record: Zeroizing<Vec<u8>>) -> Result<Identity, CredentialError> {
    if record.len() < 8 || record.len() > MAX_RECORD || &record[..4] != b"LI01" {
        return Err(CredentialError::InvalidRecord);
    }
    let cert_len = usize::from(u16::from_be_bytes(record[4..6].try_into().unwrap()));
    let key_len = usize::from(u16::from_be_bytes(record[6..8].try_into().unwrap()));
    if record.len() != 8 + cert_len + key_len {
        return Err(CredentialError::InvalidRecord);
    }
    Identity::from_der(
        record[8..8 + cert_len].to_vec(),
        record[8 + cert_len..].to_vec(),
    )
    .map_err(|_| CredentialError::InvalidRecord)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_survive_reopening_and_revocation_removes_durable_trust() {
        let backend = keyring_core::mock::Store::new().unwrap();
        let credentials = DeviceCredentials::with_backend(backend.clone(), "test");
        let identity = credentials.load_or_create_identity().unwrap();
        let peer = Identity::generate().unwrap();
        credentials
            .save_peer(peer.certificate(), "windows")
            .unwrap();
        drop(credentials);
        let reopened = DeviceCredentials::with_backend(backend, "test");
        assert_eq!(
            reopened.load_or_create_identity().unwrap().fingerprint(),
            identity.fingerprint()
        );
        assert_eq!(
            reopened.peers().unwrap()[0].fingerprint(),
            peer.fingerprint()
        );
        reopened.remove_peer(peer.fingerprint()).unwrap();
        assert!(reopened.peers().unwrap().is_empty());
    }

    #[test]
    fn orphan_records_are_not_trusted_and_corruption_does_not_reset_an_identity() {
        let credentials =
            DeviceCredentials::with_backend(keyring_core::mock::Store::new().unwrap(), "test");
        credentials
            .entry("identity-v1")
            .unwrap()
            .set_secret(b"broken")
            .unwrap();
        assert!(matches!(
            credentials.load_or_create_identity(),
            Err(CredentialError::InvalidRecord)
        ));
        credentials
            .entry(&"01".repeat(32))
            .unwrap()
            .set_secret(b"orphan")
            .unwrap();
        assert!(credentials.peers().unwrap().is_empty());
        credentials
            .entry("peer-index-v1")
            .unwrap()
            .set_secret(b"broken")
            .unwrap();
        assert!(matches!(
            credentials.peers(),
            Err(CredentialError::InvalidRecord)
        ));
    }

    #[test]
    fn an_inaccessible_store_does_not_mint_a_replacement_identity() {
        let credentials =
            DeviceCredentials::with_backend(keyring_core::mock::Store::new().unwrap(), "test");
        let entry = credentials.entry("identity-v1").unwrap();
        let mock = entry
            .as_any()
            .downcast_ref::<keyring_core::mock::Cred>()
            .unwrap();
        mock.set_error(keyring_core::Error::NoStorageAccess(Box::new(
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        )));
        assert!(matches!(
            credentials.load_or_create_identity(),
            Err(CredentialError::AccessDenied)
        ));
        assert!(matches!(
            entry.get_secret(),
            Err(keyring_core::Error::NoEntry)
        ));
    }
}
