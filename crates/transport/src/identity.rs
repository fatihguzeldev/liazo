use std::{fmt, str::FromStr};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};

use crate::TransportError;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Fingerprint(pub [u8; 32]);

impl fmt::Display for Fingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Fingerprint {
    type Err = TransportError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != 64 || !text.is_ascii() {
            return Err(TransportError::InvalidIdentity);
        }
        let mut bytes = [0_u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
                .map_err(|_| TransportError::InvalidIdentity)?;
        }
        Ok(Self(bytes))
    }
}

pub fn fingerprint(certificate: &CertificateDer<'_>) -> Fingerprint {
    Fingerprint(Sha256::digest(certificate.as_ref()).into())
}

pub struct Identity {
    certificate: CertificateDer<'static>,
    pub(crate) key: PrivateKeyDer<'static>,
}

impl fmt::Debug for Identity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Identity")
            .field("fingerprint", &self.fingerprint())
            .finish()
    }
}

impl Identity {
    pub fn generate() -> Result<Self, TransportError> {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["liazo.local".into()])?;
        Self::from_der(cert.der().to_vec(), signing_key.serialize_der())
    }

    pub fn from_der(certificate: Vec<u8>, key: Vec<u8>) -> Result<Self, TransportError> {
        if certificate.is_empty()
            || certificate.len() > 16 * 1024
            || key.is_empty()
            || key.len() > 16 * 1024
        {
            return Err(TransportError::InvalidIdentity);
        }
        let certificate = CertificateDer::from(certificate);
        let key: PrivateKeyDer<'static> = PrivatePkcs8KeyDer::from(key).into();
        rustls::sign::CertifiedKey::from_der(
            vec![certificate.clone()],
            key.clone_key(),
            &rustls::crypto::ring::default_provider(),
        )?;
        Ok(Self { certificate, key })
    }

    pub fn certificate(&self) -> &CertificateDer<'static> {
        &self.certificate
    }
    pub fn fingerprint(&self) -> Fingerprint {
        fingerprint(&self.certificate)
    }

    // Only for the local OS credential-store adapter; never expose through application interfaces.
    pub fn secret_key_der(&self) -> &[u8] {
        self.key.secret_der()
    }
}
