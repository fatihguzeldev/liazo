use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, ServerName},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout, timeout_at},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, server::TlsStream};

use crate::{Fingerprint, Identity, TransportError, fingerprint};

const ALPN: &[u8] = b"liazo-pair/1";
const LIFETIME: Duration = Duration::from_secs(120);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_MESSAGE: usize = 32 * 1024;
const INVITATION_PREFIX: &str = "liazo1.";

// The invitation is transferred through a trusted local UI. Discovery never supplies trust.
#[derive(Clone)]
pub struct PairingInvitation {
    address: SocketAddr,
    certificate: CertificateDer<'static>,
    token: [u8; 32],
}

impl fmt::Debug for PairingInvitation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PairingInvitation")
            .field("address", &self.address)
            .field("provider", &fingerprint(&self.certificate))
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvitationWire {
    version: u16,
    address: SocketAddr,
    certificate: String,
    token: String,
}

impl PairingInvitation {
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn provider_certificate(&self) -> &CertificateDer<'static> {
        &self.certificate
    }

    pub fn encode(&self) -> Result<String, TransportError> {
        let wire = InvitationWire {
            version: 1,
            address: self.address,
            certificate: URL_SAFE_NO_PAD.encode(self.certificate.as_ref()),
            token: URL_SAFE_NO_PAD.encode(self.token),
        };
        let json = serde_json::to_vec(&wire).map_err(|_| TransportError::Protocol)?;
        let encoded = format!("{INVITATION_PREFIX}{}", URL_SAFE_NO_PAD.encode(json));
        if encoded.len() > MAX_MESSAGE {
            return Err(TransportError::PairingClosed);
        }
        Ok(encoded)
    }

    pub fn decode(encoded: &str) -> Result<Self, TransportError> {
        if encoded.len() > MAX_MESSAGE {
            return Err(TransportError::PairingClosed);
        }
        let json = URL_SAFE_NO_PAD
            .decode(
                encoded
                    .strip_prefix(INVITATION_PREFIX)
                    .ok_or(TransportError::PairingClosed)?,
            )
            .map_err(|_| TransportError::PairingClosed)?;
        let wire: InvitationWire =
            serde_json::from_slice(&json).map_err(|_| TransportError::PairingClosed)?;
        if wire.version != 1
            || wire.address.port() == 0
            || wire.address.ip().is_unspecified()
            || wire.address.ip().is_multicast()
        {
            return Err(TransportError::PairingClosed);
        }
        let certificate = parse_certificate(
            URL_SAFE_NO_PAD
                .decode(wire.certificate)
                .map_err(|_| TransportError::PairingClosed)?,
        )?;
        let token = URL_SAFE_NO_PAD
            .decode(wire.token)
            .map_err(|_| TransportError::PairingClosed)?
            .try_into()
            .map_err(|_| TransportError::PairingClosed)?;
        Ok(Self {
            address: wire.address,
            certificate,
            token,
        })
    }
}

pub struct PairingServer {
    acceptor: TlsAcceptor,
    invitation: PairingInvitation,
    expires: Instant,
    used: AtomicBool,
}

impl PairingServer {
    pub fn open(identity: &Identity, address: SocketAddr) -> Result<Self, TransportError> {
        let mut token = [0; 32];
        SystemRandom::new()
            .fill(&mut token)
            .map_err(|_| TransportError::InvalidIdentity)?;
        // This listener accepts pairing requests only. Media always requires PeerServer mTLS.
        let mut config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(
                vec![identity.certificate().clone()],
                identity.key.clone_key(),
            )?;
        config.alpn_protocols = vec![ALPN.to_vec()];
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            invitation: PairingInvitation {
                address,
                certificate: identity.certificate().clone(),
                token,
            },
            expires: Instant::now() + LIFETIME,
            used: AtomicBool::new(false),
        })
    }

    pub fn invitation(&self) -> PairingInvitation {
        self.invitation.clone()
    }

    pub async fn accept(&self, socket: TcpStream) -> Result<PendingPairing, TransportError> {
        if self.used.load(Ordering::Acquire) || Instant::now() >= self.expires {
            return Err(TransportError::PairingClosed);
        }
        socket.set_nodelay(true)?;
        let mut io = timeout(IO_TIMEOUT, self.acceptor.accept(socket))
            .await
            .map_err(|_| TransportError::Timeout)??;
        if io.get_ref().1.alpn_protocol() != Some(ALPN) {
            return Err(TransportError::Protocol);
        }
        let request: PairRequest = timeout(IO_TIMEOUT, read_json(&mut io))
            .await
            .map_err(|_| TransportError::Timeout)??;
        validate_label(&request.label)?;
        if request.version != 1
            || !bool::from(request.token.ct_eq(&self.invitation.token))
            || Instant::now() >= self.expires
        {
            return Err(TransportError::PairingClosed);
        }
        let certificate = parse_certificate(request.certificate)?;
        self.used
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| TransportError::PairingClosed)?;
        Ok(PendingPairing {
            io,
            peer: ApprovedPeer {
                certificate,
                label: request.label,
            },
            expires: self.expires,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairRequest {
    version: u16,
    token: [u8; 32],
    label: String,
    certificate: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PairReply {
    Approved,
    Denied,
}

pub struct ApprovedPeer {
    certificate: CertificateDer<'static>,
    label: String,
}

impl ApprovedPeer {
    pub fn certificate(&self) -> &CertificateDer<'static> {
        &self.certificate
    }
    pub fn fingerprint(&self) -> Fingerprint {
        fingerprint(&self.certificate)
    }
    pub fn label(&self) -> &str {
        &self.label
    }
}

pub struct PendingPairing {
    io: TlsStream<TcpStream>,
    peer: ApprovedPeer,
    expires: Instant,
}

impl PendingPairing {
    pub fn certificate(&self) -> &CertificateDer<'static> {
        self.peer.certificate()
    }
    pub fn label(&self) -> &str {
        self.peer.label()
    }
    pub fn fingerprint(&self) -> Fingerprint {
        self.peer.fingerprint()
    }

    pub async fn approve(mut self) -> Result<ApprovedPeer, TransportError> {
        if Instant::now() >= self.expires {
            return Err(TransportError::PairingClosed);
        }
        timeout_at(self.expires.min(Instant::now() + IO_TIMEOUT), async {
            write_json(&mut self.io, &PairReply::Approved).await?;
            self.io.shutdown().await?;
            Ok::<_, TransportError>(())
        })
        .await
        .map_err(|_| TransportError::Timeout)??;
        Ok(self.peer)
    }

    pub async fn deny(mut self) -> Result<(), TransportError> {
        timeout(IO_TIMEOUT, async {
            write_json(&mut self.io, &PairReply::Denied).await?;
            self.io.shutdown().await?;
            Ok(())
        })
        .await
        .map_err(|_| TransportError::Timeout)?
    }
}

pub async fn request_pairing(
    invitation: &PairingInvitation,
    identity: &Identity,
    label: &str,
) -> Result<(), TransportError> {
    validate_label(label)?;
    let mut roots = RootCertStore::empty();
    roots.add(invitation.certificate.clone())?;
    let mut config = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![ALPN.to_vec()];
    let connector = TlsConnector::from(Arc::new(config));
    let socket = timeout(IO_TIMEOUT, TcpStream::connect(invitation.address))
        .await
        .map_err(|_| TransportError::Timeout)??;
    socket.set_nodelay(true)?;
    let mut io = timeout(
        IO_TIMEOUT,
        connector.connect(ServerName::try_from("liazo.local").unwrap(), socket),
    )
    .await
    .map_err(|_| TransportError::Timeout)??;
    let connection = io.get_ref().1;
    let certificate = connection
        .peer_certificates()
        .and_then(|c| c.first())
        .ok_or(TransportError::UntrustedPeer)?;
    if fingerprint(certificate) != fingerprint(&invitation.certificate)
        || connection.alpn_protocol() != Some(ALPN)
    {
        return Err(TransportError::UntrustedPeer);
    }
    let request = PairRequest {
        version: 1,
        token: invitation.token,
        label: label.to_owned(),
        certificate: identity.certificate().to_vec(),
    };
    timeout(IO_TIMEOUT, write_json(&mut io, &request))
        .await
        .map_err(|_| TransportError::Timeout)??;
    let reply: PairReply = timeout(LIFETIME, read_json(&mut io))
        .await
        .map_err(|_| TransportError::Timeout)??;
    match reply {
        PairReply::Approved => Ok(()),
        PairReply::Denied => Err(TransportError::PairingDenied),
    }
}

fn parse_certificate(bytes: Vec<u8>) -> Result<CertificateDer<'static>, TransportError> {
    if bytes.is_empty() || bytes.len() > 16 * 1024 {
        return Err(TransportError::InvalidIdentity);
    }
    let certificate = CertificateDer::from(bytes);
    RootCertStore::empty().add(certificate.clone())?;
    Ok(certificate)
}

fn validate_label(label: &str) -> Result<(), TransportError> {
    if label.trim().is_empty() || label.len() > 80 || label.chars().any(char::is_control) {
        return Err(TransportError::Protocol);
    }
    Ok(())
}

async fn read_json<R: AsyncRead + Unpin, T: DeserializeOwned>(
    io: &mut R,
) -> Result<T, TransportError> {
    let len = io.read_u32().await? as usize;
    if len == 0 || len > MAX_MESSAGE {
        return Err(TransportError::Protocol);
    }
    let mut data = vec![0; len];
    io.read_exact(&mut data).await?;
    serde_json::from_slice(&data).map_err(|_| TransportError::Protocol)
}

async fn write_json<W: AsyncWrite + Unpin, T: Serialize>(
    io: &mut W,
    message: &T,
) -> Result<(), TransportError> {
    let data = serde_json::to_vec(message).map_err(|_| TransportError::Protocol)?;
    if data.is_empty() || data.len() > MAX_MESSAGE {
        return Err(TransportError::Protocol);
    }
    io.write_u32(data.len() as u32).await?;
    io.write_all(&data).await?;
    io.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::duplex, net::TcpListener};

    #[tokio::test]
    async fn a_bad_token_does_not_consume_the_real_invitation() {
        let provider = Identity::generate().unwrap();
        let receiver = Identity::generate().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = PairingServer::open(&provider, listener.local_addr().unwrap()).unwrap();
        let mut wrong = server.invitation();
        wrong.token[0] ^= 1;
        let client =
            tokio::spawn(async move { request_pairing(&wrong, &receiver, "windows").await });
        let (socket, _) = listener.accept().await.unwrap();
        assert!(server.accept(socket).await.is_err());
        assert!(!server.used.load(Ordering::Acquire));
        assert!(client.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn an_expired_invitation_is_closed_without_waiting_for_tls() {
        let provider = Identity::generate().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut server = PairingServer::open(&provider, listener.local_addr().unwrap()).unwrap();
        server.expires = Instant::now() - Duration::from_secs(1);
        let socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        assert!(matches!(
            server.accept(socket).await,
            Err(TransportError::PairingClosed)
        ));
    }

    #[tokio::test]
    async fn oversized_pairing_messages_fail_before_reading_the_body() {
        let (mut writer, mut reader) = duplex(4);
        writer.write_u32(MAX_MESSAGE as u32 + 1).await.unwrap();
        assert!(matches!(
            read_json::<_, PairRequest>(&mut reader).await,
            Err(TransportError::Protocol)
        ));
    }
}
