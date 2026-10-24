use std::{
    collections::HashSet,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, ServerName},
    server::WebPkiClientVerifier,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use crate::{Fingerprint, Identity, TransportError, fingerprint};

const ALPN: &[u8] = b"liazo/1";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct PeerServer {
    acceptor: TlsAcceptor,
    paired: Arc<HashSet<Fingerprint>>,
}

impl PeerServer {
    pub fn new(
        identity: &Identity,
        trusted: &[CertificateDer<'static>],
    ) -> Result<Self, TransportError> {
        if trusted.is_empty() || trusted.len() > 32 {
            return Err(TransportError::InvalidIdentity);
        }
        let mut roots = RootCertStore::empty();
        for certificate in trusted {
            roots.add(certificate.clone())?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| TransportError::Verifier(e.to_string()))?;
        let mut config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![identity.certificate().clone()],
                identity.key.clone_key(),
            )?;
        config.alpn_protocols = vec![ALPN.to_vec()];
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            paired: Arc::new(trusted.iter().map(fingerprint).collect()),
        })
    }

    pub async fn accept(&self, socket: TcpStream) -> Result<PeerConnection, TransportError> {
        socket.set_nodelay(true)?;
        let io = timeout(HANDSHAKE_TIMEOUT, self.acceptor.accept(socket))
            .await
            .map_err(|_| TransportError::Timeout)??;
        let connection = io.get_ref().1;
        let certificate = connection
            .peer_certificates()
            .and_then(|certs| certs.first())
            .ok_or(TransportError::UntrustedPeer)?;
        let peer = fingerprint(certificate);
        if !self.paired.contains(&peer) {
            return Err(TransportError::UntrustedPeer);
        }
        if connection.alpn_protocol() != Some(ALPN) {
            return Err(TransportError::Protocol);
        }
        Ok(PeerConnection {
            io: TlsStream::Server(io),
            peer,
        })
    }
}

#[derive(Clone)]
pub struct PeerClient {
    connector: TlsConnector,
    provider: Fingerprint,
}

impl PeerClient {
    pub fn new(
        identity: &Identity,
        provider: &CertificateDer<'static>,
    ) -> Result<Self, TransportError> {
        let mut roots = RootCertStore::empty();
        roots.add(provider.clone())?;
        let mut config = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![identity.certificate().clone()],
                identity.key.clone_key(),
            )?;
        config.alpn_protocols = vec![ALPN.to_vec()];
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            provider: fingerprint(provider),
        })
    }

    pub async fn connect(&self, address: SocketAddr) -> Result<PeerConnection, TransportError> {
        let socket = timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| TransportError::Timeout)??;
        socket.set_nodelay(true)?;
        let io = timeout(
            HANDSHAKE_TIMEOUT,
            self.connector
                .connect(ServerName::try_from("liazo.local").unwrap(), socket),
        )
        .await
        .map_err(|_| TransportError::Timeout)??;
        let connection = io.get_ref().1;
        let certificate = connection
            .peer_certificates()
            .and_then(|certs| certs.first())
            .ok_or(TransportError::UntrustedPeer)?;
        let peer = fingerprint(certificate);
        if peer != self.provider {
            return Err(TransportError::UntrustedPeer);
        }
        if connection.alpn_protocol() != Some(ALPN) {
            return Err(TransportError::Protocol);
        }
        Ok(PeerConnection {
            io: TlsStream::Client(io),
            peer,
        })
    }
}

pub struct PeerConnection {
    io: TlsStream<TcpStream>,
    peer: Fingerprint,
}

impl PeerConnection {
    pub fn fingerprint(&self) -> Fingerprint {
        self.peer
    }
}

impl AsyncRead for PeerConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for PeerConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
