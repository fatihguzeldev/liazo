use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, RwLock},
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
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use crate::session::{SessionLease, SessionState};
use crate::{Fingerprint, Identity, TransportError, fingerprint};

const ALPN: &[u8] = b"liazo/1";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

struct Interrupt {
    future: Pin<Box<WaitForCancellationFutureOwned>>,
    kind: io::ErrorKind,
    message: &'static str,
}

impl Interrupt {
    fn revoked(token: CancellationToken) -> Self {
        Self {
            future: Box::pin(token.cancelled_owned()),
            kind: io::ErrorKind::PermissionDenied,
            message: "peer authorization revoked",
        }
    }
    fn session(token: CancellationToken) -> Self {
        Self {
            future: Box::pin(token.cancelled_owned()),
            kind: io::ErrorKind::ConnectionAborted,
            message: "peer session closed",
        }
    }
}

#[derive(Clone)]
pub struct PeerServer {
    state: Arc<RwLock<ServerState>>,
}

struct ServerState {
    identity: Fingerprint,
    acceptor: Option<TlsAcceptor>,
    paired: HashMap<Fingerprint, CancellationToken>,
}

impl PeerServer {
    pub fn new(
        identity: &Identity,
        trusted: &[CertificateDer<'static>],
    ) -> Result<Self, TransportError> {
        let acceptor = Self::build_acceptor(identity, trusted)?;
        Ok(Self {
            state: Arc::new(RwLock::new(ServerState {
                identity: identity.fingerprint(),
                acceptor,
                paired: trusted
                    .iter()
                    .map(|cert| (fingerprint(cert), CancellationToken::new()))
                    .collect(),
            })),
        })
    }

    fn build_acceptor(
        identity: &Identity,
        trusted: &[CertificateDer<'static>],
    ) -> Result<Option<TlsAcceptor>, TransportError> {
        if trusted.len() > 32 {
            return Err(TransportError::InvalidIdentity);
        }
        if trusted.is_empty() {
            return Ok(None);
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
        Ok(Some(TlsAcceptor::from(Arc::new(config))))
    }

    pub fn replace_trusted(
        &self,
        identity: &Identity,
        trusted: &[CertificateDer<'static>],
    ) -> Result<(), TransportError> {
        let acceptor = Self::build_acceptor(identity, trusted)?;
        let pins: HashSet<_> = trusted.iter().map(fingerprint).collect();
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        if state.identity != identity.fingerprint() {
            return Err(TransportError::InvalidIdentity);
        }
        state.paired.retain(|pin, token| {
            if pins.contains(pin) {
                true
            } else {
                token.cancel();
                false
            }
        });
        for pin in pins {
            state.paired.entry(pin).or_default();
        }
        state.acceptor = acceptor;
        Ok(())
    }

    pub fn revoke(&self, peer: Fingerprint) -> bool {
        let token = self
            .state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .paired
            .remove(&peer);
        if let Some(token) = token {
            token.cancel();
            true
        } else {
            false
        }
    }

    pub async fn accept(&self, socket: TcpStream) -> Result<PeerConnection, TransportError> {
        socket.set_nodelay(true)?;
        let acceptor = self
            .state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .acceptor
            .clone()
            .ok_or(TransportError::UntrustedPeer)?;
        let io = timeout(HANDSHAKE_TIMEOUT, acceptor.accept(socket))
            .await
            .map_err(|_| TransportError::Timeout)??;
        let connection = io.get_ref().1;
        let certificate = connection
            .peer_certificates()
            .and_then(|certs| certs.first())
            .ok_or(TransportError::UntrustedPeer)?;
        let peer = fingerprint(certificate);
        let token = self
            .state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .paired
            .get(&peer)
            .cloned()
            .ok_or(TransportError::UntrustedPeer)?;
        if connection.alpn_protocol() != Some(ALPN) {
            return Err(TransportError::Protocol);
        }
        Ok(PeerConnection {
            io: TlsStream::Server(io),
            peer,
            cancelled_read: vec![Interrupt::revoked(token.clone())],
            cancelled_write: vec![Interrupt::revoked(token)],
            session: None,
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
            cancelled_read: Vec::new(),
            cancelled_write: Vec::new(),
            session: None,
        })
    }
}

pub struct PeerConnection {
    io: TlsStream<TcpStream>,
    peer: Fingerprint,
    cancelled_read: Vec<Interrupt>,
    cancelled_write: Vec<Interrupt>,
    session: Option<SessionLease>,
}

impl PeerConnection {
    pub fn fingerprint(&self) -> Fingerprint {
        self.peer
    }

    pub(crate) fn bind_session(&mut self, state: Arc<SessionState>) {
        self.cancelled_read
            .push(Interrupt::session(state.cancelled.clone()));
        self.cancelled_write
            .push(Interrupt::session(state.cancelled.clone()));
        self.session = Some(SessionLease(state));
    }

    fn close_session(&self) {
        if let Some(session) = &self.session {
            session.close();
        }
    }
}

impl AsyncRead for PeerConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(error) = interrupted(&mut self.cancelled_read, cx) {
            return Poll::Ready(Err(error));
        }
        let before = buf.filled().len();
        let capacity = buf.remaining();
        let result = Pin::new(&mut self.io).poll_read(cx, buf);
        if matches!(&result, Poll::Ready(Err(_)))
            || (matches!(&result, Poll::Ready(Ok(())))
                && capacity > 0
                && buf.filled().len() == before)
        {
            self.close_session();
        }
        result
    }
}

impl AsyncWrite for PeerConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(error) = interrupted(&mut self.cancelled_write, cx) {
            return Poll::Ready(Err(error));
        }
        let result = Pin::new(&mut self.io).poll_write(cx, buf);
        if matches!(&result, Poll::Ready(Err(_))) {
            self.close_session();
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(error) = interrupted(&mut self.cancelled_write, cx) {
            return Poll::Ready(Err(error));
        }
        let result = Pin::new(&mut self.io).poll_flush(cx);
        if matches!(&result, Poll::Ready(Err(_))) {
            self.close_session();
        }
        result
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(error) = interrupted(&mut self.cancelled_write, cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

fn interrupted(interrupts: &mut [Interrupt], cx: &mut Context<'_>) -> Option<io::Error> {
    interrupts.iter_mut().find_map(|interrupt| {
        interrupt
            .future
            .as_mut()
            .poll(cx)
            .is_ready()
            .then(|| io::Error::new(interrupt.kind, interrupt.message))
    })
}
