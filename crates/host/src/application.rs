use liazo_core::protocol::SourceKey;
use liazo_runtime::{
    ConnectionState, RuntimeError, archive::ArchiveLimits, receiver::ReceiverSession,
};
use liazo_transport::{Fingerprint, Identity, PeerClient, SessionBinding, StoredPeer};
use serde::Serialize;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock, oneshot};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("no active session")]
    NoSession,
    #[error("stop the current session before connecting another computer")]
    ActiveSession,
    #[error("computer is not paired")]
    Unpaired,
    #[error("invalid request")]
    Invalid,
    #[error("operation interrupted")]
    Interrupted,
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Transport(#[from] liazo_transport::TransportError),
    #[error(transparent)]
    Transcription(#[from] liazo_transcription::TranscriptionError),
}

#[derive(Clone, Serialize, schemars::JsonSchema)]
pub struct SessionSummary {
    pub binding: SessionBinding,
    pub provider: String,
    pub address: SocketAddr,
    pub connection: ConnectionState,
}

struct Active {
    peer: Fingerprint,
    address: SocketAddr,
    receiver: ReceiverSession,
}
struct Inner {
    identity: Identity,
    peers: RwLock<Vec<StoredPeer>>,
    active: RwLock<Option<Active>>,
    operation: Arc<Mutex<()>>,
    archive_root: PathBuf,
}

#[derive(Clone)]
pub struct SessionHost(Arc<Inner>);

impl SessionHost {
    pub async fn observe(
        &self,
        args: crate::requests::Observe,
    ) -> Result<liazo_runtime::Observation, HostError> {
        validate_sources(&args.sources)?;
        if args.wait_ms > 25_000 {
            return Err(HostError::Invalid);
        }
        Ok(self
            .receiver(args.binding)
            .await?
            .observe(
                args.after,
                args.current,
                &args.sources,
                Duration::from_millis(args.wait_ms),
            )
            .await)
    }
    pub async fn capture(
        &self,
        args: crate::requests::Capture,
    ) -> Result<liazo_runtime::Observation, HostError> {
        validate_sources(&args.sources)?;
        Ok(self
            .receiver(args.binding)
            .await?
            .capture_now(args.sources)
            .await?)
    }
    pub fn new(identity: Identity, peers: Vec<StoredPeer>, archive_root: PathBuf) -> Self {
        Self(Arc::new(Inner {
            identity,
            peers: RwLock::new(peers),
            active: RwLock::new(None),
            operation: Arc::new(Mutex::new(())),
            archive_root,
        }))
    }

    pub async fn peers(&self) -> Vec<(String, String)> {
        self.0
            .peers
            .read()
            .await
            .iter()
            .map(|peer| (peer.fingerprint().to_string(), peer.label.clone()))
            .collect()
    }

    pub async fn receiver(&self, binding: SessionBinding) -> Result<ReceiverSession, HostError> {
        let active = self.0.active.read().await;
        let receiver = &active.as_ref().ok_or(HostError::NoSession)?.receiver;
        if receiver.archive_identity().binding != binding {
            return Err(HostError::NoSession);
        }
        Ok(receiver.clone())
    }

    pub async fn current(&self) -> Option<SessionSummary> {
        let active = self.0.active.read().await;
        let active = active.as_ref()?;
        let observed = active.receiver.observe(0, false, &[], Duration::ZERO).await;
        Some(SessionSummary {
            binding: observed.binding,
            provider: active.peer.to_string(),
            address: active.address,
            connection: observed.connection,
        })
    }

    pub async fn connect(
        &self,
        address: SocketAddr,
        peer: Fingerprint,
    ) -> Result<SessionSummary, HostError> {
        if address.port() == 0 || address.ip().is_unspecified() || address.ip().is_multicast() {
            return Err(HostError::Invalid);
        }
        let host = self.clone();
        self.owned(async move { host.connect_inner(address, peer).await })
            .await
    }

    async fn connect_inner(
        &self,
        address: SocketAddr,
        peer: Fingerprint,
    ) -> Result<SessionSummary, HostError> {
        if let Some(current) = self.current().await
            && current.connection == ConnectionState::Connected
        {
            return if current.provider == peer.to_string() && current.address == address {
                Ok(current)
            } else {
                Err(HostError::ActiveSession)
            };
        }
        let previous = self
            .0
            .active
            .read()
            .await
            .as_ref()
            .map(|active| active.receiver.clone());
        if let Some(previous) = previous {
            previous.disconnect().await?;
        }
        let certificate = self
            .0
            .peers
            .read()
            .await
            .iter()
            .find(|candidate| candidate.fingerprint() == peer)
            .map(|peer| peer.certificate.clone())
            .ok_or(HostError::Unpaired)?;
        let receiver = ReceiverSession::connect(
            &PeerClient::new(&self.0.identity, &certificate)?,
            address,
            Uuid::new_v4(),
        )
        .await?;
        let binding = receiver.archive_identity().binding;
        receiver
            .start_archive(
                self.0
                    .archive_root
                    .join(binding.session.to_string())
                    .join(binding.epoch.to_string()),
                ArchiveLimits::default(),
            )
            .await?;
        *self.0.active.write().await = Some(Active {
            peer,
            address,
            receiver,
        });
        self.current().await.ok_or(HostError::NoSession)
    }

    pub async fn stop(&self, binding: SessionBinding) -> Result<(), HostError> {
        let host = self.clone();
        self.owned(async move {
            host.receiver(binding)
                .await?
                .stop()
                .await
                .map_err(Into::into)
        })
        .await
    }

    pub async fn replace_peers(&self, peers: Vec<StoredPeer>) -> Result<(), HostError> {
        let _operation = self.0.operation.lock().await;
        *self.0.peers.write().await = peers;
        let mut active = self.0.active.write().await;
        if let Some(session) = active.as_ref()
            && !self
                .0
                .peers
                .read()
                .await
                .iter()
                .any(|peer| peer.fingerprint() == session.peer)
        {
            // Revocation invalidates existing receiver handles as well as the host lookup.
            let _ = session.receiver.disconnect().await;
            active.take();
        }
        Ok(())
    }

    pub async fn transcribe(
        &self,
        binding: SessionBinding,
        path: PathBuf,
        threads: u8,
        sources: Vec<SourceKey>,
    ) -> Result<(), HostError> {
        if !(1..=16).contains(&threads) || sources.is_empty() || sources.len() > 8 {
            return Err(HostError::Invalid);
        }
        let host = self.clone();
        self.owned(async move {
            let receiver = host.receiver(binding).await?;
            let model =
                tokio::task::spawn_blocking(move || liazo_transcription::WhisperModel::load(&path))
                    .await
                    .map_err(|_| HostError::Interrupted)??;
            receiver
                .start_transcription(
                    model,
                    threads,
                    sources,
                    liazo_transcription::streaming::StreamingSettings::default(),
                )
                .await?;
            Ok(())
        })
        .await
    }

    async fn owned<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = Result<T, HostError>> + Send + 'static,
    ) -> Result<T, HostError> {
        let operation = self.0.clone();
        let permit = operation
            .operation
            .clone()
            .try_lock_owned()
            .map_err(|_| HostError::Interrupted)?;
        let (reply, result) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = reply.send(future.await);
        });
        result.await.map_err(|_| HostError::Interrupted)?
    }
}

fn validate_sources(sources: &[SourceKey]) -> Result<(), HostError> {
    let mut seen = std::collections::HashSet::new();
    if sources.len() > 8 {
        return Err(HostError::Invalid);
    }
    for source in sources {
        source.validate().map_err(|_| HostError::Invalid)?;
        if !seen.insert(source) {
            return Err(HostError::Invalid);
        }
    }
    Ok(())
}
