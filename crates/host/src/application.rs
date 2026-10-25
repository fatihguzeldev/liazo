use liazo_core::protocol::SourceKey;
use liazo_runtime::{
    ConnectionState, RuntimeError, archive::ArchiveLimits, receiver::ReceiverSession,
};
use liazo_transport::{
    CredentialError, Fingerprint, Identity, PairingInvitation, PeerClient, SessionBinding,
    StoredPeer, request_pairing,
};
use serde::Serialize;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock, Semaphore, oneshot};
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
    #[error("another operation is in progress")]
    Busy,
    #[error("paired trust storage is unavailable")]
    NoTrustStore,
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error(transparent)]
    Model(#[from] crate::models::ModelError),
    #[error(transparent)]
    Archive(#[from] liazo_runtime::archive::ArchiveError),
    #[error("archive history is unavailable")]
    HistoryUnavailable,
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
    slots: Arc<Semaphore>,
    stops: Arc<Semaphore>,
    pairing: Arc<Mutex<()>>,
    trust_ops: Mutex<()>,
    trust: Option<Arc<dyn crate::TrustStore>>,
    archive_root: PathBuf,
    catalog: Option<liazo_runtime::archive::ArchiveCatalog>,
    models: crate::models::ModelStore,
}

#[derive(Clone)]
pub struct SessionHost(Arc<Inner>);

impl SessionHost {
    pub(crate) fn catalog(&self) -> Result<&liazo_runtime::archive::ArchiveCatalog, HostError> {
        self.0.catalog.as_ref().ok_or(HostError::HistoryUnavailable)
    }
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
        let models = archive_root.join("models");
        Self::compose(identity, peers, archive_root, models, None)
    }
    pub fn with_trust_store(
        identity: Identity,
        peers: Vec<StoredPeer>,
        archive_root: PathBuf,
        model_root: PathBuf,
        trust: Arc<dyn crate::TrustStore>,
    ) -> Self {
        Self::compose(identity, peers, archive_root, model_root, Some(trust))
    }
    fn compose(
        identity: Identity,
        peers: Vec<StoredPeer>,
        archive_root: PathBuf,
        model_root: PathBuf,
        trust: Option<Arc<dyn crate::TrustStore>>,
    ) -> Self {
        let catalog = liazo_runtime::archive::ArchiveCatalog::spawn(archive_root.clone()).ok();
        Self(Arc::new(Inner {
            identity,
            peers: RwLock::new(peers),
            active: RwLock::new(None),
            operation: Arc::new(Mutex::new(())),
            slots: Arc::new(Semaphore::new(8)),
            stops: Arc::new(Semaphore::new(2)),
            pairing: Arc::new(Mutex::new(())),
            trust_ops: Mutex::new(()),
            trust,
            archive_root,
            catalog,
            models: crate::models::ModelStore::new(model_root),
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
    pub fn models(&self) -> &crate::models::ModelStore {
        &self.0.models
    }
    pub async fn discover(
        &self,
    ) -> Result<Vec<liazo_transport::discovery::DiscoveredProvider>, HostError> {
        Ok(liazo_transport::discovery::discover(Duration::from_secs(3)).await?)
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
            previous.retire_archive().await?;
            self.0.active.write().await.take();
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
        let trusted = self.0.peers.read().await;
        if !trusted
            .iter()
            .any(|candidate| candidate.fingerprint() == peer)
        {
            drop(trusted);
            receiver.disconnect().await?;
            return Err(HostError::Unpaired);
        }
        *self.0.active.write().await = Some(Active {
            peer,
            address,
            receiver,
        });
        drop(trusted);
        self.current().await.ok_or(HostError::NoSession)
    }

    pub async fn stop(&self, binding: SessionBinding) -> Result<(), HostError> {
        let host = self.clone();
        Self::spawn_owned(self.0.stops.clone(), async move {
            host.receiver(binding)
                .await?
                .stop()
                .await
                .map_err(Into::into)
        })
        .await
    }

    pub async fn replace_peers(&self, peers: Vec<StoredPeer>) -> Result<(), HostError> {
        let revoked = {
            let mut trusted = self.0.peers.write().await;
            *trusted = peers;
            let mut active = self.0.active.write().await;
            if active.as_ref().is_some_and(|session| {
                !trusted
                    .iter()
                    .any(|peer| peer.fingerprint() == session.peer)
            }) {
                active.take().map(|session| session.receiver)
            } else {
                None
            }
        };
        if let Some(receiver) = revoked {
            let _ = receiver.disconnect().await;
            let _ = receiver.retire_archive().await;
        }
        Ok(())
    }

    pub async fn refresh_trust(&self) -> Result<(), HostError> {
        let _trust = self.0.trust_ops.lock().await;
        let store = self.0.trust.clone().ok_or(HostError::NoTrustStore)?;
        let peers = tokio::task::spawn_blocking(move || store.peers())
            .await
            .map_err(|_| HostError::Interrupted)??;
        self.replace_peers(peers).await
    }

    pub async fn pair(
        &self,
        code: String,
        name: String,
        label: String,
    ) -> Result<(String, String), HostError> {
        let invitation = PairingInvitation::decode(code.trim())?;
        for text in [&name, &label] {
            if text.trim().is_empty() || text.len() > 128 || text.chars().any(char::is_control) {
                return Err(HostError::Invalid);
            }
        }
        let store = self.0.trust.clone().ok_or(HostError::NoTrustStore)?;
        let pairing = self
            .0
            .pairing
            .clone()
            .try_lock_owned()
            .map_err(|_| HostError::Busy)?;
        let host = self.clone();
        self.run_owned(async move {
            let _pairing = pairing;
            request_pairing(&invitation, &host.0.identity, &name).await?;
            let peer = StoredPeer {
                certificate: invitation.provider_certificate().clone(),
                label,
            };
            let result = (peer.fingerprint().to_string(), peer.label.clone());
            let _trust = host.0.trust_ops.lock().await;
            let peers = tokio::task::spawn_blocking(move || {
                store.save(peer)?;
                store.peers()
            })
            .await
            .map_err(|_| HostError::Interrupted)??;
            let retained = peers
                .iter()
                .any(|peer| peer.fingerprint().to_string() == result.0);
            host.replace_peers(peers).await?;
            if !retained {
                return Err(HostError::Unpaired);
            }
            Ok(result)
        })
        .await
    }

    pub async fn forget(&self, peer: Fingerprint) -> Result<bool, HostError> {
        let store = self.0.trust.clone().ok_or(HostError::NoTrustStore)?;
        let host = self.clone();
        self.run_owned(async move {
            let _trust = host.0.trust_ops.lock().await;
            let (removed, peers) = tokio::task::spawn_blocking(move || {
                let removed = store.remove(peer)?;
                Ok::<_, CredentialError>((removed, store.peers()?))
            })
            .await
            .map_err(|_| HostError::Interrupted)??;
            host.replace_peers(peers).await?;
            Ok(removed)
        })
        .await
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
            .map_err(|_| HostError::Busy)?;
        self.run_owned(async move {
            let _permit = permit;
            future.await
        })
        .await
    }

    async fn run_owned<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = Result<T, HostError>> + Send + 'static,
    ) -> Result<T, HostError> {
        Self::spawn_owned(self.0.slots.clone(), future).await
    }

    async fn spawn_owned<T: Send + 'static>(
        slots: Arc<Semaphore>,
        future: impl std::future::Future<Output = Result<T, HostError>> + Send + 'static,
    ) -> Result<T, HostError> {
        let permit = slots.try_acquire_owned().map_err(|_| HostError::Busy)?;
        let (reply, result) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = reply.send(future.await);
        });
        result.await.map_err(|_| HostError::Interrupted)?
    }
}

pub(crate) fn validate_sources(sources: &[SourceKey]) -> Result<(), HostError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stop_is_available_while_other_owned_operations_are_busy() {
        let root = tempfile::tempdir().unwrap();
        let host = SessionHost::new(
            Identity::generate().unwrap(),
            vec![],
            root.path().to_owned(),
        );
        let _operation = host.0.operation.lock().await;
        let _slots = host.0.slots.acquire_many(8).await.unwrap();
        let binding = SessionBinding {
            session: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        };
        let result = tokio::time::timeout(Duration::from_secs(1), host.stop(binding))
            .await
            .unwrap();
        assert!(matches!(result, Err(HostError::NoSession)));
    }
}
