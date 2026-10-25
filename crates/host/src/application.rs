use liazo_core::protocol::SourceKey;
use liazo_runtime::{ConnectionState, RuntimeError, receiver::ReceiverSession};
use liazo_transport::{
    CredentialError, Fingerprint, Identity, PairingInvitation, SessionBinding, StoredPeer,
    request_pairing,
};
use serde::Serialize;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, RwLock, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;
mod recovery;
use recovery::start_speech;

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
    pub recovery: Vec<RecoveryIssue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOperation {
    Capture,
    Archive,
    RawAudio,
    Transcription,
}

#[derive(Clone, Debug, Serialize, schemars::JsonSchema)]
pub struct RecoveryIssue {
    pub operation: RecoveryOperation,
    pub source: Option<SourceKey>,
    pub message: String,
}

#[derive(Clone)]
struct SpeechIntent {
    model: liazo_transcription::WhisperModel,
    threads: u8,
    sources: Vec<SourceKey>,
}

struct Active {
    peer: Fingerprint,
    address: SocketAddr,
    receiver: ReceiverSession,
    reconnect: bool,
    raw_audio: Vec<SourceKey>,
    speech: Option<SpeechIntent>,
    speech_revision: u64,
    recovery: Vec<RecoveryIssue>,
}
struct Retirement {
    binding: SessionBinding,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Retirement {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct Inner {
    identity: Identity,
    peers: RwLock<Vec<StoredPeer>>,
    active: RwLock<Option<Active>>,
    operation: Arc<Mutex<()>>,
    speech_operation: Arc<Mutex<()>>,
    slots: Arc<Semaphore>,
    stops: Arc<Semaphore>,
    pairing: Arc<Mutex<()>>,
    trust_ops: Mutex<()>,
    trust: Option<Arc<dyn crate::TrustStore>>,
    archive_root: PathBuf,
    catalog: Option<liazo_runtime::archive::ArchiveCatalog>,
    models: crate::models::ModelStore,
    revision: AtomicU64,
    transition: StdMutex<CancellationToken>,
    monitor: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    retirement: StdMutex<Option<Retirement>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = self
            .monitor
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            task.abort();
        }
        self.retirement
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }
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
            speech_operation: Arc::new(Mutex::new(())),
            slots: Arc::new(Semaphore::new(8)),
            stops: Arc::new(Semaphore::new(2)),
            pairing: Arc::new(Mutex::new(())),
            trust_ops: Mutex::new(()),
            trust,
            archive_root,
            catalog,
            models: crate::models::ModelStore::new(model_root),
            revision: AtomicU64::new(0),
            transition: StdMutex::new(CancellationToken::new()),
            monitor: StdMutex::new(None),
            retirement: StdMutex::new(None),
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
        Some(SessionSummary {
            binding: active.receiver.archive_identity().binding,
            provider: active.peer.to_string(),
            address: active.address,
            connection: active.receiver.connection(),
            recovery: active.recovery.clone(),
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
        self.owned(async move { host.connect_inner(address, peer, None).await })
            .await
    }

    pub async fn stop(&self, binding: SessionBinding) -> Result<(), HostError> {
        let receiver = {
            let mut active = self.0.active.write().await;
            let active = active.as_mut().ok_or(HostError::NoSession)?;
            if active.receiver.archive_identity().binding != binding {
                return Err(HostError::NoSession);
            }
            active.reconnect = false;
            active.speech = None;
            active.speech_revision = active.speech_revision.saturating_add(1);
            self.0.revision.fetch_add(1, Ordering::AcqRel);
            self.0
                .transition
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .cancel();
            active.receiver.clone()
        };
        let speech = self.0.speech_operation.clone();
        Self::spawn_owned(self.0.stops.clone(), async move {
            let _speech = speech.lock().await;
            receiver.stop().await.map_err(Into::into)
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
                self.0.revision.fetch_add(1, Ordering::AcqRel);
                self.0
                    .transition
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .cancel();
                active.take().map(|session| session.receiver)
            } else {
                None
            }
        };
        if let Some(receiver) = revoked {
            self.retire_previous(receiver).await?;
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
        validate_sources(&sources)?;
        let host = self.clone();
        self.owned(async move {
            let receiver = host.receiver(binding).await?;
            let revision = host
                .0
                .active
                .read()
                .await
                .as_ref()
                .ok_or(HostError::NoSession)?
                .speech_revision;
            let model =
                tokio::task::spawn_blocking(move || liazo_transcription::WhisperModel::load(&path))
                    .await
                    .map_err(|_| HostError::Interrupted)??;
            let speech = SpeechIntent {
                model,
                threads,
                sources,
            };
            let _speech = host.0.speech_operation.lock().await;
            {
                let active = host.0.active.read().await;
                let active = active.as_ref().ok_or(HostError::NoSession)?;
                if active.speech_revision != revision || !active.reconnect {
                    return Err(HostError::Interrupted);
                }
            }
            start_speech(&receiver, &speech).await?;
            let installed = {
                let mut active = host.0.active.write().await;
                let active = active.as_mut().ok_or(HostError::NoSession)?;
                if active.speech_revision == revision && active.reconnect {
                    active.speech = Some(speech);
                    active
                        .recovery
                        .retain(|issue| issue.operation != RecoveryOperation::Transcription);
                    true
                } else {
                    false
                }
            };
            if !installed {
                receiver.stop_transcription().await;
                return Err(HostError::Interrupted);
            }
            Ok(())
        })
        .await
    }

    pub async fn stop_transcription(&self, binding: SessionBinding) -> Result<(), HostError> {
        let receiver = {
            let mut active = self.0.active.write().await;
            let active = active.as_mut().ok_or(HostError::NoSession)?;
            if active.receiver.archive_identity().binding != binding {
                return Err(HostError::NoSession);
            }
            active.speech = None;
            active.speech_revision = active.speech_revision.saturating_add(1);
            self.0.revision.fetch_add(1, Ordering::AcqRel);
            self.0
                .transition
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .cancel();
            active
                .recovery
                .retain(|issue| issue.operation != RecoveryOperation::Transcription);
            active.receiver.clone()
        };
        let speech = self.0.speech_operation.clone();
        Self::spawn_owned(self.0.stops.clone(), async move {
            let _speech = speech.lock().await;
            receiver.stop_transcription().await;
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
    use uuid::Uuid;

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
