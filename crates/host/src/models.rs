use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Once},
    time::Duration,
};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use tokio_util::sync::CancellationToken;

const REVISION: &str = "5359861c739e955e79d9a303bcbc70fb988958b1";
struct Definition {
    id: &'static str,
    file: &'static str,
    bytes: u64,
    sha256: &'static str,
}
const MODELS: [Definition; 2] = [
    Definition {
        id: "base",
        file: "ggml-base.bin",
        bytes: 147951465,
        sha256: "60ed5bc3dd14eea856493d334349b405782ddcaf0028d4b5df4088345fba2efe",
    },
    Definition {
        id: "small-q8",
        file: "ggml-small-q8_0.bin",
        bytes: 264464607,
        sha256: "49c8fb02b65e6049d5fa6c04f81f53b867b5ec9540406812c643f177317f779f",
    },
];

#[derive(Clone, Copy, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelPhase {
    Verifying,
    Missing,
    Downloading,
    Ready,
    Failed,
}
#[derive(Clone, Serialize, schemars::JsonSchema)]
pub struct ModelStatus {
    pub id: String,
    pub bytes: u64,
    pub sha256: String,
    pub phase: ModelPhase,
    pub received_bytes: u64,
    pub path: Option<PathBuf>,
    pub error: Option<String>,
}
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    #[error("unknown speech model")]
    Unknown,
    #[error("a model download is already running")]
    Busy,
    #[error("speech models are still being verified")]
    Verifying,
    #[error("model download failed; check the connection and retry")]
    Network,
    #[error("model verification failed")]
    Verification,
    #[error("model download cancelled")]
    Cancelled,
    #[error("model storage is unavailable: {0}")]
    Storage(#[from] std::io::Error),
    #[error("model verification interrupted")]
    Interrupted,
}
struct State {
    rows: Vec<ModelStatus>,
    active: Option<CancellationToken>,
}
struct Inner {
    root: PathBuf,
    initialized: Once,
    state: Mutex<State>,
}
#[derive(Clone)]
pub struct ModelStore(Arc<Inner>);

impl ModelStore {
    pub fn new(root: PathBuf) -> Self {
        Self(Arc::new(Inner {
            root,
            initialized: Once::new(),
            state: Mutex::new(State {
                rows: MODELS
                    .iter()
                    .map(|model| ModelStatus {
                        id: model.id.into(),
                        bytes: model.bytes,
                        sha256: model.sha256.into(),
                        phase: ModelPhase::Verifying,
                        received_bytes: 0,
                        path: None,
                        error: None,
                    })
                    .collect(),
                active: None,
            }),
        }))
    }
    fn initialize(&self) {
        self.0.initialized.call_once(|| {
            let store = self.clone();
            tokio::spawn(async move {
                let root = store.0.root.clone();
                let result = tokio::task::spawn_blocking(move || {
                    std::fs::create_dir_all(&root)?;
                    for entry in std::fs::read_dir(&root)? {
                        let entry = entry?;
                        if entry.file_type()?.is_file()
                            && entry.file_name().to_str().is_some_and(|name| {
                                name.starts_with("download-") && name.ends_with(".part")
                            })
                        {
                            std::fs::remove_file(entry.path())?;
                        }
                    }
                    Ok::<_, ModelError>(
                        MODELS
                            .iter()
                            .map(|model| {
                                let path = root.join(model.file);
                                let (phase, error) = match verify(&path, model) {
                                    Ok(true) => (ModelPhase::Ready, None),
                                    Ok(false) => (ModelPhase::Missing, None),
                                    Err(error) => (ModelPhase::Failed, Some(error.to_string())),
                                };
                                ModelStatus {
                                    id: model.id.into(),
                                    bytes: model.bytes,
                                    sha256: model.sha256.into(),
                                    phase,
                                    received_bytes: if phase == ModelPhase::Ready {
                                        model.bytes
                                    } else {
                                        0
                                    },
                                    path: (phase == ModelPhase::Ready).then_some(path),
                                    error,
                                }
                            })
                            .collect(),
                    )
                })
                .await
                .map_err(|_| ModelError::Interrupted)
                .and_then(|result| result);
                let mut state = store.0.state.lock().await;
                match result {
                    Ok(rows) => state.rows = rows,
                    Err(error) => {
                        for row in &mut state.rows {
                            row.phase = ModelPhase::Failed;
                            row.error = Some(error.to_string());
                        }
                    }
                }
            });
        });
    }
    pub async fn status(&self) -> Vec<ModelStatus> {
        self.initialize();
        self.0.state.lock().await.rows.clone()
    }
    pub async fn install(&self, id: &str) -> Result<(), ModelError> {
        let model = MODELS
            .iter()
            .find(|model| model.id == id)
            .ok_or(ModelError::Unknown)?;
        self.initialize();
        let mut state = self.0.state.lock().await;
        let index = MODELS.iter().position(|entry| entry.id == id).unwrap();
        if state.rows[index].phase == ModelPhase::Verifying {
            return Err(ModelError::Verifying);
        }
        if state.rows[index].phase == ModelPhase::Ready {
            return Ok(());
        }
        if state.active.is_some() {
            return if state.rows[index].phase == ModelPhase::Downloading {
                Ok(())
            } else {
                Err(ModelError::Busy)
            };
        }
        let cancelled = CancellationToken::new();
        state.active = Some(cancelled.clone());
        state.rows[index].phase = ModelPhase::Downloading;
        state.rows[index].received_bytes = 0;
        state.rows[index].error = None;
        let store = self.clone();
        tokio::spawn(async move {
            let result = store.download(model, &cancelled).await;
            let mut state = store.0.state.lock().await;
            let row = &mut state.rows[index];
            match result {
                Ok(()) => {
                    row.phase = ModelPhase::Ready;
                    row.path = Some(store.0.root.join(model.file));
                }
                Err(ModelError::Cancelled) => {
                    row.phase = ModelPhase::Missing;
                    row.received_bytes = 0;
                }
                Err(error) => {
                    row.phase = ModelPhase::Failed;
                    row.error = Some(error.to_string());
                }
            }
            state.active = None;
        });
        Ok(())
    }
    pub async fn cancel(&self) {
        if let Some(cancelled) = &self.0.state.lock().await.active {
            cancelled.cancel();
        }
    }
    async fn download(
        &self,
        model: &Definition,
        cancelled: &CancellationToken,
    ) -> Result<(), ModelError> {
        let client = crate::http::client_builder()
            .https_only(true)
            .no_proxy()
            .connect_timeout(Duration::from_secs(15))
            .read_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(600))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .map_err(|_| ModelError::Network)?;
        let url = format!(
            "https://huggingface.co/ggerganov/whisper.cpp/resolve/{REVISION}/{}",
            model.file
        );
        let response = tokio::select! {
            _ = cancelled.cancelled() => return Err(ModelError::Cancelled),
            response = client.get(url).send() => response.map_err(|_| ModelError::Network)?.error_for_status().map_err(|_| ModelError::Network)?,
        };
        self.receive(model, response, cancelled).await
    }
    async fn receive(
        &self,
        model: &Definition,
        mut response: reqwest::Response,
        cancelled: &CancellationToken,
    ) -> Result<(), ModelError> {
        if response
            .content_length()
            .is_some_and(|length| length != model.bytes)
        {
            return Err(ModelError::Verification);
        }
        tokio::fs::create_dir_all(&self.0.root).await?;
        let temporary = Temporary(
            self.0
                .root
                .join(format!("download-{}.part", uuid::Uuid::new_v4())),
        );
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary.0)
            .await?;
        let mut hash = Sha256::new();
        let mut received = 0u64;
        loop {
            let chunk = tokio::select! {
                _ = cancelled.cancelled() => return Err(ModelError::Cancelled),
                chunk = response.chunk() => chunk.map_err(|_| ModelError::Network)?,
            };
            let Some(chunk) = chunk else { break };
            received = received
                .checked_add(chunk.len() as u64)
                .ok_or(ModelError::Verification)?;
            if received > model.bytes {
                return Err(ModelError::Verification);
            }
            file.write_all(&chunk).await?;
            // Finish the OS write before the next cancellable network wait.
            file.flush().await?;
            hash.update(&chunk);
            if let Some(row) = self
                .0
                .state
                .lock()
                .await
                .rows
                .iter_mut()
                .find(|row| row.id == model.id)
            {
                row.received_bytes = received;
            }
        }
        if received != model.bytes || format!("{:x}", hash.finalize()) != model.sha256 {
            return Err(ModelError::Verification);
        }
        if cancelled.is_cancelled() {
            return Err(ModelError::Cancelled);
        }
        file.sync_all().await?;
        drop(file);
        let target = self.0.root.join(model.file);
        if tokio::fs::try_exists(&target).await? {
            tokio::fs::remove_file(&target).await?;
        }
        tokio::fs::rename(&temporary.0, target).await?;
        Ok(())
    }
}
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn verify(path: &Path, model: &Definition) -> Result<bool, ModelError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.len() != model.bytes {
        return Err(ModelError::Verification);
    }
    let mut file = std::fs::File::open(path)?.take(model.bytes + 1);
    let mut hash = Sha256::new();
    let mut bytes = [0; 65536];
    loop {
        let read = file.read(&mut bytes)?;
        if read == 0 {
            break;
        }
        hash.update(&bytes[..read]);
    }
    if format!("{:x}", hash.finalize()) != model.sha256 {
        return Err(ModelError::Verification);
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, routing::get};
    use bytes::Bytes;
    use futures_util::{StreamExt, stream};
    use tokio::net::TcpListener;

    const FIXTURE: Definition = Definition {
        id: "base",
        file: "fixture.bin",
        bytes: 3,
        sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    };

    async fn initialized(store: &ModelStore) -> Vec<ModelStatus> {
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let rows = store.status().await;
                if rows.iter().all(|row| row.phase != ModelPhase::Verifying) {
                    return rows;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn status_returns_while_initial_verification_is_pending() {
        let root = tempfile::tempdir().unwrap();
        let store = ModelStore::new(root.path().to_owned());
        assert!(
            store
                .status()
                .await
                .iter()
                .all(|row| row.phase == ModelPhase::Verifying)
        );
        assert!(matches!(
            store.install("base").await,
            Err(ModelError::Verifying)
        ));
        assert!(
            initialized(&store)
                .await
                .iter()
                .all(|row| row.phase == ModelPhase::Missing)
        );
    }
    async fn response(body: Body) -> (reqwest::Response, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = Arc::new(Mutex::new(Some(body)));
        let app = Router::new().route(
            "/",
            get(move || {
                let body = body.clone();
                async move { body.lock().await.take().unwrap() }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let response = crate::http::client_builder()
            .build()
            .unwrap()
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        (response, server)
    }
    #[tokio::test]
    async fn only_complete_verified_bytes_replace_a_model_and_failed_parts_are_removed() {
        let root = tempfile::tempdir().unwrap();
        let store = ModelStore::new(root.path().to_owned());
        initialized(&store).await;
        let (body, server) = response(Body::from("abc")).await;
        store
            .receive(&FIXTURE, body, &CancellationToken::new())
            .await
            .unwrap();
        server.abort();
        assert!(verify(&root.path().join(FIXTURE.file), &FIXTURE).unwrap());
        for bytes in ["abd", "ab", "abcd"] {
            let (body, server) = response(Body::from(bytes)).await;
            assert!(
                store
                    .receive(&FIXTURE, body, &CancellationToken::new())
                    .await
                    .is_err()
            );
            server.abort();
            assert_eq!(
                std::fs::read(root.path().join(FIXTURE.file)).unwrap(),
                b"abc"
            );
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        }
        std::fs::write(root.path().join(FIXTURE.file), b"abd").unwrap();
        assert!(matches!(
            verify(&root.path().join(FIXTURE.file), &FIXTURE),
            Err(ModelError::Verification)
        ));
    }
    #[tokio::test]
    async fn cancelling_a_stalled_body_releases_its_file_and_preserves_existing_models() {
        let root = tempfile::tempdir().unwrap();
        let store = ModelStore::new(root.path().to_owned());
        initialized(&store).await;
        std::fs::write(root.path().join(FIXTURE.file), b"abc").unwrap();
        let body = Body::from_stream(
            stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(b"a")) })
                .chain(stream::pending()),
        );
        let (body, server) = response(body).await;
        let cancelled = CancellationToken::new();
        let token = cancelled.clone();
        let downloading = store.clone();
        let task = tokio::spawn(async move { downloading.receive(&FIXTURE, body, &token).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.status().await[0].received_bytes != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancelled.cancel();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap(),
            Err(ModelError::Cancelled)
        ));
        server.abort();
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        assert!(verify(&root.path().join(FIXTURE.file), &FIXTURE).unwrap());
    }
    #[tokio::test]
    async fn restart_rejects_corruption_and_cleans_interrupted_downloads() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(MODELS[0].file), b"not a model").unwrap();
        std::fs::write(root.path().join("download-interrupted.part"), b"partial").unwrap();
        let store = ModelStore::new(root.path().to_owned());
        let rows = initialized(&store).await;
        assert!(rows[0].phase == ModelPhase::Failed && rows[0].path.is_none());
        assert!(rows[1].phase == ModelPhase::Missing);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        assert!(matches!(
            store.install("../other").await,
            Err(ModelError::Unknown)
        ));
    }
    #[tokio::test]
    #[ignore = "downloads the actual 148 MB pinned base model over HTTPS"]
    async fn pinned_upstream_download_produces_a_verified_model_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let path = std::env::var_os("LIAZO_TEST_MODEL_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.path().join("şö語"));
        let store = ModelStore::new(path.clone());
        initialized(&store).await;
        store.install("base").await.unwrap();
        tokio::time::timeout(Duration::from_secs(610), async {
            loop {
                let rows = store.status().await;
                if rows[0].phase != ModelPhase::Downloading {
                    assert!(rows[0].phase == ModelPhase::Ready, "{:?}", rows[0].error);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        let restarted = ModelStore::new(path);
        assert!(initialized(&restarted).await[0].phase == ModelPhase::Ready);
    }
}
