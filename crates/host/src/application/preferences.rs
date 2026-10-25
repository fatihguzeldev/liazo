use liazo_core::protocol::{CaptureSettings, SourceKey};
use liazo_transport::{Fingerprint, SessionBinding};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;

const MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub(super) enum PreferenceError {
    #[error("saved session settings are unavailable")]
    Storage,
    #[error("saved session settings are invalid")]
    Invalid,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SpeechSettings {
    pub path: PathBuf,
    pub threads: u8,
    pub sources: Vec<SourceKey>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResumeIntent {
    pub version: u8,
    pub owner: String,
    pub provider: String,
    pub address: SocketAddr,
    pub binding: SessionBinding,
    pub capture: CaptureSettings,
    pub raw_audio: Vec<SourceKey>,
    pub speech: Option<SpeechSettings>,
}
impl ResumeIntent {
    fn validate(&self, owner: Fingerprint) -> Result<(), PreferenceError> {
        if self.version != 1
            || self.owner != owner.to_string()
            || self
                .provider
                .parse::<Fingerprint>()
                .ok()
                .map(|pin| pin.to_string())
                != Some(self.provider.clone())
            || self.binding.session.is_nil()
            || self.binding.epoch.is_nil()
            || self.address.port() == 0
            || self.address.ip().is_unspecified()
            || self.address.ip().is_multicast()
        {
            return Err(PreferenceError::Invalid);
        }
        self.capture
            .validate()
            .map_err(|_| PreferenceError::Invalid)?;
        let selected = |sources: &[SourceKey]| {
            sources.len() <= 8
                && sources
                    .iter()
                    .all(|key| key.validate().is_ok() && self.capture.audio.contains(key))
                && sources
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    == sources.len()
        };
        if !selected(&self.raw_audio) {
            return Err(PreferenceError::Invalid);
        }
        if let Some(speech) = &self.speech
            && (speech.sources.is_empty()
                || !selected(&speech.sources)
                || !(1..=16).contains(&speech.threads)
                || !speech.path.is_absolute()
                || speech
                    .path
                    .to_str()
                    .is_none_or(|path| path.len() > 4096 || path.chars().any(char::is_control)))
        {
            return Err(PreferenceError::Invalid);
        }
        Ok(())
    }
}
struct Save {
    intent: Option<ResumeIntent>,
    reply: oneshot::Sender<Result<(), PreferenceError>>,
}
#[derive(Default)]
struct Pending {
    latest: u64,
    save: Option<Save>,
    closed: bool,
}
#[derive(Default)]
struct Mailbox {
    pending: Mutex<Pending>,
    changed: Condvar,
    #[cfg(test)]
    pause: Mutex<Option<TestPause>>,
}
type Loaded = Result<Option<ResumeIntent>, PreferenceError>;
#[cfg(test)]
struct TestPause {
    entered: oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
struct Owner {
    mailbox: Arc<Mailbox>,
    initial: tokio::sync::Mutex<Option<oneshot::Receiver<Loaded>>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.mailbox
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closed = true;
        self.mailbox.changed.notify_one();
    }
}
#[derive(Clone)]
pub(super) struct PreferenceStore(Arc<Owner>);
impl PreferenceStore {
    pub fn spawn(root: PathBuf, owner: Fingerprint) -> Result<Self, PreferenceError> {
        let mailbox = Arc::new(Mailbox::default());
        let worker = mailbox.clone();
        let (loaded, initial) = oneshot::channel();
        std::thread::Builder::new()
            .name("liazo-settings".into())
            .spawn(move || {
                let mut directory = Directory::open_retry(root.clone());
                let _ = loaded.send(
                    directory
                        .as_ref()
                        .map_err(|_| PreferenceError::Storage)
                        .and_then(|directory| directory.read(owner)),
                );
                loop {
                    let save = {
                        let mut pending = worker
                            .pending
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        while pending.save.is_none() && !pending.closed {
                            pending = worker
                                .changed
                                .wait(pending)
                                .unwrap_or_else(|error| error.into_inner());
                        }
                        if pending.closed && pending.save.is_none() {
                            break;
                        }
                        pending.save.take().expect("pending save")
                    };
                    #[cfg(test)]
                    if let Some(pause) = worker
                        .pause
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take()
                    {
                        let _ = pause.entered.send(());
                        let _ = pause.release.recv();
                    }
                    if directory.is_err() {
                        directory = Directory::open(root.clone());
                    }
                    let result = directory
                        .as_ref()
                        .map_err(|_| PreferenceError::Storage)
                        .and_then(|directory| directory.save(owner, save.intent));
                    let _ = save.reply.send(result);
                }
            })
            .map_err(|_| PreferenceError::Storage)?;
        Ok(Self(Arc::new(Owner {
            mailbox,
            initial: tokio::sync::Mutex::new(Some(initial)),
        })))
    }
    pub async fn load(&self) -> Result<Option<ResumeIntent>, PreferenceError> {
        let initial = self
            .0
            .initial
            .lock()
            .await
            .take()
            .ok_or(PreferenceError::Storage)?;
        tokio::time::timeout(Duration::from_secs(2), initial)
            .await
            .map_err(|_| PreferenceError::Storage)?
            .map_err(|_| PreferenceError::Storage)?
    }
    pub async fn save(
        &self,
        version: u64,
        intent: Option<ResumeIntent>,
    ) -> Result<(), PreferenceError> {
        let (reply, result) = oneshot::channel();
        {
            let mut pending = self
                .0
                .mailbox
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if pending.closed {
                return Err(PreferenceError::Storage);
            }
            if version < pending.latest {
                return Ok(());
            }
            pending.latest = version;
            if let Some(previous) = pending.save.replace(Save { intent, reply }) {
                // A later complete snapshot contains the accepted state. Coalescing never
                // permits an older captured snapshot to overwrite a later explicit stop.
                let _ = previous.reply.send(Ok(()));
            }
        }
        self.0.mailbox.changed.notify_one();
        tokio::time::timeout(Duration::from_secs(2), result)
            .await
            .map_err(|_| PreferenceError::Storage)?
            .map_err(|_| PreferenceError::Storage)?
    }
}
struct Directory {
    root: PathBuf,
    _owner: File,
}
impl Directory {
    fn open_retry(root: PathBuf) -> std::io::Result<Self> {
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            match Self::open(root.clone()) {
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10))
                }
                result => return result,
            }
        }
    }

    fn open(root: PathBuf) -> std::io::Result<Self> {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
            builder.mode(0o700);
            builder.create(&root)?;
            if !std::fs::symlink_metadata(&root)?.is_dir() {
                return Err(std::io::Error::other("invalid settings directory"));
            }
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
        }
        #[cfg(not(unix))]
        {
            builder.create(&root)?;
        }
        if !std::fs::symlink_metadata(&root)?.is_dir() {
            return Err(std::io::Error::other("invalid settings directory"));
        }
        let lock = root.join(".resume.lock");
        if std::fs::symlink_metadata(&lock).is_ok_and(|metadata| !metadata.is_file()) {
            return Err(std::io::Error::other("invalid settings lock"));
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let owner = options.open(lock)?;
        owner.try_lock().map_err(|error| match error {
            std::fs::TryLockError::WouldBlock => {
                std::io::Error::from(std::io::ErrorKind::WouldBlock)
            }
            error => std::io::Error::other(error),
        })?;
        let temporary = root.join("resume.next");
        if let Ok(metadata) = std::fs::symlink_metadata(&temporary) {
            if !metadata.is_file() || metadata.len() > MAX_BYTES {
                return Err(std::io::Error::other("invalid pending settings"));
            }
            std::fs::remove_file(temporary)?;
        }
        Ok(Self {
            root,
            _owner: owner,
        })
    }
    fn read(&self, owner: Fingerprint) -> Result<Option<ResumeIntent>, PreferenceError> {
        let path = self.root.join("resume.json");
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            _ => return Err(PreferenceError::Storage),
        };
        if !metadata.is_file() || metadata.len() > MAX_BYTES {
            return Err(PreferenceError::Invalid);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(PreferenceError::Invalid);
            }
        }
        let mut bytes = vec![];
        File::open(path)
            .map_err(|_| PreferenceError::Storage)?
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| PreferenceError::Storage)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(PreferenceError::Invalid);
        }
        let intent: ResumeIntent =
            serde_json::from_slice(&bytes).map_err(|_| PreferenceError::Invalid)?;
        intent.validate(owner)?;
        Ok(Some(intent))
    }
    fn save(
        &self,
        owner: Fingerprint,
        intent: Option<ResumeIntent>,
    ) -> Result<(), PreferenceError> {
        let destination = self.root.join("resume.json");
        if std::fs::symlink_metadata(&destination).is_ok_and(|metadata| !metadata.is_file()) {
            return Err(PreferenceError::Invalid);
        }
        let Some(intent) = intent else {
            match std::fs::remove_file(destination) {
                Ok(()) => sync(&self.root).map_err(|_| PreferenceError::Storage),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                _ => Err(PreferenceError::Storage),
            }?;
            return Ok(());
        };
        intent.validate(owner)?;
        let bytes = serde_json::to_vec(&intent).map_err(|_| PreferenceError::Invalid)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(PreferenceError::Invalid);
        }
        let temporary = self.root.join("resume.next");
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let result = (|| -> std::io::Result<()> {
            let mut file = options.open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&temporary, destination)?;
            sync(&self.root)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result.map_err(|_| PreferenceError::Storage)
    }
}
fn sync(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use liazo_core::protocol::{ImageFormat, ImageSubscription};
    use liazo_transport::Identity;
    use uuid::Uuid;

    fn intent(owner: Fingerprint) -> ResumeIntent {
        ResumeIntent {
            version: 1,
            owner: owner.to_string(),
            provider: Identity::generate().unwrap().fingerprint().to_string(),
            address: "127.0.0.1:4875".parse().unwrap(),
            binding: SessionBinding {
                session: Uuid::new_v4(),
                epoch: Uuid::new_v4(),
            },
            capture: CaptureSettings {
                images: vec![ImageSubscription {
                    source: SourceKey {
                        id: "display:fixture".into(),
                        generation: Uuid::new_v4(),
                    },
                    interval_ms: 250,
                    max_width: None,
                    crop: None,
                    format: ImageFormat::Png,
                    quality: 90,
                    include_cursor: false,
                }],
                audio: vec![],
            },
            raw_audio: vec![],
            speech: None,
        }
    }
    #[tokio::test]
    async fn atomic_replacement_and_later_disable_survive_a_new_store_and_ignore_stale_snapshots() {
        let root = tempfile::tempdir().unwrap();
        let owner = Identity::generate().unwrap().fingerprint();
        let store = PreferenceStore::spawn(root.path().to_owned(), owner).unwrap();
        assert!(store.load().await.unwrap().is_none());
        let first = intent(owner);
        store.save(1, Some(first.clone())).await.unwrap();
        let mut second = first.clone();
        second.capture.images[0].interval_ms = 800;
        store.save(2, Some(second)).await.unwrap();
        let saved: ResumeIntent =
            serde_json::from_slice(&std::fs::read(root.path().join("resume.json")).unwrap())
                .unwrap();
        assert_eq!(saved.capture.images[0].interval_ms, 800);
        assert!(
            std::fs::metadata(root.path().join("resume.json"))
                .unwrap()
                .len()
                <= MAX_BYTES
        );
        store.save(3, None).await.unwrap();
        store.save(1, Some(first)).await.unwrap();
        assert!(!root.path().join("resume.json").exists());
        drop(store);
        let reopened = PreferenceStore::spawn(root.path().to_owned(), owner).unwrap();
        assert!(reopened.load().await.unwrap().is_none());
    }
    #[tokio::test]
    async fn an_inflight_save_and_coalesced_disable_cannot_be_overwritten_by_a_late_old_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let owner = Identity::generate().unwrap().fingerprint();
        let store = PreferenceStore::spawn(root.path().to_owned(), owner).unwrap();
        store.load().await.unwrap();
        let (entered, held) = oneshot::channel();
        let (release, paused) = std::sync::mpsc::channel();
        *store.0.mailbox.pause.lock().unwrap() = Some(TestPause {
            entered,
            release: paused,
        });
        let first_store = store.clone();
        let first = intent(owner);
        let saved = first.clone();
        let inflight = tokio::spawn(async move { first_store.save(1, Some(saved)).await });
        tokio::time::timeout(Duration::from_secs(1), held)
            .await
            .unwrap()
            .unwrap();
        let newer_store = store.clone();
        let saved = first.clone();
        let newer = tokio::spawn(async move { newer_store.save(3, Some(saved)).await });
        while store.0.mailbox.pending.lock().unwrap().latest < 3 {
            tokio::task::yield_now().await;
        }
        let stop_store = store.clone();
        let stop = tokio::spawn(async move { stop_store.save(4, None).await });
        while store.0.mailbox.pending.lock().unwrap().latest < 4 {
            tokio::task::yield_now().await;
        }
        store.save(2, Some(first)).await.unwrap();
        assert!(
            store
                .0
                .mailbox
                .pending
                .lock()
                .unwrap()
                .save
                .as_ref()
                .unwrap()
                .intent
                .is_none()
        );
        newer.await.unwrap().unwrap();
        release.send(()).unwrap();
        inflight.await.unwrap().unwrap();
        stop.await.unwrap().unwrap();
        assert!(!root.path().join("resume.json").exists());
    }
    #[test]
    fn identity_binding_source_and_oversized_records_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let owner = Identity::generate().unwrap().fingerprint();
        let directory = Directory::open(root.path().to_owned()).unwrap();
        let saved = intent(owner);
        directory.save(owner, Some(saved.clone())).unwrap();
        assert!(matches!(
            directory.read(Identity::generate().unwrap().fingerprint()),
            Err(PreferenceError::Invalid)
        ));
        let mut bad = saved.clone();
        bad.binding.epoch = Uuid::nil();
        std::fs::write(
            root.path().join("resume.json"),
            serde_json::to_vec(&bad).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            directory.read(owner),
            Err(PreferenceError::Invalid)
        ));
        bad = saved;
        bad.capture.images[0].source.generation = Uuid::nil();
        std::fs::write(
            root.path().join("resume.json"),
            serde_json::to_vec(&bad).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            directory.read(owner),
            Err(PreferenceError::Invalid)
        ));
        std::fs::write(
            root.path().join("resume.json"),
            vec![b' '; MAX_BYTES as usize + 1],
        )
        .unwrap();
        assert!(matches!(
            directory.read(owner),
            Err(PreferenceError::Invalid)
        ));
    }
    #[test]
    fn interrupted_bounded_temporary_file_is_removed_before_loading_the_durable_record() {
        let root = tempfile::tempdir().unwrap();
        let owner = Identity::generate().unwrap().fingerprint();
        let first = intent(owner);
        let directory = Directory::open(root.path().to_owned()).unwrap();
        directory.save(owner, Some(first.clone())).unwrap();
        std::fs::write(root.path().join("resume.next"), b"interrupted publication").unwrap();
        drop(directory);
        let directory = Directory::open(root.path().to_owned()).unwrap();
        assert!(!root.path().join("resume.next").exists());
        assert_eq!(
            directory.read(owner).unwrap().unwrap().binding,
            first.binding
        );
    }
    #[cfg(unix)]
    #[test]
    fn symlink_records_and_permissive_files_are_rejected_without_touching_the_target() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let root = tempfile::tempdir().unwrap();
        let target = tempfile::NamedTempFile::new().unwrap();
        let owner = Identity::generate().unwrap().fingerprint();
        let directory = Directory::open(root.path().to_owned()).unwrap();
        symlink(target.path(), root.path().join("resume.json")).unwrap();
        assert!(matches!(
            directory.read(owner),
            Err(PreferenceError::Invalid)
        ));
        assert!(directory.save(owner, None).is_err());
        assert!(target.path().exists());
        std::fs::remove_file(root.path().join("resume.json")).unwrap();
        directory.save(owner, Some(intent(owner))).unwrap();
        std::fs::set_permissions(
            root.path().join("resume.json"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(matches!(
            directory.read(owner),
            Err(PreferenceError::Invalid)
        ));
    }
}
