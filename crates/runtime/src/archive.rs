use liazo_core::protocol::MAX_MEDIA_BYTES;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

mod index;
pub use index::{ArchiveIdentity, ArchiveIndex};
mod retention;
pub use retention::{ArchiveLimits, ArchiveStats};
mod transcripts;
pub use transcripts::{ArchivedTranscript, ReceiverTimeRange, TranscriptPage};
mod lifecycle;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("archive storage is unavailable")]
    Io(#[from] std::io::Error),
    #[error("invalid archive payload")]
    InvalidPayload,
    #[error("archived media is missing or corrupt")]
    CorruptPayload,
    #[error("archive payload identity is already used")]
    IdentityCollision,
    #[error("archive is already open")]
    Busy,
    #[error("archive index is unavailable")]
    Database(#[from] rusqlite::Error),
    #[error("archive metadata is invalid")]
    Metadata(#[from] serde_json::Error),
    #[error("archive belongs to another session or version")]
    SessionMismatch,
    #[error("archive limit reached")]
    Capacity,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PayloadDescriptor {
    pub id: Uuid,
    pub length: u64,
    pub sha256: [u8; 32],
}

// Blocking filesystem operations belong to the archive worker, never live ingestion.
pub struct PayloadStore {
    root: PathBuf,
    _owner: File,
}

impl PayloadStore {
    pub fn open(root: &Path) -> Result<Self, ArchiveError> {
        let mut directory = std::fs::DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory.create(root)?;
        if std::fs::symlink_metadata(root)?.file_type().is_symlink() {
            return Err(ArchiveError::InvalidPayload);
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let owner = options.open(root.join(".writer.lock"))?;
        owner.try_lock().map_err(|_| ArchiveError::Busy)?;
        Ok(Self {
            root: root.to_path_buf(),
            _owner: owner,
        })
    }

    pub fn put(&self, id: Uuid, bytes: &[u8]) -> Result<PayloadDescriptor, ArchiveError> {
        if id.is_nil() || bytes.is_empty() || bytes.len() > MAX_MEDIA_BYTES {
            return Err(ArchiveError::InvalidPayload);
        }
        let descriptor = PayloadDescriptor {
            id,
            length: bytes.len() as u64,
            sha256: Sha256::digest(bytes).into(),
        };
        let destination = self.path(id);
        if destination.try_exists()? {
            return if self.read(&descriptor).is_ok() {
                Ok(descriptor)
            } else {
                Err(ArchiveError::IdentityCollision)
            };
        }
        let temporary = self.root.join(format!("{id}.part"));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        let pending = PendingFile(temporary.clone());
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, destination)?;
        #[cfg(unix)]
        File::open(&self.root)?.sync_all()?;
        drop(pending);
        Ok(descriptor)
    }

    pub fn read(&self, descriptor: &PayloadDescriptor) -> Result<Vec<u8>, ArchiveError> {
        if descriptor.id.is_nil()
            || descriptor.length == 0
            || descriptor.length > MAX_MEDIA_BYTES as u64
        {
            return Err(ArchiveError::InvalidPayload);
        }
        let path = self.path(descriptor.id);
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ArchiveError::CorruptPayload
            } else {
                error.into()
            }
        })?;
        if !metadata.is_file() || metadata.len() != descriptor.length {
            return Err(ArchiveError::CorruptPayload);
        }
        let mut file = File::open(path)?;
        let mut bytes = vec![0; descriptor.length as usize];
        file.read_exact(&mut bytes)
            .map_err(|_| ArchiveError::CorruptPayload)?;
        let mut extra = [0];
        if file.read(&mut extra)? != 0
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != descriptor.sha256
        {
            return Err(ArchiveError::CorruptPayload);
        }
        Ok(bytes)
    }

    pub fn remove(&self, id: Uuid) -> Result<(), ArchiveError> {
        for path in [self.path(id), self.root.join(format!("{id}.part"))] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    pub fn recover(&self, referenced: &HashSet<Uuid>) -> Result<u64, ArchiveError> {
        let mut removed = 0;
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|name| name.to_str()) else {
                continue;
            };
            let Ok(id) = stem.parse::<Uuid>() else {
                continue;
            };
            let temporary = path
                .extension()
                .is_some_and(|extension| extension == "part");
            let orphan = path.extension().is_some_and(|extension| extension == "bin")
                && !referenced.contains(&id);
            if temporary || orphan {
                std::fs::remove_file(path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn path(&self, id: Uuid) -> PathBuf {
        self.root.join(format!("{id}.bin"))
    }
}

struct PendingFile(PathBuf);
impl Drop for PendingFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stored_payloads_are_immutable_bounded_and_verified_before_reading() {
        let root = tempfile::tempdir().unwrap();
        let store = PayloadStore::open(root.path()).unwrap();
        assert!(matches!(
            PayloadStore::open(root.path()),
            Err(ArchiveError::Busy)
        ));
        let id = Uuid::new_v4();
        let descriptor = store.put(id, b"fixture media").unwrap();
        assert_eq!(store.read(&descriptor).unwrap(), b"fixture media");
        assert!(store.put(id, b"fixture media").is_ok());
        assert!(matches!(
            store.put(id, b"other content"),
            Err(ArchiveError::IdentityCollision)
        ));
        let mut oversized = descriptor.clone();
        oversized.length = u64::MAX;
        assert!(matches!(
            store.read(&oversized),
            Err(ArchiveError::InvalidPayload)
        ));
        std::fs::write(store.path(id), b"changed media").unwrap();
        assert!(matches!(
            store.read(&descriptor),
            Err(ArchiveError::CorruptPayload)
        ));
        drop(store);
        assert!(PayloadStore::open(root.path()).is_ok());
    }

    #[test]
    fn recovery_removes_unpublished_media_and_partial_writes_but_preserves_references() {
        let root = tempfile::tempdir().unwrap();
        let store = PayloadStore::open(root.path()).unwrap();
        let retained = store.put(Uuid::new_v4(), b"retained").unwrap();
        let orphan = store.put(Uuid::new_v4(), b"orphan").unwrap();
        std::fs::write(
            root.path().join(format!("{}.part", Uuid::new_v4())),
            b"partial",
        )
        .unwrap();
        std::fs::write(root.path().join("unrelated.bin"), b"unrelated").unwrap();
        assert_eq!(store.recover(&HashSet::from([retained.id])).unwrap(), 2);
        assert!(store.path(retained.id).exists());
        assert!(!store.path(orphan.id).exists());
        assert!(root.path().join("unrelated.bin").exists());
        assert_eq!(store.read(&retained).unwrap(), b"retained");
    }
}
