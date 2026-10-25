use super::{ArchiveError, ArchiveInfo, PayloadStore, budget};
use liazo_transport::SessionBinding;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const MARKER: &str = ".delete.json";
#[derive(Serialize, Deserialize)]
struct Deletion {
    version: u8,
    session: Uuid,
    epoch: Uuid,
    accounted_bytes: u64,
}

fn ordinary(path: &Path, directory: bool) -> Result<u64, ArchiveError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || metadata.is_dir() != directory
        || (!directory && !metadata.is_file())
    {
        return Err(ArchiveError::InvalidPayload);
    }
    Ok(metadata.len().div_ceil(4096) * 4096 + 4096)
}

/// Reject unfamiliar files before any destructive operation. Count orphan/partial media too.
pub(super) fn validate(root: &Path) -> Result<u64, ArchiveError> {
    ordinary(root, true)?;
    let mut bytes = 8192_u64;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "payloads" {
            ordinary(&entry.path(), true)?;
            for payload in std::fs::read_dir(entry.path())? {
                let payload = payload?;
                let name = payload.file_name();
                let name = name.to_str().ok_or(ArchiveError::InvalidPayload)?;
                if name != ".writer.lock" {
                    let Some((id, extension)) = name.rsplit_once('.') else {
                        return Err(ArchiveError::InvalidPayload);
                    };
                    let uuid = id
                        .parse::<Uuid>()
                        .map_err(|_| ArchiveError::InvalidPayload)?;
                    if uuid.to_string() != id || !matches!(extension, "bin" | "part") {
                        return Err(ArchiveError::InvalidPayload);
                    }
                }
                bytes = bytes.saturating_add(ordinary(&payload.path(), false)?);
            }
        } else if matches!(
            name.to_str(),
            Some(
                "index.sqlite3"
                    | "index.sqlite3-wal"
                    | "index.sqlite3-shm"
                    | ".delete.json"
                    | ".delete.part"
            )
        ) {
            bytes = bytes.saturating_add(ordinary(&entry.path(), false)?);
        } else {
            return Err(ArchiveError::InvalidPayload);
        }
    }
    Ok(bytes)
}

fn trash(root: &Path, binding: SessionBinding) -> PathBuf {
    root.join(format!(".deleted-{}-{}", binding.session, binding.epoch))
}

fn trash_binding(name: &std::ffi::OsStr) -> Option<SessionBinding> {
    let text = name.to_str()?.strip_prefix(".deleted-")?;
    if text.len() != 73 || text.as_bytes()[36] != b'-' {
        return None;
    }
    let session: Uuid = text[..36].parse().ok()?;
    let epoch: Uuid = text[37..].parse().ok()?;
    let binding = SessionBinding { session, epoch };
    (format!("{session}-{epoch}") == text).then_some(binding)
}

fn marker(root: &Path, binding: SessionBinding) -> Result<Deletion, ArchiveError> {
    ordinary(&root.join(MARKER), false)?;
    let file = File::open(root.join(MARKER))?;
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(ArchiveError::InvalidPayload);
    }
    let marker: Deletion = serde_json::from_slice(&bytes)?;
    if marker.version != 1
        || marker.session != binding.session
        || marker.epoch != binding.epoch
        || marker.accounted_bytes == 0
    {
        return Err(ArchiveError::SessionMismatch);
    }
    Ok(marker)
}

fn sync_directory(path: &Path) -> Result<(), ArchiveError> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn write_marker(root: &Path, info: &ArchiveInfo) -> Result<(), ArchiveError> {
    if root.join(MARKER).try_exists()? {
        marker(root, info.binding)?;
        return Ok(());
    }
    let temporary = root.join(".delete.part");
    if temporary.try_exists()? {
        ordinary(&temporary, false)?;
        std::fs::remove_file(&temporary)?;
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let deletion = Deletion {
        version: 1,
        session: info.binding.session,
        epoch: info.binding.epoch,
        accounted_bytes: budget::closed_bytes(info),
    };
    file.write_all(&serde_json::to_vec(&deletion)?)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temporary, root.join(MARKER))?;
    sync_directory(root)
}

pub(super) fn erase(root: &Path, info: &ArchiveInfo) -> Result<(), ArchiveError> {
    let source = budget::expected(root, info.binding);
    validate(&source)?;
    let payloads = PayloadStore::open(&source.join("payloads"))?;
    if ArchiveInfo::read(&source)?.binding != info.binding {
        return Err(ArchiveError::SessionMismatch);
    }
    write_marker(&source, info)?;
    let destination = trash(root, info.binding);
    if destination.try_exists()? {
        return Err(ArchiveError::Busy);
    }
    // Windows can deny renaming a parent while its lock handle is open. The
    // committed marker now prevents every ArchiveIndex opener from proceeding.
    drop(payloads);
    std::fs::rename(&source, &destination)?;
    sync_directory(root)?;
    remove_trash(&destination, info.binding)?;
    let session = root.join(info.binding.session.to_string());
    match std::fs::remove_dir(session) {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::NotFound
            ) => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn remove_trash(path: &Path, binding: SessionBinding) -> Result<(), ArchiveError> {
    ordinary(path, true)?;
    if std::fs::read_dir(path)?.next().is_none() {
        std::fs::remove_dir(path)?;
        return Ok(());
    }
    marker(path, binding)?;
    validate(path)?;
    // The marker is removed last: interruption or a failed unlink remains charged.
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_name() == MARKER {
            continue;
        }
        if entry.file_name() == "payloads" {
            for payload in std::fs::read_dir(entry.path())? {
                std::fs::remove_file(payload?.path())?;
            }
            std::fs::remove_dir(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    sync_directory(path)?;
    std::fs::remove_file(path.join(MARKER))?;
    sync_directory(path)?;
    std::fs::remove_dir(path)?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

/// Retry interrupted cleanup. Failed tombstones remain fully charged.
#[cfg(test)]
fn recover(root: &Path) -> Result<(u64, usize), ArchiveError> {
    recover_except(root, None)
}

pub(super) fn recover_except(
    root: &Path,
    active: Option<SessionBinding>,
) -> Result<(u64, usize), ArchiveError> {
    if !root.try_exists()? {
        return Ok((0, 0));
    }
    let mut bytes = 0_u64;
    let mut failures = 0;
    // A crash can leave a committed marker before the original directory rename.
    for session in std::fs::read_dir(root)? {
        let session = session?;
        if !session.file_type()?.is_dir() || canonical(&session.file_name()).is_none() {
            continue;
        }
        for epoch in std::fs::read_dir(session.path())? {
            let epoch = epoch?;
            if !epoch.file_type()?.is_dir() || !epoch.path().join(MARKER).try_exists()? {
                continue;
            }
            let info = ArchiveInfo::read(&epoch.path())?;
            if active == Some(info.binding) {
                continue;
            }
            if expected_binding(&session.file_name(), &epoch.file_name()) != Some(info.binding) {
                return Err(ArchiveError::SessionMismatch);
            }
            marker(&epoch.path(), info.binding)?;
            // Failed staging remains in the normal scan and is still charged.
            if erase(root, &info).is_err() {
                failures += 1;
            }
        }
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let Some(binding) = trash_binding(&entry.file_name()) else {
            continue;
        };
        ordinary(&entry.path(), true)?;
        if std::fs::read_dir(entry.path())?.next().is_none() {
            std::fs::remove_dir(entry.path())?;
            continue;
        }
        let marker = marker(&entry.path(), binding)?;
        let allocated = validate(&entry.path())?;
        if remove_trash(&entry.path(), binding).is_err() {
            bytes = bytes.saturating_add(marker.accounted_bytes.max(allocated));
            failures += 1;
        } else {
            // This marker authenticated the parent session, so its now-empty
            // directory is known to belong to the completed cleanup.
            match std::fs::remove_dir(root.join(binding.session.to_string())) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::NotFound
                    ) => {}
                Err(_) => failures += 1,
            }
        }
    }
    for session in std::fs::read_dir(root)? {
        let session = session?;
        if !session.file_type()?.is_dir() || canonical(&session.file_name()).is_none() {
            continue;
        }
        if active.is_some_and(|binding| {
            session.file_name() == std::ffi::OsStr::new(&binding.session.to_string())
        }) {
            continue;
        }
        if std::fs::read_dir(session.path())?.next().is_none() {
            // Preserve a directory with no remaining authenticated identity and
            // charge it; this also covers a failed final parent-directory unlink.
            bytes = bytes.saturating_add(8192);
        }
    }
    Ok((bytes, failures))
}

fn canonical(name: &std::ffi::OsStr) -> Option<Uuid> {
    let text = name.to_str()?;
    let id: Uuid = text.parse().ok()?;
    (!id.is_nil() && id.to_string() == text).then_some(id)
}

fn expected_binding(session: &std::ffi::OsStr, epoch: &std::ffi::OsStr) -> Option<SessionBinding> {
    let session_text = session.to_str()?;
    let epoch_text = epoch.to_str()?;
    let session: Uuid = session_text.parse().ok()?;
    let epoch: Uuid = epoch_text.parse().ok()?;
    (session.to_string() == session_text && epoch.to_string() == epoch_text)
        .then_some(SessionBinding { session, epoch })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{ArchiveIdentity, ArchiveIndex, ArchiveLimits};
    use bytes::Bytes;
    use liazo_core::{
        protocol::{ImageFormat, SourceKey},
        wire::ImageMeta,
    };
    struct Fixture {
        root: tempfile::TempDir,
        path: PathBuf,
        info: ArchiveInfo,
        payload: Uuid,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let identity = ArchiveIdentity {
                binding: SessionBinding {
                    session: Uuid::new_v4(),
                    epoch: Uuid::new_v4(),
                },
                provider: liazo_transport::Identity::generate().unwrap().fingerprint(),
                wall_anchor_unix_ms: 1_700_000_000_000,
            };
            let path = budget::expected(root.path(), identity.binding);
            let mut index = ArchiveIndex::open(
                &path,
                identity,
                ArchiveLimits {
                    duration_ms: 60_000,
                    max_bytes: 4 * 1024 * 1024,
                },
            )
            .unwrap();
            let payload = Uuid::new_v4();
            index
                .record_image(
                    ImageMeta {
                        source: SourceKey {
                            id: "display:cleanup".into(),
                            generation: Uuid::new_v4(),
                        },
                        sequence: 1,
                        configuration_revision: 1,
                        capture_started_ns: 1,
                        captured_ns: 2,
                        width: 1,
                        height: 1,
                        format: ImageFormat::Png,
                        fingerprint: [1; 32],
                    },
                    payload,
                    Bytes::from_static(b"retained media"),
                    10_000_000,
                    1_000_000_000,
                )
                .unwrap();
            index.finish(20_000_000).unwrap();
            drop(index);
            let info = ArchiveInfo::read(&path).unwrap();
            Self {
                root,
                path,
                info,
                payload,
            }
        }
        fn mark(&self) {
            let owner = PayloadStore::open(&self.path.join("payloads")).unwrap();
            write_marker(&self.path, &self.info).unwrap();
            drop(owner);
        }
        fn stage(&self) -> PathBuf {
            self.mark();
            let path = trash(self.root.path(), self.info.binding);
            std::fs::rename(&self.path, &path).unwrap();
            path
        }
    }

    #[test]
    fn closed_epoch_deletion_releases_locks_before_renaming_and_removes_the_directory() {
        let fixture = Fixture::new();
        erase(fixture.root.path(), &fixture.info).unwrap();
        assert!(!fixture.path.exists());
        assert!(!trash(fixture.root.path(), fixture.info.binding).exists());
        assert!(
            !fixture
                .root
                .path()
                .join(fixture.info.binding.session.to_string())
                .exists()
        );
    }

    #[test]
    fn a_committed_marker_rejects_reopening_and_recovers_a_crash_before_rename() {
        let fixture = Fixture::new();
        fixture.mark();
        assert!(matches!(
            ArchiveIndex::open_existing(&fixture.path),
            Err(ArchiveError::NotFound)
        ));
        assert_eq!(recover(fixture.root.path()).unwrap(), (0, 0));
        assert!(!fixture.path.exists());
    }

    #[test]
    fn interrupted_payload_deletion_recovers_without_needing_the_removed_index() {
        let fixture = Fixture::new();
        let staged = fixture.stage();
        std::fs::remove_file(staged.join("index.sqlite3")).unwrap();
        assert!(staged.join(MARKER).exists());
        assert_eq!(recover(fixture.root.path()).unwrap(), (0, 0));
        assert!(!staged.exists());
    }

    #[test]
    fn unknown_files_are_preserved_before_any_deletion_starts() {
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("keep.txt"), b"keep me").unwrap();
        assert!(matches!(
            erase(fixture.root.path(), &fixture.info),
            Err(ArchiveError::InvalidPayload)
        ));
        assert!(fixture.path.join("index.sqlite3").exists());
        assert!(!fixture.path.join(MARKER).exists());
        assert_eq!(
            std::fs::read(fixture.path.join("keep.txt")).unwrap(),
            b"keep me"
        );
    }

    #[cfg(unix)]
    #[test]
    fn payload_symlinks_never_cause_their_target_to_be_deleted() {
        let fixture = Fixture::new();
        let target = fixture.root.path().join("outside");
        std::fs::write(&target, b"outside media").unwrap();
        std::os::unix::fs::symlink(
            &target,
            fixture
                .path
                .join("payloads")
                .join(format!("{}.bin", Uuid::new_v4())),
        )
        .unwrap();
        assert!(matches!(
            erase(fixture.root.path(), &fixture.info),
            Err(ArchiveError::InvalidPayload)
        ));
        assert_eq!(std::fs::read(target).unwrap(), b"outside media");
        assert!(!fixture.path.join(MARKER).exists());
    }

    #[cfg(unix)]
    struct DeleteOutage {
        path: PathBuf,
        permissions: std::fs::Permissions,
    }
    #[cfg(unix)]
    impl DeleteOutage {
        fn new(path: &Path, _payload: Uuid) -> Self {
            use std::os::unix::fs::PermissionsExt;
            let path = path.join("payloads");
            let permissions = std::fs::metadata(&path).unwrap().permissions();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o500)).unwrap();
            Self { path, permissions }
        }
    }
    #[cfg(unix)]
    impl Drop for DeleteOutage {
        fn drop(&mut self) {
            std::fs::set_permissions(&self.path, self.permissions.clone()).unwrap();
        }
    }
    #[cfg(windows)]
    struct DeleteOutage {
        _held: File,
    }
    #[cfg(windows)]
    impl DeleteOutage {
        fn new(path: &Path, payload: Uuid) -> Self {
            use std::os::windows::fs::OpenOptionsExt;
            // A real Windows handle that permits reads/writes but forbids deletion.
            let held = OpenOptions::new()
                .read(true)
                .share_mode(3)
                .open(path.join("payloads").join(format!("{payload}.bin")))
                .unwrap();
            Self { _held: held }
        }
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn failed_unlinks_keep_the_marker_and_full_charge_until_the_bytes_are_removed() {
        let fixture = Fixture::new();
        let staged = fixture.stage();
        let outage = DeleteOutage::new(&staged, fixture.payload);
        assert!(remove_trash(&staged, fixture.info.binding).is_err());
        assert!(staged.join(MARKER).exists());
        let charge = marker(&staged, fixture.info.binding)
            .unwrap()
            .accounted_bytes;
        assert_eq!(recover(fixture.root.path()).unwrap(), (charge + 8192, 1));
        assert!(staged.exists());
        drop(outage);
        assert_eq!(recover(fixture.root.path()).unwrap(), (0, 0));
        assert!(!staged.exists());
    }
}
