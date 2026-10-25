use liazo_runtime::archive::{
    ArchiveError, ArchiveIdentity, ArchiveIndex, ArchiveInfo, ArchiveLimits,
};
use liazo_transport::{Identity, SessionBinding};
use uuid::Uuid;

#[test]
fn existing_archives_preserve_their_identity_and_limits_without_becoming_new_sessions() {
    let root = tempfile::tempdir().unwrap();
    assert!(matches!(
        ArchiveInfo::read(root.path()),
        Err(ArchiveError::NotFound)
    ));
    assert!(!root.path().join("index.sqlite3").exists());
    let identity = ArchiveIdentity {
        binding: SessionBinding {
            session: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        },
        provider: Identity::generate().unwrap().fingerprint(),
        wall_anchor_unix_ms: 1_700_000_000_000,
    };
    let limits = ArchiveLimits {
        duration_ms: 30_000,
        max_bytes: 4 * 1024 * 1024,
    };
    let mut archive = ArchiveIndex::open(root.path(), identity, limits).unwrap();
    archive.begin().unwrap();
    let live = ArchiveInfo::read(root.path()).unwrap();
    assert!(live.writer_active);
    assert_eq!(live.binding, identity.binding);
    assert!(matches!(
        ArchiveIndex::open_existing(root.path()),
        Err(ArchiveError::Busy)
    ));
    archive.finish(123_000_000).unwrap();
    drop(archive);
    let info = ArchiveInfo::read(root.path()).unwrap();
    assert!(!info.writer_active);
    assert_eq!(info.closed_at_ns, Some(123_000_000));
    assert_eq!(info.limits.duration_ms, 30_000);
    assert!(info.accounted_bytes < limits.max_bytes);
    let archive = ArchiveIndex::open_existing(root.path()).unwrap();
    drop(archive);
    let after = ArchiveInfo::read(root.path()).unwrap();
    assert_eq!(after.closed_at_ns, info.closed_at_ns);
    assert_eq!(after.provider, info.provider);
    assert_eq!(after.limits.max_bytes, limits.max_bytes);
}
