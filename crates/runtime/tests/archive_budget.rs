use bytes::Bytes;
use liazo_core::{
    protocol::{ImageFormat, SourceKey},
    wire::ImageMeta,
};
use liazo_runtime::archive::{
    ArchiveCatalog, ArchiveError, ArchiveIdentity, ArchiveIndex, ArchiveInfo, ArchiveLimits,
};
use liazo_transport::{Identity, SessionBinding};
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const CAP: u64 = 4 * 1024 * 1024;
fn limits() -> ArchiveLimits {
    ArchiveLimits {
        duration_ms: 60_000,
        max_bytes: CAP,
    }
}
fn identity() -> ArchiveIdentity {
    ArchiveIdentity {
        binding: SessionBinding {
            session: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        },
        provider: Identity::generate().unwrap().fingerprint(),
        wall_anchor_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    }
}
fn path(root: &Path, identity: ArchiveIdentity) -> PathBuf {
    root.join(identity.binding.session.to_string())
        .join(identity.binding.epoch.to_string())
}
fn image(index: &mut ArchiveIndex, bytes: usize, at: u64) {
    index
        .record_image(
            ImageMeta {
                source: SourceKey {
                    id: "display:budget".into(),
                    generation: Uuid::from_u128(1),
                },
                sequence: at,
                configuration_revision: 1,
                capture_started_ns: at,
                captured_ns: at + 1,
                width: 1,
                height: 1,
                format: ImageFormat::Png,
                fingerprint: [at as u8; 32],
            },
            Uuid::new_v4(),
            Bytes::from(vec![0; bytes]),
            at,
            1_000_000_000,
        )
        .unwrap();
}
fn closed(root: &Path, identity: ArchiveIdentity, length: usize) -> PathBuf {
    let path = path(root, identity);
    let mut index = ArchiveIndex::open(&path, identity, limits()).unwrap();
    index.begin().unwrap();
    image(&mut index, length, 10_000_000);
    index.finish(20_000_000).unwrap();
    path
}

#[tokio::test]
async fn admission_evicts_old_closed_epochs_before_writing_new_media() {
    let root = tempfile::tempdir().unwrap();
    let mut first = identity();
    first.wall_anchor_unix_ms -= 1000;
    let second = identity();
    let old = closed(root.path(), first, 900_000);
    let retained = closed(root.path(), second, 900_000);
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let held = catalog
        .image(
            first.binding,
            SourceKey {
                id: "display:budget".into(),
                generation: Uuid::from_u128(1),
            },
            10_000_000,
        )
        .await
        .unwrap()
        .payload
        .unwrap();
    let budget = catalog.budget(CAP).unwrap();
    let current = identity();
    let active_path = path(root.path(), current);
    let mut active =
        ArchiveIndex::open_with_budget(&active_path, current, limits(), Some(budget)).unwrap();
    active.begin().unwrap();
    assert!(old.exists());
    assert!(retained.exists());
    image(&mut active, 1_100_000, 30_000_000);
    assert!(
        !old.exists(),
        "the oldest closed epoch must be reclaimed before the new write"
    );
    assert!(retained.exists());
    assert_eq!(
        held.as_ref(),
        vec![0; 900_000].as_slice(),
        "owned read bytes remain usable after their file is evicted"
    );
    let closed_bytes = ArchiveInfo::read(&retained).unwrap().accounted_bytes + 8192;
    assert!(active.stats().unwrap().accounted_bytes + closed_bytes + 64 * 1024 <= CAP);
    assert_eq!(catalog.list(0).await.unwrap().entries.len(), 2);
    active.finish(40_000_000).unwrap();
    drop(active);
    assert!(
        catalog
            .sources(current.binding, None)
            .await
            .unwrap()
            .entries[0]
            .image
    );
}

#[tokio::test]
async fn writer_ownership_and_unknown_files_fail_closed_without_deleting_user_files() {
    let root = tempfile::tempdir().unwrap();
    let first = identity();
    let closed = closed(root.path(), first, 900_000);
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let active = identity();
    let mut writer = ArchiveIndex::open_with_budget(
        &path(root.path(), active),
        active,
        limits(),
        Some(catalog.budget(CAP).unwrap()),
    )
    .unwrap();
    writer.begin().unwrap();
    let other = identity();
    assert!(matches!(
        ArchiveIndex::open_with_budget(
            &path(root.path(), other),
            other,
            limits(),
            Some(catalog.budget(CAP).unwrap())
        ),
        Err(ArchiveError::Busy)
    ));
    writer.finish(1).unwrap();
    drop(writer);
    std::fs::write(closed.join("keep.txt"), b"user-owned content").unwrap();
    assert!(matches!(
        ArchiveIndex::open_with_budget(
            &path(root.path(), other),
            other,
            limits(),
            Some(catalog.budget(CAP).unwrap())
        ),
        Err(ArchiveError::InvalidPayload)
    ));
    assert_eq!(
        std::fs::read(closed.join("keep.txt")).unwrap(),
        b"user-owned content"
    );
    assert!(closed.join("index.sqlite3").exists());
}

#[tokio::test]
async fn an_unmanaged_live_writer_is_not_counted_as_a_closed_fixed_allocation() {
    let root = tempfile::tempdir().unwrap();
    let existing = identity();
    let mut writer = ArchiveIndex::open(&path(root.path(), existing), existing, limits()).unwrap();
    writer.begin().unwrap();
    image(&mut writer, 400_000, 1);
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let candidate = identity();
    assert!(matches!(
        ArchiveIndex::open_with_budget(
            &path(root.path(), candidate),
            candidate,
            limits(),
            Some(catalog.budget(CAP).unwrap())
        ),
        Err(ArchiveError::Busy)
    ));
    writer.finish(2).unwrap();
    drop(writer);
    assert!(
        ArchiveIndex::open_with_budget(
            &path(root.path(), candidate),
            candidate,
            limits(),
            Some(catalog.budget(CAP).unwrap())
        )
        .is_ok()
    );
}

#[tokio::test]
async fn a_second_catalog_cannot_admit_a_concurrent_profile_writer() {
    let root = tempfile::tempdir().unwrap();
    let first = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let second = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let a = identity();
    let b = identity();
    let writer = ArchiveIndex::open_with_budget(
        &path(root.path(), a),
        a,
        limits(),
        Some(first.budget(CAP).unwrap()),
    )
    .unwrap();
    assert!(matches!(
        ArchiveIndex::open_with_budget(
            &path(root.path(), b),
            b,
            limits(),
            Some(second.budget(CAP).unwrap())
        ),
        Err(ArchiveError::Busy)
    ));
    drop(writer);
    drop(first);
    assert!(
        ArchiveIndex::open_with_budget(
            &path(root.path(), b),
            b,
            limits(),
            Some(second.budget(CAP).unwrap())
        )
        .is_ok()
    );
}

#[tokio::test]
async fn a_restarted_catalog_preserves_the_selected_profile_limit() {
    let root = tempfile::tempdir().unwrap();
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let initial = identity();
    let mut writer = ArchiveIndex::open_with_budget(
        &path(root.path(), initial),
        initial,
        limits(),
        Some(catalog.budget(CAP).unwrap()),
    )
    .unwrap();
    writer.finish(1).unwrap();
    drop(writer);
    drop(catalog);
    let mut entries = vec![];
    // Seed retained epochs as if they had survived the previous process exit.
    for number in 0..5 {
        let mut epoch = identity();
        epoch.wall_anchor_unix_ms -= (5 - number) * 1000;
        entries.push((epoch, closed(root.path(), epoch, 550_000)));
    }
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let latest = entries.last().unwrap().0.binding;
    catalog.sources(latest, None).await.unwrap();
    assert!(
        !entries[0].1.exists(),
        "the saved 4-MiB policy must constrain read/recovery work after restart"
    );
    let retained = catalog.list(0).await.unwrap().entries;
    assert!(
        retained
            .iter()
            .map(|info| info.accounted_bytes + 8192)
            .sum::<u64>()
            + 64 * 1024
            <= CAP
    );
}
