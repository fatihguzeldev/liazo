use bytes::Bytes;
use liazo_core::{
    protocol::{ImageFormat, SourceKey},
    wire::ImageMeta,
};
use liazo_runtime::{
    archive::{ArchiveCatalog, ArchiveError, ArchiveIdentity, ArchiveIndex, ArchiveLimits},
    history::{GapReason, HistoryState},
};
use liazo_transport::{Identity, SessionBinding};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn identity(session: Uuid) -> ArchiveIdentity {
    ArchiveIdentity {
        binding: SessionBinding {
            session,
            epoch: Uuid::new_v4(),
        },
        provider: Identity::generate().unwrap().fingerprint(),
        wall_anchor_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    }
}
fn source() -> SourceKey {
    SourceKey {
        id: "display:retained".into(),
        generation: Uuid::from_u128(1),
    }
}
fn image() -> ImageMeta {
    ImageMeta {
        source: source(),
        sequence: 1,
        configuration_revision: 1,
        capture_started_ns: 1,
        captured_ns: 2,
        width: 1,
        height: 1,
        format: ImageFormat::Png,
        fingerprint: [1; 32],
    }
}
fn write(root: &std::path::Path, identity: ArchiveIdentity, bytes: &'static [u8]) -> ArchiveIndex {
    let path = root
        .join(identity.binding.session.to_string())
        .join(identity.binding.epoch.to_string());
    let mut index = ArchiveIndex::open(&path, identity, ArchiveLimits::default()).unwrap();
    index.begin().unwrap();
    index
        .record_image(
            image(),
            Uuid::new_v4(),
            Bytes::from_static(bytes),
            100_000_000,
            1_000_000_000,
        )
        .unwrap();
    index
}

#[tokio::test]
async fn old_epochs_remain_distinct_and_queryable_after_a_catalog_restart() {
    let root = tempfile::tempdir().unwrap();
    let session = Uuid::new_v4();
    let first = identity(session);
    let mut second = identity(session);
    second.provider = first.provider;
    let mut index = write(root.path(), first, b"first epoch");
    index.finish(200_000_000).unwrap();
    drop(index);
    let mut index = write(root.path(), second, b"second epoch");
    index.finish(200_000_000).unwrap();
    drop(index);
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let entries = catalog.list(0).await.unwrap();
    assert_eq!(entries.entries.len(), 2);
    assert!(!entries.truncated);
    let sources = catalog.sources(first.binding, None).await.unwrap();
    assert_eq!(sources.entries[0].key, source());
    assert!(sources.entries[0].image);
    assert!(!sources.entries[0].audio);
    let old = catalog
        .image(first.binding, source(), 150_000_000)
        .await
        .unwrap();
    let new = catalog
        .image(second.binding, source(), 150_000_000)
        .await
        .unwrap();
    assert_eq!(old.payload.unwrap().as_ref(), b"first epoch");
    assert_eq!(new.payload.unwrap().as_ref(), b"second epoch");
    assert_eq!(
        catalog
            .image(first.binding, source(), 300_000_000)
            .await
            .unwrap()
            .state,
        HistoryState::Gap(GapReason::ArchiveStopped)
    );
    drop(catalog);
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    assert_eq!(
        catalog
            .image(first.binding, source(), 150_000_000)
            .await
            .unwrap()
            .payload
            .unwrap()
            .as_ref(),
        b"first epoch"
    );
}

#[tokio::test]
async fn a_live_writer_is_not_recovered_or_read_by_a_second_archive_owner() {
    let root = tempfile::tempdir().unwrap();
    let identity = identity(Uuid::new_v4());
    let mut writer = write(root.path(), identity, b"live epoch");
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    assert!(catalog.list(0).await.unwrap().entries[0].writer_active);
    assert!(matches!(
        catalog.image(identity.binding, source(), 100_000_000).await,
        Err(ArchiveError::Busy)
    ));
    writer.finish(200_000_000).unwrap();
    drop(writer);
    assert!(
        catalog
            .image(identity.binding, source(), 100_000_000)
            .await
            .unwrap()
            .payload
            .is_some()
    );
}

#[tokio::test]
async fn retained_source_pages_can_reach_sources_after_the_first_page() {
    let root = tempfile::tempdir().unwrap();
    let identity = identity(Uuid::new_v4());
    let path = root
        .path()
        .join(identity.binding.session.to_string())
        .join(identity.binding.epoch.to_string());
    let mut index = ArchiveIndex::open(&path, identity, ArchiveLimits::default()).unwrap();
    let payload = Uuid::new_v4();
    for number in 0..257 {
        let mut metadata = image();
        metadata.source.id = format!("display:{number:04}");
        index
            .record_image(
                metadata,
                payload,
                Bytes::from_static(b"shared bytes"),
                100_000_000,
                1_000_000_000,
            )
            .unwrap();
    }
    index.finish(200_000_000).unwrap();
    drop(index);
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let first = catalog.sources(identity.binding, None).await.unwrap();
    assert_eq!(first.entries.len(), 256);
    assert!(first.truncated);
    let last = catalog
        .sources(identity.binding, first.next_after)
        .await
        .unwrap();
    assert_eq!(last.entries.len(), 1);
    assert_eq!(last.entries[0].key.id, "display:0256");
    assert!(!last.truncated);
    assert!(last.next_after.is_none());
}

#[tokio::test]
async fn a_directory_cannot_relabel_an_archive_as_a_different_epoch() {
    let root = tempfile::tempdir().unwrap();
    let identity = identity(Uuid::new_v4());
    let mut writer = write(root.path(), identity, b"real epoch");
    writer.finish(200_000_000).unwrap();
    drop(writer);
    let renamed = SessionBinding {
        session: identity.binding.session,
        epoch: Uuid::new_v4(),
    };
    let session = root.path().join(identity.binding.session.to_string());
    std::fs::rename(
        session.join(identity.binding.epoch.to_string()),
        session.join(renamed.epoch.to_string()),
    )
    .unwrap();
    let catalog = ArchiveCatalog::spawn(root.path().to_owned()).unwrap();
    let page = catalog.list(0).await.unwrap();
    assert!(page.entries.is_empty());
    assert_eq!(page.unavailable, 1);
    assert!(matches!(
        catalog.image(renamed, source(), 100_000_000).await,
        Err(ArchiveError::SessionMismatch)
    ));
}
