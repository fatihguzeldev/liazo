use bytes::Bytes;
use liazo_core::{
    protocol::{ImageFormat, SourceKey},
    wire::{Frame, Header, ImageMeta},
};
use liazo_runtime::images::{LatestImages, SentImages};
use uuid::Uuid;

fn key() -> SourceKey {
    SourceKey {
        id: "display:1".into(),
        generation: Uuid::from_u128(1),
    }
}
fn image(seq: u64, hash: u8, bytes: &'static [u8]) -> Frame {
    let meta = ImageMeta {
        source: key(),
        sequence: seq,
        captured_ns: seq,
        capture_started_ns: seq,
        configuration_revision: 1,
        width: 1,
        height: 1,
        format: ImageFormat::Png,
        fingerprint: [hash; 32],
    };
    Frame::new(
        if bytes.is_empty() {
            Header::Unchanged(meta)
        } else {
            Header::Image(meta)
        },
        Bytes::from_static(bytes),
    )
    .unwrap()
}

#[tokio::test]
async fn duplicate_after_an_unsent_image_still_delivers_the_complete_current_payload() {
    let images = LatestImages::new(100);
    images.select(&[key()]).unwrap();
    images.publish(image(1, 1, b"a")).unwrap();
    images.publish(image(2, 2, b"b")).unwrap();
    images.publish(image(3, 2, b"")).unwrap();
    let mut sent = SentImages::default();
    let newest = images.next(&mut sent).await;
    assert!(matches!(
        newest.header,
        Header::Image(ImageMeta { sequence: 3, .. })
    ));
    assert_eq!(newest.payload.as_ref(), b"b");
    images.publish(image(4, 2, b"")).unwrap();
    assert!(matches!(
        images.next(&mut sent).await.header,
        Header::Unchanged(_)
    ));
}

#[tokio::test]
async fn reselecting_a_source_forces_a_complete_image_even_with_a_reset_sequence() {
    let images = LatestImages::new(100);
    let mut sent = SentImages::default();
    images.select(&[key()]).unwrap();
    images.publish(image(42, 1, b"a")).unwrap();
    images.next(&mut sent).await;
    images.select(&[]).unwrap();
    images.select(&[key()]).unwrap();
    images.publish(image(1, 1, b"a")).unwrap();
    assert!(matches!(
        images.next(&mut sent).await.header,
        Header::Image(_)
    ));
    assert_eq!(images.bytes_held(), 1);
}

#[test]
fn a_budget_failure_preserves_the_previous_complete_image() {
    let images = LatestImages::new(1);
    images.select(&[key()]).unwrap();
    images.publish(image(1, 1, b"a")).unwrap();
    assert!(images.publish(image(2, 2, b"large")).is_err());
    assert_eq!(images.bytes_held(), 1);
    images.select(&[]).unwrap();
    assert_eq!(images.bytes_held(), 0);
}
