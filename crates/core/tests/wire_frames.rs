use bytes::Bytes;
use liazo_core::{
    protocol::{AudioMeta, ImageFormat, SourceKey},
    wire::{Frame, Header, ImageMeta, WireError},
};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

fn key() -> SourceKey {
    SourceKey {
        id: "audio:system".into(),
        generation: Uuid::from_u128(1),
    }
}

#[tokio::test]
async fn binary_audio_roundtrips_without_serializing_samples_to_json() {
    let (mut sender, mut receiver) = tokio::io::duplex(8192);
    let meta = AudioMeta {
        source: key(),
        sequence: 3,
        captured_ns: 400,
        sample_position: 1024,
        sample_rate: 48_000,
        channels: 2,
        frames: 512,
        discontinuity: false,
    };
    let frame = Frame::new(Header::Audio(meta), Bytes::from(vec![1; 4096])).unwrap();
    frame.write_to(&mut sender).await.unwrap();
    drop(sender);
    let received = Frame::read_from(&mut receiver).await.unwrap().unwrap();
    assert_eq!(received.payload.len(), 4096);
    assert_eq!(received.payload[4095], 1);
    assert!(matches!(
        received.header,
        Header::Audio(AudioMeta { sequence: 3, .. })
    ));
    assert!(Frame::read_from(&mut receiver).await.unwrap().is_none());
}

#[tokio::test]
async fn oversized_lengths_are_rejected_before_waiting_for_or_allocating_the_body() {
    let (mut sender, mut receiver) = tokio::io::duplex(32);
    sender.write_all(b"LZ01").await.unwrap();
    sender.write_all(&2_u32.to_be_bytes()).await.unwrap();
    sender.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        Frame::read_from(&mut receiver),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(WireError::TooLarge)));
}

#[tokio::test]
async fn partial_header_is_not_mistaken_for_a_clean_disconnect() {
    let (mut sender, mut receiver) = tokio::io::duplex(32);
    sender.write_all(b"LZ0").await.unwrap();
    drop(sender);
    assert!(matches!(
        Frame::read_from(&mut receiver).await,
        Err(WireError::Io(_))
    ));
}

#[test]
fn control_messages_cannot_smuggle_a_binary_payload() {
    assert!(Frame::new(Header::Pong { nonce: 1 }, Bytes::from_static(b"extra")).is_err());
}

#[test]
fn unchanged_images_have_metadata_but_no_duplicate_payload() {
    let image = ImageMeta {
        source: key(),
        sequence: 1,
        captured_ns: 20_000,
        configuration_revision: 1,
        width: 10,
        height: 10,
        format: ImageFormat::Png,
        fingerprint: [0; 32],
    };
    assert!(Frame::new(Header::Unchanged(image.clone()), Bytes::new()).is_ok());
    assert!(Frame::new(Header::Image(image.clone()), Bytes::new()).is_err());
    assert!(Frame::new(Header::Unchanged(image), Bytes::from_static(b"extra")).is_err());
}
