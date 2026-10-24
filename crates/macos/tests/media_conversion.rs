#![cfg(target_os = "macos")]
use bytes::Bytes;
use liazo_core::{protocol::SourceKey, wire::Header};
use liazo_macos::NativePacket;
use serde_json::json;

fn key() -> SourceKey {
    SourceKey {
        id: "display:1".into(),
        generation: uuid::Uuid::from_u128(1),
    }
}

#[test]
fn native_image_timing_identity_and_payload_are_preserved() {
    let metadata = json!({
        "width": 10, "height": 20, "format": "png", "fingerprint": "00".repeat(32),
        "requested_ns": 100, "completed_ns": 200, "unchanged": false,
    });
    let frame = NativePacket {
        metadata,
        payload: Bytes::from_static(b"image"),
    }
    .into_image_frame(key(), 7, 4)
    .unwrap();
    assert_eq!(frame.payload.as_ref(), b"image");
    let Header::Image(image) = frame.header else {
        panic!("wrong frame kind")
    };
    assert_eq!(image.source, key());
    assert_eq!((image.sequence, image.configuration_revision), (7, 4));
    assert_eq!((image.capture_started_ns, image.captured_ns), (100, 200));
}

#[test]
fn malformed_native_images_are_not_published() {
    let frame = NativePacket {
        metadata: json!({"width": 1}),
        payload: Bytes::new(),
    };
    assert!(frame.into_image_frame(key(), 1, 1).is_err());
}

#[test]
fn native_audio_uses_actual_sample_format_and_source_clock() {
    let metadata = json!({
        "sequence": 2, "host_time_ns": 200, "sample_position": 100,
        "sample_rate": 48000.0, "channels": 2, "frames": 2, "discontinuity": true,
    });
    let frame = NativePacket {
        metadata,
        payload: Bytes::from(vec![0; 16]),
    }
    .into_audio_frame(key())
    .unwrap();
    let Header::Audio(audio) = frame.header else {
        panic!("wrong frame kind")
    };
    assert_eq!(
        (audio.channels, audio.frames, audio.sample_rate),
        (2, 2, 48_000)
    );
    assert_eq!(audio.captured_ns, 200);
    assert!(audio.discontinuity);
}
