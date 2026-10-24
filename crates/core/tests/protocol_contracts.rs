use liazo_core::protocol::*;
use uuid::Uuid;

fn source() -> SourceKey {
    SourceKey {
        id: "display:1".into(),
        generation: Uuid::from_u128(1),
    }
}

#[test]
fn audio_size_is_derived_from_samples_and_channels() {
    let mut audio = AudioMeta {
        stream_id: Uuid::from_u128(2),
        source: source(),
        sequence: 4,
        captured_ns: 10_000,
        sample_position: 1024,
        sample_rate: 48_000,
        channels: 2,
        frames: 512,
        discontinuity: false,
    };
    assert!(audio.validate_payload(4096).is_ok());
    assert!(audio.validate_payload(4095).is_err());
    audio.frames = 0;
    assert!(audio.validate_payload(0).is_err());
    audio.frames = 512;
    audio.channels = 255;
    assert!(audio.validate_payload(512 * 255 * 4).is_err());
    audio.channels = 2;
    audio.sample_position = u64::MAX;
    assert!(audio.validate_payload(4096).is_err());
}

#[test]
fn crop_is_in_source_pixels_and_cannot_escape_or_overflow() {
    let crop = CropPixels {
        x: 10,
        y: 20,
        width: 500,
        height: 300,
    };
    assert!(crop.validate_within(1920, 1080).is_ok());
    assert!(crop.validate_within(400, 1080).is_err());
    assert!(
        CropPixels {
            x: u32::MAX,
            ..crop
        }
        .validate_within(1920, 1080)
        .is_err()
    );
}

#[test]
fn configuration_rejects_duplicate_sources_and_invalid_limits() {
    let image = ImageSubscription {
        source: source(),
        interval_ms: 500,
        max_width: None,
        crop: None,
        format: ImageFormat::Png,
        quality: 90,
        include_cursor: false,
    };
    let mut settings = CaptureSettings {
        images: vec![image.clone()],
        audio: vec![],
    };
    assert!(settings.validate().is_ok());
    settings.images.push(image);
    assert!(settings.validate().is_err());
    settings.images.pop();
    settings.images[0].interval_ms = 0;
    assert!(settings.validate().is_err());
    settings.images[0].interval_ms = 500;
    settings.images[0].max_width = Some(0);
    assert!(settings.validate().is_err());
}

#[test]
fn source_generations_survive_a_wire_roundtrip() {
    let key = source();
    let encoded = serde_json::to_vec(&key).unwrap();
    let decoded: SourceKey = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, key);
    assert_ne!(
        decoded,
        SourceKey {
            generation: Uuid::from_u128(2),
            ..key
        }
    );
    assert!(
        SourceKey {
            id: "bad\0source".into(),
            generation: Uuid::nil()
        }
        .validate()
        .is_err()
    );
}
