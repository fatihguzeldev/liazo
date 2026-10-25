use bytes::Bytes;
use liazo_core::{
    protocol::{ImageFormat, SourceKey},
    wire::ImageMeta,
};
use liazo_runtime::history::{GapReason, HistoryLimits, HistoryState, RamHistory};
use std::time::Duration;
use uuid::Uuid;

fn source() -> SourceKey {
    SourceKey {
        id: "display:test".into(),
        generation: Uuid::from_u128(1),
    }
}
fn meta(sequence: u64) -> ImageMeta {
    ImageMeta {
        source: source(),
        sequence,
        captured_ns: sequence * 1_000_000,
        capture_started_ns: sequence * 1_000_000,
        configuration_revision: 1,
        width: 1,
        height: 1,
        format: ImageFormat::Png,
        fingerprint: [1; 32],
    }
}
fn limits(bytes: u64) -> HistoryLimits {
    HistoryLimits {
        duration_ms: 1000,
        max_bytes: bytes,
    }
}

#[test]
fn unchanged_images_keep_one_payload_and_recent_coverage_survives_old_history_expiration() {
    let mut history = RamHistory::new(limits(16_384)).unwrap();
    let id = Uuid::new_v4();
    let payload = Bytes::from(vec![7; 4000]);
    for sequence in 1..=100 {
        history.image(
            meta(sequence),
            id,
            payload.clone(),
            sequence * 100_000_000,
            Duration::from_millis(300),
        );
    }
    assert_eq!(history.stats().payloads, 1);
    assert_eq!(history.stats().intervals, 1);
    let recent = history.at(9_500_000_000, 10_000_000_000, &[source()]);
    assert_eq!(recent.images[0].state, HistoryState::Available);
    assert_eq!(
        recent.images[0].payload.as_ref().unwrap().as_ref(),
        payload.as_ref()
    );
    assert_eq!(
        history
            .at(1_000_000_000, 10_000_000_000, &[source()])
            .images[0]
            .state,
        HistoryState::Expired
    );
    history
        .configure(
            HistoryLimits {
                duration_ms: 20_000,
                max_bytes: 16_384,
            },
            10_000_000_000,
        )
        .unwrap();
    assert_eq!(
        history
            .at(1_000_000_000, 10_000_000_000, &[source()])
            .images[0]
            .state,
        HistoryState::Expired
    );
}

#[test]
fn a_disconnect_is_a_gap_and_an_unchanged_image_cannot_bridge_it() {
    let mut history = RamHistory::new(limits(16_384)).unwrap();
    let id = Uuid::new_v4();
    let payload = Bytes::from_static(b"fixture");
    history.image(
        meta(1),
        id,
        payload.clone(),
        100_000_000,
        Duration::from_millis(300),
    );
    history.gap(source(), 200_000_000, GapReason::Disconnected);
    history.image(
        meta(2),
        id,
        payload,
        800_000_000,
        Duration::from_millis(300),
    );
    let missing = history.at(500_000_000, 900_000_000, &[source()]);
    assert_eq!(
        missing.images[0].state,
        HistoryState::Gap(GapReason::Disconnected)
    );
    assert!(missing.images[0].payload.is_none());
    assert_eq!(history.stats().intervals, 2);
    assert_eq!(history.stats().payloads, 1);
    assert_eq!(
        history.at(800_000_000, 900_000_000, &[source()]).images[0].state,
        HistoryState::Available
    );
}

#[test]
fn byte_pressure_and_silence_cannot_return_fictitious_or_unbounded_history() {
    let mut history = RamHistory::new(limits(8192)).unwrap();
    for sequence in 1..=100 {
        history.image(
            meta(sequence),
            Uuid::new_v4(),
            Bytes::from(vec![7; 3000]),
            sequence * 1_000_000,
            Duration::from_millis(5),
        );
    }
    assert!(history.stats().accounted_bytes <= 8192);
    assert!(history.stats().payloads <= 2);
    assert_eq!(
        history.at(1_000_000, 100_000_000, &[source()]).images[0].state,
        HistoryState::NotRetained
    );
    assert_eq!(
        history.at(200_000_000, 300_000_000, &[source()]).images[0].state,
        HistoryState::Gap(GapReason::NoData)
    );
    history.trim(2_000_000_000);
    assert_eq!(history.stats().payloads, 0);
    let mut recycled = source();
    recycled.generation = Uuid::new_v4();
    assert_eq!(
        history.at(2_000_000_000, 2_000_000_000, &[recycled]).images[0].state,
        HistoryState::NotObserved
    );
}
