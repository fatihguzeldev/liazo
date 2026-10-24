#![cfg(target_os = "macos")]

use liazo_core::protocol::{ImageFormat, ImageSubscription, SourceKey};

fn require_send<T: Send>(_: T) {}

#[test]
fn native_requests_can_run_on_worker_threads() {
    let settings = ImageSubscription {
        source: SourceKey {
            id: "display:1".into(),
            generation: uuid::Uuid::from_u128(1),
        },
        interval_ms: 500,
        max_width: None,
        crop: None,
        format: ImageFormat::Png,
        quality: 90,
        include_cursor: false,
    };
    require_send(liazo_macos::sources());
    require_send(liazo_macos::screenshot(&settings, None));
    require_send(liazo_macos::AudioStream::start("audio:system"));
}
