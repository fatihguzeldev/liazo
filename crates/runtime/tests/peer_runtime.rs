use async_trait::async_trait;
use bytes::Bytes;
use liazo_core::{
    catalog::SourceSnapshot,
    control::{Failure, FailureCode},
    protocol::{
        CaptureSettings, ImageFormat, ImageSubscription, Source, SourceAvailability, SourceKey,
        SourceKind,
    },
    wire::{Frame, Header, ImageMeta},
};
use liazo_runtime::{
    backend::{AudioTrack, CaptureBackend},
    provider,
    receiver::ReceiverSession,
};
use liazo_transport::{Identity, LaneKind, PeerClient, PeerServer, PeerSession, SessionServer};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct Capture {
    permission_required: AtomicBool,
    clock: Instant,
    gate: Semaphore,
    started: Notify,
    calls: AtomicUsize,
    active: AtomicUsize,
}
fn source() -> SourceKey {
    SourceKey {
        id: "display:1".into(),
        generation: Uuid::from_u128(1),
    }
}
fn settings() -> CaptureSettings {
    CaptureSettings {
        images: vec![ImageSubscription {
            source: source(),
            interval_ms: 60_000,
            max_width: Some(800),
            crop: None,
            format: ImageFormat::Png,
            quality: 90,
            include_cursor: false,
        }],
        audio: vec![],
    }
}

#[async_trait]
impl CaptureBackend for Capture {
    fn platform(&self) -> &'static str {
        "fixture"
    }
    fn clock_ns(&self) -> u64 {
        self.clock.elapsed().as_nanos() as u64
    }
    async fn discover(&self) -> Result<SourceSnapshot, Failure> {
        Ok(SourceSnapshot {
            revision: 1,
            sources: vec![Source {
                key: source(),
                label: "fixture".into(),
                kind: SourceKind::Display,
                availability: if self.permission_required.load(Ordering::SeqCst) {
                    SourceAvailability::PermissionRequired
                } else {
                    SourceAvailability::Available
                },
                pixel_width: Some(1200),
                pixel_height: Some(800),
                scale: Some(1.0),
                process_id: None,
            }],
            issues: vec![],
        })
    }
    async fn image(
        &self,
        settings: &ImageSubscription,
        previous: Option<[u8; 32]>,
        sequence: u64,
        revision: u64,
    ) -> Result<Frame, Failure> {
        if self.permission_required.load(Ordering::SeqCst) {
            return Err(Failure::new(
                FailureCode::PermissionRequired,
                "fixture permission revoked",
            ));
        }
        struct Active<'a>(&'a AtomicUsize);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active(&self.active);
        let started = self.clock_ns();
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        let _permission = self.gate.acquire().await.unwrap();
        let meta = ImageMeta {
            source: settings.source.clone(),
            sequence,
            configuration_revision: revision,
            capture_started_ns: started,
            captured_ns: self.clock_ns(),
            width: 1,
            height: 1,
            format: ImageFormat::Png,
            fingerprint: [7; 32],
        };
        Ok(if previous.is_some() {
            Frame::new(Header::Unchanged(meta), Bytes::new()).unwrap()
        } else {
            Frame::new(Header::Image(meta), Bytes::from_static(b"fixture-image")).unwrap()
        })
    }
    async fn audio(&self, _: &SourceKey, _: Uuid) -> Result<Box<dyn AudioTrack>, Failure> {
        Err(Failure::new(
            FailureCode::Unavailable,
            "fixture has no audio",
        ))
    }
}

async fn pair(
    capture: Arc<Capture>,
) -> (
    ReceiverSession,
    tokio::task::JoinHandle<Result<(), liazo_runtime::RuntimeError>>,
) {
    let identity = Identity::generate().unwrap();
    let receiver = Identity::generate().unwrap();
    let server =
        SessionServer::new(PeerServer::new(&identity, &[receiver.certificate().clone()]).unwrap());
    let client = PeerClient::new(&receiver, identity.certificate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let provider = tokio::spawn(async move {
        let mut lanes = Vec::new();
        let mut binding = None;
        for expected in [LaneKind::Control, LaneKind::Images, LaneKind::Audio] {
            let (socket, _) = listener.accept().await.unwrap();
            let lane = server.accept(socket).await.unwrap();
            assert_eq!(lane.kind, expected);
            binding = Some(lane.binding);
            lanes.push(lane.connection);
        }
        let audio = lanes.pop().unwrap();
        let images = lanes.pop().unwrap();
        let control = lanes.pop().unwrap();
        provider::serve(
            capture,
            PeerSession {
                binding: binding.unwrap(),
                control,
                images,
                audio,
            },
            CancellationToken::new(),
        )
        .await
    });
    let session = ReceiverSession::connect(&client, address, Uuid::new_v4())
        .await
        .unwrap();
    (session, provider)
}

fn capture() -> Arc<Capture> {
    Arc::new(Capture {
        permission_required: AtomicBool::new(false),
        clock: Instant::now(),
        gate: Semaphore::new(0),
        started: Notify::new(),
        calls: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn blocked_capture_does_not_block_control_or_leave_a_worker_after_stop() {
    let capture = capture();
    let (session, provider) = pair(capture.clone()).await;
    session.configure(settings()).await.unwrap();
    timeout(Duration::from_secs(1), capture.started.notified())
        .await
        .unwrap();
    assert_eq!(session.sources().sources.len(), 1);
    timeout(Duration::from_secs(1), session.stop())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), provider)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(capture.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn fresh_capture_waits_for_a_capture_started_after_the_request_and_cursors_are_independent() {
    let capture = capture();
    let (session, provider) = pair(capture.clone()).await;
    session.configure(settings()).await.unwrap();
    capture.started.notified().await;
    let fresh = session.clone();
    let request = tokio::spawn(async move { fresh.capture_now(vec![source()]).await });
    tokio::task::yield_now().await;
    capture.gate.add_permits(1);
    let first = timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(capture.calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        first.images[0].payload.as_ref().unwrap().as_ref(),
        b"fixture-image"
    );
    let second_consumer = session.observe(0, false, &[], Duration::ZERO).await;
    assert!(second_consumer.images[0].payload.is_some());
    let same_consumer = session
        .observe(first.cursor, false, &[], Duration::ZERO)
        .await;
    assert!(same_consumer.images[0].payload.is_none());
    session.stop().await.unwrap();
    provider.await.unwrap().unwrap();
}

#[tokio::test]
async fn history_expires_in_the_background_while_the_current_live_image_stays_available() {
    use liazo_runtime::history::{GapReason, HistoryLimits, HistoryState};
    let capture = capture();
    capture.gate.add_permits(1);
    let (session, provider) = pair(capture).await;
    session.configure(settings()).await.unwrap();
    timeout(Duration::from_secs(1), async {
        let mut cursor = 0;
        loop {
            let observed = session
                .observe(cursor, false, &[], Duration::from_millis(100))
                .await;
            if !observed.images.is_empty() {
                break;
            }
            cursor = observed.cursor;
        }
    })
    .await
    .unwrap();
    let retained = session
        .history_at(Duration::ZERO, &[source()])
        .await
        .unwrap();
    assert!(matches!(
        retained.images[0].state,
        HistoryState::Available | HistoryState::Sampled
    ));
    assert_eq!(
        retained.images[0].payload.as_ref().unwrap().as_ref(),
        b"fixture-image"
    );
    assert!(
        session
            .history_at(Duration::ZERO, &[source(), source()])
            .await
            .is_err()
    );
    session
        .configure_history(HistoryLimits {
            duration_ms: 100,
            max_bytes: 4096,
        })
        .unwrap();
    timeout(Duration::from_secs(1), async {
        while session.history_stats().payloads != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let current = session.observe(0, true, &[], Duration::ZERO).await;
    assert_eq!(
        current.images[0].payload.as_ref().unwrap().as_ref(),
        b"fixture-image"
    );
    session.stop().await.unwrap();
    let stopped = session
        .history_at(Duration::ZERO, &[source()])
        .await
        .unwrap();
    assert_eq!(
        stopped.images[0].state,
        HistoryState::Gap(GapReason::Stopped)
    );
    provider.await.unwrap().unwrap();
}

#[tokio::test]
async fn permission_changes_close_history_and_fresh_capture_reports_the_source_error_promptly() {
    use liazo_runtime::{
        Freshness, RuntimeError,
        history::{GapReason, HistoryState},
    };
    let capture = capture();
    capture.gate.add_permits(1);
    let (session, provider) = pair(capture.clone()).await;
    session.configure(settings()).await.unwrap();
    timeout(Duration::from_secs(1), async {
        let mut cursor = 0;
        loop {
            let observed = session
                .observe(cursor, false, &[], Duration::from_millis(100))
                .await;
            if !observed.images.is_empty() {
                break;
            }
            cursor = observed.cursor;
        }
    })
    .await
    .unwrap();
    capture.permission_required.store(true, Ordering::SeqCst);
    timeout(Duration::from_secs(3), async {
        let mut cursor = 0;
        loop {
            let observed = session
                .observe(cursor, false, &[], Duration::from_millis(250))
                .await;
            if observed.images[0].freshness == Freshness::PermissionRequired {
                break;
            }
            cursor = observed.cursor;
        }
    })
    .await
    .unwrap();
    let history = session
        .history_at(Duration::ZERO, &[source()])
        .await
        .unwrap();
    assert_eq!(
        history.images[0].state,
        HistoryState::Gap(GapReason::PermissionRequired)
    );
    let error = timeout(Duration::from_secs(1), session.capture_now(vec![source()]))
        .await
        .unwrap();
    assert!(matches!(
        error,
        Err(RuntimeError::Failure(Failure {
            code: FailureCode::PermissionRequired,
            ..
        }))
    ));
    assert_eq!(
        session.observe(0, false, &[], Duration::ZERO).await.images[0].freshness,
        Freshness::PermissionRequired
    );
    session.stop().await.unwrap();
    provider.await.unwrap().unwrap();
}
