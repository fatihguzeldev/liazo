use async_trait::async_trait;
use bytes::Bytes;
use liazo_core::{
    catalog::SourceSnapshot,
    control::{Failure, FailureCode},
    protocol::{
        AudioMeta, CaptureSettings, CropPixels, ImageFormat, ImageSubscription, Source,
        SourceAvailability, SourceKey, SourceKind,
    },
    wire::{Frame, Header, ImageMeta},
};
use liazo_host::{HostError, SessionHost};
use liazo_runtime::{
    ConnectionState,
    backend::{AudioTrack, CaptureBackend},
    provider,
};
use liazo_transport::{
    Identity, PeerServer, PeerSession, SessionBinding, SessionServer, StoredPeer,
};
use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn image_key(generation: Uuid) -> SourceKey {
    SourceKey {
        id: "display:fixture".into(),
        generation,
    }
}
fn audio_key() -> SourceKey {
    SourceKey {
        id: "audio:fixture".into(),
        generation: Uuid::from_u128(2),
    }
}
struct Capture {
    clock: Instant,
    generation: RwLock<Uuid>,
    calls: AtomicUsize,
    blocked: AtomicBool,
    entered: Notify,
    release: Notify,
    pcm: RwLock<Option<Arc<Vec<f32>>>>,
    gate: Arc<Semaphore>,
}
struct Audio {
    clock: Instant,
    stream: Uuid,
    sequence: u64,
    pcm: Option<Arc<Vec<f32>>>,
    gate: Arc<Semaphore>,
}
#[async_trait]
impl AudioTrack for Audio {
    async fn next(&mut self) -> Result<Option<Frame>, Failure> {
        if self.sequence == 0 && self.pcm.is_some() {
            self.gate.acquire().await.unwrap().forget();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        let start = self.sequence as usize * 160;
        let bytes = self
            .pcm
            .as_ref()
            .map(|pcm| {
                (start..start + 160)
                    .flat_map(|index| pcm.get(index).copied().unwrap_or(0.).to_le_bytes())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![0; 640]);
        self.sequence += 1;
        Ok(Some(
            Frame::new(
                Header::Audio(AudioMeta {
                    source: audio_key(),
                    stream_id: self.stream,
                    sequence: self.sequence,
                    sample_position: (self.sequence - 1) * 160,
                    sample_rate: 16000,
                    channels: 1,
                    frames: 160,
                    captured_ns: self.clock.elapsed().as_nanos() as u64,
                    discontinuity: self.sequence == 1,
                }),
                Bytes::from(bytes),
            )
            .unwrap(),
        ))
    }
    async fn stop(self: Box<Self>) {}
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
        if self.blocked.load(Ordering::Acquire) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(SourceSnapshot {
            revision: 1,
            issues: vec![],
            sources: vec![
                Source {
                    key: image_key(*self.generation.read().unwrap()),
                    label: "fixture".into(),
                    kind: SourceKind::Display,
                    availability: SourceAvailability::Available,
                    pixel_width: Some(8),
                    pixel_height: Some(8),
                    scale: Some(1.),
                    process_id: None,
                },
                Source {
                    key: audio_key(),
                    label: "fixture audio".into(),
                    kind: SourceKind::SystemAudio,
                    availability: SourceAvailability::Available,
                    pixel_width: None,
                    pixel_height: None,
                    scale: None,
                    process_id: None,
                },
            ],
        })
    }
    async fn image(
        &self,
        settings: &ImageSubscription,
        previous: Option<[u8; 32]>,
        sequence: u64,
        revision: u64,
    ) -> Result<Frame, Failure> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let meta = ImageMeta {
            source: settings.source.clone(),
            sequence,
            configuration_revision: revision,
            capture_started_ns: self.clock_ns(),
            captured_ns: self.clock_ns(),
            width: 1,
            height: 1,
            format: settings.format,
            fingerprint: [7; 32],
        };
        Ok(if previous.is_some() {
            Frame::new(Header::Unchanged(meta), Bytes::new()).unwrap()
        } else {
            Frame::new(Header::Image(meta), Bytes::from_static(b"fixture image")).unwrap()
        })
    }
    async fn audio(&self, key: &SourceKey, stream: Uuid) -> Result<Box<dyn AudioTrack>, Failure> {
        if *key != audio_key() {
            return Err(Failure::new(FailureCode::Unavailable, "unknown audio"));
        }
        Ok(Box::new(Audio {
            clock: self.clock,
            stream,
            sequence: 0,
            pcm: self.pcm.read().unwrap().clone(),
            gate: self.gate.clone(),
        }))
    }
}
struct Fixture {
    host: SessionHost,
    capture: Arc<Capture>,
    peer: liazo_transport::Fingerprint,
    address: std::net::SocketAddr,
    accepted: mpsc::Receiver<CancellationToken>,
    root: std::path::PathBuf,
    task: JoinHandle<()>,
}
impl Fixture {
    async fn new(root: std::path::PathBuf) -> Self {
        let provider_identity = Identity::generate().unwrap();
        let receiver_identity = Identity::generate().unwrap();
        let peer = provider_identity.fingerprint();
        let receiver_certificate = receiver_identity.certificate().clone();
        let host = SessionHost::new(
            receiver_identity,
            vec![StoredPeer {
                label: "fixture".into(),
                certificate: provider_identity.certificate().clone(),
            }],
            root.clone(),
        );
        let server = SessionServer::new(
            PeerServer::new(&provider_identity, &[receiver_certificate]).unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let capture = Arc::new(Capture {
            clock: Instant::now(),
            generation: RwLock::new(Uuid::from_u128(1)),
            calls: AtomicUsize::new(0),
            blocked: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            pcm: RwLock::new(None),
            gate: Arc::new(Semaphore::new(0)),
        });
        let backend = capture.clone();
        let (send, accepted) = mpsc::channel(8);
        let task = tokio::spawn(async move {
            let mut running = tokio::task::JoinSet::new();
            loop {
                let mut lanes = vec![];
                let mut binding = None;
                for _ in 0..3 {
                    let (socket, _) = listener.accept().await.unwrap();
                    let Ok(lane) = server.accept(socket).await else {
                        continue;
                    };
                    binding = Some(lane.binding);
                    lanes.push(lane.connection);
                }
                if lanes.len() != 3 {
                    continue;
                }
                let audio = lanes.pop().unwrap();
                let images = lanes.pop().unwrap();
                let control = lanes.pop().unwrap();
                let cancel = CancellationToken::new();
                let _ = send.send(cancel.clone()).await;
                running.spawn(provider::serve(
                    backend.clone(),
                    PeerSession {
                        binding: binding.unwrap(),
                        control,
                        images,
                        audio,
                    },
                    cancel,
                ));
                while running.try_join_next().is_some() {}
            }
        });
        Self {
            host,
            capture,
            peer,
            address,
            accepted,
            root,
            task,
        }
    }
    async fn connect(&mut self) -> (SessionBinding, CancellationToken) {
        let current = self.host.connect(self.address, self.peer).await.unwrap();
        let cancel = self.accepted.recv().await.unwrap();
        (current.binding, cancel)
    }
    async fn epoch_after(&self, previous: SessionBinding) -> liazo_host::SessionSummary {
        timeout(Duration::from_secs(8), async {
            loop {
                let current = self.host.current().await.unwrap();
                if current.binding != previous && current.connection == ConnectionState::Connected {
                    break current;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn settings() -> CaptureSettings {
    CaptureSettings {
        images: vec![ImageSubscription {
            source: image_key(Uuid::from_u128(1)),
            interval_ms: 75,
            max_width: Some(4),
            crop: Some(CropPixels {
                x: 1,
                y: 1,
                width: 4,
                height: 4,
            }),
            format: ImageFormat::Jpeg,
            quality: 82,
            include_cursor: true,
        }],
        audio: vec![audio_key()],
    }
}
async fn wait_disconnected(host: &SessionHost) {
    timeout(Duration::from_secs(3), async {
        while host.current().await.unwrap().connection != ConnectionState::Disconnected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn reconnect_preserves_settings_across_failed_attempt_and_keeps_closed_epoch_readable() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, cancel) = fixture.connect().await;
    let old = fixture.host.receiver(first).await.unwrap();
    old.configure(settings()).await.unwrap();
    old.configure_raw_audio(vec![audio_key()]).await.unwrap();
    let observed = old
        .capture_now(vec![image_key(Uuid::from_u128(1))])
        .await
        .unwrap();
    assert!(observed.images[0].payload.is_some());
    timeout(Duration::from_secs(2), async {
        while old
            .observe(0, false, &[], Duration::ZERO)
            .await
            .audio
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // The public archive API uses whole milliseconds; select an instant after the
    // received frame instead of rounding its arrival into the preceding millisecond.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let at_ms = old.elapsed().as_millis() as u64;
    cancel.cancel();
    wait_disconnected(&fixture.host).await;
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wrong = unused.local_addr().unwrap();
    drop(unused);
    assert!(fixture.host.connect(wrong, fixture.peer).await.is_err());
    assert_eq!(fixture.host.current().await.unwrap().binding, first);
    let closed_audio = fixture
        .host
        .archive_audio(liazo_host::requests::ArchiveRange {
            binding: first,
            from_ms: 0,
            to_ms: at_ms,
            sources: vec![audio_key()],
        })
        .await
        .unwrap();
    assert!(!closed_audio.clips.is_empty());
    let current = fixture.epoch_after(first).await;
    assert_eq!(current.binding.session, first.session);
    assert_ne!(current.binding.epoch, first.epoch);
    assert!(current.recovery.is_empty());
    let new = fixture.host.receiver(current.binding).await.unwrap();
    assert_eq!(new.settings(), settings());
    let fresh = new
        .capture_now(vec![image_key(Uuid::from_u128(1))])
        .await
        .unwrap();
    assert_eq!(fresh.binding, current.binding);
    assert_eq!(
        fresh.images[0].payload.as_ref().unwrap().as_ref(),
        b"fixture image"
    );
    assert_eq!(
        new.archive_status().unwrap().raw_audio_sources,
        vec![audio_key()]
    );
    assert!(matches!(
        fixture.host.receiver(first).await,
        Err(HostError::NoSession)
    ));
    assert!(old.archive_status().is_none());
    let archived = fixture
        .host
        .archive_images(liazo_host::requests::ArchiveImages {
            binding: first,
            at_ms,
            sources: vec![settings().images[0].source.clone()],
        })
        .await
        .unwrap();
    assert_eq!(
        archived.images[0].payload.as_ref().unwrap().as_ref(),
        b"fixture image"
    );
    fixture.host.stop(current.binding).await.unwrap();
}

#[tokio::test]
async fn changed_source_generation_is_reported_without_remapping_and_stop_starts_a_new_session() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, cancel) = fixture.connect().await;
    fixture
        .host
        .receiver(first)
        .await
        .unwrap()
        .configure(settings())
        .await
        .unwrap();
    *fixture.capture.generation.write().unwrap() = Uuid::new_v4();
    cancel.cancel();
    let current = fixture.epoch_after(first).await;
    let receiver = fixture.host.receiver(current.binding).await.unwrap();
    assert!(receiver.settings().images.is_empty());
    assert_eq!(receiver.settings().audio, vec![audio_key()]);
    assert!(
        current
            .recovery
            .iter()
            .any(|issue| issue.source.as_ref() == Some(&settings().images[0].source))
    );
    fixture.host.stop(current.binding).await.unwrap();
    let restarted = fixture
        .host
        .connect(fixture.address, fixture.peer)
        .await
        .unwrap();
    assert_ne!(restarted.binding.session, current.binding.session);
    assert_eq!(
        fixture
            .host
            .receiver(restarted.binding)
            .await
            .unwrap()
            .settings(),
        CaptureSettings::default()
    );
    fixture.host.stop(restarted.binding).await.unwrap();
}

#[tokio::test]
async fn stop_and_revocation_cancel_pending_automatic_reconnect() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, cancel) = fixture.connect().await;
    fixture
        .host
        .receiver(first)
        .await
        .unwrap()
        .configure(settings())
        .await
        .unwrap();
    fixture.capture.blocked.store(true, Ordering::Release);
    cancel.cancel();
    timeout(Duration::from_secs(4), fixture.capture.entered.notified())
        .await
        .unwrap();
    fixture.host.stop(first).await.unwrap();
    fixture.capture.blocked.store(false, Ordering::Release);
    fixture.capture.release.notify_waiters();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let stopped = fixture.host.current().await.unwrap();
    assert_eq!(stopped.binding, first);
    assert_eq!(stopped.connection, ConnectionState::Stopped);
    let resumed = fixture
        .host
        .connect(fixture.address, fixture.peer)
        .await
        .unwrap();
    let _cancelled_attempt = fixture.accepted.recv().await.unwrap();
    let resumed_cancel = fixture.accepted.recv().await.unwrap();
    fixture.capture.blocked.store(true, Ordering::Release);
    resumed_cancel.cancel();
    timeout(Duration::from_secs(4), fixture.capture.entered.notified())
        .await
        .unwrap();
    fixture.host.replace_peers(vec![]).await.unwrap();
    fixture.capture.blocked.store(false, Ordering::Release);
    fixture.capture.release.notify_waiters();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(fixture.host.current().await.is_none());
    assert!(matches!(
        fixture.host.connect(fixture.address, fixture.peer).await,
        Err(HostError::Unpaired)
    ));
    assert_ne!(resumed.binding.session, first.session);
}

#[tokio::test]
async fn archive_initialization_failure_does_not_prevent_live_capture() {
    let root = tempfile::tempdir().unwrap();
    let unavailable = root.path().join("sessions");
    std::fs::write(&unavailable, b"not a directory").unwrap();
    let mut fixture = Fixture::new(unavailable).await;
    let (binding, _cancel) = fixture.connect().await;
    let summary = fixture.host.current().await.unwrap();
    assert!(
        summary
            .recovery
            .iter()
            .any(|issue| matches!(issue.operation, liazo_host::RecoveryOperation::Archive))
    );
    let receiver = fixture.host.receiver(binding).await.unwrap();
    receiver.configure(settings()).await.unwrap();
    let image = receiver
        .capture_now(vec![image_key(Uuid::from_u128(1))])
        .await
        .unwrap();
    assert_eq!(
        image.images[0].payload.as_ref().unwrap().as_ref(),
        b"fixture image"
    );
    assert!(receiver.archive_status().is_none());
    std::fs::remove_file(&fixture.root).unwrap();
    timeout(Duration::from_secs(5), async {
        while receiver.archive_status().is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        fixture
            .host
            .current()
            .await
            .unwrap()
            .recovery
            .iter()
            .all(|issue| !matches!(issue.operation, liazo_host::RecoveryOperation::Archive))
    );
    fixture.host.stop(binding).await.unwrap();
}

#[tokio::test]
#[ignore = "requires LIAZO_TEST_MODEL and a 16 khz mono LIAZO_TEST_SPEECH fixture"]
async fn real_speech_reconnects_and_recovers_without_undoing_explicit_transcription_stop() {
    let model = std::path::PathBuf::from(std::env::var_os("LIAZO_TEST_MODEL").expect("model path"));
    let mut wav =
        hound::WavReader::open(std::env::var_os("LIAZO_TEST_SPEECH").expect("speech fixture"))
            .unwrap();
    assert_eq!(wav.spec().sample_rate, 16000);
    assert_eq!(wav.spec().channels, 1);
    let pcm = Arc::new(
        wav.samples::<i16>()
            .map(|sample| f32::from(sample.unwrap()) / 32768.)
            .collect::<Vec<_>>(),
    );
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    *fixture.capture.pcm.write().unwrap() = Some(pcm);
    let (first, cancel) = fixture.connect().await;
    let first_receiver = fixture.host.receiver(first).await.unwrap();
    first_receiver.configure(settings()).await.unwrap();
    fixture
        .host
        .transcribe(first, model, 4, vec![audio_key()])
        .await
        .unwrap();
    fixture.capture.gate.add_permits(1);
    timeout(Duration::from_secs(30), async {
        loop {
            let observation = first_receiver.observe(0, false, &[], Duration::ZERO).await;
            if observation
                .transcripts
                .iter()
                .any(|text| !text.text.is_empty())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    let current = fixture.epoch_after(first).await;
    assert_eq!(current.binding.session, first.session);
    assert!(current.recovery.is_empty());
    let receiver = fixture.host.receiver(current.binding).await.unwrap();
    assert!(
        receiver
            .observe(0, false, &[], Duration::ZERO)
            .await
            .transcription
            .active
    );
    fixture.capture.gate.add_permits(1);
    timeout(Duration::from_secs(35), async {
        loop {
            let observation = receiver.observe(0, false, &[], Duration::ZERO).await;
            if observation
                .transcripts
                .iter()
                .any(|text| text.ending.is_some() && text.text.to_lowercase().contains("country"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    // A local worker interruption preserves host intent and restarts real inference.
    receiver.stop_transcription().await;
    timeout(Duration::from_secs(5), async {
        while !receiver
            .observe(0, false, &[], Duration::ZERO)
            .await
            .transcription
            .active
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Stop while a retry has already copied speech intent; it cannot install that intent.
    fixture.capture.blocked.store(true, Ordering::Release);
    fixture.accepted.recv().await.unwrap().cancel();
    timeout(Duration::from_secs(4), fixture.capture.entered.notified())
        .await
        .unwrap();
    fixture
        .host
        .stop_transcription(current.binding)
        .await
        .unwrap();
    fixture.capture.blocked.store(false, Ordering::Release);
    fixture.capture.release.notify_waiters();
    let after_stop = fixture.epoch_after(current.binding).await;
    assert!(
        !fixture
            .host
            .receiver(after_stop.binding)
            .await
            .unwrap()
            .observe(0, false, &[], Duration::ZERO)
            .await
            .transcription
            .active
    );
    fixture.host.stop(after_stop.binding).await.unwrap();
}
