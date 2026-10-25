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
    collections::VecDeque,
    sync::{
        Arc, Mutex, RwLock,
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
    audio_progress: Arc<Mutex<AudioProgress>>,
}
#[derive(Default, serde::Serialize)]
struct AudioProgress {
    opened: VecDeque<Uuid>,
    released: VecDeque<Uuid>,
}
struct Audio {
    clock: Instant,
    origin: Option<(Instant, u64)>,
    stream: Uuid,
    sequence: u64,
    pcm: Option<Arc<Vec<f32>>>,
    gate: Arc<Semaphore>,
    progress: Arc<Mutex<AudioProgress>>,
}
#[async_trait]
impl AudioTrack for Audio {
    async fn next(&mut self) -> Result<Option<Frame>, Failure> {
        if self.origin.is_none() {
            if self.pcm.is_some() {
                self.gate.acquire().await.unwrap().forget();
                let mut progress = self.progress.lock().unwrap();
                progress.released.push_back(self.stream);
                if progress.released.len() > 8 {
                    progress.released.pop_front();
                }
            }
            self.origin = Some((Instant::now(), self.clock.elapsed().as_nanos() as u64));
        }
        let (started, anchor_ns) = self.origin.unwrap();
        let offset_ns = self.sequence * 10_000_000;
        tokio::time::sleep_until((started + Duration::from_nanos(offset_ns)).into()).await;
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
                    captured_ns: anchor_ns + offset_ns,
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
        let mut progress = self.audio_progress.lock().unwrap();
        progress.opened.push_back(stream);
        if progress.opened.len() > 8 {
            progress.opened.pop_front();
        }
        Ok(Box::new(Audio {
            clock: self.clock,
            origin: None,
            stream,
            sequence: 0,
            pcm: self.pcm.read().unwrap().clone(),
            gate: self.gate.clone(),
            progress: self.audio_progress.clone(),
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
    receiver_certificate: Vec<u8>,
    receiver_secret: Vec<u8>,
    stored_peer: StoredPeer,
    task: JoinHandle<()>,
}
impl Fixture {
    async fn new(root: std::path::PathBuf) -> Self {
        let provider_identity = Identity::generate().unwrap();
        let receiver_identity = Identity::generate().unwrap();
        let peer = provider_identity.fingerprint();
        let receiver_certificate = receiver_identity.certificate().clone();
        let receiver_secret = receiver_identity.secret_key_der().to_vec();
        let stored_peer = StoredPeer {
            label: "fixture".into(),
            certificate: provider_identity.certificate().clone(),
        };
        let host = SessionHost::new(receiver_identity, vec![stored_peer.clone()], root.clone());
        let server = SessionServer::new(
            PeerServer::new(
                &provider_identity,
                std::slice::from_ref(&receiver_certificate),
            )
            .unwrap(),
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
            audio_progress: Arc::new(Mutex::new(AudioProgress::default())),
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
            receiver_certificate: receiver_certificate.to_vec(),
            receiver_secret,
            stored_peer,
            task,
        }
    }
    fn restart(&mut self) {
        self.host = SessionHost::new(
            Identity::from_der(
                self.receiver_certificate.clone(),
                self.receiver_secret.clone(),
            )
            .unwrap(),
            vec![self.stored_peer.clone()],
            self.root.clone(),
        );
        self.host.resume();
    }
    async fn connect(&mut self) -> (SessionBinding, CancellationToken) {
        let current = self.host.connect(self.address, self.peer).await.unwrap();
        let cancel = self.accepted.recv().await.unwrap();
        (current.binding, cancel)
    }
    async fn epoch_after(&self, previous: SessionBinding) -> liazo_host::SessionSummary {
        timeout(Duration::from_secs(8), async {
            loop {
                if let Some(current) = self.host.current().await
                    && current.binding != previous
                    && current.connection == ConnectionState::Connected
                {
                    break current;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn archive_ready(&self, binding: SessionBinding) -> liazo_host::SessionSummary {
        let receiver = self.host.receiver(binding).await.unwrap();
        let mut last = String::from("waiting for archive recovery");
        timeout(Duration::from_secs(15), async {
            loop {
                let current = self.host.current().await.unwrap();
                let archive = receiver.archive_status();
                last = serde_json::json!({
                    "session": &current,
                    "archive": &archive,
                    "transcription": receiver.transcription_status(),
                })
                .to_string();
                assert_eq!(current.binding, binding, "epoch changed: {last}");
                assert_eq!(
                    current.connection,
                    ConnectionState::Connected,
                    "live connection was lost: {last}"
                );
                if current.recovery.is_empty()
                    && archive.is_some_and(|status| {
                        status.phase == liazo_runtime::archive::ArchivePhase::Recording
                    })
                {
                    return current;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("archive recovery did not become ready: {last}"))
    }
    async fn speech_state(
        &self,
        receiver: &liazo_runtime::receiver::ReceiverSession,
        phase: &str,
    ) -> (liazo_runtime::Observation, String) {
        let observed = receiver.observe(0, false, &[], Duration::ZERO).await;
        let recent: Vec<_> = observed
            .transcripts
            .iter()
            .rev()
            .take(8)
            .map(|text| {
                serde_json::json!({
                    "id": text.id,
                    "revision": text.revision,
                    "start_ns": text.start_ns,
                    "end_ns": text.end_ns,
                    "ending": text.ending,
                    "words": text.text.split_whitespace().count(),
                    "country": text.text.to_lowercase().contains("country"),
                })
            })
            .collect();
        let current = self.host.current().await;
        let state = serde_json::json!({
            "phase": phase,
            "session": current,
            "receiver_binding": observed.binding,
            "receiver_connection": &observed.connection,
            "selected_audio": &observed.settings.audio,
            "audio": &observed.audio,
            "transcription": &observed.transcription,
            "archive": &observed.archive,
            "live_gaps": observed.gaps,
            "source_failures": observed.failures.len(),
            "transcript_deltas_lost": observed.transcript_deltas_lost,
            "transcripts": {
                "total": observed.transcripts.len(),
                "partials": observed.transcripts.iter().filter(|text| text.ending.is_none()).count(),
                "finals": observed.transcripts.iter().filter(|text| text.ending.is_some()).count(),
                "matching_finals": observed.transcripts.iter().filter(|text| {
                    text.ending.is_some() && text.text.to_lowercase().contains("country")
                }).count(),
                "recent": recent,
            },
            "gate": &*self.capture.audio_progress.lock().unwrap(),
        })
        .to_string();
        (observed, state)
    }
    async fn speech_final(&self, binding: SessionBinding, phase: &str) {
        let receiver = self.host.receiver(binding).await.unwrap();
        let started = Instant::now();
        let mut last = String::from("waiting for speech observation");
        timeout(Duration::from_secs(35), async {
            loop {
                let (observed, state) = self.speech_state(&receiver, phase).await;
                last = state;
                assert_eq!(observed.binding, binding, "epoch changed: {last}");
                if observed.transcripts.iter().any(|text| {
                    text.ending.is_some() && text.text.to_lowercase().contains("country")
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{phase} final exceeded 35 s correctness guard: {last}"));
        eprintln!("{phase} final after {:?}: {last}", started.elapsed());
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
async fn delayed_audio_delivery_preserves_the_prerecorded_sample_clock() {
    use liazo_transcription::audio::Normalizer;

    let mut track = Audio {
        clock: Instant::now(),
        origin: None,
        stream: Uuid::new_v4(),
        sequence: 0,
        pcm: Some(Arc::new(vec![0.25; 320])),
        gate: Arc::new(Semaphore::new(1)),
        progress: Arc::new(Mutex::new(AudioProgress::default())),
    };
    let first = track.next().await.unwrap().unwrap();
    let Header::Audio(first_metadata) = first.header else {
        panic!("expected audio metadata");
    };
    let mut normalizer = Normalizer::new(audio_key());
    assert!(
        !normalizer
            .push(&first_metadata, &first.payload)
            .unwrap()
            .discontinuity
    );
    // Delayed delivery of the same recorded samples is not a source-clock gap.
    tokio::time::sleep(Duration::from_millis(525)).await;
    let second = track.next().await.unwrap().unwrap();
    let Header::Audio(second_metadata) = second.header else {
        panic!("expected audio metadata");
    };
    let normalized = normalizer.push(&second_metadata, &second.payload).unwrap();
    assert!(
        !normalized.discontinuity,
        "scheduler delay must not split a continuous recorded utterance"
    );
    assert_eq!(
        second_metadata.captured_ns - first_metadata.captured_ns,
        10_000_000
    );
    assert_eq!(normalized.start_ns, first_metadata.captured_ns + 10_000_000);
}

#[tokio::test]
async fn reconnect_preserves_settings_across_failed_attempt_and_keeps_closed_epoch_readable() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, cancel) = fixture.connect().await;
    let old = fixture.host.receiver(first).await.unwrap();
    old.configure(settings()).await.unwrap();
    fixture.archive_ready(first).await;
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
    timeout(Duration::from_secs(3), async {
        while old.archive_status().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
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
    let current = fixture.archive_ready(current.binding).await;
    assert!(current.recovery.is_empty());
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
    let mut last = String::from("waiting for repaired archive storage");
    let repaired = timeout(Duration::from_secs(15), async {
        loop {
            let current = fixture.host.current().await.unwrap();
            let archive = receiver.archive_status();
            last = serde_json::json!({"session":&current,"archive":&archive}).to_string();
            assert_eq!(current.binding, binding, "epoch changed: {last}");
            assert_eq!(current.connection, ConnectionState::Connected, "{last}");
            if archive.is_some_and(|status| {
                status.phase == liazo_runtime::archive::ArchivePhase::Recording
            }) && current
                .recovery
                .iter()
                .all(|issue| !matches!(issue.operation, liazo_host::RecoveryOperation::Archive))
            {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("archive storage did not recover: {last}"));
    // The intentionally invalid application-data root also prevented preference
    // persistence. Archive recovery must not erase that independent failure.
    if summary
        .recovery
        .iter()
        .any(|issue| matches!(issue.operation, liazo_host::RecoveryOperation::Settings))
    {
        assert!(
            repaired
                .recovery
                .iter()
                .any(|issue| matches!(issue.operation, liazo_host::RecoveryOperation::Settings))
        );
    }
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
    let mut last = String::from("waiting for first speech observation");
    timeout(Duration::from_secs(30), async {
        loop {
            let (observation, state) = fixture
                .speech_state(&first_receiver, "initial_partial")
                .await;
            last = state;
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
    .unwrap_or_else(|_| panic!("initial speech exceeded 30 s correctness guard: {last}"));
    eprintln!("initial speech ready: {last}");
    cancel.cancel();
    let current = fixture.epoch_after(first).await;
    assert_eq!(current.binding.session, first.session);
    let receiver = fixture.host.receiver(current.binding).await.unwrap();
    assert!(
        receiver
            .observe(0, false, &[], Duration::ZERO)
            .await
            .transcription
            .active,
        "speech is inactive after reconnect: {}",
        serde_json::json!({
            "session": &current,
            "transcription": receiver.transcription_status(),
        })
    );
    let fresh = timeout(
        Duration::from_secs(1),
        receiver.capture_now(vec![image_key(Uuid::from_u128(1))]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fresh.binding, current.binding);
    assert!(fresh.images[0].payload.is_some());
    // Live/STT reconnect can precede completion of the previous archive's
    // retirement. Its explicit recovery issue must subsequently clear.
    let current = fixture.archive_ready(current.binding).await;
    fixture.capture.gate.add_permits(1);
    fixture.speech_final(current.binding, "reconnect").await;
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
    // A host restart reloads the model path/settings, rather than serializing a model or requiring the GUI.
    fixture.accepted.recv().await.unwrap();
    drop(receiver);
    drop(first_receiver);
    fixture.restart();
    let current = fixture.epoch_after(current.binding).await;
    let receiver = fixture.host.receiver(current.binding).await.unwrap();
    let mut last = String::from("waiting for restarted speech worker");
    timeout(Duration::from_secs(30), async {
        while !receiver.transcription_status().active {
            last = fixture.speech_state(&receiver, "restart_active").await.1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("restarted speech worker exceeded 30 s guard: {last}"));
    fixture.capture.gate.add_permits(1);
    fixture.speech_final(current.binding, "restart").await;
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

#[tokio::test]
async fn stalled_archive_uses_one_retirement_slot_while_multiple_live_epochs_continue() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, cancel) = fixture.connect().await;
    let old = fixture.host.receiver(first).await.unwrap();
    old.configure(settings()).await.unwrap();
    fixture.archive_ready(first).await;
    old.configure_raw_audio(vec![audio_key()]).await.unwrap();
    old.capture_now(vec![image_key(Uuid::from_u128(1))])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let at_ms = old.elapsed().as_millis() as u64;
    let hold = old.hold_archive().await.unwrap();
    cancel.cancel();
    wait_disconnected(&fixture.host).await;
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wrong = unused.local_addr().unwrap();
    drop(unused);
    assert!(fixture.host.connect(wrong, fixture.peer).await.is_err());
    assert_eq!(fixture.host.current().await.unwrap().binding, first);
    match fixture
        .host
        .archive_images(liazo_host::requests::ArchiveImages {
            binding: first,
            at_ms,
            sources: vec![image_key(Uuid::from_u128(1))],
        })
        .await
    {
        Ok(history) => assert_eq!(
            history.images[0].payload.as_ref().unwrap().as_ref(),
            b"fixture image"
        ),
        Err(HostError::Archive(liazo_runtime::archive::ArchiveError::Busy)) => {}
        _ => panic!("retained data must remain historical or report the held archive as busy"),
    }
    let mut current = fixture.epoch_after(first).await;
    assert!(matches!(
        fixture
            .host
            .archive_images(liazo_host::requests::ArchiveImages {
                binding: first,
                at_ms: 0,
                sources: vec![image_key(Uuid::from_u128(1))],
            })
            .await,
        Err(HostError::Archive(
            liazo_runtime::archive::ArchiveError::Busy
        ))
    ));

    for _ in 0..2 {
        let receiver = fixture.host.receiver(current.binding).await.unwrap();
        assert!(receiver.archive_status().is_none());
        assert!(
            current
                .recovery
                .iter()
                .any(|issue| issue.operation == liazo_host::RecoveryOperation::Archive)
        );
        assert_eq!(receiver.settings(), settings());
        let fresh = receiver
            .capture_now(vec![image_key(Uuid::from_u128(1))])
            .await
            .unwrap();
        assert_eq!(fresh.binding, current.binding);
        assert!(fresh.images[0].payload.is_some());
        fixture.accepted.recv().await.unwrap().cancel();
        current = fixture.epoch_after(current.binding).await;
    }
    assert_eq!(current.binding.session, first.session);
    assert!(
        current
            .recovery
            .iter()
            .any(|issue| issue.operation == liazo_host::RecoveryOperation::Archive)
    );
    assert!(old.archive_status().unwrap().queued_bytes <= 64 * 1024 * 1024);
    let epochs = std::fs::read_dir(root.path().join(first.session.to_string()))
        .unwrap()
        .count();
    assert_eq!(
        epochs, 1,
        "a held writer must not create another archive thread/index per retry"
    );
    drop(hold);
    let receiver = fixture.host.receiver(current.binding).await.unwrap();
    fixture.archive_ready(current.binding).await;
    assert!(old.archive_status().is_none());
    assert_eq!(
        receiver.archive_status().unwrap().raw_audio_sources,
        vec![audio_key()]
    );
    let retained = fixture
        .host
        .archive_images(liazo_host::requests::ArchiveImages {
            binding: first,
            at_ms,
            sources: vec![image_key(Uuid::from_u128(1))],
        })
        .await
        .unwrap();
    assert_eq!(
        retained.images[0].payload.as_ref().unwrap().as_ref(),
        b"fixture image"
    );
    fixture.host.stop(current.binding).await.unwrap();
}

#[tokio::test]
async fn restart_restores_private_intent_and_explicit_stop_disables_future_restarts() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, _) = fixture.connect().await;
    fixture.host.configure(first, settings()).await.unwrap();
    fixture.archive_ready(first).await;
    fixture
        .host
        .configure_raw_audio(first, vec![audio_key()])
        .await
        .unwrap();
    fixture.restart();
    let next = fixture.epoch_after(first).await;
    assert_eq!(next.binding.session, first.session);
    assert!(!next.pending);
    let receiver = fixture.host.receiver(next.binding).await.unwrap();
    assert_eq!(receiver.settings(), settings());
    fixture.archive_ready(next.binding).await;
    assert_eq!(
        receiver.archive_status().unwrap().raw_audio_sources,
        vec![audio_key()]
    );
    fixture.host.stop(next.binding).await.unwrap();
    assert!(
        !receiver.archive_retiring(),
        "a sealed archive still serves readers before retirement"
    );
    fixture
        .host
        .archive_audio(liazo_host::requests::ArchiveRange {
            binding: next.binding,
            from_ms: 0,
            to_ms: receiver.elapsed().as_millis() as u64,
            sources: vec![audio_key()],
        })
        .await
        .unwrap();
    assert!(!root.path().join("resume.json").exists());
    fixture.restart();
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(fixture.host.current().await.is_none());
}

#[tokio::test]
async fn pending_startup_stop_and_revocation_are_durable_before_a_receiver_exists() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, _) = fixture.connect().await;
    fixture.host.configure(first, settings()).await.unwrap();
    fixture.capture.blocked.store(true, Ordering::Release);
    fixture.restart();
    timeout(Duration::from_secs(3), async {
        loop {
            if fixture
                .host
                .current()
                .await
                .is_some_and(|current| current.pending)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    timeout(Duration::from_secs(4), fixture.capture.entered.notified())
        .await
        .unwrap();
    fixture.host.stop(first).await.unwrap();
    fixture.capture.blocked.store(false, Ordering::Release);
    fixture.capture.release.notify_waiters();
    fixture.restart();
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(fixture.host.current().await.is_none());
    let restarted = fixture
        .host
        .connect(fixture.address, fixture.peer)
        .await
        .unwrap();
    fixture
        .host
        .configure(restarted.binding, settings())
        .await
        .unwrap();
    fixture.restart();
    timeout(Duration::from_secs(3), async {
        while fixture.host.current().await.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.host.replace_peers(vec![]).await.unwrap();
    fixture.restart();
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(fixture.host.current().await.is_none());
}

#[tokio::test]
async fn revocation_before_startup_load_cannot_resurrect_when_the_peer_is_paired_again() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, _) = fixture.connect().await;
    fixture.host.configure(first, settings()).await.unwrap();
    fixture.restart();
    fixture.host.replace_peers(vec![]).await.unwrap();
    assert!(!root.path().join("resume.json").exists());
    fixture.restart();
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(fixture.host.current().await.is_none());
}

#[tokio::test]
async fn startup_retains_exact_source_generation_and_rejects_a_foreign_owner_record() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::new(root.path().to_owned()).await;
    let (first, _) = fixture.connect().await;
    fixture.host.configure(first, settings()).await.unwrap();
    *fixture.capture.generation.write().unwrap() = Uuid::new_v4();
    fixture.restart();
    let current = fixture.epoch_after(first).await;
    let receiver = fixture.host.receiver(current.binding).await.unwrap();
    assert!(receiver.settings().images.is_empty());
    assert_eq!(receiver.settings().audio, vec![audio_key()]);
    assert!(
        current
            .recovery
            .iter()
            .any(|issue| issue.source.as_ref() == Some(&image_key(Uuid::from_u128(1))))
    );
    let path = root.path().join("resume.json");
    // current media becomes ready before the asynchronous durable publication ends.
    // Corrupt the completed record, rather than racing a legitimate queued write.
    timeout(Duration::from_secs(3), async {
        loop {
            if std::fs::read(&path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .is_some_and(|saved| saved["binding"] == serde_json::json!(current.binding))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    saved["owner"] =
        serde_json::Value::String(Identity::generate().unwrap().fingerprint().to_string());
    std::fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
    drop(receiver);
    let before = fixture.capture.calls.load(Ordering::Acquire);
    fixture.restart();
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(fixture.host.current().await.is_none());
    assert_eq!(fixture.capture.calls.load(Ordering::Acquire), before);
}
