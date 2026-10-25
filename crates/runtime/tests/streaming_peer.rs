use async_trait::async_trait;
use bytes::Bytes;
use liazo_core::{
    catalog::SourceSnapshot,
    control::Failure,
    protocol::{
        AudioMeta, CaptureSettings, ImageFormat, ImageSubscription, Source, SourceAvailability,
        SourceKey, SourceKind,
    },
    wire::{Frame, Header, ImageMeta},
};
use liazo_runtime::{
    backend::{AudioTrack, CaptureBackend},
    provider,
    receiver::ReceiverSession,
};
use liazo_transcription::{WhisperModel, streaming::StreamingSettings};
use liazo_transport::{Identity, PeerClient, PeerServer, PeerSession, SessionServer};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::Semaphore, time::timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn source(audio: bool) -> SourceKey {
    SourceKey {
        id: if audio {
            "audio:fixture"
        } else {
            "display:fixture"
        }
        .into(),
        generation: Uuid::from_u128(if audio { 1 } else { 2 }),
    }
}

struct Fixture {
    clock: Instant,
    pcm: Arc<Vec<f32>>,
    gate: Arc<Semaphore>,
    active: Arc<AtomicUsize>,
    changing: AtomicBool,
}
struct Track {
    clock: Instant,
    origin: Option<(Instant, u64)>,
    source: SourceKey,
    stream: Uuid,
    pcm: Arc<Vec<f32>>,
    position: usize,
    gate: Arc<Semaphore>,
    active: Arc<AtomicUsize>,
}

impl Drop for Track {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl AudioTrack for Track {
    async fn next(&mut self) -> Result<Option<Frame>, Failure> {
        if self.origin.is_none() {
            self.gate.acquire().await.unwrap().forget();
            self.origin = Some((Instant::now(), self.clock.elapsed().as_nanos() as u64));
        }
        let (start, ns) = self.origin.unwrap();
        tokio::time::sleep_until(
            (start + Duration::from_nanos(self.position as u64 * 1_000_000_000 / 48_000)).into(),
        )
        .await;
        let pcm: Vec<_> = (self.position..self.position + 960)
            .flat_map(|index| self.pcm.get(index).copied().unwrap_or(0.0).to_le_bytes())
            .collect();
        let meta = AudioMeta {
            source: self.source.clone(),
            stream_id: self.stream,
            sequence: self.position as u64 / 960 + 1,
            sample_position: self.position as u64,
            sample_rate: 48_000,
            channels: 1,
            frames: 960,
            captured_ns: ns + self.position as u64 * 1_000_000_000 / 48_000,
            discontinuity: false,
        };
        self.position += 960;
        Ok(Some(Frame::new(Header::Audio(meta), pcm.into()).unwrap()))
    }
    async fn stop(self: Box<Self>) {}
}

#[async_trait]
impl CaptureBackend for Fixture {
    fn platform(&self) -> &'static str {
        "speech-fixture"
    }
    fn clock_ns(&self) -> u64 {
        self.clock.elapsed().as_nanos() as u64
    }
    async fn discover(&self) -> Result<SourceSnapshot, Failure> {
        Ok(SourceSnapshot {
            revision: 1,
            issues: vec![],
            sources: [false, true]
                .into_iter()
                .map(|audio| Source {
                    key: source(audio),
                    label: "fixture".into(),
                    kind: if audio {
                        SourceKind::SystemAudio
                    } else {
                        SourceKind::Display
                    },
                    availability: SourceAvailability::Available,
                    pixel_width: (!audio).then_some(1),
                    pixel_height: (!audio).then_some(1),
                    scale: (!audio).then_some(1.0),
                    process_id: None,
                })
                .collect(),
        })
    }
    async fn image(
        &self,
        image: &ImageSubscription,
        previous: Option<[u8; 32]>,
        sequence: u64,
        revision: u64,
    ) -> Result<Frame, Failure> {
        let started = self.clock_ns();
        let changing = self.changing.load(Ordering::SeqCst);
        let fingerprint = [if changing { sequence as u8 } else { 1 }; 32];
        let meta = ImageMeta {
            source: image.source.clone(),
            sequence,
            captured_ns: self.clock_ns(),
            capture_started_ns: started,
            configuration_revision: revision,
            width: 1,
            height: 1,
            format: ImageFormat::Png,
            fingerprint,
        };
        Ok(if previous == Some(fingerprint) {
            Frame::new(Header::Unchanged(meta), Bytes::new()).unwrap()
        } else {
            let bytes = if changing {
                Bytes::from(format!("image-fixture-{sequence}"))
            } else {
                Bytes::from_static(b"image-fixture")
            };
            Frame::new(Header::Image(meta), bytes).unwrap()
        })
    }
    async fn audio(
        &self,
        source: &SourceKey,
        stream: Uuid,
    ) -> Result<Box<dyn AudioTrack>, Failure> {
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Track {
            clock: self.clock,
            origin: None,
            source: source.clone(),
            stream,
            pcm: self.pcm.clone(),
            position: 0,
            gate: self.gate.clone(),
            active: self.active.clone(),
        }))
    }
}

fn fixture(pcm: Vec<f32>, changing: bool) -> Arc<Fixture> {
    Arc::new(Fixture {
        clock: Instant::now(),
        pcm: Arc::new(pcm),
        gate: Arc::new(Semaphore::new(0)),
        active: Arc::new(AtomicUsize::new(0)),
        changing: AtomicBool::new(changing),
    })
}
async fn pair(
    fixture: Arc<Fixture>,
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
    let capture = fixture.clone();
    let provider = tokio::spawn(async move {
        let mut lanes = Vec::new();
        let mut binding = None;
        for _ in 0..3 {
            let (socket, _) = listener.accept().await.unwrap();
            let lane = server.accept(socket).await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires LIAZO_TEST_MODEL and a 16 khz mono LIAZO_TEST_SPEECH fixture"]
async fn real_speech_crosses_peer_transport_and_shared_observation_without_blocking_images() {
    let model = PathBuf::from(std::env::var_os("LIAZO_TEST_MODEL").expect("model path"));
    let model = tokio::task::spawn_blocking(move || WhisperModel::load(&model))
        .await
        .unwrap()
        .unwrap();
    let mut reader =
        hound::WavReader::open(std::env::var_os("LIAZO_TEST_SPEECH").expect("fixture path"))
            .unwrap();
    assert_eq!(
        (reader.spec().sample_rate, reader.spec().channels),
        (16_000, 1)
    );
    // A controlled 48 kHz source also exercises the actual streaming resampler.
    let pcm: Vec<_> = reader
        .samples::<i16>()
        .flat_map(|sample| std::iter::repeat_n(f32::from(sample.unwrap()) / 32768.0, 3))
        .collect();
    let fixture = fixture(pcm, false);
    let (session, provider) = pair(fixture.clone()).await;
    let archive = tempfile::tempdir().unwrap();
    session
        .start_archive(
            archive.path().to_owned(),
            liazo_runtime::archive::ArchiveLimits::default(),
        )
        .await
        .unwrap();
    let history_start = Instant::now();
    session
        .configure(CaptureSettings {
            images: vec![ImageSubscription {
                source: source(false),
                interval_ms: 100,
                max_width: None,
                crop: None,
                format: ImageFormat::Png,
                quality: 90,
                include_cursor: false,
            }],
            audio: vec![source(true)],
        })
        .await
        .unwrap();
    session
        .start_transcription(model, 4, vec![source(true)], StreamingSettings::default())
        .await
        .unwrap();
    session
        .configure_raw_audio(vec![source(true)])
        .await
        .unwrap();
    #[cfg(feature = "test-support")]
    let mut disk_hold = {
        fixture.changing.store(true, Ordering::SeqCst);
        session.set_archive_database_full(true).await.unwrap();
        Some(session.hold_archive().await.unwrap())
    };
    #[cfg(feature = "test-support")]
    let mut storage_recovered = false;
    fixture.gate.add_permits(1);
    let mut cursor = 0;
    let mut partials = Vec::new();
    let mut final_ = None;
    let mut responsive = false;
    timeout(Duration::from_secs(20), async {
        while final_.is_none() {
            let observed = session
                .observe(cursor, false, &[], Duration::from_millis(500))
                .await;
            cursor = observed.cursor;
            assert_eq!(observed.transcription.gaps, 0);
            #[cfg(feature = "test-support")]
            if responsive
                && !storage_recovered
                && observed.archive.as_ref().is_some_and(|status| {
                    status.phase == liazo_runtime::archive::ArchivePhase::Failed
                })
            {
                session.set_archive_database_full(false).await.unwrap();
                fixture.changing.store(false, Ordering::SeqCst);
                storage_recovered = true;
            }
            for transcript in observed.transcripts {
                assert_eq!(transcript.source, source(true));
                if transcript.ending.is_none() && !transcript.text.is_empty() {
                    partials.push(transcript.id);
                    if !responsive {
                        let image = timeout(
                            Duration::from_secs(1),
                            session.capture_now(vec![source(false)]),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                        assert!(image.images[0].payload.is_some());
                        responsive = true;
                        #[cfg(feature = "test-support")]
                        {
                            assert!(session.archive_status().unwrap().dropped_records > 0);
                            drop(disk_hold.take());
                        }
                    }
                } else if transcript.ending.is_some()
                    && transcript.text.split_whitespace().count() >= 3
                {
                    final_ = Some(transcript);
                }
            }
        }
    })
    .await
    .unwrap();
    #[cfg(feature = "test-support")]
    assert!(
        storage_recovered,
        "real STT must continue through both held and full archive storage"
    );
    assert!(responsive && partials.contains(&final_.as_ref().unwrap().id));
    let independent = session.observe(0, false, &[], Duration::ZERO).await;
    assert!(
        independent
            .transcripts
            .iter()
            .any(|transcript| transcript.id == final_.as_ref().unwrap().id
                && transcript.ending.is_some())
    );
    assert!(
        session
            .observe(independent.cursor, false, &[], Duration::ZERO)
            .await
            .transcripts
            .is_empty()
    );
    session.stop_transcription().await;
    session.flush_archive().await.unwrap();
    let stored = session
        .history_transcripts(history_start.elapsed(), Duration::ZERO, &[source(true)])
        .await
        .unwrap();
    let expected = final_.as_ref().unwrap();
    let final_record = stored
        .entries
        .iter()
        .find(|entry| entry.transcript.id == expected.id)
        .expect("final transcript archived");
    assert_eq!(
        serde_json::to_value(&final_record.transcript).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert!(final_record.receiver_range.is_some());
    let retained_audio = session
        .history_audio(history_start.elapsed(), Duration::ZERO, &[source(true)])
        .await
        .unwrap();
    assert!(!retained_audio.clips.is_empty());
    assert_eq!(retained_audio.policy.len(), 1);
    assert!(
        retained_audio
            .clips
            .iter()
            .all(|clip| clip.segment.source == source(true) && clip.segment.sample_rate == 48000)
    );
    let wav = hound::WavReader::new(std::io::Cursor::new(&retained_audio.clips[0].wav)).unwrap();
    assert_eq!(wav.spec().sample_rate, 48000);
    let idle = session.observe(0, false, &[], Duration::ZERO).await;
    let waiting = Instant::now();
    let quiet = session
        .observe(idle.cursor, false, &[], Duration::from_millis(180))
        .await;
    assert!(waiting.elapsed() >= Duration::from_millis(150));
    assert_eq!(quiet.cursor, idle.cursor);
    assert!(quiet.audio[0].sequence > idle.audio[0].sequence);
    assert!(
        session
            .observe(idle.cursor, false, &[], Duration::ZERO)
            .await
            .transcripts
            .is_empty()
    );
    session.stop().await.unwrap();
    timeout(Duration::from_secs(2), provider)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(fixture.active.load(Ordering::SeqCst), 0);
    assert!(
        !session
            .observe(0, false, &[], Duration::ZERO)
            .await
            .transcription
            .active
    );
}

#[cfg(feature = "test-support")]
mod storage_failures {
    use super::*;
    use liazo_runtime::{
        archive::{ArchiveLimits, ArchivePhase},
        history::GapReason,
    };

    async fn setup() -> (
        Arc<Fixture>,
        ReceiverSession,
        tokio::task::JoinHandle<Result<(), liazo_runtime::RuntimeError>>,
        tempfile::TempDir,
    ) {
        let fixture = fixture(vec![], true);
        let (session, provider) = pair(fixture.clone()).await;
        let archive = tempfile::tempdir().unwrap();
        session
            .start_archive(
                archive.path().to_owned(),
                ArchiveLimits {
                    duration_ms: 60_000,
                    max_bytes: 4 * 1024 * 1024,
                },
            )
            .await
            .unwrap();
        session
            .configure(CaptureSettings {
                images: vec![ImageSubscription {
                    source: source(false),
                    interval_ms: 50,
                    max_width: None,
                    crop: None,
                    format: ImageFormat::Png,
                    quality: 90,
                    include_cursor: false,
                }],
                audio: vec![source(true)],
            })
            .await
            .unwrap();
        session
            .configure_raw_audio(vec![source(true)])
            .await
            .unwrap();
        fixture.gate.add_permits(1);
        progress(&session, 0, 0).await;
        session.flush_archive().await.unwrap();
        (fixture, session, provider, archive)
    }

    async fn progress(
        session: &ReceiverSession,
        image: u64,
        audio: u64,
    ) -> liazo_runtime::Observation {
        timeout(Duration::from_secs(3), async {
            loop {
                let observed = session.observe(0, true, &[], Duration::ZERO).await;
                if observed
                    .images
                    .first()
                    .is_some_and(|value| value.metadata.sequence > image)
                    && observed
                        .audio
                        .first()
                        .is_some_and(|value| value.sequence > audio)
                {
                    assert_eq!(observed.gaps, 0);
                    assert!(observed.images[0].payload.is_some());
                    return observed;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("live image and audio must progress independently of disk")
    }

    fn ordered_gap(directory: &std::path::Path, reason: GapReason) {
        let database = rusqlite::Connection::open_with_flags(
            directory.join("index.sqlite3"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let (first, end): (u64, u64) = database.query_row(
            "SELECT first_ns,end_ns FROM gaps WHERE source=?1 AND reason=?2 ORDER BY first_ns LIMIT 1",
            rusqlite::params![source(false).id, serde_json::to_string(&reason).unwrap()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).expect("the missing image interval must close after recovery");
        assert!(first < end);
        let preceding: u64 = database
            .query_row(
                "SELECT MAX(last_ns) FROM images WHERE source=?1 AND last_ns<?2",
                rusqlite::params![source(false).id, first],
                |row| row.get(0),
            )
            .expect("older accepted queued images must survive before the loss marker");
        assert!(preceding < first);
        let inside: u64 = database
            .query_row(
                "SELECT COUNT(*) FROM images WHERE source=?1 AND first_ns>=?2 AND first_ns<?3",
                rusqlite::params![source(false).id, first, end],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            inside, 0,
            "queued pre-gap data cannot be reclassified inside the later missing interval"
        );
    }

    async fn finish(
        session: ReceiverSession,
        provider: tokio::task::JoinHandle<Result<(), liazo_runtime::RuntimeError>>,
        fixture: Arc<Fixture>,
    ) {
        session.stop().await.unwrap();
        timeout(Duration::from_secs(3), provider)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fixture.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stalled_archive_preserves_live_tls_media_and_source_ordered_backpressure_gaps() {
        let (fixture, session, provider, archive) = setup().await;
        let before = session.observe(0, false, &[], Duration::ZERO).await;
        let held = session.hold_archive().await.unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                if session.archive_status().unwrap().dropped_records > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the bounded archive queue must report overflow while its writer is held");
        let after = progress(
            &session,
            before.images[0].metadata.sequence,
            before.audio[0].sequence,
        )
        .await;
        let status = after.archive.unwrap();
        assert!(status.dropped_records > 0);
        assert!((1..=64 * 1024 * 1024).contains(&status.queued_bytes));
        let fresh = timeout(
            Duration::from_secs(2),
            session.capture_now(vec![source(false)]),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(fresh.images[0].metadata.sequence > before.images[0].metadata.sequence);
        drop(held);
        session.flush_archive().await.unwrap();
        session.capture_now(vec![source(false)]).await.unwrap();
        session.flush_archive().await.unwrap();
        ordered_gap(archive.path(), GapReason::ArchiveBackpressure);
        let audio = session
            .history_audio(session.elapsed(), Duration::ZERO, &[source(true)])
            .await
            .unwrap();
        assert!(
            audio
                .gaps
                .iter()
                .any(|gap| gap.reason == GapReason::ArchiveBackpressure)
        );
        let recovered = session.archive_status().unwrap();
        assert_eq!(recovered.phase, ArchivePhase::Recording);
        assert!(recovered.queued_bytes <= 64 * 1024 * 1024);
        finish(session, provider, fixture).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sqlite_full_does_not_block_live_tls_media_and_recovery_keeps_a_real_failure_gap() {
        let (fixture, session, provider, archive) = setup().await;
        let before = session.observe(0, false, &[], Duration::ZERO).await;
        session.set_archive_database_full(true).await.unwrap();
        timeout(Duration::from_secs(4), async {
            loop {
                let status = session.archive_status().unwrap();
                if status.phase == ArchivePhase::Failed && status.dropped_records > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the actual SQLite page limit must fail an archive write");
        progress(
            &session,
            before.images[0].metadata.sequence,
            before.audio[0].sequence,
        )
        .await;
        let fresh = timeout(
            Duration::from_secs(2),
            session.capture_now(vec![source(false)]),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(fresh.images[0].metadata.sequence > before.images[0].metadata.sequence);
        session.set_archive_database_full(false).await.unwrap();
        progress(
            &session,
            fresh.images[0].metadata.sequence,
            before.audio[0].sequence,
        )
        .await;
        session.flush_archive().await.unwrap();
        ordered_gap(archive.path(), GapReason::ArchiveFailure);
        assert_eq!(
            session.archive_status().unwrap().phase,
            ArchivePhase::Recording
        );
        finish(session, provider, fixture).await;
    }
}
