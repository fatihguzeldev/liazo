use super::{
    ArchiveError, ArchiveIdentity, ArchiveIndex, ArchiveLimits, ArchiveStats, ReceiverTimeRange,
    TranscriptPage,
};
use crate::history::{GapReason, HistoricalImage};
use bytes::Bytes;
use liazo_core::{protocol::SourceKey, transcript::Transcript, wire::ImageMeta};
use serde::Serialize;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use uuid::Uuid;

const BUDGET_KIB: usize = 64 * 1024;
const QUEUE: usize = 32;
const MAX_PENDING_SOURCES: usize = 64;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArchivePhase {
    Recording,
    Failed,
    Stopping,
    Stopped,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArchiveStatus {
    pub phase: ArchivePhase,
    pub queued_bytes: u64,
    pub dropped_records: u64,
    pub stats: Option<ArchiveStats>,
}

pub(crate) enum ArchiveRecord {
    Image {
        metadata: ImageMeta,
        payload_id: Uuid,
        bytes: Bytes,
        arrived_ns: u64,
        max_gap_ns: u64,
    },
    Gap {
        source: SourceKey,
        at: u64,
        reason: GapReason,
    },
    Transcript {
        transcript: Transcript,
        received_ns: u64,
        range: Option<ReceiverTimeRange>,
    },
}
impl ArchiveRecord {
    fn source(&self) -> &SourceKey {
        match self {
            Self::Image { metadata, .. } => &metadata.source,
            Self::Gap { source, .. } => source,
            Self::Transcript { transcript, .. } => &transcript.source,
        }
    }
    fn time(&self) -> u64 {
        match self {
            Self::Image { arrived_ns, .. } => *arrived_ns,
            Self::Gap { at, .. } => *at,
            Self::Transcript { received_ns, .. } => *received_ns,
        }
    }
    fn bytes(&self) -> usize {
        2048 + match self {
            Self::Image { bytes, .. } => bytes.len(),
            Self::Transcript { transcript, .. } => transcript.text.len(),
            _ => 0,
        }
    }
    fn write(self, index: &mut ArchiveIndex) -> Result<(), ArchiveError> {
        match self {
            Self::Image {
                metadata,
                payload_id,
                bytes,
                arrived_ns,
                max_gap_ns,
            } => index.record_image(metadata, payload_id, bytes, arrived_ns, max_gap_ns),
            Self::Gap { source, at, reason } => index.record_gap(&source, at, reason),
            Self::Transcript {
                transcript,
                received_ns,
                range,
            } => index.record_transcript(transcript, received_ns, range),
        }
    }
}

#[derive(Clone, Copy)]
struct Span {
    first: u64,
    last: u64,
}
impl Span {
    fn merge(self, other: Self) -> Self {
        Self {
            first: self.first.min(other.first),
            last: self.last.max(other.last),
        }
    }
}
#[derive(Default)]
struct Missing {
    sources: HashMap<SourceKey, Span>,
    global: Option<Span>,
}
impl Missing {
    fn add(&mut self, source: SourceKey, span: Span) {
        if self.sources.contains_key(&source) || self.sources.len() < MAX_PENDING_SOURCES {
            self.sources
                .entry(source)
                .and_modify(|old| *old = old.merge(span))
                .or_insert(span);
        } else {
            self.global = Some(self.global.map_or(span, |old| old.merge(span)));
        }
    }
    fn restore(&mut self, other: Self) {
        for (source, span) in other.sources {
            self.add(source, span);
        }
        if let Some(span) = other.global {
            self.global = Some(self.global.map_or(span, |old| old.merge(span)));
        }
    }
    fn persist(&self, index: &mut ArchiveIndex, reason: GapReason) -> Result<(), ArchiveError> {
        for (source, span) in &self.sources {
            index.record_gap(source, span.first, reason)?;
        }
        if let Some(span) = self.global {
            index.record_global_gap(span.first, reason)?;
        }
        Ok(())
    }
}

struct Control {
    sealed: AtomicBool,
    ended_at: AtomicU64,
    clock: Instant,
    closing: AtomicBool,
    abort: AtomicBool,
    dropped: AtomicU64,
    budget: Arc<Semaphore>,
    missing: Mutex<Missing>,
    status: Mutex<ArchiveStatus>,
}
enum Command {
    Record {
        record: ArchiveRecord,
        prior: Option<Span>,
        global: Option<Span>,
        _budget: OwnedSemaphorePermit,
    },
    Tick {
        seal: bool,
        now: u64,
        missing: Missing,
        reply: Option<oneshot::Sender<Result<(), ArchiveError>>>,
    },
    Image {
        source: SourceKey,
        at: u64,
        reply: oneshot::Sender<Result<HistoricalImage, ArchiveError>>,
    },
    Transcripts {
        first: u64,
        last: u64,
        selected: Vec<SourceKey>,
        reply: oneshot::Sender<Result<TranscriptPage, ArchiveError>>,
    },
    Wake,
}

#[derive(Clone)]
pub struct ArchiveClient {
    sender: mpsc::Sender<Command>,
    control: Arc<Control>,
}
pub struct ArchiveWorker {
    client: ArchiveClient,
    done: Option<oneshot::Receiver<()>>,
    thread: Option<JoinHandle<()>>,
    ticker: tokio::task::JoinHandle<()>,
}

impl ArchiveClient {
    pub fn status(&self) -> ArchiveStatus {
        let mut status = self
            .control
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        status.queued_bytes = (BUDGET_KIB - self.control.budget.available_permits()) as u64 * 1024;
        status.dropped_records = self.control.dropped.load(Ordering::Acquire);
        status
    }
    pub(crate) fn submit(&self, record: ArchiveRecord) {
        if self.control.closing.load(Ordering::Acquire) {
            return;
        }
        let source = record.source().clone();
        let at = record.time();
        let count = record.bytes().div_ceil(1024).min(u32::MAX as usize) as u32;
        let mut missing = self
            .control
            .missing
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.control.sealed.load(Ordering::Acquire)
            || self.control.closing.load(Ordering::Acquire)
        {
            return;
        }
        let Ok(budget) = self.control.budget.clone().try_acquire_many_owned(count) else {
            missing.add(
                source,
                Span {
                    first: at,
                    last: at,
                },
            );
            self.control.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let prior = missing.sources.remove(&source);
        let global = missing.global.take();
        let command = Command::Record {
            record,
            prior,
            global,
            _budget: budget,
        };
        if let Err(error) = self.sender.try_send(command) {
            if let Command::Record { prior, global, .. } = error.into_inner() {
                missing.add(
                    source,
                    prior.map_or(
                        Span {
                            first: at,
                            last: at,
                        },
                        |span| {
                            span.merge(Span {
                                first: at,
                                last: at,
                            })
                        },
                    ),
                );
                if let Some(span) = global {
                    missing.global = Some(missing.global.map_or(span, |old| old.merge(span)));
                }
            }
            self.control.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn tick(&self, now: u64) {
        if self.control.closing.load(Ordering::Acquire) {
            return;
        }
        let Ok(permit) = self.sender.try_reserve() else {
            return;
        };
        let mut missing = self
            .control
            .missing
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        permit.send(Command::Tick {
            seal: false,
            now,
            missing: std::mem::take(&mut *missing),
            reply: None,
        });
    }
    pub async fn flush(&self) -> Result<(), ArchiveError> {
        self.barrier(false).await
    }
    pub async fn seal(&self) -> Result<(), ArchiveError> {
        self.barrier(true).await
    }
    async fn barrier(&self, seal: bool) -> Result<(), ArchiveError> {
        if self.control.closing.load(Ordering::Acquire) {
            return Err(ArchiveError::Stopped);
        }
        let permit = tokio::time::timeout(Duration::from_secs(5), self.sender.reserve())
            .await
            .map_err(|_| ArchiveError::Busy)?
            .map_err(|_| ArchiveError::Stopped)?;
        let (reply, done) = oneshot::channel();
        {
            let mut missing = self
                .control
                .missing
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let current = self
                .control
                .clock
                .elapsed()
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64;
            let now = if seal {
                if !self.control.sealed.swap(true, Ordering::AcqRel) {
                    self.control.ended_at.store(current, Ordering::Release);
                }
                self.control.ended_at.load(Ordering::Acquire)
            } else {
                current
            };
            permit.send(Command::Tick {
                now,
                seal,
                missing: std::mem::take(&mut *missing),
                reply: Some(reply),
            });
        }
        tokio::time::timeout(Duration::from_secs(5), done)
            .await
            .map_err(|_| ArchiveError::Busy)?
            .map_err(|_| ArchiveError::Stopped)?
    }
    pub async fn image(&self, source: SourceKey, at: u64) -> Result<HistoricalImage, ArchiveError> {
        if self.control.closing.load(Ordering::Acquire) {
            return Err(ArchiveError::Stopped);
        }
        let (reply, result) = oneshot::channel();
        self.sender
            .try_send(Command::Image { source, at, reply })
            .map_err(|_| ArchiveError::Busy)?;
        tokio::time::timeout(Duration::from_secs(3), result)
            .await
            .map_err(|_| ArchiveError::Busy)?
            .map_err(|_| ArchiveError::Stopped)?
    }
    pub async fn transcripts(
        &self,
        first: u64,
        last: u64,
        selected: Vec<SourceKey>,
    ) -> Result<TranscriptPage, ArchiveError> {
        if self.control.closing.load(Ordering::Acquire) {
            return Err(ArchiveError::Stopped);
        }
        let (reply, result) = oneshot::channel();
        self.sender
            .try_send(Command::Transcripts {
                first,
                last,
                selected,
                reply,
            })
            .map_err(|_| ArchiveError::Busy)?;
        tokio::time::timeout(Duration::from_secs(3), result)
            .await
            .map_err(|_| ArchiveError::Busy)?
            .map_err(|_| ArchiveError::Stopped)?
    }
}

impl ArchiveWorker {
    pub async fn start(
        root: PathBuf,
        identity: ArchiveIdentity,
        limits: ArchiveLimits,
        clock: Instant,
    ) -> Result<Self, ArchiveError> {
        let (sender, receiver) = mpsc::channel(QUEUE);
        let control = Arc::new(Control {
            sealed: AtomicBool::new(false),
            ended_at: AtomicU64::new(0),
            clock,
            closing: AtomicBool::new(false),
            abort: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
            budget: Arc::new(Semaphore::new(BUDGET_KIB)),
            missing: Mutex::new(Missing::default()),
            status: Mutex::new(ArchiveStatus {
                phase: ArchivePhase::Recording,
                queued_bytes: 0,
                dropped_records: 0,
                stats: None,
            }),
        });
        let client = ArchiveClient {
            sender,
            control: control.clone(),
        };
        let (started, ready) = oneshot::channel();
        let (completed, done) = oneshot::channel();
        let worker_control = control.clone();
        let thread = std::thread::Builder::new()
            .name("liazo-archive".into())
            .spawn(move || {
                let mut index =
                    match ArchiveIndex::open(&root, identity, limits).and_then(|mut index| {
                        index.begin()?;
                        Ok(index)
                    }) {
                        Ok(index) => {
                            let _ = started.send(Ok(()));
                            index
                        }
                        Err(error) => {
                            let _ = started.send(Err(error));
                            let _ = completed.send(());
                            return;
                        }
                    };
                run(&mut index, receiver, &worker_control, clock);
                drop(index);
                let _ = completed.send(());
            })?;
        let ticks = client.clone();
        let ticker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if ticks.control.closing.load(Ordering::Acquire) {
                    break;
                }
                ticks.tick(clock.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
            }
        });
        let worker = Self {
            client,
            done: Some(done),
            thread: Some(thread),
            ticker,
        };
        ready.await.map_err(|_| ArchiveError::Stopped)??;
        Ok(worker)
    }
    pub fn client(&self) -> ArchiveClient {
        self.client.clone()
    }
    pub async fn stop(&mut self) -> bool {
        self.ticker.abort();
        {
            let _missing = self
                .client
                .control
                .missing
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.client.control.closing.store(true, Ordering::Release);
            if !self.client.control.sealed.load(Ordering::Acquire) {
                self.client.control.ended_at.store(
                    self.client
                        .control
                        .clock
                        .elapsed()
                        .as_nanos()
                        .min(u128::from(u64::MAX)) as u64,
                    Ordering::Release,
                );
            }
        }
        self.client
            .control
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .phase = ArchivePhase::Stopping;
        let _ = self.client.sender.try_send(Command::Wake);
        if let Some(done) = &mut self.done {
            if tokio::time::timeout(Duration::from_secs(5), done)
                .await
                .is_err()
            {
                self.client.control.abort.store(true, Ordering::Release);
                return false;
            }
            self.done = None;
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        true
    }
}
impl Drop for ArchiveWorker {
    fn drop(&mut self) {
        self.ticker.abort();
        self.client.control.closing.store(true, Ordering::Release);
        self.client.control.abort.store(true, Ordering::Release);
        let _ = self.client.sender.try_send(Command::Wake);
    }
}

fn run(
    index: &mut ArchiveIndex,
    mut receiver: mpsc::Receiver<Command>,
    control: &Control,
    clock: Instant,
) {
    let mut failed = Missing::default();
    loop {
        if control.abort.load(Ordering::Acquire) {
            break;
        }
        let command = if control.closing.load(Ordering::Acquire) {
            receiver.try_recv().ok()
        } else {
            receiver.blocking_recv()
        };
        let Some(command) = command else {
            let pending =
                std::mem::take(&mut *control.missing.lock().unwrap_or_else(|e| e.into_inner()));
            let result = pending
                .persist(index, GapReason::ArchiveBackpressure)
                .and_then(|()| failed.persist(index, GapReason::ArchiveFailure))
                .and_then(|()| {
                    index
                        .enforce_limits(clock.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64)
                        .map(|_| ())
                })
                .and_then(|()| {
                    if control.sealed.load(Ordering::Acquire) {
                        Ok(())
                    } else {
                        index.finish(control.ended_at.load(Ordering::Acquire))
                    }
                });
            let stats = index.stats().ok();
            let mut status = control.status.lock().unwrap_or_else(|e| e.into_inner());
            status.phase = if result.is_ok() {
                ArchivePhase::Stopped
            } else {
                ArchivePhase::Failed
            };
            status.stats = stats;
            return;
        };
        let result = match command {
            Command::Record {
                record,
                prior,
                global,
                _budget,
            } => {
                let source = record.source().clone();
                let time = record.time();
                let mut missing = Missing::default();
                if let Some(span) = prior {
                    missing.add(source.clone(), span);
                }
                missing.global = global;
                let result = failed
                    .persist(index, GapReason::ArchiveFailure)
                    .and_then(|()| missing.persist(index, GapReason::ArchiveBackpressure))
                    .and_then(|()| record.write(index));
                if result.is_err() {
                    failed.restore(missing);
                    failed.add(
                        source,
                        Span {
                            first: time,
                            last: time,
                        },
                    );
                    control.dropped.fetch_add(1, Ordering::Relaxed);
                } else {
                    failed = Missing::default();
                }
                drop(_budget);
                result
            }
            Command::Tick {
                seal,
                now,
                missing,
                reply,
            } => {
                let result = failed
                    .persist(index, GapReason::ArchiveFailure)
                    .and_then(|()| missing.persist(index, GapReason::ArchiveBackpressure))
                    .and_then(|()| index.enforce_limits(now).map(|_| ()))
                    .and_then(|()| if seal { index.finish(now) } else { Ok(()) });
                let ok = result.is_ok();
                if ok {
                    failed = Missing::default();
                } else {
                    failed.restore(missing);
                }
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
                if ok {
                    Ok(())
                } else {
                    Err(ArchiveError::Stopped)
                }
            }
            Command::Image { source, at, reply } => {
                if !reply.is_closed() {
                    let _ = reply.send(index.at(&source, at));
                }
                continue;
            }
            Command::Transcripts {
                first,
                last,
                selected,
                reply,
            } => {
                if !reply.is_closed() {
                    let _ = reply.send(index.transcripts_between(first, last, &selected));
                }
                continue;
            }
            Command::Wake => continue,
        };
        let stats = index.stats().ok();
        let mut status = control.status.lock().unwrap_or_else(|e| e.into_inner());
        status.phase = if result.is_ok() {
            if control.sealed.load(Ordering::Acquire) {
                ArchivePhase::Stopped
            } else {
                ArchivePhase::Recording
            }
        } else {
            ArchivePhase::Failed
        };
        status.stats = stats;
    }
    control
        .status
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .phase = ArchivePhase::Failed;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::HistoryState;
    use liazo_core::protocol::ImageFormat;
    use liazo_transport::{Identity, SessionBinding};

    fn source() -> SourceKey {
        SourceKey {
            id: "display:fixture".into(),
            generation: Uuid::new_v4(),
        }
    }

    #[tokio::test]
    async fn queue_pressure_is_bounded_and_marks_loss_before_the_next_record() {
        let (sender, mut receiver) = mpsc::channel(1);
        let control = Arc::new(Control {
            clock: Instant::now(),
            sealed: AtomicBool::new(false),
            ended_at: AtomicU64::new(0),
            closing: AtomicBool::new(false),
            abort: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
            budget: Arc::new(Semaphore::new(4)),
            missing: Mutex::new(Missing::default()),
            status: Mutex::new(ArchiveStatus {
                phase: ArchivePhase::Recording,
                queued_bytes: 0,
                dropped_records: 0,
                stats: None,
            }),
        });
        let client = ArchiveClient { sender, control };
        let source = source();
        let record = |at| ArchiveRecord::Gap {
            source: source.clone(),
            at,
            reason: GapReason::Unavailable,
        };
        client.submit(record(1));
        client.submit(record(2));
        assert_eq!(client.status().dropped_records, 1);
        assert_eq!(client.control.budget.available_permits(), 2);
        match receiver.recv().await.unwrap() {
            Command::Record { prior, record, .. } => {
                assert!(prior.is_none());
                assert_eq!(record.time(), 1);
            }
            _ => panic!("expected first record"),
        }
        client.submit(record(3));
        match receiver.recv().await.unwrap() {
            Command::Record {
                prior: Some(span),
                record,
                ..
            } => {
                assert_eq!((span.first, span.last), (2, 2));
                assert_eq!(record.time(), 3);
            }
            _ => panic!("expected bounded loss marker before recovery"),
        }
        assert_eq!(client.control.budget.available_permits(), 4);
        assert!(client.control.missing.lock().unwrap().sources.is_empty());
    }

    #[tokio::test]
    async fn idle_maintenance_expires_archives_and_sealing_keeps_reads_available() {
        let root = tempfile::tempdir().unwrap();
        let identity = ArchiveIdentity {
            provider: Identity::generate().unwrap().fingerprint(),
            binding: SessionBinding {
                session: Uuid::new_v4(),
                epoch: Uuid::new_v4(),
            },
            wall_anchor_unix_ms: 1_700_000_000_000,
        };
        let clock = Instant::now();
        let mut worker = ArchiveWorker::start(
            root.path().to_owned(),
            identity,
            ArchiveLimits {
                duration_ms: 60_000,
                max_bytes: 4 * 1024 * 1024,
            },
            clock,
        )
        .await
        .unwrap();
        let client = worker.client();
        let source = source();
        let at = clock.elapsed().as_nanos() as u64;
        client.submit(ArchiveRecord::Image {
            metadata: ImageMeta {
                source: source.clone(),
                sequence: 1,
                configuration_revision: 1,
                capture_started_ns: 1,
                captured_ns: 2,
                width: 1,
                height: 1,
                format: ImageFormat::Png,
                fingerprint: [7; 32],
            },
            payload_id: Uuid::new_v4(),
            bytes: Bytes::from_static(b"fixture-image"),
            arrived_ns: at,
            max_gap_ns: 1_000_000_000,
        });
        client.flush().await.unwrap();
        assert_eq!(
            client.image(source.clone(), at).await.unwrap().state,
            HistoryState::Available
        );
        client.seal().await.unwrap();
        client.seal().await.unwrap();
        assert_eq!(
            client.image(source.clone(), at).await.unwrap().state,
            HistoryState::Available
        );
        // Drive the idle maintenance path with epoch time, independently of fsync latency.
        client.tick(at + 60_001_000_000);
        client.flush().await.unwrap();
        assert_eq!(client.status().stats.unwrap().payloads, 0);
        assert!(matches!(
            client.image(source.clone(), at).await.unwrap().state,
            HistoryState::Expired | HistoryState::NotRetained
        ));
        assert!(worker.stop().await);
        assert!(matches!(
            client.image(source, at).await,
            Err(ArchiveError::Stopped)
        ));
    }
}
