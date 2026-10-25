use crate::{
    RuntimeError,
    live::{ClockMap, ConnectionState, LiveState, Observation},
};
use bytes::Bytes;
use liazo_core::{
    catalog::SourceSnapshot,
    control::{Command, Event, Failure, FailureCode, ReplyValue, Request},
    protocol::{CaptureSettings, PROTOCOL_VERSION, SourceKey},
    wire::{Frame, Header},
};
use liazo_transport::{Fingerprint, PeerClient, PeerSession};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Notify, Semaphore, broadcast, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::{Instant as Deadline, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod archive;

struct Pending {
    command: Command,
    reply: oneshot::Sender<ReplyValue>,
}
struct Router {
    next_id: u64,
    pending: HashMap<u64, Pending>,
}
pub(crate) struct Shared {
    provider: Fingerprint,
    wall_anchor_unix_ms: u64,
    pub(crate) state: Mutex<LiveState>,
    router: Mutex<Router>,
    sender: mpsc::Sender<Frame>,
    permits: Semaphore,
    changed: Notify,
    sources_dirty: Notify,
    pub(crate) cancelled: CancellationToken,
    clock: Instant,
    pub(crate) audio: broadcast::Sender<Frame>,
}

impl Shared {
    pub(crate) fn now_ns(&self) -> u64 {
        self.clock.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }
    pub(crate) fn signal(&self) {
        self.changed.notify_waiters();
    }
}

struct PendingGuard {
    shared: Arc<Shared>,
    id: u64,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.shared
            .router
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending
            .remove(&self.id);
    }
}

struct Owner {
    archive: tokio::sync::Mutex<Option<crate::archive::ArchiveWorker>>,
    cleanup: JoinHandle<()>,
    speech: tokio::sync::Mutex<Option<crate::speech::SpeechTask>>,
    shared: Arc<Shared>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    config: tokio::sync::Mutex<()>,
    stop: tokio::sync::Mutex<()>,
    operations: Arc<Semaphore>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.cancelled.cancel();
        self.cleanup.abort();
    }
}

#[derive(Clone)]
pub struct ReceiverSession(Arc<Owner>);

impl ReceiverSession {
    pub async fn connect(
        client: &PeerClient,
        address: SocketAddr,
        session: Uuid,
    ) -> Result<Self, RuntimeError> {
        Self::attach(PeerSession::connect(client, address, session).await?).await
    }

    pub async fn attach(peer: PeerSession) -> Result<Self, RuntimeError> {
        let provider = peer.control.fingerprint();
        if peer.images.fingerprint() != provider || peer.audio.fingerprint() != provider {
            return Err(RuntimeError::Protocol);
        }
        let clock = Instant::now();
        let wall_anchor_unix_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| failure(FailureCode::Internal, "system clock is unavailable"))?
                .as_millis(),
        )
        .map_err(|_| failure(FailureCode::Internal, "system clock is out of range"))?;
        let (sender, mut outgoing) = mpsc::channel::<Frame>(32);
        let (audio, _) = broadcast::channel(32);
        let shared = Arc::new(Shared {
            provider,
            wall_anchor_unix_ms,
            state: Mutex::new(LiveState::new(peer.binding)),
            router: Mutex::new(Router {
                next_id: 1,
                pending: HashMap::new(),
            }),
            sender,
            permits: Semaphore::new(16),
            changed: Notify::new(),
            sources_dirty: Notify::new(),
            cancelled: CancellationToken::new(),
            clock,
            audio,
        });
        let (mut control_reader, mut control_writer) = tokio::io::split(peer.control);
        let mut tasks: JoinSet<Result<(), RuntimeError>> = JoinSet::new();
        let reader = shared.clone();
        tasks.spawn(async move {
            loop {
                let frame = tokio::select! { _ = reader.cancelled.cancelled() => return Ok(()), frame = Frame::read_from(&mut control_reader) => frame? };
                match frame.map(|frame| frame.header) {
                    Some(Header::Reply(reply)) => {
                        let pending = reader.router.lock().unwrap_or_else(|e| e.into_inner()).pending.remove(&reply.id);
                        if let Some(pending) = pending {
                            if !reply_matches(&pending.command, &reply.result) { return Err(RuntimeError::Protocol); }
                            {
                                let mut state = reader.state.lock().unwrap_or_else(|e| e.into_inner());
                                match &reply.result {
                                    ReplyValue::Applied { revision, settings } => state.applied(*revision, settings.clone(), reader.now_ns()),
                                    ReplyValue::Stopped => {
                                        state.connection = ConnectionState::Stopped;
                                        state.connection_ended(reader.now_ns());
                                        state.changed();
                                        let receipt = Frame::new(Header::Pong { nonce: reply.id }, Bytes::new())?;
                                        reader.sender.try_send(receipt).map_err(|_| RuntimeError::Closed)?;
                                    },
                                    _ => {},
                                }
                            }
                            let _ = pending.reply.send(reply.result);
                            reader.signal();
                        }
                    }
                    Some(Header::Event(Event::CatalogChanged { .. })) => reader.sources_dirty.notify_one(),
                    Some(Header::Event(event)) => {
                        reader.state.lock().unwrap_or_else(|e| e.into_inner()).event(event, reader.now_ns());
                        reader.signal();
                    }
                    _ => return Err(RuntimeError::Protocol),
                }
            }
        });
        let writer = shared.clone();
        tasks.spawn(async move {
            loop {
                let frame = tokio::select! { _ = writer.cancelled.cancelled() => return Ok(()), frame = outgoing.recv() => { let Some(frame) = frame else { return Ok(()); }; frame } };
                timeout(Duration::from_secs(5), frame.write_to(&mut control_writer)).await.map_err(|_| failure(FailureCode::Timeout, "provider request stalled"))??;
            }
        });
        let images = shared.clone();
        tasks.spawn(async move {
            let mut stream = peer.images;
            loop {
                let frame = tokio::select! { _ = images.cancelled.cancelled() => return Ok(()), frame = Frame::read_from(&mut stream) => frame? };
                images.state.lock().unwrap_or_else(|e| e.into_inner()).image(frame.ok_or(RuntimeError::Closed)?, images.now_ns()).map_err(|_| RuntimeError::Protocol)?;
                images.signal();
            }
        });
        let audio = shared.clone();
        tasks.spawn(async move {
            let mut stream = peer.audio;
            loop {
                let mut frame = tokio::select! { _ = audio.cancelled.cancelled() => return Ok(()), frame = Frame::read_from(&mut stream) => frame?.ok_or(RuntimeError::Closed)? };
                let (selected, significant) = audio.state.lock().unwrap_or_else(|e| e.into_inner()).audio(&mut frame).map_err(|_| RuntimeError::Protocol)?;
                if selected { let _ = audio.audio.send(frame); }
                if significant { audio.signal(); }
            }
        });
        let refresh = shared.clone();
        tasks.spawn(async move {
            loop {
                let ready = refresh.changed.notified();
                tokio::pin!(ready);
                ready.as_mut().enable();
                if refresh.state.lock().unwrap_or_else(|e| e.into_inner()).connection == ConnectionState::Connected { break; }
                tokio::select! { _ = refresh.cancelled.cancelled() => return Ok(()), _ = ready => {} }
            }
            let mut clock = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = refresh.cancelled.cancelled() => return Ok(()),
                    _ = refresh.sources_dirty.notified() => { fetch_sources(&refresh).await?; },
                    _ = clock.tick() => { sync_clock(&refresh).await?; },
                }
            }
        });
        let supervisor_state = shared.clone();
        let supervisor = tokio::spawn(async move {
            let _ = tasks.join_next().await;
            supervisor_state.cancelled.cancel();
            {
                let mut state = supervisor_state
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if state.connection != ConnectionState::Stopped {
                    state.connection = ConnectionState::Disconnected;
                    state.gaps += 1;
                }
                state.connection_ended(supervisor_state.now_ns());
                state.changed();
            }
            supervisor_state
                .router
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pending
                .clear();
            supervisor_state.signal();
            while tasks.join_next().await.is_some() {}
        });
        let retention = shared.clone();
        let cleanup = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_millis(250));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                let mut state = retention.state.lock().unwrap_or_else(|e| e.into_inner());
                let now = retention.now_ns();
                state.history.trim(now);
                state.transcripts.trim(now);
            }
        });
        let receiver = Self(Arc::new(Owner {
            archive: tokio::sync::Mutex::new(None),
            cleanup,
            speech: tokio::sync::Mutex::new(None),
            shared,
            supervisor: Mutex::new(Some(supervisor)),
            config: tokio::sync::Mutex::new(()),
            stop: tokio::sync::Mutex::new(()),
            operations: Arc::new(Semaphore::new(8)),
        }));
        let hello = rpc(
            &receiver.0.shared,
            Command::Hello {
                version: PROTOCOL_VERSION,
            },
        )
        .await?;
        if !matches!(
            hello,
            ReplyValue::Hello {
                version: PROTOCOL_VERSION,
                ..
            }
        ) {
            return Err(RuntimeError::Protocol);
        }
        sync_clock(&receiver.0.shared).await?;
        fetch_sources(&receiver.0.shared).await?;
        {
            let mut state = receiver
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.connection == ConnectionState::Disconnected {
                return Err(RuntimeError::Closed);
            }
            state.connection = ConnectionState::Connected;
            state.changed();
        }
        receiver.0.shared.signal();
        Ok(receiver)
    }

    pub fn sources(&self) -> SourceSnapshot {
        self.0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sources
            .clone()
    }
    pub fn settings(&self) -> CaptureSettings {
        self.0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .settings
            .clone()
    }
    pub fn subscribe_audio(&self) -> broadcast::Receiver<Frame> {
        self.0.shared.audio.subscribe()
    }

    pub fn archive_identity(&self) -> crate::archive::ArchiveIdentity {
        crate::archive::ArchiveIdentity {
            binding: self
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .binding,
            provider: self.0.shared.provider,
            wall_anchor_unix_ms: self.0.shared.wall_anchor_unix_ms,
        }
    }

    pub fn configure_history(
        &self,
        limits: crate::history::HistoryLimits,
    ) -> Result<crate::history::HistoryLimits, RuntimeError> {
        let mut state = self
            .0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state
            .history
            .configure(limits, self.0.shared.now_ns())
            .map_err(|message| failure(FailureCode::InvalidRequest, message))?;
        state.changed();
        drop(state);
        self.0.shared.signal();
        Ok(limits)
    }

    pub fn history_stats(&self) -> crate::history::HistoryStats {
        self.0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .history
            .stats()
    }

    pub async fn history_at(
        &self,
        age: Duration,
        selected: &[SourceKey],
    ) -> Result<crate::history::HistorySnapshot, RuntimeError> {
        if selected.len() > 8 {
            return Err(failure(
                FailureCode::InvalidRequest,
                "select at most eight image sources",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for source in selected {
            source
                .validate()
                .map_err(|error| failure(FailureCode::InvalidRequest, &error.to_string()))?;
            if !seen.insert(source) {
                return Err(failure(
                    FailureCode::InvalidRequest,
                    "duplicate history source",
                ));
            }
        }
        let (mut snapshot, archive) = {
            let mut state = self
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let now = self.0.shared.now_ns();
            let ago = u64::try_from(age.as_nanos())
                .map_err(|_| failure(FailureCode::InvalidRequest, "invalid history time"))?;
            let at = now.checked_sub(ago).ok_or_else(|| {
                failure(
                    FailureCode::InvalidRequest,
                    "requested time predates this session",
                )
            })?;
            let selected: Vec<_> = if selected.is_empty() {
                state
                    .settings
                    .images
                    .iter()
                    .map(|image| image.source.clone())
                    .collect()
            } else {
                selected.to_vec()
            };
            (state.history.at(at, now, &selected), state.archive.clone())
        };
        if let Some(archive) = archive {
            use crate::history::{GapReason, HistoryState};
            if let Some(stats) = archive.status().stats {
                snapshot.retention_floor_ns =
                    snapshot.retention_floor_ns.min(stats.retention_floor_ns);
            }
            for image in &mut snapshot.images {
                if matches!(
                    image.state,
                    HistoryState::Expired
                        | HistoryState::NotRetained
                        | HistoryState::NotObserved
                        | HistoryState::Gap(GapReason::Capacity | GapReason::NoData)
                ) {
                    let stored = archive
                        .image(image.source.clone(), snapshot.requested_ns)
                        .await?;
                    if stored.state != HistoryState::NotObserved {
                        *image = stored;
                    }
                }
            }
        }
        Ok(snapshot)
    }

    pub async fn start_transcription(
        &self,
        model: liazo_transcription::WhisperModel,
        threads: u8,
        sources: Vec<SourceKey>,
        settings: liazo_transcription::streaming::StreamingSettings,
    ) -> Result<(), RuntimeError> {
        settings.validate()?;
        if sources.is_empty() || sources.len() > 8 {
            return Err(failure(
                FailureCode::InvalidRequest,
                "select between one and eight audio sources",
            ));
        }
        let permit = self
            .0
            .operations
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Closed)?;
        let receiver = self.clone();
        let (reply, result) = oneshot::channel();
        // Accepted operations finish even if their consumer disconnects.
        tokio::spawn(async move {
            let _permit = permit;
            let result = async {
                let mut speech = receiver.0.speech.lock().await;
                let _configuration = receiver.0.config.lock().await;
                crate::speech::validate_selection(&receiver.0.shared, &sources)?;
                if let Some(previous) = speech.as_mut() {
                    previous.stop().await;
                }
                speech.take();
                *speech = Some(
                    crate::speech::SpeechTask::start(
                        receiver.0.shared.clone(),
                        model,
                        threads,
                        sources,
                        settings,
                    )
                    .await?,
                );
                Ok(())
            }
            .await;
            let _ = reply.send(result);
        });
        result.await.map_err(|_| RuntimeError::Closed)?
    }

    pub async fn stop_transcription(&self) {
        let mut speech = self.0.speech.lock().await;
        if let Some(current) = speech.as_mut() {
            current.stop().await;
        }
        speech.take();
    }

    pub async fn configure(&self, settings: CaptureSettings) -> Result<u64, RuntimeError> {
        settings
            .validate()
            .map_err(|error| failure(FailureCode::InvalidRequest, &error.to_string()))?;
        let permit = self
            .0
            .operations
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Closed)?;
        let receiver = self.clone();
        let (reply, result) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let _configuration = receiver.0.config.lock().await;
            receiver
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .preparing(&settings);
            let response = rpc(&receiver.0.shared, Command::Configure(settings)).await;
            if response.is_err() {
                receiver
                    .0
                    .shared
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .config_failed();
            }
            let result = response.and_then(|reply| match reply {
                ReplyValue::Applied { revision, .. } => Ok(revision),
                _ => Err(RuntimeError::Protocol),
            });
            let _ = reply.send(result);
        });
        result.await.map_err(|_| RuntimeError::Closed)?
    }

    pub async fn observe(
        &self,
        after: u64,
        include_current: bool,
        selected: &[SourceKey],
        wait: Duration,
    ) -> Observation {
        let deadline = Deadline::now() + wait.min(Duration::from_secs(30));
        loop {
            let changed = self.0.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let snapshot = self
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .observe(after, include_current, selected, self.0.shared.now_ns());
            if snapshot.cursor > after
                || Deadline::now() >= deadline
                || matches!(
                    snapshot.connection,
                    ConnectionState::Disconnected | ConnectionState::Stopped
                )
            {
                return snapshot;
            }
            let _ = timeout_at(deadline, changed).await;
        }
    }

    pub async fn capture_now(&self, sources: Vec<SourceKey>) -> Result<Observation, RuntimeError> {
        let queued = rpc(
            &self.0.shared,
            Command::CaptureNow {
                sources: sources.clone(),
            },
        )
        .await?;
        let ReplyValue::CaptureQueued {
            revision,
            not_before_ns,
        } = queued
        else {
            return Err(RuntimeError::Protocol);
        };
        let deadline = Deadline::now() + Duration::from_secs(4);
        loop {
            let changed = self.0.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self
                    .0
                    .shared
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if state.connection != ConnectionState::Connected {
                    return Err(RuntimeError::Closed);
                }
                if state.revision != revision {
                    return Err(failure(
                        FailureCode::SourceChanged,
                        "capture settings changed",
                    ));
                }
                if let Some(failure) = state.capture_failure(&sources, not_before_ns) {
                    return Err(failure.into());
                }
                if state.fresh_after(&sources, revision, not_before_ns) {
                    return Ok(state.observe(0, true, &sources, self.0.shared.now_ns()));
                }
            }
            timeout_at(deadline, changed)
                .await
                .map_err(|_| failure(FailureCode::Timeout, "fresh capture timed out"))?;
        }
    }

    pub async fn stop(&self) -> Result<(), RuntimeError> {
        let _stopping = self.0.stop.lock().await;
        if self
            .0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .connection
            == ConnectionState::Stopped
        {
            self.stop_transcription().await;
            self.seal_archive().await?;
            return Ok(());
        }
        rpc(&self.0.shared, Command::Stop).await?;
        let supervisor = self
            .0
            .supervisor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(supervisor) = supervisor
            && timeout(Duration::from_secs(5), supervisor).await.is_err()
        {
            self.0.shared.cancelled.cancel();
        }
        self.stop_transcription().await;
        self.seal_archive().await?;
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), RuntimeError> {
        let _stopping = self.0.stop.lock().await;
        self.0.shared.cancelled.cancel();
        {
            let mut state = self
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.connection != ConnectionState::Stopped {
                state.connection = ConnectionState::Disconnected;
                state.connection_ended(self.0.shared.now_ns());
                state.changed();
            }
        }
        self.0.shared.signal();
        self.stop_transcription().await;
        self.seal_archive().await
    }
}

fn failure(code: FailureCode, message: &str) -> RuntimeError {
    Failure::new(code, message).into()
}

async fn rpc(shared: &Arc<Shared>, command: Command) -> Result<ReplyValue, RuntimeError> {
    let _permit = shared
        .permits
        .acquire()
        .await
        .map_err(|_| RuntimeError::Closed)?;
    if shared.cancelled.is_cancelled() {
        return Err(RuntimeError::Closed);
    }
    let (reply, result) = oneshot::channel();
    let guard = {
        let mut router = shared.router.lock().unwrap_or_else(|e| e.into_inner());
        let id = router.next_id;
        router.next_id = id.checked_add(1).ok_or(RuntimeError::Protocol)?;
        let frame = Frame::new(
            Header::Request(Request {
                id,
                command: command.clone(),
            }),
            Bytes::new(),
        )?;
        router.pending.insert(id, Pending { command, reply });
        if shared.sender.try_send(frame).is_err() {
            router.pending.remove(&id);
            return Err(RuntimeError::Closed);
        }
        PendingGuard {
            shared: shared.clone(),
            id,
        }
    };
    let result = timeout(Duration::from_secs(5), result)
        .await
        .map_err(|_| failure(FailureCode::Timeout, "provider request timed out"))?
        .map_err(|_| RuntimeError::Closed)?;
    drop(guard);
    match result {
        ReplyValue::Failed(failed) => Err(failed.into()),
        result => Ok(result),
    }
}

fn reply_matches(command: &Command, reply: &ReplyValue) -> bool {
    matches!(
        (command, reply),
        (_, ReplyValue::Failed(_))
            | (Command::Hello { .. }, ReplyValue::Hello { .. })
            | (Command::Sources { .. }, ReplyValue::Sources(_))
            | (Command::Configure(_), ReplyValue::Applied { .. })
            | (Command::CaptureNow { .. }, ReplyValue::CaptureQueued { .. })
            | (Command::Clock { .. }, ReplyValue::Clock { .. })
            | (Command::Stop, ReplyValue::Stopped)
    )
}

async fn sync_clock(shared: &Arc<Shared>) -> Result<(), RuntimeError> {
    let sent = shared.now_ns();
    let clock = rpc(
        shared,
        Command::Clock {
            client_send_ns: sent,
        },
    )
    .await?;
    let received = shared.now_ns();
    let ReplyValue::Clock {
        client_send_ns,
        received_ns,
        sent_ns,
    } = clock
    else {
        return Err(RuntimeError::Protocol);
    };
    if client_send_ns != sent {
        return Err(RuntimeError::Protocol);
    }
    let mapping =
        ClockMap::new(sent, received, received_ns, sent_ns).ok_or(RuntimeError::Protocol)?;
    shared.state.lock().unwrap_or_else(|e| e.into_inner()).clock = Some(mapping);
    Ok(())
}

async fn fetch_sources(shared: &Arc<Shared>) -> Result<(), RuntimeError> {
    for _ in 0..3 {
        let mut after = None;
        let mut snapshot = SourceSnapshot {
            revision: 0,
            sources: vec![],
            issues: vec![],
        };
        let mut restart = false;
        for page_index in 0..32 {
            let reply = rpc(
                shared,
                Command::Sources {
                    after: after.clone(),
                },
            )
            .await?;
            let ReplyValue::Sources(page) = reply else {
                return Err(RuntimeError::Protocol);
            };
            if page_index > 0 && page.revision != snapshot.revision {
                restart = true;
                break;
            }
            if page
                .sources
                .windows(2)
                .any(|sources| sources[0].key.id >= sources[1].key.id)
                || page.sources.first().is_some_and(|source| {
                    after.as_ref().is_some_and(|after| source.key.id <= *after)
                })
            {
                return Err(RuntimeError::Protocol);
            }
            snapshot.revision = page.revision;
            snapshot.issues = page.issues;
            snapshot.sources.extend(page.sources);
            after = page.next_after;
            if after.is_none() {
                let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
                if snapshot.revision >= state.sources.revision {
                    state.sources_changed(snapshot, shared.now_ns());
                }
                drop(state);
                shared.signal();
                return Ok(());
            }
        }
        if !restart {
            return Err(RuntimeError::Protocol);
        }
    }
    Err(failure(
        FailureCode::Busy,
        "sources changed during discovery",
    ))
}
