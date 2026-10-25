use crate::{
    RuntimeError,
    backend::CaptureBackend,
    capture::{CaptureOutput, ImageWork, ImageWorker},
    images::{LatestImages, SentImages},
};
use bytes::Bytes;
use liazo_core::{
    catalog::SourceSnapshot,
    control::{
        Command, Event, Failure, FailureCode, Reply, ReplyValue, Request, SOURCE_PAGE_SIZE,
        SourcePage,
    },
    protocol::{CaptureSettings, PROTOCOL_VERSION, SourceAvailability, SourceKey, SourceKind},
    wire::{Frame, Header},
};
use liazo_transport::PeerSession;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
    time::{MissedTickBehavior, interval, timeout},
};
use tokio_util::sync::CancellationToken;

struct Outgoing {
    frame: Frame,
    flushed: Option<oneshot::Sender<()>>,
}

enum ControlInput {
    Request(Request),
    Receipt(u64),
}

#[derive(Default)]
struct Workers {
    images: HashMap<SourceKey, ImageWorker>,
    audio: HashMap<SourceKey, CancellationToken>,
    jobs: JoinSet<()>,
}

impl Workers {
    fn apply(
        &mut self,
        settings: &CaptureSettings,
        revision: u64,
        output: &CaptureOutput,
        cancel: &CancellationToken,
    ) -> Result<(), Failure> {
        while self.jobs.try_join_next().is_some() {}
        if self.jobs.len() + settings.images.len() + settings.audio.len() > 32 {
            return Err(Failure::new(
                FailureCode::Busy,
                "capture workers are stopping",
            ));
        }
        output.images.configure(
            &settings
                .images
                .iter()
                .map(|image| image.source.clone())
                .collect::<Vec<_>>(),
            revision,
        )?;
        self.images.retain(|source, worker| {
            let keep = settings.images.iter().any(|image| image.source == *source);
            if !keep {
                worker.cancel.cancel();
            }
            keep
        });
        self.audio.retain(|source, worker| {
            let keep = settings.audio.contains(source);
            if !keep {
                worker.cancel();
                output
                    .active_audio
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(source);
            }
            keep
        });
        for image in &settings.images {
            if let Some(worker) = self.images.get(&image.source) {
                worker.work.send_modify(|work| {
                    work.image = image.clone();
                    work.revision = revision;
                });
            } else {
                let worker = output.spawn_image(
                    ImageWork {
                        image: image.clone(),
                        revision,
                        force: 0,
                    },
                    cancel.child_token(),
                    &mut self.jobs,
                );
                self.images.insert(image.source.clone(), worker);
            }
        }
        for source in &settings.audio {
            if !self.audio.contains_key(source) {
                let stopped = cancel.child_token();
                output.spawn_audio(source.clone(), revision, stopped.clone(), &mut self.jobs);
                self.audio.insert(source.clone(), stopped);
            }
        }
        Ok(())
    }

    fn capture_now(&self, sources: &[SourceKey]) -> Result<(), Failure> {
        for source in sources {
            if !self.images.contains_key(source) {
                return Err(Failure::new(
                    FailureCode::InvalidRequest,
                    "image source is not selected",
                ));
            }
        }
        for source in sources {
            self.images[source]
                .work
                .send_modify(|work| work.force = work.force.wrapping_add(1));
        }
        Ok(())
    }

    async fn stop(&mut self) {
        for worker in self.images.values() {
            worker.cancel.cancel();
        }
        for worker in self.audio.values() {
            worker.cancel();
        }
        while self.jobs.join_next().await.is_some() {}
    }
}

pub async fn serve(
    backend: Arc<dyn CaptureBackend>,
    peer: PeerSession,
    shutdown: CancellationToken,
) -> Result<(), RuntimeError> {
    let cancel = shutdown.child_token();
    let mut catalog = timeout(Duration::from_secs(3), backend.discover())
        .await
        .map_err(|_| Failure::new(FailureCode::Timeout, "source discovery timed out"))??;
    catalog.sources.sort_by(|a, b| a.key.id.cmp(&b.key.id));
    let (control_reader, mut control_writer) = tokio::io::split(peer.control);
    let (requests, mut incoming) = mpsc::channel::<ControlInput>(16);
    let (outgoing, mut control_output) = mpsc::channel::<Outgoing>(32);
    let (events, mut event_output) = mpsc::channel::<Frame>(32);
    let (audio, mut audio_output) = mpsc::channel::<Frame>(32);
    let images = Arc::new(LatestImages::new(256 * 1024 * 1024));
    let output = CaptureOutput {
        backend: backend.clone(),
        images: images.clone(),
        audio,
        events,
        active_audio: Arc::new(Mutex::new(HashMap::new())),
        permits: Arc::new(Semaphore::new(2)),
    };
    let mut io_tasks = JoinSet::new();
    let io_cancel = cancel.clone();
    io_tasks.spawn(async move {
        let mut reader = control_reader;
        loop {
            let frame = tokio::select! { _ = io_cancel.cancelled() => return Ok(()), frame = Frame::read_from(&mut reader) => frame? };
            let input = match frame.map(|frame| frame.header) {
                Some(Header::Request(request)) => ControlInput::Request(request),
                Some(Header::Pong { nonce }) => ControlInput::Receipt(nonce),
                _ => return Err(RuntimeError::Protocol),
            };
            if requests.send(input).await.is_err() { return Ok(()); }
        }
    });
    let io_cancel = cancel.clone();
    io_tasks.spawn(async move {
        loop {
            let outgoing = tokio::select! {
                biased;
                _ = io_cancel.cancelled() => return Ok(()),
                message = control_output.recv() => { let Some(message) = message else { return Ok(()); }; message },
                event = event_output.recv() => { let Some(frame) = event else { return Ok(()); }; Outgoing { frame, flushed: None } },
            };
            timeout(Duration::from_secs(5), outgoing.frame.write_to(&mut control_writer)).await
                .map_err(|_| Failure::new(FailureCode::Timeout, "control connection stalled"))??;
            if let Some(done) = outgoing.flushed { let _ = done.send(()); }
        }
    });
    let io_cancel = cancel.clone();
    io_tasks.spawn(async move {
        let mut writer = peer.images;
        let mut sent = SentImages::default();
        loop {
            let frame = tokio::select! { _ = io_cancel.cancelled() => return Ok(()), frame = images.next(&mut sent) => frame };
            timeout(Duration::from_secs(5), frame.write_to(&mut writer)).await
                .map_err(|_| Failure::new(FailureCode::Timeout, "image connection stalled"))??;
        }
    });
    let io_cancel = cancel.clone();
    let active_audio = output.active_audio.clone();
    io_tasks.spawn(async move {
        let mut writer = peer.audio;
        loop {
            let frame = tokio::select! { _ = io_cancel.cancelled() => return Ok(()), frame = audio_output.recv() => { let Some(frame) = frame else { return Ok(()); }; frame } };
            let Header::Audio(meta) = &frame.header else { return Err(RuntimeError::Protocol); };
            if active_audio.lock().unwrap_or_else(|e| e.into_inner()).get(&meta.source) != Some(&meta.stream_id) { continue; }
            timeout(Duration::from_secs(5), frame.write_to(&mut writer)).await
                .map_err(|_| Failure::new(FailureCode::Timeout, "audio connection stalled"))??;
        }
    });
    let mut workers = Workers::default();
    let mut discoveries = JoinSet::new();
    let mut refresh = interval(Duration::from_secs(1));
    refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut revision = 0;
    let mut greeted = false;
    let mut last_id = 0;
    let outcome = loop {
        tokio::select! {
            _ = cancel.cancelled() => break Ok(()),
            task = io_tasks.join_next() => break task.map_or(Err(RuntimeError::Closed), |task| task.unwrap_or(Err(RuntimeError::Closed))),
            joined = workers.jobs.join_next(), if !workers.jobs.is_empty() => {
                if matches!(joined, Some(Err(_))) { break Err(Failure::new(FailureCode::Internal, "capture worker failed").into()); }
            },
            _ = refresh.tick(), if discoveries.is_empty() => {
                let backend = backend.clone();
                discoveries.spawn(async move { timeout(Duration::from_secs(3), backend.discover()).await });
            }
            discovered = discoveries.join_next(), if !discoveries.is_empty() => {
                if let Some(Ok(Ok(Ok(mut current)))) = discovered {
                    current.sources.sort_by(|a, b| a.key.id.cmp(&b.key.id));
                    if current.sources != catalog.sources || !issues_equal(&current, &catalog) {
                        current.revision = catalog.revision.saturating_add(1);
                        catalog = current;
                        if let Ok(frame) = Frame::new(Header::Event(Event::CatalogChanged { revision: catalog.revision }), Bytes::new()) {
                            let _ = outgoing.send(Outgoing { frame, flushed: None }).await;
                        }
                    }
                }
            }
            request = incoming.recv() => {
                let Some(ControlInput::Request(request)) = request else { break Err(RuntimeError::Closed); };
                if request.id <= last_id { break Err(RuntimeError::Protocol); }
                last_id = request.id;
                let received_ns = backend.clock_ns();
                let stopping = matches!(request.command, Command::Stop);
                let result = if !greeted && !matches!(request.command, Command::Hello { .. }) {
                    Err(Failure::new(FailureCode::InvalidRequest, "hello is required"))
                } else {
                    match request.command {
                        Command::Hello { version } => {
                            if version != PROTOCOL_VERSION { Err(Failure::new(FailureCode::UnsupportedVersion, "unsupported provider protocol")) }
                            else { greeted = true; Ok(ReplyValue::Hello { version, platform: backend.platform().into(), max_images: 8, max_audio: 8 }) }
                        }
                        Command::Sources { after } => {
                            let remaining: Vec<_> = catalog.sources.iter().filter(|source| after.as_ref().is_none_or(|after| source.key.id > *after)).collect();
                            let sources: Vec<_> = remaining.iter().take(SOURCE_PAGE_SIZE).map(|source| (*source).clone()).collect();
                            let next_after = (remaining.len() > SOURCE_PAGE_SIZE).then(|| sources.last().unwrap().key.id.clone());
                            Ok(ReplyValue::Sources(SourcePage { revision: catalog.revision, sources, issues: catalog.issues.clone(), next_after }))
                        }
                        Command::Configure(settings) => {
                            match validate_settings(&settings, &catalog).and_then(|()| workers.apply(&settings, revision + 1, &output, &cancel)) {
                                Ok(()) => { revision += 1; Ok(ReplyValue::Applied { revision, settings }) }, Err(error) => Err(error),
                            }
                        }
                        Command::CaptureNow { sources } => workers.capture_now(&sources).map(|()| ReplyValue::CaptureQueued { revision, not_before_ns: received_ns }),
                        Command::Clock { client_send_ns } => Ok(ReplyValue::Clock { client_send_ns, received_ns, sent_ns: backend.clock_ns() }),
                        Command::Stop => { workers.stop().await; Ok(ReplyValue::Stopped) },
                    }
                };
                let (done, flushed) = oneshot::channel();
                let reply = Reply { id: request.id, result: result.unwrap_or_else(ReplyValue::Failed) };
                let frame = match Frame::new(Header::Reply(reply), Bytes::new()) { Ok(frame) => frame, Err(error) => break Err(error.into()) };
                if outgoing.send(Outgoing { frame, flushed: stopping.then_some(done) }).await.is_err() { break Err(RuntimeError::Closed); }
                if stopping {
                    let _ = timeout(Duration::from_secs(5), flushed).await;
                    // Independent media lanes must not deliver EOF before the stop reply.
                    let receipt = async {
                        while let Some(input) = incoming.recv().await {
                            if matches!(input, ControlInput::Receipt(id) if id == request.id) { break; }
                        }
                    };
                    let _ = timeout(Duration::from_secs(3), receipt).await;
                    break Ok(());
                }
            }
        }
    };
    cancel.cancel();
    drop(incoming);
    discoveries.abort_all();
    workers.stop().await;
    while io_tasks.join_next().await.is_some() {}
    outcome
}

fn issues_equal(a: &SourceSnapshot, b: &SourceSnapshot) -> bool {
    a.issues.len() == b.issues.len()
        && a.issues.iter().zip(&b.issues).all(|(a, b)| {
            a.kind == b.kind && a.availability == b.availability && a.message == b.message
        })
}

fn validate_settings(settings: &CaptureSettings, catalog: &SourceSnapshot) -> Result<(), Failure> {
    settings
        .validate()
        .map_err(|error| Failure::new(FailureCode::InvalidRequest, error.to_string()))?;
    for (key, image) in settings
        .images
        .iter()
        .map(|image| (&image.source, Some(image)))
        .chain(settings.audio.iter().map(|key| (key, None)))
    {
        let source = catalog
            .sources
            .iter()
            .find(|source| source.key == *key)
            .ok_or_else(|| {
                Failure::new(FailureCode::SourceChanged, "source is missing or changed")
            })?;
        match source.availability {
            SourceAvailability::PermissionRequired => {
                return Err(Failure::new(
                    FailureCode::PermissionRequired,
                    "source permission is required",
                ));
            }
            SourceAvailability::Unavailable => {
                return Err(Failure::new(
                    FailureCode::Unavailable,
                    "source is unavailable",
                ));
            }
            SourceAvailability::Available => {}
        }
        if image.is_some() != matches!(source.kind, SourceKind::Display | SourceKind::Window) {
            return Err(Failure::new(
                FailureCode::InvalidRequest,
                "incorrect source kind",
            ));
        }
        if let Some(image) = image
            && let Some(crop) = image.crop
        {
            crop.validate_within(
                source.pixel_width.unwrap_or(0),
                source.pixel_height.unwrap_or(0),
            )
            .map_err(|error| Failure::new(FailureCode::InvalidRequest, error.to_string()))?;
        }
    }
    Ok(())
}
