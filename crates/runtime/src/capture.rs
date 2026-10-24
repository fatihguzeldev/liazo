use crate::{backend::CaptureBackend, images::LatestImages};
use liazo_core::{
    control::{Event, Failure},
    protocol::{ImageSubscription, SourceKey},
    wire::{Frame, Header},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, watch},
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct ImageWork {
    pub image: ImageSubscription,
    pub revision: u64,
    pub force: u64,
}
pub(crate) struct ImageWorker {
    pub work: watch::Sender<ImageWork>,
    pub cancel: CancellationToken,
}
pub(crate) type ActiveAudio = Arc<Mutex<HashMap<SourceKey, Uuid>>>;

#[derive(Clone)]
pub(crate) struct CaptureOutput {
    pub backend: Arc<dyn CaptureBackend>,
    pub images: Arc<LatestImages>,
    pub audio: mpsc::Sender<Frame>,
    pub events: mpsc::Sender<Frame>,
    pub active_audio: ActiveAudio,
    pub permits: Arc<Semaphore>,
}

impl CaptureOutput {
    fn status(&self, source: SourceKey, revision: u64, failure: Option<Failure>) {
        let event = Event::SourceStatus {
            source,
            revision,
            observed_ns: self.backend.clock_ns(),
            failure,
        };
        if let Ok(frame) = Frame::new(Header::Event(event), Default::default()) {
            let _ = self.events.try_send(frame);
        }
    }

    pub fn spawn_image(
        &self,
        work: ImageWork,
        cancel: CancellationToken,
        jobs: &mut JoinSet<()>,
    ) -> ImageWorker {
        let (sender, receiver) = watch::channel(work);
        let worker = ImageWorker {
            work: sender,
            cancel: cancel.clone(),
        };
        let output = self.clone();
        jobs.spawn(async move { output.image_loop(receiver, cancel).await });
        worker
    }

    async fn image_loop(self, mut work: watch::Receiver<ImageWork>, cancel: CancellationToken) {
        let mut previous = None;
        let mut encoding: Option<ImageSubscription> = None;
        let mut sequence = 0;
        let mut deadline = Instant::now();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                changed = work.changed() => { if changed.is_err() { break; } },
                _ = sleep_until(deadline) => {},
            }
            let current = work.borrow_and_update().clone();
            let started = Instant::now();
            let permit = tokio::select! {
                _ = cancel.cancelled() => break,
                permit = self.permits.acquire() => { let Ok(permit) = permit else { break; }; permit },
            };
            if work.borrow().revision != current.revision {
                drop(permit);
                continue;
            }
            if encoding.as_ref().is_some_and(|old| {
                let mut comparison = old.clone();
                comparison.interval_ms = current.image.interval_ms;
                comparison != current.image
            }) {
                previous = None;
            }
            sequence += 1;
            let captured = tokio::select! {
                _ = cancel.cancelled() => break,
                result = timeout(Duration::from_secs(3), self.backend.image(&current.image, previous, sequence, current.revision)) => {
                    result.unwrap_or_else(|_| Err(Failure::new(liazo_core::control::FailureCode::Timeout, "capture timed out")))
                },
            };
            drop(permit);
            if work.borrow().revision == current.revision {
                match captured {
                    Ok(frame) => {
                        let fingerprint = match &frame.header {
                            Header::Image(meta) | Header::Unchanged(meta) => Some(meta.fingerprint),
                            _ => None,
                        };
                        match self.images.publish(frame) {
                            Ok(()) => {
                                previous = fingerprint;
                                encoding = Some(current.image.clone());
                            }
                            Err(error) => self.status(
                                current.image.source.clone(),
                                current.revision,
                                Some(error),
                            ),
                        }
                    }
                    Err(error) => {
                        self.status(current.image.source.clone(), current.revision, Some(error))
                    }
                }
            }
            deadline = (started + Duration::from_millis(u64::from(current.image.interval_ms)))
                .max(Instant::now());
        }
    }

    pub fn spawn_audio(
        &self,
        source: SourceKey,
        revision: u64,
        cancel: CancellationToken,
        jobs: &mut JoinSet<()>,
    ) {
        let output = self.clone();
        jobs.spawn(async move {
            loop {
                let stream = Uuid::new_v4();
                {
                    let mut active = output.active_audio.lock().unwrap_or_else(|e| e.into_inner());
                    if cancel.is_cancelled() { break; }
                    active.insert(source.clone(), stream);
                }
                output.clone().audio_loop(source.clone(), stream, revision, cancel.clone()).await;
                if cancel.is_cancelled() { break; }
                tokio::select! { _ = cancel.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            }
        });
    }

    async fn audio_loop(
        self,
        source: SourceKey,
        stream: Uuid,
        revision: u64,
        cancel: CancellationToken,
    ) {
        let opened = tokio::select! {
            _ = cancel.cancelled() => return,
            opened = timeout(Duration::from_secs(3), self.backend.audio(&source, stream)) => opened,
        };
        let mut track = match opened {
            Ok(Ok(track)) => track,
            failure => {
                let failure = match failure {
                    Ok(Err(error)) => error,
                    _ => Failure::new(
                        liazo_core::control::FailureCode::Timeout,
                        "audio start timed out",
                    ),
                };
                self.status(source.clone(), revision, Some(failure));
                return;
            }
        };
        let mut discontinuity = true;
        loop {
            let packet =
                tokio::select! { _ = cancel.cancelled() => break, packet = track.next() => packet };
            match packet {
                Ok(Some(mut frame)) => {
                    let Header::Audio(meta) = &mut frame.header else {
                        break;
                    };
                    if meta.source != source || meta.stream_id != stream {
                        break;
                    }
                    if discontinuity {
                        meta.discontinuity = true;
                    }
                    discontinuity = self.audio.try_send(frame).is_err();
                }
                Ok(None) => break,
                Err(error) => {
                    self.status(source.clone(), revision, Some(error));
                    break;
                }
            }
        }
        let _ = timeout(Duration::from_secs(3), track.stop()).await;
    }
}
