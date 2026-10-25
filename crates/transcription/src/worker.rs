use crate::{TranscriptionError, WhisperModel, streaming::InferenceWindow};
use liazo_core::{
    protocol::SourceKey,
    transcript::{Transcript, TranscriptionGap, TranscriptionGapReason},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

const MAX_PENDING_BYTES: usize = 32 * 1024 * 1024;
const MAX_FINALS: usize = 16;

pub enum RecognitionUpdate {
    Transcript(Transcript),
    Gap(TranscriptionGap),
}

struct Running {
    id: Uuid,
    partial: bool,
    abort: Arc<AtomicBool>,
}
#[derive(Default)]
struct Queue {
    partials: HashMap<SourceKey, InferenceWindow>,
    finals: VecDeque<InferenceWindow>,
    running: Option<Running>,
    closing: bool,
    cancelled: bool,
}

#[derive(Default)]
struct Mailbox {
    queue: Mutex<Queue>,
    ready: Condvar,
}

pub struct InferenceWorker {
    mailbox: Arc<Mailbox>,
    updates: mpsc::Receiver<RecognitionUpdate>,
    done: Option<oneshot::Receiver<()>>,
    thread: Option<JoinHandle<()>>,
}

impl InferenceWorker {
    pub async fn start(model: WhisperModel, threads: u8) -> Result<Self, TranscriptionError> {
        let mailbox = Arc::new(Mailbox::default());
        let (output, updates) = mpsc::channel(32);
        let (started, ready) = oneshot::channel();
        let (completed, done) = oneshot::channel();
        let queue = mailbox.clone();
        let thread = std::thread::Builder::new()
            .name("liazo-speech".into())
            .spawn(move || {
                let mut decoder = match model.decoder(threads) {
                    Ok(decoder) => {
                        let _ = started.send(Ok(()));
                        decoder
                    }
                    Err(error) => {
                        let _ = started.send(Err(error));
                        let _ = completed.send(());
                        return;
                    }
                };
                while let Some((window, abort)) = queue.take() {
                    let result = decoder.transcribe(&window.samples, abort.clone());
                    queue
                        .queue
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .running = None;
                    if abort.load(Ordering::Acquire) {
                        continue;
                    }
                    let update = match result {
                        Ok(segments) => {
                            let mut text = String::new();
                            for segment in segments
                                .into_iter()
                                .filter(|segment| segment.no_speech_probability <= 0.8)
                            {
                                for ch in segment.text.trim().chars().filter(|ch| !ch.is_control())
                                {
                                    if text.len() + ch.len_utf8() > 16_383 {
                                        break;
                                    }
                                    text.push(ch);
                                }
                                if !text.is_empty() && text.len() < 16_384 {
                                    text.push(' ');
                                }
                            }
                            let text = text.trim().to_owned();
                            if text.is_empty() && window.ending.is_none() {
                                continue;
                            }
                            RecognitionUpdate::Transcript(Transcript {
                                id: window.id,
                                source: window.source,
                                stream_id: window.stream_id,
                                revision: window.revision,
                                start_ns: window.start_ns,
                                end_ns: window.end_ns,
                                text,
                                ending: window.ending,
                            })
                        }
                        Err(TranscriptionError::Cancelled) => continue,
                        Err(_) => RecognitionUpdate::Gap(TranscriptionGap {
                            source: window.source,
                            utterance: Some(window.id),
                            reason: TranscriptionGapReason::EngineFailure,
                        }),
                    };
                    if output.blocking_send(update).is_err() {
                        break;
                    }
                }
                drop(decoder);
                drop(model);
                let _ = completed.send(());
            })
            .map_err(TranscriptionError::ModelFile)?;
        // Own the cancellation guard before awaiting native initialization.
        let worker = Self {
            mailbox,
            updates,
            done: Some(done),
            thread: Some(thread),
        };
        ready
            .await
            .map_err(|_| TranscriptionError::WorkerStopped)??;
        Ok(worker)
    }

    pub fn submit(
        &self,
        window: InferenceWindow,
    ) -> Result<Vec<TranscriptionGap>, TranscriptionError> {
        Transcript {
            id: window.id,
            source: window.source.clone(),
            stream_id: window.stream_id,
            revision: window.revision,
            start_ns: window.start_ns,
            end_ns: window.end_ns,
            text: String::new(),
            ending: window.ending,
        }
        .validate()
        .map_err(|_| TranscriptionError::InvalidInput)?;
        if window.samples.is_empty()
            || window.samples.len() > crate::MAX_WINDOW_SAMPLES
            || window.end_ns - window.start_ns != window.samples.len() as u64 * 62_500
            || window
                .samples
                .iter()
                .any(|sample| !sample.is_finite() || sample.abs() > 1.0)
        {
            return Err(TranscriptionError::InvalidInput);
        }
        self.mailbox.submit(window)
    }

    pub async fn next(&mut self) -> Option<RecognitionUpdate> {
        self.updates.recv().await
    }

    pub fn finish(&self) {
        self.mailbox
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closing = true;
        self.mailbox.ready.notify_one();
    }

    pub fn cancel(&mut self) {
        let mut queue = self.mailbox.queue.lock().unwrap_or_else(|e| e.into_inner());
        queue.cancelled = true;
        queue.partials.clear();
        queue.finals.clear();
        if let Some(running) = &queue.running {
            running.abort.store(true, Ordering::Release);
        }
        self.updates.close();
        self.mailbox.ready.notify_one();
    }

    pub async fn join(&mut self) -> Result<(), TranscriptionError> {
        if let Some(done) = &mut self.done {
            done.await.map_err(|_| TranscriptionError::WorkerStopped)?;
            self.done = None;
        }
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| TranscriptionError::WorkerStopped)?;
        }
        Ok(())
    }
}

impl Drop for InferenceWorker {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl Mailbox {
    fn submit(&self, window: InferenceWindow) -> Result<Vec<TranscriptionGap>, TranscriptionError> {
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        if queue.cancelled || queue.closing {
            return Err(TranscriptionError::WorkerStopped);
        }
        if window.ending.is_some() {
            if let Some(running) = &queue.running
                && running.id == window.id
                && running.partial
            {
                running.abort.store(true, Ordering::Release);
            }
            if queue
                .partials
                .get(&window.source)
                .is_some_and(|partial| partial.id == window.id)
            {
                queue.partials.remove(&window.source);
            }
            queue.finals.push_back(window);
        } else {
            if queue.partials.len() >= 8 && !queue.partials.contains_key(&window.source) {
                return Err(TranscriptionError::InvalidInput);
            }
            queue.partials.insert(window.source.clone(), window);
        }
        let mut gaps = Vec::new();
        while queue.finals.len() > MAX_FINALS || queue.bytes() > MAX_PENDING_BYTES {
            if queue.bytes() > MAX_PENDING_BYTES && !queue.partials.is_empty() {
                let source = queue.partials.keys().next().unwrap().clone();
                queue.partials.remove(&source);
            } else if let Some(dropped) = queue.finals.pop_front() {
                gaps.push(TranscriptionGap {
                    source: dropped.source,
                    utterance: Some(dropped.id),
                    reason: TranscriptionGapReason::Backpressure,
                });
            }
        }
        drop(queue);
        self.ready.notify_one();
        Ok(gaps)
    }

    fn take(&self) -> Option<(InferenceWindow, Arc<AtomicBool>)> {
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if queue.cancelled {
                return None;
            }
            let window = queue.finals.pop_front().or_else(|| {
                let source = queue
                    .partials
                    .values()
                    .min_by_key(|window| window.end_ns)
                    .map(|window| window.source.clone())?;
                queue.partials.remove(&source)
            });
            if let Some(window) = window {
                let abort = Arc::new(AtomicBool::new(false));
                queue.running = Some(Running {
                    id: window.id,
                    partial: window.ending.is_none(),
                    abort: abort.clone(),
                });
                return Some((window, abort));
            }
            if queue.closing {
                return None;
            }
            queue = self.ready.wait(queue).unwrap_or_else(|e| e.into_inner());
        }
    }
}

impl Queue {
    fn bytes(&self) -> usize {
        self.partials
            .values()
            .chain(&self.finals)
            .map(|window| window.samples.len() * 4)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liazo_core::transcript::EndReason;
    fn window(id: Uuid, revision: u32, ending: Option<EndReason>) -> InferenceWindow {
        InferenceWindow {
            id,
            source: SourceKey {
                id: "audio:test".into(),
                generation: Uuid::from_u128(1),
            },
            stream_id: Uuid::from_u128(2),
            revision,
            start_ns: 0,
            end_ns: 1_000_000_000,
            ending,
            samples: vec![0.0; 16000].into(),
        }
    }
    #[test]
    fn slow_inference_coalesces_partials_and_finals_cancel_obsolete_work() {
        let queue = Mailbox::default();
        let id = Uuid::new_v4();
        for revision in 1..=100 {
            queue.submit(window(id, revision, None)).unwrap();
        }
        let (partial, abort) = queue.take().unwrap();
        assert_eq!(partial.revision, 100);
        queue
            .submit(window(id, 101, Some(EndReason::Silence)))
            .unwrap();
        assert!(abort.load(Ordering::Acquire));
        assert_eq!(queue.take().unwrap().0.revision, 101);
        for _ in 0..100 {
            queue
                .submit(window(Uuid::new_v4(), 1, Some(EndReason::Limit)))
                .unwrap();
        }
        let gaps = queue
            .submit(window(Uuid::new_v4(), 1, Some(EndReason::Limit)))
            .unwrap();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].reason, TranscriptionGapReason::Backpressure);
        assert_eq!(queue.queue.lock().unwrap().finals.len(), MAX_FINALS);
    }
}
