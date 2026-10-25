use crate::{ConnectionState, RuntimeError, receiver::Shared};
use liazo_core::{
    catalog::SourceSnapshot,
    control::{Failure, FailureCode},
    protocol::{CaptureSettings, SourceAvailability, SourceKey, SourceKind},
    transcript::{EndReason, TranscriptionGap, TranscriptionGapReason},
    wire::Header,
};
use liazo_transcription::{
    WhisperModel,
    audio::Normalizer,
    streaming::{InferenceWindow, Segmenter, StreamingSettings},
    worker::{InferenceWorker, RecognitionUpdate},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::broadcast,
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

pub(crate) struct SpeechTask {
    stop: CancellationToken,
    task: JoinHandle<()>,
}
struct Input {
    normalizer: Normalizer,
    segmenter: Segmenter,
    received: Option<Instant>,
}

pub(crate) fn validate_selection(
    shared: &Shared,
    selected: &[SourceKey],
) -> Result<(), RuntimeError> {
    let state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
    if state.connection != ConnectionState::Connected {
        return Err(RuntimeError::Closed);
    }
    check_selection(&state.settings, &state.sources, selected)
}

fn check_selection(
    settings: &CaptureSettings,
    catalog: &SourceSnapshot,
    selected: &[SourceKey],
) -> Result<(), RuntimeError> {
    let mut seen = HashSet::new();
    let mut system = false;
    let mut process = false;
    if selected.is_empty() || selected.len() > 8 {
        return Err(invalid("select between one and eight audio sources"));
    }
    for key in selected {
        if !seen.insert(key) || !settings.audio.contains(key) {
            return Err(invalid("transcription requires selected audio sources"));
        }
        let source = catalog
            .sources
            .iter()
            .find(|source| source.key == *key)
            .ok_or_else(|| invalid("audio source is unavailable"))?;
        if !matches!(
            source.kind,
            SourceKind::SystemAudio | SourceKind::ProcessAudio | SourceKind::Microphone
        ) {
            return Err(invalid("transcription requires audio sources"));
        }
        match source.availability {
            SourceAvailability::Available => {}
            SourceAvailability::PermissionRequired => {
                return Err(Failure::new(
                    FailureCode::PermissionRequired,
                    "audio permission is required",
                )
                .into());
            }
            SourceAvailability::Unavailable => {
                return Err(
                    Failure::new(FailureCode::Unavailable, "audio source is unavailable").into(),
                );
            }
        }
        system |= source.kind == SourceKind::SystemAudio;
        process |= source.kind == SourceKind::ProcessAudio;
    }
    if system && process {
        return Err(invalid(
            "choose system audio or process audio for transcription",
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> RuntimeError {
    Failure::new(FailureCode::InvalidRequest, message).into()
}

impl SpeechTask {
    pub async fn start(
        shared: Arc<Shared>,
        model: WhisperModel,
        threads: u8,
        selected: Vec<SourceKey>,
        settings: StreamingSettings,
    ) -> Result<Self, RuntimeError> {
        let mut inputs = HashMap::new();
        for source in &selected {
            inputs.insert(
                source.clone(),
                Input {
                    normalizer: Normalizer::new(source.clone()),
                    segmenter: Segmenter::new(source.clone(), settings.clone())?,
                    received: None,
                },
            );
        }
        let worker = InferenceWorker::start(model, threads).await?;
        validate_selection(&shared, &selected)?;
        let audio = shared.audio.subscribe();
        {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            state.transcripts.begin(selected);
            state.changed();
        }
        shared.signal();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let publication = Publication(shared.clone());
        let task = tokio::spawn(async move {
            let _publication = publication;
            run(shared, audio, inputs, worker, stopping).await;
        });
        Ok(Self { stop, task })
    }

    pub async fn stop(&mut self) {
        self.stop.cancel();
        if timeout(Duration::from_secs(5), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
            let _ = (&mut self.task).await;
        }
    }
}

impl Drop for SpeechTask {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

struct Publication(Arc<Shared>);
impl Drop for Publication {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let cursor = state.cursor.saturating_add(1);
        let now = self.0.now_ns();
        state.transcripts.stop(cursor, now);
        state.archive_transcripts(cursor.saturating_sub(1), now);
        state.changed();
        drop(state);
        self.0.signal();
    }
}

fn publish(shared: &Shared, update: RecognitionUpdate) {
    let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
    let cursor = state.cursor.saturating_add(1);
    let now = shared.now_ns();
    if state.transcripts.publish(update, cursor, now) {
        state.archive_transcripts(cursor.saturating_sub(1), now);
        state.changed();
    }
    drop(state);
    shared.signal();
}

fn gap(shared: &Shared, source: &SourceKey, reason: TranscriptionGapReason) {
    publish(
        shared,
        RecognitionUpdate::Gap(TranscriptionGap {
            source: source.clone(),
            utterance: None,
            reason,
        }),
    );
}

fn enqueue(shared: &Shared, worker: &InferenceWorker, windows: Vec<InferenceWindow>) {
    for window in windows {
        let source = window.source.clone();
        let id = window.id;
        match worker.submit(window) {
            Ok(gaps) => {
                for gap in gaps {
                    publish(shared, RecognitionUpdate::Gap(gap));
                }
            }
            Err(_) => publish(
                shared,
                RecognitionUpdate::Gap(TranscriptionGap {
                    source,
                    utterance: Some(id),
                    reason: TranscriptionGapReason::EngineFailure,
                }),
            ),
        }
    }
}

fn finish_input(
    shared: &Shared,
    worker: &InferenceWorker,
    source: &SourceKey,
    input: &mut Input,
    reason: EndReason,
) {
    if let Ok(Some(audio)) = input.normalizer.finish()
        && let Ok(windows) = input.segmenter.push(audio)
    {
        enqueue(shared, worker, windows);
    }
    enqueue(
        shared,
        worker,
        input.segmenter.finish(reason).into_iter().collect(),
    );
    input.normalizer = Normalizer::new(source.clone());
    input.received = None;
}

async fn run(
    shared: Arc<Shared>,
    mut audio: broadcast::Receiver<liazo_core::wire::Frame>,
    mut inputs: HashMap<SourceKey, Input>,
    mut worker: InferenceWorker,
    stop: CancellationToken,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let ending = loop {
        tokio::select! {
            _ = stop.cancelled() => break EndReason::Stopped,
            _ = shared.cancelled.cancelled() => {
                break if shared.state.lock().unwrap_or_else(|e| e.into_inner()).connection == ConnectionState::Stopped { EndReason::Stopped } else { EndReason::Gap };
            }
            update = worker.next() => {
                if let Some(update) = update { publish(&shared, update); } else { break EndReason::Gap; }
            }
            frame = audio.recv() => match frame {
                Ok(frame) => {
                    let Header::Audio(meta) = frame.header else { continue; };
                    let Some(input) = inputs.get_mut(&meta.source) else { continue; };
                    input.received = Some(Instant::now());
                    match input.normalizer.push(&meta, &frame.payload) {
                        Ok(audio) => {
                            if audio.discontinuity { gap(&shared, &meta.source, TranscriptionGapReason::AudioDiscontinuity); }
                            match input.segmenter.push(audio) {
                                Ok(windows) => enqueue(&shared, &worker, windows),
                                Err(_) => { gap(&shared, &meta.source, TranscriptionGapReason::InvalidAudio); finish_input(&shared, &worker, &meta.source, input, EndReason::Gap); }
                            }
                        }
                        Err(_) => { gap(&shared, &meta.source, TranscriptionGapReason::InvalidAudio); finish_input(&shared, &worker, &meta.source, input, EndReason::Gap); }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    for (source, input) in &mut inputs { gap(&shared, source, TranscriptionGapReason::Backpressure); finish_input(&shared, &worker, source, input, EndReason::Gap); }
                }
                Err(broadcast::error::RecvError::Closed) => break EndReason::Gap,
            },
            _ = tick.tick() => {
                let selected = shared.state.lock().unwrap_or_else(|e| e.into_inner()).settings.audio.clone();
                let mut removed = Vec::new();
                for (source, input) in &mut inputs {
                    if !selected.contains(source) { finish_input(&shared, &worker, source, input, EndReason::Stopped); removed.push(source.clone()); }
                    else if input.received.is_some_and(|received| received.elapsed() > Duration::from_secs(1)) {
                        gap(&shared, source, TranscriptionGapReason::AudioDiscontinuity);
                        finish_input(&shared, &worker, source, input, EndReason::Gap);
                    }
                }
                if !removed.is_empty() {
                    for source in removed { inputs.remove(&source); }
                    let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
                    state.transcripts.status.sources = inputs.keys().cloned().collect();
                    state.changed();
                    drop(state);
                    shared.signal();
                }
                if inputs.is_empty() { break EndReason::Stopped; }
            }
        }
    };
    for (source, input) in &mut inputs {
        if ending == EndReason::Gap {
            gap(&shared, source, TranscriptionGapReason::AudioDiscontinuity);
        }
        finish_input(&shared, &worker, source, input, ending);
    }
    worker.finish();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match tokio::time::timeout_at(deadline, worker.next()).await {
            Ok(Some(update)) => publish(&shared, update),
            Ok(None) => break,
            Err(_) => {
                worker.cancel();
                break;
            }
        }
    }
    let _ = timeout(Duration::from_secs(1), worker.join()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use liazo_core::protocol::Source;
    use uuid::Uuid;
    #[test]
    fn transcription_requires_current_audio_and_prevents_duplicate_mix_and_process_subscriptions() {
        let sources: Vec<_> = [
            SourceKind::SystemAudio,
            SourceKind::ProcessAudio,
            SourceKind::Microphone,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, kind)| Source {
            key: SourceKey {
                id: format!("audio:{index}"),
                generation: Uuid::new_v4(),
            },
            label: "fixture".into(),
            kind,
            availability: SourceAvailability::Available,
            pixel_width: None,
            pixel_height: None,
            scale: None,
            process_id: None,
        })
        .collect();
        let keys: Vec<_> = sources.iter().map(|source| source.key.clone()).collect();
        let mut catalog = SourceSnapshot {
            revision: 1,
            sources,
            issues: vec![],
        };
        let mut settings = CaptureSettings {
            images: vec![],
            audio: keys.clone(),
        };
        assert!(check_selection(&settings, &catalog, &keys[..2]).is_err());
        assert!(check_selection(&settings, &catalog, &keys[1..]).is_ok());
        assert!(check_selection(&settings, &catalog, &[keys[1].clone(), keys[1].clone()]).is_err());
        catalog.sources[1].availability = SourceAvailability::PermissionRequired;
        assert!(matches!(
            check_selection(&settings, &catalog, &keys[1..]),
            Err(RuntimeError::Failure(Failure {
                code: FailureCode::PermissionRequired,
                ..
            }))
        ));
        settings.audio.clear();
        assert!(check_selection(&settings, &catalog, &keys[2..]).is_err());
    }
}
