use crate::{SAMPLE_RATE, TranscriptionError, audio::NormalizedAudio};
use liazo_core::{protocol::SourceKey, transcript::EndReason};
use std::{collections::VecDeque, sync::Arc};
use uuid::Uuid;

const FRAME: usize = 320;
const PREROLL: usize = 3200;
const MIN_VOICED: usize = 1920;

#[derive(Clone, Debug)]
pub struct StreamingSettings {
    pub rms_threshold: f32,
    pub partial_interval_ms: u32,
    pub silence_ms: u32,
    pub max_utterance_ms: u32,
}

impl Default for StreamingSettings {
    fn default() -> Self {
        Self {
            rms_threshold: 0.003,
            partial_interval_ms: 750,
            silence_ms: 600,
            max_utterance_ms: 20_000,
        }
    }
}

impl StreamingSettings {
    pub fn validate(&self) -> Result<(), TranscriptionError> {
        if !self.rms_threshold.is_finite()
            || !(0.0001..=0.1).contains(&self.rms_threshold)
            || !(200..=5000).contains(&self.partial_interval_ms)
            || !(200..=2000).contains(&self.silence_ms)
            || !self.silence_ms.is_multiple_of(20)
            || !(2000..=30_000).contains(&self.max_utterance_ms)
            || !self.max_utterance_ms.is_multiple_of(20)
        {
            return Err(TranscriptionError::InvalidInput);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct InferenceWindow {
    pub id: Uuid,
    pub source: SourceKey,
    pub stream_id: Uuid,
    pub revision: u32,
    pub start_ns: u64,
    pub end_ns: u64,
    pub ending: Option<EndReason>,
    pub samples: Arc<[f32]>,
}

struct Utterance {
    id: Uuid,
    stream_id: Uuid,
    start_ns: u64,
    revision: u32,
    samples: Vec<f32>,
    voiced: usize,
    silence: usize,
    last_partial: usize,
}

pub struct Segmenter {
    source: SourceKey,
    settings: StreamingSettings,
    stream: Option<Uuid>,
    next_ns: u64,
    pending_ns: u64,
    pending: VecDeque<f32>,
    preroll: VecDeque<f32>,
    active: Option<Utterance>,
}

impl Segmenter {
    pub fn new(source: SourceKey, settings: StreamingSettings) -> Result<Self, TranscriptionError> {
        source
            .validate()
            .map_err(|_| TranscriptionError::InvalidInput)?;
        settings.validate()?;
        Ok(Self {
            source,
            settings,
            stream: None,
            next_ns: 0,
            pending_ns: 0,
            pending: VecDeque::new(),
            preroll: VecDeque::new(),
            active: None,
        })
    }

    pub fn push(
        &mut self,
        audio: NormalizedAudio,
    ) -> Result<Vec<InferenceWindow>, TranscriptionError> {
        if audio.stream_id.is_nil()
            || audio.samples.len() > 32_768
            || audio
                .samples
                .iter()
                .any(|sample| !sample.is_finite() || sample.abs() > 1.0)
        {
            return Err(TranscriptionError::InvalidInput);
        }
        let mut windows = Vec::new();
        if self.stream.is_some_and(|stream| {
            stream != audio.stream_id || audio.discontinuity || self.next_ns != audio.start_ns
        }) {
            windows.extend(self.finish(EndReason::Gap));
        }
        self.stream = Some(audio.stream_id);
        if self.pending.is_empty() {
            self.pending_ns = audio.start_ns;
        }
        self.next_ns = audio
            .start_ns
            .saturating_add(audio.samples.len() as u64 * 62_500);
        self.pending.extend(audio.samples);
        while self.pending.len() >= FRAME {
            let frame: Vec<_> = self.pending.drain(..FRAME).collect();
            let end_ns = self.pending_ns.saturating_add(20_000_000);
            let voiced = frame.iter().map(|sample| sample * sample).sum::<f32>() / FRAME as f32
                >= self.settings.rms_threshold * self.settings.rms_threshold;
            if let Some(active) = &mut self.active {
                active.samples.extend(&frame);
                if voiced {
                    active.voiced += FRAME;
                    active.silence = 0;
                } else {
                    active.silence += FRAME;
                }
            } else {
                self.preroll.extend(&frame);
                while self.preroll.len() > PREROLL {
                    self.preroll.pop_front();
                }
                if voiced {
                    let samples: Vec<_> = self.preroll.drain(..).collect();
                    self.active = Some(Utterance {
                        id: Uuid::new_v4(),
                        stream_id: audio.stream_id,
                        start_ns: end_ns.saturating_sub(samples.len() as u64 * 62_500),
                        revision: 0,
                        samples,
                        voiced: FRAME,
                        silence: 0,
                        last_partial: 0,
                    });
                }
            }
            self.pending_ns = end_ns;
            if let Some(active) = &self.active {
                let ending = if active.silence >= self.settings.silence_ms as usize * 16 {
                    Some(EndReason::Silence)
                } else if active.samples.len() >= self.settings.max_utterance_ms as usize * 16 {
                    Some(EndReason::Limit)
                } else {
                    None
                };
                if let Some(reason) = ending {
                    windows.extend(self.finish_active(reason));
                } else if active.voiced >= MIN_VOICED
                    && active.samples.len() - active.last_partial
                        >= self.settings.partial_interval_ms as usize * 16
                {
                    windows.push(self.active.as_mut().unwrap().window(&self.source, None));
                }
            }
        }
        Ok(windows)
    }

    pub fn finish(&mut self, reason: EndReason) -> Option<InferenceWindow> {
        if let Some(active) = &mut self.active {
            active.samples.extend(self.pending.drain(..));
        }
        self.stream = None;
        self.pending.clear();
        self.preroll.clear();
        self.finish_active(reason)
    }

    fn finish_active(&mut self, reason: EndReason) -> Option<InferenceWindow> {
        self.active
            .take()
            .filter(|active| active.voiced >= MIN_VOICED)
            .map(|mut active| active.window(&self.source, Some(reason)))
    }
}

impl Utterance {
    fn window(&mut self, source: &SourceKey, ending: Option<EndReason>) -> InferenceWindow {
        self.revision += 1;
        self.last_partial = self.samples.len();
        InferenceWindow {
            id: self.id,
            source: source.clone(),
            stream_id: self.stream_id,
            revision: self.revision,
            start_ns: self.start_ns,
            end_ns: self
                .start_ns
                .saturating_add(self.samples.len() as u64 * 1_000_000_000 / SAMPLE_RATE as u64),
            ending,
            samples: self.samples.clone().into(),
        }
    }
}
