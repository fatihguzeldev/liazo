use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use serde::Serialize;
use whisper_rs::{
    FullParams, SamplingStrategy, WhisperContext, WhisperContextParameters, WhisperState,
};

mod acceleration;
pub mod audio;
pub mod streaming;
mod vad;
pub mod worker;

use acceleration::with_cpu_fallback;

pub const SAMPLE_RATE: usize = 16_000;
pub const MAX_WINDOW_SAMPLES: usize = 30 * SAMPLE_RATE;

#[derive(Debug, thiserror::Error)]
pub enum TranscriptionError {
    #[error("invalid transcription settings or audio")]
    InvalidInput,
    #[error("audio resampling failed")]
    Resampling,
    #[error("speech worker stopped")]
    WorkerStopped,
    #[error("transcription was cancelled")]
    Cancelled,
    #[error("speech model is unavailable: {0}")]
    ModelFile(#[from] std::io::Error),
    #[error("speech engine failed: {0}")]
    Engine(#[from] whisper_rs::WhisperError),
}

#[derive(Clone, Debug, Serialize)]
pub struct RecognizedSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    pub no_speech_probability: f32,
    pub average_log_probability: Option<f32>,
}

#[derive(Clone)]
pub struct WhisperModel(Arc<Model>);

struct Model {
    path: PathBuf,
    context: Mutex<LoadedContext>,
}

struct LoadedContext {
    context: WhisperContext,
    // This is a preference, not proof that native scheduling selected a GPU.
    gpu_requested: bool,
}

fn load_context(path: &Path, gpu: Option<i32>) -> Result<WhisperContext, whisper_rs::WhisperError> {
    let mut parameters = WhisperContextParameters::default();
    parameters.use_gpu(gpu.is_some());
    if let Some(device) = gpu {
        parameters.gpu_device(device);
    }
    WhisperContext::new_with_params(path, parameters)
}

impl WhisperModel {
    pub fn load(path: &Path) -> Result<Self, TranscriptionError> {
        let gpu = cfg!(all(target_os = "macos", feature = "metal"))
            || cfg!(all(
                any(target_os = "windows", target_os = "linux"),
                feature = "vulkan"
            ));
        Self::load_with_gpu(path, gpu)
    }

    pub fn load_cpu(path: &Path) -> Result<Self, TranscriptionError> {
        Self::load_with_gpu(path, false)
    }

    fn load_with_gpu(path: &Path, gpu: bool) -> Result<Self, TranscriptionError> {
        // Disable native logs, including debug token text. Results belong only in observations.
        whisper_rs::install_logging_hooks();
        let size = std::fs::metadata(path)?.len();
        if size == 0 || size > 2 * 1024 * 1024 * 1024 {
            return Err(TranscriptionError::InvalidInput);
        }
        let device = gpu.then(acceleration::gpu_device).flatten();
        let (context, gpu_requested) = with_cpu_fallback(device.is_some(), |gpu| {
            load_context(path, if gpu { device } else { None })
        })?;
        Ok(Self(Arc::new(Model {
            path: path.to_owned(),
            context: Mutex::new(LoadedContext {
                context,
                gpu_requested,
            }),
        })))
    }

    pub fn decoder(&self, threads: u8) -> Result<WhisperDecoder, TranscriptionError> {
        if !(1..=16).contains(&threads) {
            return Err(TranscriptionError::InvalidInput);
        }
        let mut loaded = self
            .0
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let gpu = loaded.gpu_requested;
        let (state, _) = with_cpu_fallback(gpu, |gpu| {
            if loaded.gpu_requested && !gpu {
                loaded.context = load_context(&self.0.path, None)?;
                loaded.gpu_requested = false;
            }
            loaded.context.create_state()
        })?;
        Ok(WhisperDecoder {
            state,
            threads,
            end_of_text_token: loaded.context.token_eot(),
            // Keep the existing recognizer available if optional VAD cannot initialize.
            speech_gate: vad::SpeechGate::new().ok(),
        })
    }
}

pub struct WhisperDecoder {
    state: WhisperState,
    threads: u8,
    end_of_text_token: whisper_rs::WhisperTokenId,
    speech_gate: Option<vad::SpeechGate>,
}

impl WhisperDecoder {
    // Run on the bounded transcription worker, never a capture/network/runtime thread.
    pub fn transcribe(
        &mut self,
        samples: &[f32],
        cancelled: Arc<AtomicBool>,
    ) -> Result<Vec<RecognizedSegment>, TranscriptionError> {
        self.transcribe_inner(samples, None, cancelled)
    }

    pub(crate) fn transcribe_window(
        &mut self,
        window: &streaming::InferenceWindow,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Vec<RecognizedSegment>, TranscriptionError> {
        self.transcribe_inner(&window.samples, Some(window), cancelled)
    }

    fn transcribe_inner(
        &mut self,
        samples: &[f32],
        window: Option<&streaming::InferenceWindow>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Vec<RecognizedSegment>, TranscriptionError> {
        validate_input(samples)?;
        if cancelled.load(Ordering::Acquire) {
            return Err(TranscriptionError::Cancelled);
        }
        // Keep filtering on this worker, with the original samples and timestamps.
        // A failed gate falls back to Whisper rather than silently losing speech.
        let speech = self.speech_gate.as_mut().is_none_or(|gate| {
            match window {
                Some(window) => gate.has_speech_window(window),
                None => gate.has_speech(samples),
            }
            .unwrap_or(true)
        });
        if cancelled.load(Ordering::Acquire) {
            return Err(TranscriptionError::Cancelled);
        }
        if !speech {
            return Ok(Vec::new());
        }
        let mut parameters = FullParams::new(SamplingStrategy::Greedy { best_of: 1 });
        parameters.set_n_threads(i32::from(self.threads));
        parameters.set_language(None);
        parameters.set_no_context(true);
        parameters.set_print_progress(false);
        parameters.set_print_realtime(false);
        parameters.set_print_special(false);
        parameters.set_print_timestamps(false);
        parameters.set_temperature(0.0);
        parameters.set_temperature_inc(0.0);
        // The borrowed AtomicBool stays alive until synchronous full() returns.
        unsafe {
            parameters.set_abort_callback(Some(should_abort));
            parameters.set_abort_callback_user_data(Arc::as_ptr(&cancelled).cast_mut().cast());
        }
        let padded;
        let input = if samples.len() < SAMPLE_RATE {
            padded = {
                let mut input = vec![0.0; SAMPLE_RATE];
                input[..samples.len()].copy_from_slice(samples);
                input
            };
            &padded[..]
        } else {
            samples
        };
        let result = self.state.full(parameters, input);
        if cancelled.load(Ordering::Acquire) {
            return Err(TranscriptionError::Cancelled);
        }
        result?;
        let duration_ms = samples.len() as u64 * 1000 / SAMPLE_RATE as u64;
        self.state
            .as_iter()
            .map(|segment| {
                let start_ms = (segment.start_timestamp().max(0) as u64 * 10).min(duration_ms);
                let end_ms = (segment.end_timestamp().max(0) as u64 * 10)
                    .min(duration_ms)
                    .max(start_ms);
                Ok(RecognizedSegment {
                    start_ms,
                    end_ms,
                    text: segment.to_str()?.trim().to_owned(),
                    no_speech_probability: segment.no_speech_probability(),
                    average_log_probability: average_text_log_probability(
                        (0..segment.n_tokens()).filter_map(|index| {
                            segment.get_token(index).map(|token| {
                                let data = token.token_data();
                                (data.id, data.plog)
                            })
                        }),
                        self.end_of_text_token,
                    ),
                })
            })
            .collect()
    }
}

fn average_text_log_probability(
    tokens: impl Iterator<Item = (whisper_rs::WhisperTokenId, f32)>,
    end_of_text_token: whisper_rs::WhisperTokenId,
) -> Option<f32> {
    let mut total = 0.0_f64;
    let mut count = 0;
    for (id, probability) in tokens {
        // Timestamp/control tokens do not describe confidence in the actual words.
        if id < 0 || id >= end_of_text_token {
            continue;
        }
        if !probability.is_finite() || probability > 0.0 {
            return None;
        }
        total += f64::from(probability);
        count += 1;
    }
    (count > 0).then(|| (total / f64::from(count)) as f32)
}

unsafe extern "C" fn should_abort(user: *mut std::ffi::c_void) -> bool {
    // full() borrows a live Arc<AtomicBool>; this callback only performs an atomic load.
    unsafe { &*user.cast::<AtomicBool>() }.load(Ordering::Acquire)
}

fn validate_input(samples: &[f32]) -> Result<(), TranscriptionError> {
    if samples.is_empty()
        || samples.len() > MAX_WINDOW_SAMPLES
        || samples
            .iter()
            .any(|sample| !sample.is_finite() || sample.abs() > 1.0)
    {
        return Err(TranscriptionError::InvalidInput);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn word_confidence_excludes_special_tokens_and_handles_missing_scores() {
        let tokens = [(1, -0.2), (2, -0.6), (10, -8.0), (12, 0.0)];
        assert_eq!(
            average_text_log_probability(tokens.into_iter(), 10),
            Some(-0.4)
        );
        assert_eq!(
            average_text_log_probability([(10, 0.0)].into_iter(), 10),
            None
        );
        assert_eq!(
            average_text_log_probability([(1, f32::NAN)].into_iter(), 10),
            None
        );
        assert_eq!(
            average_text_log_probability([(1, f32::NEG_INFINITY)].into_iter(), 10),
            None
        );
    }

    #[cfg(feature = "vulkan")]
    #[test]
    #[ignore = "requires Vulkan with no available GPU and LIAZO_TEST_MODEL"]
    fn native_no_gpu_initializes_cpu_context_and_state() {
        whisper_rs::install_logging_hooks();
        assert_eq!(acceleration::gpu_device(), None);
        let path = PathBuf::from(std::env::var_os("LIAZO_TEST_MODEL").expect("model path"));
        let model = WhisperModel::load(&path).unwrap();
        assert!(!model.0.context.lock().unwrap().gpu_requested);
        let _decoder = model.decoder(4).unwrap();
        assert!(!model.0.context.lock().unwrap().gpu_requested);
        eprintln!("native_gpu_unavailable cpu_context_and_state=true");
    }

    #[cfg(any(feature = "metal", feature = "vulkan"))]
    #[test]
    #[ignore = "requires a native GPU backend and LIAZO_TEST_MODEL"]
    fn native_gpu_preference_survives_context_and_state_initialization() {
        whisper_rs::install_logging_hooks();
        let ordinal = acceleration::gpu_device().expect("a real native GPU must be available");
        let path = PathBuf::from(std::env::var_os("LIAZO_TEST_MODEL").expect("model path"));
        let model = WhisperModel::load(&path).unwrap();
        assert!(model.0.context.lock().unwrap().gpu_requested);
        let _decoder = model.decoder(4).unwrap();
        assert!(model.0.context.lock().unwrap().gpu_requested);
        eprintln!("native_gpu_preference ordinal={ordinal} wrapper_cpu_retry=false");
    }

    #[test]
    fn malformed_audio_is_rejected_before_native_inference() {
        assert!(validate_input(&[f32::NAN]).is_err());
        assert!(validate_input(&[f32::INFINITY]).is_err());
        assert!(validate_input(&vec![0.0; MAX_WINDOW_SAMPLES + 1]).is_err());
        assert!(validate_input(&[]).is_err());
        assert!(validate_input(&[0.5; 160]).is_ok());
    }
}
