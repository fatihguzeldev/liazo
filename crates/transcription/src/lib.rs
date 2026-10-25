use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use serde::Serialize;
use whisper_rs::{
    FullParams, SamplingStrategy, WhisperContext, WhisperContextParameters, WhisperState,
};

pub mod audio;
pub mod streaming;
pub mod worker;

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
}

#[derive(Clone)]
pub struct WhisperModel(Arc<WhisperContext>);

impl WhisperModel {
    pub fn load(path: &Path) -> Result<Self, TranscriptionError> {
        // Disable native logs, including debug token text. Results belong only in observations.
        whisper_rs::install_logging_hooks();
        let size = std::fs::metadata(path)?.len();
        if size == 0 || size > 2 * 1024 * 1024 * 1024 {
            return Err(TranscriptionError::InvalidInput);
        }
        let mut parameters = WhisperContextParameters::default();
        parameters.use_gpu(false);
        Ok(Self(Arc::new(WhisperContext::new_with_params(
            path, parameters,
        )?)))
    }

    pub fn decoder(&self, threads: u8) -> Result<WhisperDecoder, TranscriptionError> {
        if !(1..=16).contains(&threads) {
            return Err(TranscriptionError::InvalidInput);
        }
        Ok(WhisperDecoder {
            state: self.0.create_state()?,
            threads,
        })
    }
}

pub struct WhisperDecoder {
    state: WhisperState,
    threads: u8,
}

impl WhisperDecoder {
    // Run on the bounded transcription worker, never a capture/network/runtime thread.
    pub fn transcribe(
        &mut self,
        samples: &[f32],
        cancelled: Arc<AtomicBool>,
    ) -> Result<Vec<RecognizedSegment>, TranscriptionError> {
        validate_input(samples)?;
        if cancelled.load(Ordering::Acquire) {
            return Err(TranscriptionError::Cancelled);
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
                })
            })
            .collect()
    }
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
    fn malformed_audio_is_rejected_before_native_inference() {
        assert!(validate_input(&[f32::NAN]).is_err());
        assert!(validate_input(&[f32::INFINITY]).is_err());
        assert!(validate_input(&vec![0.0; MAX_WINDOW_SAMPLES + 1]).is_err());
        assert!(validate_input(&[]).is_err());
        assert!(validate_input(&[0.5; 160]).is_ok());
    }
}
