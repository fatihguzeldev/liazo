use crate::{TranscriptionError, streaming::InferenceWindow, validate_input};
use liazo_core::protocol::SourceKey;
use std::{collections::VecDeque, ffi::c_void, ptr::NonNull};
use uuid::Uuid;
use whisper_rs::whisper_rs_sys as native;

// MIT, Silero Team. Converted by ggml-org; pinned to HF revision
// https://huggingface.co/ggml-org/whisper-vad/tree/9ffd54a1e1ee413ddf265af9913beaf518d1639b
// SHA256: 29940d98d42b91fbd05ce489f3ecf7c72f0a42f027e4875919a28fb4c04ea2cf
const MODEL: &[u8] = include_bytes!("../models/ggml-silero-v5.1.2.bin");
const FRAME_SAMPLES: usize = 512;
// The segmenter already requires 120ms of activity. Allow 96ms here so the
// model's 32ms frame boundaries do not discard a genuine short answer.
const MIN_SPEECH_SAMPLES: usize = 3 * FRAME_SAMPLES;

struct ConfirmedWindow {
    id: Uuid,
    source: SourceKey,
    stream: Uuid,
    start_ns: u64,
    samples: usize,
}

#[derive(Default)]
// Locally generated windows append audio to the same utterance. Once a prefix
// contains speech, later revisions still contain it. Negatives/errors can change.
struct SpeechCache(VecDeque<ConfirmedWindow>);

impl SpeechCache {
    fn check(
        &mut self,
        window: &InferenceWindow,
        detect: impl FnOnce() -> Result<bool, TranscriptionError>,
    ) -> Result<bool, TranscriptionError> {
        let same = |known: &ConfirmedWindow| {
            known.id == window.id
                && known.source == window.source
                && known.stream == window.stream_id
                && known.start_ns == window.start_ns
        };
        let cached = self
            .0
            .iter()
            .any(|known| same(known) && known.samples <= window.samples.len());
        let result = if cached { Ok(true) } else { detect() };
        if window.ending.is_some() {
            self.0.retain(|known| !same(known));
        } else if !cached && matches!(result, Ok(true)) {
            self.0.retain(|known| !same(known));
            if self.0.len() == 16 {
                self.0.pop_front();
            }
            self.0.push_back(ConfirmedWindow {
                id: window.id,
                source: window.source.clone(),
                stream: window.stream_id,
                start_ns: window.start_ns,
                samples: window.samples.len(),
            });
        }
        result
    }
}

pub(crate) struct SpeechGate {
    context: NonNull<native::whisper_vad_context>,
    positive: SpeechCache,
}

// The context has exclusive ownership and is only accessed through &mut self.
unsafe impl Send for SpeechGate {}

impl SpeechGate {
    pub(crate) fn new() -> Result<Self, TranscriptionError> {
        whisper_rs::install_logging_hooks();
        let mut reader = ModelReader { position: 0 };
        let mut loader = native::whisper_model_loader {
            context: (&mut reader as *mut ModelReader).cast(),
            read: Some(read_model),
            eof: Some(model_eof),
            close: Some(close_model),
        };
        let parameters = native::whisper_vad_context_params {
            n_threads: 1,
            use_gpu: false,
            gpu_device: 0,
        };
        // Initialization synchronously copies the static model's weights. The
        // loader and its stack-owned reader are never retained by native code.
        let context = unsafe { native::whisper_vad_init_with_params(&mut loader, parameters) };
        NonNull::new(context)
            .map(|context| Self {
                context,
                positive: SpeechCache::default(),
            })
            .ok_or_else(|| whisper_rs::WhisperError::NullPointer.into())
    }

    pub(crate) fn has_speech(&mut self, samples: &[f32]) -> Result<bool, TranscriptionError> {
        validate_input(samples)?;
        Self::detect(self.context, samples)
    }

    pub(crate) fn has_speech_window(
        &mut self,
        window: &InferenceWindow,
    ) -> Result<bool, TranscriptionError> {
        validate_input(&window.samples)?;
        let context = self.context;
        self.positive
            .check(window, || Self::detect(context, &window.samples))
    }

    fn detect(
        context: NonNull<native::whisper_vad_context>,
        samples: &[f32],
    ) -> Result<bool, TranscriptionError> {
        // Native VAD resets recurrent state on each call. Process each bounded
        // window in full, without sharing recurrent history between sources.
        let detected = unsafe {
            native::whisper_vad_detect_speech(
                context.as_ptr(),
                samples.as_ptr(),
                samples.len() as i32,
            )
        };
        if !detected {
            return Err(whisper_rs::WhisperError::GenericError(-1).into());
        }
        let count = unsafe { native::whisper_vad_n_probs(context.as_ptr()) };
        let probabilities = unsafe { native::whisper_vad_probs(context.as_ptr()) };
        if count != samples.len().div_ceil(FRAME_SAMPLES) as i32 || probabilities.is_null() {
            return Err(whisper_rs::WhisperError::GenericError(-1).into());
        }
        // Native owns this array until its next call; &mut self prevents another
        // call or drop while the slice is borrowed.
        let probabilities = unsafe { std::slice::from_raw_parts(probabilities, count as usize) };
        has_speech(probabilities, samples.len())
    }
}

impl Drop for SpeechGate {
    fn drop(&mut self) {
        unsafe { native::whisper_vad_free(self.context.as_ptr()) };
    }
}

fn has_speech(probabilities: &[f32], sample_count: usize) -> Result<bool, TranscriptionError> {
    if probabilities
        .iter()
        .any(|probability| !probability.is_finite() || !(0.0..=1.0).contains(probability))
    {
        return Err(whisper_rs::WhisperError::GenericError(-1).into());
    }
    let mut active = false;
    let mut voiced = 0;
    for (index, probability) in probabilities.iter().enumerate() {
        if *probability >= 0.5 {
            active = true;
        } else if *probability < 0.35 {
            active = false;
            voiced = 0;
        }
        if active {
            voiced += FRAME_SAMPLES.min(sample_count - index * FRAME_SAMPLES);
            if voiced >= MIN_SPEECH_SAMPLES {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

struct ModelReader {
    position: usize,
}

unsafe extern "C" fn read_model(context: *mut c_void, output: *mut c_void, size: usize) -> usize {
    // Native initialization calls these callbacks synchronously with the live
    // ModelReader and a writable destination of `size` bytes.
    let reader = unsafe { &mut *context.cast::<ModelReader>() };
    let remaining = &MODEL[reader.position..];
    let count = remaining.len().min(size);
    unsafe { std::ptr::copy_nonoverlapping(remaining.as_ptr(), output.cast(), count) };
    reader.position += count;
    count
}

unsafe extern "C" fn model_eof(context: *mut c_void) -> bool {
    unsafe { &*context.cast::<ModelReader>() }.position == MODEL.len()
}

unsafe extern "C" fn close_model(_: *mut c_void) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MAX_WINDOW_SAMPLES, SAMPLE_RATE};

    fn window() -> InferenceWindow {
        InferenceWindow {
            id: Uuid::new_v4(),
            source: SourceKey {
                id: "mic".into(),
                generation: Uuid::new_v4(),
            },
            stream_id: Uuid::new_v4(),
            revision: 1,
            start_ns: 1_000_000_000,
            end_ns: 2_000_000_000,
            ending: None,
            samples: vec![0.0; SAMPLE_RATE].into(),
        }
    }

    #[test]
    fn confirmed_speech_is_reused_for_expanding_windows_and_removed_on_final() {
        let mut cache = SpeechCache::default();
        let mut window = window();
        assert!(cache.check(&window, || Ok(true)).unwrap());
        window.samples = vec![0.0; 2 * SAMPLE_RATE].into();
        window.end_ns += 1_000_000_000;
        assert!(
            cache
                .check(&window, || panic!("positive VAD ran twice"))
                .unwrap()
        );
        window.ending = Some(liazo_core::transcript::EndReason::Silence);
        assert!(
            cache
                .check(&window, || panic!("final reran positive VAD"))
                .unwrap()
        );
        assert!(cache.0.is_empty());
    }

    #[test]
    fn negative_and_failed_decisions_are_rechecked_when_audio_grows() {
        let mut cache = SpeechCache::default();
        let mut window = window();
        assert!(!cache.check(&window, || Ok(false)).unwrap());
        assert!(
            cache
                .check(&window, || Err(TranscriptionError::InvalidInput))
                .is_err()
        );
        assert!(cache.0.is_empty());
        window.samples = vec![0.0; 2 * SAMPLE_RATE].into();
        assert!(cache.check(&window, || Ok(true)).unwrap());
        assert_eq!(cache.0.len(), 1);
    }

    #[test]
    fn cached_speech_cannot_cross_utterance_source_stream_or_audio_prefix() {
        let mut cache = SpeechCache::default();
        let base = window();
        assert!(cache.check(&base, || Ok(true)).unwrap());
        for field in 0..6 {
            let mut other = base.clone();
            match field {
                0 => other.id = Uuid::new_v4(),
                1 => other.source.id = "other mic".into(),
                2 => other.source.generation = Uuid::new_v4(),
                3 => other.stream_id = Uuid::new_v4(),
                4 => other.start_ns += 1,
                _ => other.samples = vec![0.0; SAMPLE_RATE / 2].into(),
            }
            assert!(!cache.check(&other, || Ok(false)).unwrap());
        }
    }

    #[test]
    fn dropped_finals_cannot_grow_the_positive_cache_without_bound() {
        let mut cache = SpeechCache::default();
        let first = window();
        assert!(cache.check(&first, || Ok(true)).unwrap());
        for _ in 0..16 {
            assert!(cache.check(&window(), || Ok(true)).unwrap());
        }
        assert_eq!(cache.0.len(), 16);
        assert!(!cache.check(&first, || Ok(false)).unwrap());
    }

    #[test]
    fn a_shorter_failed_final_still_removes_the_confirmed_prefix() {
        let mut cache = SpeechCache::default();
        let mut window = window();
        assert!(cache.check(&window, || Ok(true)).unwrap());
        window.samples = vec![0.0; SAMPLE_RATE / 2].into();
        window.ending = Some(liazo_core::transcript::EndReason::Gap);
        assert!(
            cache
                .check(&window, || Err(TranscriptionError::Cancelled))
                .is_err()
        );
        assert!(cache.0.is_empty());
    }

    #[test]
    fn silence_does_not_reach_the_recognizer() {
        let mut gate = SpeechGate::new().unwrap();
        assert!(!gate.has_speech(&vec![0.0; SAMPLE_RATE]).unwrap());
    }

    #[test]
    fn keyboard_like_clicks_above_the_energy_gate_are_not_speech() {
        let mut samples = vec![0.0; SAMPLE_RATE];
        for start in (320..SAMPLE_RATE - 160).step_by(1600) {
            for offset in 0..80 {
                let decay = 1.0 - offset as f32 / 80.0;
                samples[start + offset] = 0.5 * decay * (offset as f32 * 1.3).sin();
            }
        }
        let energy =
            samples.iter().map(|sample| sample * sample).sum::<f32>() / samples.len() as f32;
        assert!(energy.sqrt() > 0.003);
        let mut gate = SpeechGate::new().unwrap();
        assert!(!gate.has_speech(&samples).unwrap());
    }

    #[test]
    fn malformed_or_unbounded_windows_are_rejected() {
        let mut gate = SpeechGate::new().unwrap();
        for samples in [&[][..], &[f32::NAN], &[f32::INFINITY], &[1.01]] {
            assert!(gate.has_speech(samples).is_err());
        }
        assert!(gate.has_speech(&vec![0.0; MAX_WINDOW_SAMPLES + 1]).is_err());
        let mut window = window();
        gate.positive.check(&window, || Ok(true)).unwrap();
        window.samples = vec![f32::NAN; SAMPLE_RATE].into();
        assert!(gate.has_speech_window(&window).is_err());
    }

    #[test]
    fn boundary_confidence_does_not_cut_off_a_short_answer() {
        assert!(has_speech(&[0.6, 0.4, 0.4], 3 * FRAME_SAMPLES).unwrap());
        assert!(!has_speech(&[0.6, 0.2, 0.6, 0.2, 0.6], 5 * FRAME_SAMPLES).unwrap());
        assert!(!has_speech(&[0.6, 0.6, 0.6], 2 * FRAME_SAMPLES + 1).unwrap());
    }

    #[test]
    #[ignore = "requires a 16 khz mono LIAZO_TEST_SPEECH fixture"]
    fn real_speech_remains_detectable_after_another_source_was_silent() {
        let samples = speech_fixture();
        let mut gate = SpeechGate::new().unwrap();
        assert!(!gate.has_speech(&vec![0.0; SAMPLE_RATE]).unwrap());
        assert!(gate.has_speech(&samples).unwrap());
        let quiet = samples
            .iter()
            .map(|sample| sample * 0.1)
            .collect::<Vec<_>>();
        assert!(gate.has_speech(&quiet).unwrap());
        assert!(!gate.has_speech(&vec![0.0; SAMPLE_RATE]).unwrap());
    }

    #[test]
    #[ignore = "requires a 16 khz mono LIAZO_TEST_SPEECH fixture; emits native VAD timings"]
    fn native_window_gate_cost() {
        use std::time::Instant;

        let samples = speech_fixture();
        let started = Instant::now();
        let mut gate = SpeechGate::new().unwrap();
        eprintln!(
            "vad_init_ms={:.3}",
            started.elapsed().as_secs_f64() * 1000.0
        );
        for seconds in [1, 20] {
            let window = samples
                .iter()
                .copied()
                .cycle()
                .take(seconds * SAMPLE_RATE)
                .collect::<Vec<_>>();
            for iteration in 0..3 {
                let started = Instant::now();
                assert!(gate.has_speech(&window).unwrap());
                eprintln!(
                    "vad_window_s={seconds} iteration={iteration} elapsed_ms={:.3}",
                    started.elapsed().as_secs_f64() * 1000.0
                );
            }
        }
        assert!(!gate.has_speech(&vec![0.0; SAMPLE_RATE]).unwrap());
    }

    #[test]
    #[ignore = "requires a 16 khz mono LIAZO_TEST_SPEECH fixture; emits cached VAD timings"]
    fn native_cached_window_gate_cost() {
        use std::time::Instant;

        let samples = speech_fixture();
        let mut gate = SpeechGate::new().unwrap();
        let mut window = window();
        window.samples = samples
            .iter()
            .copied()
            .cycle()
            .take(SAMPLE_RATE)
            .collect::<Vec<_>>()
            .into();
        let started = Instant::now();
        assert!(gate.has_speech_window(&window).unwrap());
        eprintln!(
            "vad_first_window_ms={:.3}",
            started.elapsed().as_secs_f64() * 1000.0
        );
        window.samples = samples
            .iter()
            .copied()
            .cycle()
            .take(20 * SAMPLE_RATE)
            .collect::<Vec<_>>()
            .into();
        window.end_ns = window.start_ns + 20_000_000_000;
        for iteration in 0..3 {
            let started = Instant::now();
            assert!(gate.has_speech_window(&window).unwrap());
            eprintln!(
                "vad_cached_20s iteration={iteration} elapsed_ms={:.3}",
                started.elapsed().as_secs_f64() * 1000.0
            );
        }
        window.ending = Some(liazo_core::transcript::EndReason::Silence);
        assert!(gate.has_speech_window(&window).unwrap());
        assert!(gate.positive.0.is_empty());
    }

    fn speech_fixture() -> Vec<f32> {
        let mut reader =
            hound::WavReader::open(std::env::var_os("LIAZO_TEST_SPEECH").expect("fixture path"))
                .unwrap();
        assert_eq!(
            (reader.spec().sample_rate, reader.spec().channels),
            (16_000, 1)
        );
        let samples = reader
            .samples::<i16>()
            .map(|sample| f32::from(sample.unwrap()) / 32768.0)
            .collect::<Vec<_>>();
        assert!(!samples.is_empty());
        samples
    }
}
