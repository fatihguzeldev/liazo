use crate::{SAMPLE_RATE, TranscriptionError};
use audioadapter_buffers::direct::InterleavedSlice;
use liazo_core::protocol::{AudioMeta, SourceKey};
use rubato::{Async, FixedAsync, Resampler, SincInterpolationParameters};
use uuid::Uuid;

const CHUNK: usize = 512;

pub struct NormalizedAudio {
    pub stream_id: Uuid,
    pub start_ns: u64,
    pub samples: Vec<f32>,
    pub discontinuity: bool,
}

pub struct Normalizer {
    source: SourceKey,
    stream: Option<Stream>,
}

struct Stream {
    id: Uuid,
    rate: u32,
    channels: u8,
    next_position: u64,
    next_sequence: u64,
    anchor_ns: u64,
    input_frames: u64,
    output_frames: u64,
    delay: usize,
    pending: Vec<f32>,
    resampler: Option<Async<f32>>,
}

impl Normalizer {
    pub fn new(source: SourceKey) -> Self {
        Self {
            source,
            stream: None,
        }
    }

    pub fn push(
        &mut self,
        meta: &AudioMeta,
        pcm: &[u8],
    ) -> Result<NormalizedAudio, TranscriptionError> {
        meta.validate_payload(pcm.len())
            .map_err(|_| TranscriptionError::InvalidInput)?;
        if meta.source != self.source {
            return Err(TranscriptionError::InvalidInput);
        }
        // Validate the complete callback before changing stream/filter state.
        let mono = pcm
            .chunks_exact(usize::from(meta.channels) * 4)
            .map(|frame| {
                let mut sum = 0.0;
                for sample in frame.chunks_exact(4) {
                    let sample = f32::from_le_bytes(sample.try_into().unwrap());
                    if !sample.is_finite() {
                        return Err(TranscriptionError::InvalidInput);
                    }
                    sum += sample.clamp(-1.0, 1.0);
                }
                Ok(sum / f32::from(meta.channels))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reset = self.stream.as_ref().is_some_and(|stream| {
            meta.discontinuity
                || stream.id != meta.stream_id
                || stream.rate != meta.sample_rate
                || stream.channels != meta.channels
                || stream.next_position != meta.sample_position
                || stream.next_sequence != meta.sequence
                || (stream.anchor_ns.saturating_add(
                    stream.input_frames.saturating_mul(1_000_000_000) / u64::from(stream.rate),
                ))
                .abs_diff(meta.captured_ns)
                    > 500_000_000
        });
        if reset || self.stream.is_none() {
            self.stream = Some(Stream::new(meta)?);
        }
        let stream = self.stream.as_mut().unwrap();
        let start_ns = stream.next_ns();
        stream.next_position = meta.sample_position + u64::from(meta.frames);
        stream.next_sequence = meta.sequence.saturating_add(1);
        stream.input_frames += u64::from(meta.frames);
        let samples = if stream.resampler.is_none() {
            stream.output_frames += mono.len() as u64;
            mono
        } else {
            stream.pending.extend(mono);
            stream.process(false)?
        };
        Ok(NormalizedAudio {
            stream_id: stream.id,
            start_ns,
            samples,
            discontinuity: reset,
        })
    }

    pub fn finish(&mut self) -> Result<Option<NormalizedAudio>, TranscriptionError> {
        let Some(mut stream) = self.stream.take() else {
            return Ok(None);
        };
        let start_ns = stream.next_ns();
        let samples = stream.process(true)?;
        Ok(Some(NormalizedAudio {
            stream_id: stream.id,
            start_ns,
            samples,
            discontinuity: false,
        }))
    }
}

impl Stream {
    fn new(meta: &AudioMeta) -> Result<Self, TranscriptionError> {
        let resampler = if meta.sample_rate as usize == SAMPLE_RATE {
            None
        } else {
            Some(
                Async::<f32>::new_sinc(
                    SAMPLE_RATE as f64 / f64::from(meta.sample_rate),
                    1.0,
                    &SincInterpolationParameters::default(),
                    CHUNK,
                    1,
                    FixedAsync::Input,
                )
                .map_err(|_| TranscriptionError::Resampling)?,
            )
        };
        let delay = resampler.as_ref().map_or(0, Resampler::output_delay);
        Ok(Self {
            id: meta.stream_id,
            rate: meta.sample_rate,
            channels: meta.channels,
            next_position: meta.sample_position,
            next_sequence: meta.sequence,
            anchor_ns: meta.captured_ns,
            input_frames: 0,
            output_frames: 0,
            pending: Vec::with_capacity(CHUNK + 8192),
            delay,
            resampler,
        })
    }

    fn next_ns(&self) -> u64 {
        self.anchor_ns
            .saturating_add(self.output_frames.saturating_mul(62_500))
    }

    fn process(&mut self, finish: bool) -> Result<Vec<f32>, TranscriptionError> {
        let Some(resampler) = &mut self.resampler else {
            return Ok(Vec::new());
        };
        let expected = self.input_frames * SAMPLE_RATE as u64 / u64::from(self.rate);
        let mut samples = Vec::new();
        let mut offset = 0;
        while self.pending.len() - offset >= CHUNK || (finish && self.output_frames < expected) {
            if self.pending.len() - offset < CHUNK {
                self.pending.resize(offset + CHUNK, 0.0);
            }
            let input = InterleavedSlice::new(&self.pending[offset..offset + CHUNK], 1, CHUNK)
                .map_err(|_| TranscriptionError::Resampling)?;
            let output = resampler
                .process(&input, None)
                .map_err(|_| TranscriptionError::Resampling)?
                .take_data();
            let skip = self.delay.min(output.len());
            self.delay -= skip;
            let available = ((expected - self.output_frames) as usize).min(output.len() - skip);
            samples.extend(
                output[skip..skip + available]
                    .iter()
                    .map(|sample| sample.clamp(-1.0, 1.0)),
            );
            self.output_frames += available as u64;
            offset += CHUNK;
        }
        self.pending.drain(..offset);
        Ok(samples)
    }
}
