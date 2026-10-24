use liazo_transcription::{MAX_WINDOW_SAMPLES, SAMPLE_RATE, WhisperModel};
use std::{
    error::Error,
    path::Path,
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    let model = args.get(1).ok_or("provide a model path")?;
    let wav = args.get(2).ok_or("provide a 16 khz mono wav fixture")?;
    let mut reader = hound::WavReader::open(wav)?;
    let format = reader.spec();
    if format.channels != 1
        || format.sample_rate != SAMPLE_RATE as u32
        || format.bits_per_sample != 16
    {
        return Err("fixture must use 16 khz mono int16 pcm".into());
    }
    let samples: Vec<f32> = reader
        .samples::<i16>()
        .take(MAX_WINDOW_SAMPLES + 1)
        .map(|sample| sample.map(|sample| f32::from(sample) / 32768.0))
        .collect::<Result<_, _>>()?;
    let started = Instant::now();
    let model = WhisperModel::load(Path::new(model))?;
    let mut decoder = model.decoder(4)?;
    let load_ms = started.elapsed().as_millis();
    for pass in 0..2 {
        let started = Instant::now();
        let segments = decoder.transcribe(&samples, Arc::new(AtomicBool::new(false)))?;
        println!(
            "{}",
            serde_json::json!({ "pass": pass, "load_ms": load_ms, "inference_ms": started.elapsed().as_millis(),
            "audio_ms": samples.len() * 1000 / SAMPLE_RATE, "segments": segments })
        );
    }
    Ok(())
}
