use liazo_transcription::{TranscriptionError, WhisperModel};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[test]
#[ignore = "requires LIAZO_TEST_MODEL and a 16 khz mono LIAZO_TEST_SPEECH fixture"]
fn cancelling_real_inference_releases_callback_state_and_allows_reuse() {
    let model = PathBuf::from(std::env::var_os("LIAZO_TEST_MODEL").expect("model path"));
    let speech = std::env::var_os("LIAZO_TEST_SPEECH").expect("fixture path");
    let mut reader = hound::WavReader::open(speech).unwrap();
    assert_eq!(reader.spec().channels, 1);
    assert_eq!(reader.spec().sample_rate, 16_000);
    let samples = reader
        .samples::<i16>()
        .map(|sample| f32::from(sample.unwrap()) / 32768.0)
        .collect::<Vec<_>>();
    let model = WhisperModel::load(&model).unwrap();
    let mut decoder = model.decoder(4).unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let signal = cancelled.clone();
    let trigger = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        signal.store(true, Ordering::Release);
    });
    assert!(matches!(
        decoder.transcribe(&samples, cancelled.clone()),
        Err(TranscriptionError::Cancelled)
    ));
    trigger.join().unwrap();
    assert_eq!(Arc::strong_count(&cancelled), 1);
    let recovered = decoder
        .transcribe(&samples, Arc::new(AtomicBool::new(false)))
        .unwrap();
    assert!(!recovered.is_empty());
    assert!(recovered.iter().any(|segment| !segment.text.is_empty()));
}
