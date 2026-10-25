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
    let model = WhisperModel::load_cpu(&model).unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LIAZO_TEST_MODEL and a 16 khz mono LIAZO_TEST_SPEECH fixture"]
async fn real_streaming_inference_publishes_revisable_partials_and_attributed_finals() {
    use liazo_core::{protocol::SourceKey, transcript::EndReason};
    use liazo_transcription::{
        audio::NormalizedAudio,
        streaming::{Segmenter, StreamingSettings},
        worker::{InferenceWorker, RecognitionUpdate},
    };
    use uuid::Uuid;
    let model = WhisperModel::load(&PathBuf::from(
        std::env::var_os("LIAZO_TEST_MODEL").expect("model path"),
    ))
    .unwrap();
    let mut reader =
        hound::WavReader::open(std::env::var_os("LIAZO_TEST_SPEECH").expect("fixture path"))
            .unwrap();
    assert_eq!(
        (reader.spec().sample_rate, reader.spec().channels),
        (16_000, 1)
    );
    let samples: Vec<_> = reader
        .samples::<i16>()
        .map(|sample| f32::from(sample.unwrap()) / 32768.0)
        .collect();
    let source = SourceKey {
        id: "audio:fixture".into(),
        generation: Uuid::new_v4(),
    };
    let stream = Uuid::new_v4();
    let mut segmenter = Segmenter::new(source.clone(), StreamingSettings::default()).unwrap();
    let mut worker = InferenceWorker::start(model, 4).await.unwrap();
    for (index, chunk) in samples.chunks(320).enumerate() {
        for window in segmenter
            .push(NormalizedAudio {
                stream_id: stream,
                start_ns: 1_000_000_000 + index as u64 * 20_000_000,
                samples: chunk.to_vec(),
                discontinuity: false,
            })
            .unwrap()
        {
            assert!(worker.submit(window).unwrap().is_empty());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if let Some(window) = segmenter.finish(EndReason::Stopped) {
        worker.submit(window).unwrap();
    }
    worker.finish();
    let mut transcripts = Vec::new();
    while let Some(update) = tokio::time::timeout(Duration::from_secs(10), worker.next())
        .await
        .unwrap()
    {
        match update {
            RecognitionUpdate::Transcript(transcript) => {
                transcript.validate().unwrap();
                transcripts.push(transcript);
            }
            RecognitionUpdate::Gap(_) => panic!("real engine dropped a speech window"),
        }
    }
    worker.join().await.unwrap();
    assert!(
        transcripts
            .iter()
            .all(|transcript| transcript.source == source && transcript.stream_id == stream)
    );
    assert!(transcripts.iter().any(|partial| partial.ending.is_none()
        && !partial.text.is_empty()
        && transcripts.iter().any(|final_| final_.id == partial.id
            && final_.ending.is_some()
            && final_.revision > partial.revision
            && final_.text.split_whitespace().count() >= 3)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LIAZO_TEST_MODEL"]
async fn nonspeech_windows_preserve_an_empty_terminal_transcript() {
    use liazo_core::{protocol::SourceKey, transcript::EndReason};
    use liazo_transcription::{
        SAMPLE_RATE,
        streaming::InferenceWindow,
        worker::{InferenceWorker, RecognitionUpdate},
    };
    use uuid::Uuid;

    let model = WhisperModel::load(&PathBuf::from(
        std::env::var_os("LIAZO_TEST_MODEL").expect("model path"),
    ))
    .unwrap();
    let mut samples = vec![0.0; SAMPLE_RATE];
    for start in (320..SAMPLE_RATE - 160).step_by(1600) {
        for offset in 0..80 {
            let decay = 1.0 - offset as f32 / 80.0;
            samples[start + offset] = 0.5 * decay * (offset as f32 * 1.3).sin();
        }
    }
    let rms =
        (samples.iter().map(|sample| sample * sample).sum::<f32>() / samples.len() as f32).sqrt();
    assert!(rms > 0.003, "fixture must pass the existing energy gate");
    let mut window = InferenceWindow {
        id: Uuid::new_v4(),
        source: SourceKey {
            id: "audio:keyboard-fixture".into(),
            generation: Uuid::new_v4(),
        },
        stream_id: Uuid::new_v4(),
        revision: 1,
        start_ns: 2_000_000_000,
        end_ns: 3_000_000_000,
        ending: None,
        samples: samples.into(),
    };
    let mut worker = InferenceWorker::start(model, 4).await.unwrap();
    assert!(worker.submit(window.clone()).unwrap().is_empty());
    // A suppressed partial emits no update. Submit its final without waiting
    // for a partial, allowing the worker's normal coalescing/cancellation path.
    window.revision = 2;
    window.ending = Some(EndReason::Stopped);
    assert!(worker.submit(window.clone()).unwrap().is_empty());
    worker.finish();

    let mut finals = 0;
    while let Some(update) = tokio::time::timeout(Duration::from_secs(10), worker.next())
        .await
        .expect("speech worker did not finish its bounded noise window")
    {
        let RecognitionUpdate::Transcript(transcript) = update else {
            panic!("suppressing noise must not produce a transcription gap");
        };
        transcript.validate().unwrap();
        assert!(transcript.text.is_empty());
        assert_eq!(transcript.ending, window.ending);
        assert_eq!(transcript.id, window.id);
        assert_eq!(transcript.source, window.source);
        assert_eq!(transcript.stream_id, window.stream_id);
        assert_eq!(transcript.revision, window.revision);
        assert_eq!(
            (transcript.start_ns, transcript.end_ns),
            (window.start_ns, window.end_ns)
        );
        finals += 1;
    }
    worker.join().await.unwrap();
    assert_eq!(
        finals, 1,
        "noise must close exactly one attributed utterance"
    );
}
