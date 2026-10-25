use liazo_core::{protocol::SourceKey, transcript::EndReason};
use liazo_transcription::{
    audio::NormalizedAudio,
    streaming::{Segmenter, StreamingSettings},
};
use uuid::Uuid;

fn audio(stream: Uuid, offset: usize, voiced: bool) -> NormalizedAudio {
    NormalizedAudio {
        stream_id: stream,
        start_ns: 2_000_000_000 + offset as u64 * 62_500,
        samples: vec![if voiced { 0.1 } else { 0.0 }; 320],
        discontinuity: false,
    }
}

#[test]
fn partial_revisions_and_final_share_an_id_and_silence_is_not_transcribed() {
    let source = SourceKey {
        id: "audio:test".into(),
        generation: Uuid::new_v4(),
    };
    let mut segmenter = Segmenter::new(source.clone(), StreamingSettings::default()).unwrap();
    let stream = Uuid::new_v4();
    let mut jobs = Vec::new();
    for frame in 0..100 {
        jobs.extend(
            segmenter
                .push(audio(stream, frame * 320, frame < 60))
                .unwrap(),
        );
    }
    assert!(jobs.len() >= 2);
    let first = &jobs[0];
    let last = jobs.last().unwrap();
    assert_eq!(last.ending, Some(EndReason::Silence));
    assert!(
        jobs.iter()
            .all(|job| job.id == first.id && job.source == source && job.stream_id == stream)
    );
    assert!(
        jobs.windows(2)
            .all(|pair| pair[0].revision < pair[1].revision)
    );
    assert_eq!(first.start_ns, 2_000_000_000);
    assert!(last.end_ns <= 4_000_000_000);
    assert!(segmenter.finish(EndReason::Stopped).is_none());
    for frame in 100..300 {
        assert!(
            segmenter
                .push(audio(stream, frame * 320, false))
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn discontinuity_finalizes_old_speech_and_new_stream_never_inherits_its_id() {
    let mut segmenter = Segmenter::new(
        SourceKey {
            id: "audio:test".into(),
            generation: Uuid::new_v4(),
        },
        StreamingSettings::default(),
    )
    .unwrap();
    let old = Uuid::new_v4();
    let mut jobs = Vec::new();
    for frame in 0..50 {
        jobs.extend(segmenter.push(audio(old, frame * 320, true)).unwrap());
    }
    let new = Uuid::new_v4();
    let mut next = audio(new, 0, true);
    next.start_ns = 10_000_000_000;
    let gap = segmenter.push(next).unwrap();
    assert_eq!(gap.len(), 1);
    assert_eq!(gap[0].id, jobs[0].id);
    assert_eq!(gap[0].ending, Some(EndReason::Gap));
    for frame in 1..40 {
        let mut next = audio(new, frame * 320, true);
        next.start_ns += 8_000_000_000;
        jobs.extend(segmenter.push(next).unwrap());
    }
    let final_job = segmenter.finish(EndReason::Stopped).unwrap();
    assert_ne!(final_job.id, gap[0].id);
    assert_eq!(final_job.start_ns, 10_000_000_000);
    assert_eq!(final_job.stream_id, new);
}

#[test]
fn continuous_noise_or_speech_cannot_grow_an_unbounded_utterance() {
    let settings = StreamingSettings {
        max_utterance_ms: 2000,
        ..StreamingSettings::default()
    };
    let mut segmenter = Segmenter::new(
        SourceKey {
            id: "audio:test".into(),
            generation: Uuid::new_v4(),
        },
        settings,
    )
    .unwrap();
    let stream = Uuid::new_v4();
    let mut final_count = 0;
    for frame in 0..1000 {
        for job in segmenter.push(audio(stream, frame * 320, true)).unwrap() {
            assert!(job.samples.len() <= 32_000);
            if job.ending.is_some() {
                assert_eq!(job.ending, Some(EndReason::Limit));
                final_count += 1;
            }
        }
    }
    assert_eq!(final_count, 10);
}
