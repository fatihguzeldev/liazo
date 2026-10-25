use liazo_core::protocol::{AudioMeta, SourceKey};
use liazo_transcription::audio::Normalizer;
use uuid::Uuid;

fn metadata(rate: u32, channels: u8) -> AudioMeta {
    AudioMeta {
        source: SourceKey {
            id: "audio:test".into(),
            generation: Uuid::new_v4(),
        },
        stream_id: Uuid::new_v4(),
        sequence: 1,
        captured_ns: 1_000_000_000,
        sample_position: 0,
        sample_rate: rate,
        channels,
        frames: 1,
        discontinuity: false,
    }
}

fn convert(rate: u32, frequency: f32, chunk: usize) -> Vec<f32> {
    let mut meta = metadata(rate, 2);
    let mut normalizer = Normalizer::new(meta.source.clone());
    let mut output = Vec::new();
    for offset in (0..rate as usize).step_by(chunk) {
        meta.frames = chunk.min(rate as usize - offset) as u32;
        meta.sample_position = offset as u64;
        meta.captured_ns = 1_000_000_000 + offset as u64 * 1_000_000_000 / u64::from(rate);
        let pcm: Vec<_> = (offset..offset + meta.frames as usize)
            .flat_map(|n| {
                let value =
                    0.5 * (n as f32 * frequency * std::f32::consts::TAU / rate as f32).sin();
                [value, value].into_iter().flat_map(f32::to_le_bytes)
            })
            .collect();
        let audio = normalizer.push(&meta, &pcm).unwrap();
        assert!(!audio.discontinuity);
        assert_eq!(audio.start_ns, 1_000_000_000 + output.len() as u64 * 62_500);
        output.extend(audio.samples);
        meta.sequence += 1;
    }
    output.extend(normalizer.finish().unwrap().unwrap().samples);
    assert_eq!(output.len(), 16_000);
    output
}

#[test]
fn resampling_preserves_timing_across_arbitrary_callback_boundaries_and_rejects_aliasing() {
    for rate in [8_000, 16_000, 44_100, 48_000, 96_000] {
        let a = convert(rate, 1000.0, 137);
        let b = convert(rate, 1000.0, 2048);
        assert!(a.iter().zip(&b).all(|(a, b)| (a - b).abs() < 1e-6));
        let rms = (a[320..15_680].iter().map(|x| x * x).sum::<f32>() / 15_360.0).sqrt();
        assert!((rms - 0.35355).abs() < 0.01, "rate {rate}: {rms}");
    }
    let aliased = convert(48_000, 12_000.0, 701);
    let rms = (aliased[320..15_680].iter().map(|x| x * x).sum::<f32>() / 15_360.0).sqrt();
    assert!(rms < 0.005, "out-of-band input leaked into speech: {rms}");
}

#[test]
fn a_gap_or_format_change_resets_filter_state_and_never_joins_different_streams() {
    let mut meta = metadata(48_000, 1);
    meta.frames = 100;
    let mut normalizer = Normalizer::new(meta.source.clone());
    assert!(
        normalizer
            .push(&meta, &vec![0; 400])
            .unwrap()
            .samples
            .is_empty()
    );
    meta.stream_id = Uuid::new_v4();
    meta.sample_rate = 16_000;
    meta.captured_ns = 5_000_000_000;
    let pcm: Vec<_> = std::iter::repeat_n(-0.25_f32, 100)
        .flat_map(f32::to_le_bytes)
        .collect();
    let reset = normalizer.push(&meta, &pcm).unwrap();
    assert!(reset.discontinuity);
    assert_eq!(reset.start_ns, 5_000_000_000);
    assert_eq!(reset.samples, vec![-0.25; 100]);
    meta.sequence += 1;
    meta.sample_position = 200;
    assert!(normalizer.push(&meta, &pcm).unwrap().discontinuity);
    assert!(normalizer.push(&meta, &[0; 4]).is_err());
    let nan: Vec<_> = std::iter::repeat_n(f32::NAN, 100)
        .flat_map(f32::to_le_bytes)
        .collect();
    assert!(normalizer.push(&meta, &nan).is_err());
}
