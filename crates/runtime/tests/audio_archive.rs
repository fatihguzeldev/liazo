use liazo_core::protocol::SourceKey;
use liazo_runtime::archive::{
    ArchiveError, ArchiveIdentity, ArchiveIndex, ArchiveLimits, AudioPacket, AudioSegment,
    ReceiverTimeRange,
};
use liazo_transport::{Identity, SessionBinding};
use uuid::Uuid;

fn identity() -> ArchiveIdentity {
    ArchiveIdentity {
        provider: Identity::generate().unwrap().fingerprint(),
        binding: SessionBinding {
            session: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        },
        wall_anchor_unix_ms: 1_700_000_000_000,
    }
}

#[test]
fn byte_pressure_evicts_old_audio_and_a_duplicate_cannot_evict_its_original_payload() {
    let root = tempfile::tempdir().unwrap();
    let mut index = ArchiveIndex::open(
        root.path(),
        identity(),
        ArchiveLimits {
            duration_ms: 60_000,
            max_bytes: 4 * 1024 * 1024,
        },
    )
    .unwrap();
    let (mut first, _) = segment();
    first.sample_rate = 384000;
    first.channels = 8;
    first.packets = (0..8)
        .map(|i| AudioPacket {
            sequence: i + 1,
            sample_position: i * 8192,
            frames: 8192,
            captured_ns: i * 22_000_000,
            received_ns: 1_000_000_000 + i * 22_000_000,
            receiver_range: None,
            discontinuity: i == 0,
        })
        .collect();
    let pcm = vec![0; 8 * 8192 * 8 * 4];
    index
        .record_audio_policy(std::slice::from_ref(&first.source), 900_000_000)
        .unwrap();
    index.record_audio(first.clone(), &pcm).unwrap();
    assert!(matches!(
        index.record_audio(first.clone(), &pcm),
        Err(ArchiveError::IdentityCollision)
    ));
    let held = index
        .audio_between(0, 2_000_000_000, &[])
        .unwrap()
        .clips
        .remove(0)
        .wav;
    let mut second = first.clone();
    second.id = Uuid::new_v4();
    for packet in &mut second.packets {
        packet.received_ns += 1_000_000_000;
        packet.captured_ns += 1_000_000_000;
    }
    index.record_audio(second.clone(), &pcm).unwrap();
    let page = index.audio_between(0, 3_000_000_000, &[]).unwrap();
    assert_eq!(page.clips.len(), 1);
    assert_eq!(page.clips[0].segment.id, second.id);
    assert!(page.capacity_before_ns.is_some());
    assert_eq!(index.stats().unwrap().payloads, 1);
    assert!(index.stats().unwrap().accounted_bytes <= 4 * 1024 * 1024);
    assert_eq!(held.len(), pcm.len() + 58);
}
fn segment() -> (AudioSegment, Vec<u8>) {
    (
        AudioSegment {
            id: Uuid::new_v4(),
            source: SourceKey {
                id: "audio:fixture".into(),
                generation: Uuid::new_v4(),
            },
            stream_id: Uuid::new_v4(),
            sample_rate: 16000,
            channels: 2,
            packets: (0..2)
                .map(|i| AudioPacket {
                    sequence: i + 1,
                    sample_position: i * 160,
                    frames: 160,
                    captured_ns: 5_000_000_000 + i * 10_000_000,
                    received_ns: 1_010_000_000 + i * 10_000_000,
                    receiver_range: Some(ReceiverTimeRange {
                        earliest_ns: 1_000_000_000 + i * 10_000_000,
                        latest_ns: 1_012_000_000 + i * 10_000_000,
                    }),
                    discontinuity: i == 0,
                })
                .collect(),
        },
        (0..640).flat_map(|_| 0.25f32.to_le_bytes()).collect(),
    )
}

#[test]
fn source_attributed_audio_roundtrips_original_packets_and_restarts_as_a_valid_float_wav() {
    let root = tempfile::tempdir().unwrap();
    let identity = identity();
    let mut index = ArchiveIndex::open(root.path(), identity, ArchiveLimits::default()).unwrap();
    index.begin().unwrap();
    let (segment, pcm) = segment();
    assert!(
        index
            .audio_between(0, 2_000_000_000, &[])
            .unwrap()
            .policy
            .is_empty()
    );
    index
        .record_audio_policy(std::slice::from_ref(&segment.source), 900_000_000)
        .unwrap();
    index.record_audio(segment.clone(), &pcm).unwrap();
    index.finish(1_030_000_000).unwrap();
    drop(index);
    let index = ArchiveIndex::open(root.path(), identity, ArchiveLimits::default()).unwrap();
    let page = index
        .audio_between(
            1_000_000_000,
            1_020_000_000,
            std::slice::from_ref(&segment.source),
        )
        .unwrap();
    assert_eq!(page.clips.len(), 1);
    assert_eq!(page.policy.len(), 1);
    assert_eq!(page.policy[0].end_ns, Some(1_030_000_000));
    assert_eq!(page.clips[0].segment.id, segment.id);
    assert_eq!(page.clips[0].segment.stream_id, segment.stream_id);
    assert_eq!(page.clips[0].segment.packets[1].sample_position, 160);
    assert_eq!(page.clips[0].segment.packets[1].captured_ns, 5_010_000_000);
    let mut wav = hound::WavReader::new(std::io::Cursor::new(&page.clips[0].wav)).unwrap();
    assert_eq!(wav.spec().channels, 2);
    assert_eq!(wav.spec().sample_rate, 16000);
    assert_eq!(wav.spec().sample_format, hound::SampleFormat::Float);
    assert_eq!(
        wav.samples::<f32>().collect::<Result<Vec<_>, _>>().unwrap(),
        vec![0.25; 640]
    );
    let other = SourceKey {
        id: segment.source.id,
        generation: Uuid::new_v4(),
    };
    assert!(
        index
            .audio_between(0, 2_000_000_000, &[other])
            .unwrap()
            .clips
            .is_empty()
    );
}

#[test]
fn discontinuous_or_corrupt_audio_cannot_be_presented_as_a_complete_segment() {
    let root = tempfile::tempdir().unwrap();
    let mut index = ArchiveIndex::open(root.path(), identity(), ArchiveLimits::default()).unwrap();
    let (mut segment, pcm) = segment();
    segment.packets[1].sequence += 1;
    assert!(index.record_audio(segment.clone(), &pcm).is_err());
    segment.packets[1].sequence -= 1;
    segment.packets[1].discontinuity = true;
    assert!(index.record_audio(segment.clone(), &pcm).is_err());
    segment.packets[1].discontinuity = false;
    assert!(
        index
            .record_audio(segment.clone(), &pcm[..pcm.len() - 1])
            .is_err()
    );
    let mut nan = pcm.clone();
    nan[..4].copy_from_slice(&f32::NAN.to_le_bytes());
    assert!(index.record_audio(segment.clone(), &nan).is_err());
    index.record_audio(segment.clone(), &pcm).unwrap();
    std::fs::write(
        root.path()
            .join("payloads")
            .join(format!("{}.bin", segment.id)),
        b"corrupt",
    )
    .unwrap();
    assert!(index.audio_between(0, 2_000_000_000, &[]).is_err());
}

#[test]
fn audio_uses_the_shared_byte_budget_and_expired_reads_are_reported_with_a_floor() {
    let root = tempfile::tempdir().unwrap();
    let mut index = ArchiveIndex::open(
        root.path(),
        identity(),
        ArchiveLimits {
            duration_ms: 100,
            max_bytes: 4 * 1024 * 1024,
        },
    )
    .unwrap();
    let (segment, pcm) = segment();
    index
        .record_audio_policy(std::slice::from_ref(&segment.source), 900_000_000)
        .unwrap();
    index.record_audio(segment.clone(), &pcm).unwrap();
    assert_eq!(index.stats().unwrap().payloads, 1);
    assert_eq!(index.stats().unwrap().audio_segments, 1);
    let held = index
        .audio_between(0, 2_000_000_000, &[])
        .unwrap()
        .clips
        .remove(0)
        .wav;
    index.enforce_limits(2_000_000_000).unwrap();
    assert_eq!(index.stats().unwrap().payloads, 0);
    assert_eq!(index.stats().unwrap().audio_segments, 0);
    let page = index.audio_between(0, 1_500_000_000, &[]).unwrap();
    assert!(page.clips.is_empty());
    assert_eq!(page.retention_floor_ns, 1_900_000_000);
    assert!(held.len() > pcm.len());
    assert!(index.stats().unwrap().accounted_bytes <= 4 * 1024 * 1024);
}
