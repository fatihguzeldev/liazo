use liazo_core::{
    protocol::SourceKey,
    transcript::{EndReason, Transcript},
};
use liazo_runtime::archive::{ArchiveIdentity, ArchiveIndex, ArchiveLimits, ReceiverTimeRange};
use liazo_transport::{Identity, SessionBinding};
use uuid::Uuid;

#[test]
fn final_revisions_are_immutable_and_search_uses_audio_time_instead_of_inference_completion() {
    let root = tempfile::tempdir().unwrap();
    let identity = ArchiveIdentity {
        provider: Identity::generate().unwrap().fingerprint(),
        binding: SessionBinding {
            session: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        },
        wall_anchor_unix_ms: 1_700_000_000_000,
    };
    let mut archive = ArchiveIndex::open(root.path(), identity, ArchiveLimits::default()).unwrap();
    let source = SourceKey {
        id: "audio:fixture".into(),
        generation: Uuid::new_v4(),
    };
    let mut text = Transcript {
        id: Uuid::new_v4(),
        source: source.clone(),
        stream_id: Uuid::new_v4(),
        revision: 1,
        start_ns: 10_000_000_000,
        end_ns: 11_000_000_000,
        text: "a partial".into(),
        ending: None,
    };
    let range = ReceiverTimeRange {
        earliest_ns: 1_000_000_000,
        latest_ns: 2_000_000_000,
    };
    archive
        .record_transcript(text.clone(), 5_000_000_000, Some(range))
        .unwrap();
    text.revision = 2;
    text.text = "a complete question".into();
    text.ending = Some(EndReason::Silence);
    archive
        .record_transcript(text.clone(), 6_000_000_000, Some(range))
        .unwrap();
    archive
        .record_transcript(text.clone(), 7_000_000_000, Some(range))
        .unwrap();
    text.revision = 3;
    text.text = "rewritten final".into();
    assert!(
        archive
            .record_transcript(text, 7_000_000_000, Some(range))
            .is_err()
    );
    drop(archive);
    let archive = ArchiveIndex::open(root.path(), identity, ArchiveLimits::default()).unwrap();
    let page = archive
        .transcripts_between(1_500_000_000, 1_600_000_000, &[source])
        .unwrap();
    assert!(!page.truncated);
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].transcript.text, "a complete question");
    assert_eq!(page.entries[0].received_ns, 6_000_000_000);
    assert!(
        archive
            .transcripts_between(5_500_000_000, 6_000_000_000, &[])
            .unwrap()
            .entries
            .is_empty()
    );
}

#[test]
fn interrupted_writers_close_partial_text_and_cannot_extrapolate_their_last_screen() {
    use bytes::Bytes;
    use liazo_core::{protocol::ImageFormat, wire::ImageMeta};
    use liazo_runtime::history::{GapReason, HistoryState};
    let root = tempfile::tempdir().unwrap();
    let identity = ArchiveIdentity {
        provider: Identity::generate().unwrap().fingerprint(),
        binding: SessionBinding {
            session: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        },
        wall_anchor_unix_ms: 1_700_000_000_000,
    };
    let mut archive = ArchiveIndex::open(root.path(), identity, ArchiveLimits::default()).unwrap();
    archive.begin().unwrap();
    let source = SourceKey {
        id: "display:fixture".into(),
        generation: Uuid::new_v4(),
    };
    archive
        .record_image(
            ImageMeta {
                source: source.clone(),
                sequence: 1,
                captured_ns: 1,
                capture_started_ns: 1,
                configuration_revision: 1,
                width: 1,
                height: 1,
                format: ImageFormat::Png,
                fingerprint: [1; 32],
            },
            Uuid::new_v4(),
            Bytes::from_static(b"fixture"),
            1_000_000_000,
            3_000_000_000,
        )
        .unwrap();
    archive
        .record_transcript(
            Transcript {
                id: Uuid::new_v4(),
                source: SourceKey {
                    id: "audio:fixture".into(),
                    generation: Uuid::new_v4(),
                },
                stream_id: Uuid::new_v4(),
                revision: 1,
                start_ns: 10,
                end_ns: 20,
                text: "unfinished".into(),
                ending: None,
            },
            2_000_000_000,
            None,
        )
        .unwrap();
    drop(archive);
    let mut archive = ArchiveIndex::open(root.path(), identity, ArchiveLimits::default()).unwrap();
    archive.begin().unwrap();
    assert_eq!(
        archive.at(&source, 2_100_000_000).unwrap().state,
        HistoryState::Gap(GapReason::ArchiveInterrupted)
    );
    assert!(
        archive
            .at(&source, 1_000_000_000)
            .unwrap()
            .payload
            .is_some()
    );
    let page = archive.transcripts_between(0, 3_000_000_000, &[]).unwrap();
    assert_eq!(page.entries[0].transcript.ending, Some(EndReason::Gap));
    archive.finish(3_000_000_000).unwrap();
    assert_eq!(
        archive.at(&source, 3_100_000_000).unwrap().state,
        HistoryState::Gap(GapReason::ArchiveStopped)
    );
}
