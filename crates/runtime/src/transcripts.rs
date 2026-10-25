use liazo_core::{
    protocol::SourceKey,
    transcript::{EndReason, Transcript, TranscriptionGap, TranscriptionGapReason},
};
use liazo_transcription::worker::RecognitionUpdate;
use serde::Serialize;
use std::collections::VecDeque;

const KEEP_NS: u64 = 60_000_000_000;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_ROWS: usize = 1024;

#[derive(Clone, Default, Serialize)]
pub struct TranscriptionStatus {
    pub active: bool,
    pub sources: Vec<SourceKey>,
    pub gaps: u64,
    pub last_gap: Option<TranscriptionGapReason>,
}

struct Row {
    changed: u64,
    received_ns: u64,
    transcript: Transcript,
}

#[derive(Default)]
pub(crate) struct TranscriptBuffer {
    pub status: TranscriptionStatus,
    rows: VecDeque<Row>,
    expired: u64,
}

impl TranscriptBuffer {
    pub fn begin(&mut self, sources: Vec<SourceKey>) {
        self.status = TranscriptionStatus {
            active: true,
            sources,
            ..TranscriptionStatus::default()
        };
    }

    pub fn publish(&mut self, update: RecognitionUpdate, cursor: u64, now: u64) -> bool {
        match update {
            RecognitionUpdate::Transcript(transcript) => {
                if transcript.validate().is_err() {
                    return false;
                }
                if let Some(index) = self
                    .rows
                    .iter()
                    .position(|row| row.transcript.id == transcript.id)
                {
                    let old = &self.rows[index].transcript;
                    if !transcript.supersedes(old) {
                        return false;
                    }
                    self.rows.remove(index);
                }
                self.rows.push_back(Row {
                    changed: cursor,
                    received_ns: now,
                    transcript,
                });
            }
            RecognitionUpdate::Gap(gap) => {
                self.status.gaps = self.status.gaps.saturating_add(1);
                self.status.last_gap = Some(gap.reason);
                self.close_partial(gap, cursor, now);
            }
        }
        self.trim(now);
        true
    }

    pub fn trim(&mut self, now: u64) {
        while self
            .rows
            .front()
            .is_some_and(|row| now.saturating_sub(row.received_ns) > KEEP_NS)
            || self.rows.len() > MAX_ROWS
            || self
                .rows
                .iter()
                .map(|row| row.transcript.text.len() + 512)
                .sum::<usize>()
                > MAX_BYTES
        {
            if let Some(row) = self.rows.pop_front() {
                self.expired = self.expired.max(row.changed);
            }
        }
    }

    fn close_partial(&mut self, gap: TranscriptionGap, cursor: u64, now: u64) {
        let Some(index) = self.rows.iter().position(|row| {
            Some(row.transcript.id) == gap.utterance
                && row.transcript.source == gap.source
                && row.transcript.ending.is_none()
        }) else {
            return;
        };
        let mut row = self.rows.remove(index).unwrap();
        row.transcript.ending = Some(EndReason::Gap);
        row.transcript.revision = row.transcript.revision.saturating_add(1);
        row.changed = cursor;
        row.received_ns = now;
        self.rows.push_back(row);
    }

    pub fn stop(&mut self, cursor: u64, now: u64) {
        self.status.active = false;
        let interrupted: Vec<_> = self
            .rows
            .iter()
            .filter(|row| row.transcript.ending.is_none())
            .map(|row| TranscriptionGap {
                source: row.transcript.source.clone(),
                utterance: Some(row.transcript.id),
                reason: TranscriptionGapReason::Interrupted,
            })
            .collect();
        for gap in interrupted {
            self.publish(RecognitionUpdate::Gap(gap), cursor, now);
        }
    }

    pub fn snapshot(
        &self,
        after: u64,
        selected: &[SourceKey],
        now: u64,
    ) -> (Vec<Transcript>, bool) {
        let expired = self
            .rows
            .iter()
            .filter(|row| now.saturating_sub(row.received_ns) > KEEP_NS)
            .map(|row| row.changed)
            .max()
            .unwrap_or(0)
            .max(self.expired);
        let transcripts = self
            .rows
            .iter()
            .filter(|row| {
                row.changed > after
                    && now.saturating_sub(row.received_ns) <= KEEP_NS
                    && (selected.is_empty() || selected.contains(&row.transcript.source))
            })
            .map(|row| row.transcript.clone())
            .collect();
        (transcripts, after != 0 && after < expired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    #[test]
    fn independent_cursors_receive_only_new_revisions_and_finals_cannot_be_rewritten() {
        let source = SourceKey {
            id: "audio:test".into(),
            generation: Uuid::new_v4(),
        };
        let mut buffer = TranscriptBuffer::default();
        buffer.begin(vec![source.clone()]);
        let mut transcript = Transcript {
            id: Uuid::new_v4(),
            source,
            stream_id: Uuid::new_v4(),
            revision: 1,
            start_ns: 0,
            end_ns: 1_000_000_000,
            text: "partial".into(),
            ending: None,
        };
        assert!(buffer.publish(RecognitionUpdate::Transcript(transcript.clone()), 4, 0));
        assert_eq!(buffer.snapshot(0, &[], 0).0.len(), 1);
        assert!(buffer.snapshot(4, &[], 0).0.is_empty());
        transcript.revision = 2;
        transcript.text = "final".into();
        transcript.ending = Some(EndReason::Silence);
        assert!(buffer.publish(RecognitionUpdate::Transcript(transcript.clone()), 8, 0));
        assert_eq!(buffer.snapshot(4, &[], 0).0[0].text, "final");
        transcript.revision = 3;
        transcript.text = "changed".into();
        assert!(!buffer.publish(RecognitionUpdate::Transcript(transcript), 9, 0));
        assert_eq!(buffer.snapshot(0, &[], 0).0[0].text, "final");
        assert!(buffer.snapshot(4, &[], KEEP_NS + 1).1);
        assert!(buffer.snapshot(0, &[], KEEP_NS + 1).0.is_empty());
    }
}
