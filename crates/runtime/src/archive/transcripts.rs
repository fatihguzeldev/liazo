use super::{ArchiveError, ArchiveIndex};
use liazo_core::{protocol::SourceKey, transcript::Transcript};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ReceiverTimeRange {
    pub earliest_ns: u64,
    pub latest_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArchivedTranscript {
    pub transcript: Transcript,
    pub received_ns: u64,
    pub receiver_range: Option<ReceiverTimeRange>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TranscriptPage {
    pub entries: Vec<ArchivedTranscript>,
    pub truncated: bool,
    pub retention_floor_ns: u64,
}

impl ArchiveIndex {
    pub(super) fn initialize_transcripts(&self) -> Result<(), ArchiveError> {
        self.database.execute_batch("CREATE TABLE IF NOT EXISTS transcripts (id TEXT PRIMARY KEY, source TEXT NOT NULL,
            generation TEXT NOT NULL, received_ns INTEGER NOT NULL, earliest_ns INTEGER, latest_ns INTEGER, data TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS transcripts_time ON transcripts(earliest_ns,latest_ns);
            CREATE INDEX IF NOT EXISTS transcripts_retention ON transcripts(received_ns);")?;
        Ok(())
    }

    pub fn record_transcript(
        &mut self,
        transcript: Transcript,
        received_ns: u64,
        range: Option<ReceiverTimeRange>,
    ) -> Result<(), ArchiveError> {
        transcript
            .validate()
            .map_err(|_| ArchiveError::InvalidPayload)?;
        if received_ns > i64::MAX as u64
            || range.is_some_and(|range| {
                range.earliest_ns > range.latest_ns || range.latest_ns > i64::MAX as u64
            })
        {
            return Err(ArchiveError::InvalidPayload);
        }
        self.make_room(received_ns, 0)?;
        let data = serde_json::to_string(&transcript)?;
        let previous: Option<String> = self
            .database
            .query_row(
                "SELECT data FROM transcripts WHERE id=?1",
                [transcript.id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(previous) = previous {
            if previous == data {
                return Ok(());
            }
            let previous: Transcript = serde_json::from_str(&previous)?;
            if !transcript.supersedes(&previous) {
                return Err(ArchiveError::IdentityCollision);
            }
        }
        self.database.execute("INSERT INTO transcripts (id,source,generation,received_ns,earliest_ns,latest_ns,data) VALUES (?1,?2,?3,?4,?5,?6,?7)
            ON CONFLICT(id) DO UPDATE SET received_ns=excluded.received_ns,earliest_ns=excluded.earliest_ns,latest_ns=excluded.latest_ns,data=excluded.data",
            params![transcript.id.to_string(), transcript.source.id, transcript.source.generation.to_string(), received_ns, range.map(|range| range.earliest_ns), range.map(|range| range.latest_ns), data])?;
        Ok(())
    }

    pub fn transcripts_between(
        &self,
        first_ns: u64,
        last_ns: u64,
        selected: &[SourceKey],
    ) -> Result<TranscriptPage, ArchiveError> {
        if first_ns > last_ns
            || last_ns > i64::MAX as u64
            || selected.len() > 8
            || selected.iter().any(|source| source.validate().is_err())
        {
            return Err(ArchiveError::InvalidPayload);
        }
        let floor = self.stats()?.retention_floor_ns;
        let mut page = TranscriptPage {
            entries: Vec::new(),
            truncated: false,
            retention_floor_ns: floor,
        };
        if last_ns < floor {
            return Ok(page);
        }
        let mut query = self.database.prepare(
            "SELECT data,received_ns,earliest_ns,latest_ns FROM transcripts
            WHERE COALESCE(latest_ns,received_ns)>=?1 AND COALESCE(earliest_ns,received_ns)<=?2
            ORDER BY COALESCE(earliest_ns,received_ns),id",
        )?;
        let mut rows = query.query(params![first_ns.max(floor), last_ns])?;
        let mut bytes = 0;
        while let Some(row) = rows.next()? {
            let data: String = row.get(0)?;
            let transcript: Transcript = serde_json::from_str(&data)?;
            transcript
                .validate()
                .map_err(|_| ArchiveError::InvalidPayload)?;
            if !selected.is_empty() && !selected.contains(&transcript.source) {
                continue;
            }
            bytes += data.len();
            if page.entries.len() == 256 || bytes > 512 * 1024 {
                page.truncated = true;
                break;
            }
            let earliest: Option<u64> = row.get(2)?;
            let latest: Option<u64> = row.get(3)?;
            let receiver_range = match (earliest, latest) {
                (Some(earliest_ns), Some(latest_ns)) if earliest_ns <= latest_ns => {
                    Some(ReceiverTimeRange {
                        earliest_ns,
                        latest_ns,
                    })
                }
                (None, None) => None,
                _ => return Err(ArchiveError::InvalidPayload),
            };
            page.entries.push(ArchivedTranscript {
                transcript,
                received_ns: row.get(1)?,
                receiver_range,
            });
        }
        Ok(page)
    }
}
