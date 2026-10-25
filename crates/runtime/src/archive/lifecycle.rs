use super::{ArchiveError, ArchiveIndex};
use crate::history::GapReason;
use liazo_core::transcript::{EndReason, Transcript};
use rusqlite::{OptionalExtension, params};

impl ArchiveIndex {
    pub(super) fn initialize_lifecycle(&self) -> Result<(), ArchiveError> {
        self.database.execute_batch("CREATE TABLE IF NOT EXISTS global_gaps (id INTEGER PRIMARY KEY,
            first_ns INTEGER NOT NULL, end_ns INTEGER, reason TEXT NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS global_open ON global_gaps(reason) WHERE end_ns IS NULL;")?;
        Ok(())
    }

    pub fn begin(&mut self) -> Result<(), ArchiveError> {
        let dirty = self
            .database
            .query_row(
                "SELECT value FROM meta WHERE key='writer_active'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .is_some_and(|value| value == "1");
        if dirty {
            let last: u64 = self.database.query_row(
                "SELECT COALESCE(MAX(time),0) FROM (
                SELECT MAX(last_ns) AS time FROM images UNION ALL SELECT MAX(first_ns) FROM gaps
                UNION ALL SELECT MAX(received_ns) FROM transcripts)",
                [],
                |row| row.get(0),
            )?;
            self.record_global_gap(last.saturating_add(1), GapReason::ArchiveInterrupted)?;
            self.close_partials(last)?;
        }
        let tx = self.database.transaction()?;
        tx.execute("INSERT INTO meta (key,value) VALUES ('writer_active','1') ON CONFLICT(key) DO UPDATE SET value='1'", [])?;
        tx.execute("DELETE FROM meta WHERE key='closed_at'", [])?;
        tx.commit()?;
        Ok(())
    }

    pub fn finish(&mut self, now: u64) -> Result<(), ArchiveError> {
        if now > i64::MAX as u64 {
            return Err(ArchiveError::InvalidPayload);
        }
        self.close_partials(now)?;
        let tx = self.database.transaction()?;
        tx.execute("INSERT INTO meta (key,value) VALUES ('writer_active','0') ON CONFLICT(key) DO UPDATE SET value='0'", [])?;
        tx.execute("INSERT INTO meta (key,value) VALUES ('closed_at',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [now.to_string()])?;
        tx.commit()?;
        self.database
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }

    pub fn record_global_gap(&mut self, at: u64, reason: GapReason) -> Result<(), ArchiveError> {
        if at > i64::MAX as u64 {
            return Err(ArchiveError::InvalidPayload);
        }
        self.database.execute("INSERT INTO global_gaps (first_ns,reason) VALUES (?1,?2)
            ON CONFLICT(reason) WHERE end_ns IS NULL DO UPDATE SET first_ns=MIN(first_ns,excluded.first_ns)", params![at, serde_json::to_string(&reason)?])?;
        Ok(())
    }

    pub(super) fn global_gap(&self, at: u64) -> Result<Option<GapReason>, ArchiveError> {
        let closed: Option<String> = self
            .database
            .query_row("SELECT value FROM meta WHERE key='closed_at'", [], |row| {
                row.get(0)
            })
            .optional()?;
        if closed.is_some_and(|closed| closed.parse::<u64>().is_ok_and(|closed| at > closed)) {
            return Ok(Some(GapReason::ArchiveStopped));
        }
        let gap: Option<String> = self.database.query_row("SELECT reason FROM global_gaps WHERE first_ns<=?1 AND (end_ns IS NULL OR end_ns>?1) ORDER BY first_ns DESC LIMIT 1", [at], |row| row.get(0)).optional()?;
        gap.map(|reason| serde_json::from_str(&reason).map_err(ArchiveError::from))
            .transpose()
    }

    fn close_partials(&mut self, now: u64) -> Result<(), ArchiveError> {
        let mut after = String::new();
        loop {
            let batch = self
                .database
                .prepare("SELECT id,data FROM transcripts WHERE id>?1 ORDER BY id LIMIT 128")?
                .query_map([&after], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            if batch.is_empty() {
                break;
            }
            after = batch.last().unwrap().0.clone();
            let tx = self.database.transaction()?;
            for (id, data) in batch {
                let mut transcript: Transcript = serde_json::from_str(&data)?;
                transcript
                    .validate()
                    .map_err(|_| ArchiveError::InvalidPayload)?;
                if transcript.ending.is_some() {
                    continue;
                }
                transcript.ending = Some(EndReason::Gap);
                transcript.revision = transcript
                    .revision
                    .checked_add(1)
                    .ok_or(ArchiveError::InvalidPayload)?;
                tx.execute(
                    "UPDATE transcripts SET data=?2,received_ns=MAX(received_ns,?3) WHERE id=?1",
                    params![id, serde_json::to_string(&transcript)?, now],
                )?;
            }
            tx.commit()?;
        }
        Ok(())
    }
}
