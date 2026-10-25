use super::{ArchiveError, ArchiveIndex};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ArchiveLimits {
    pub duration_ms: u64,
    pub max_bytes: u64,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            duration_ms: 21_600_000,
            max_bytes: 16 * 1024 * 1024 * 1024,
        }
    }
}

impl ArchiveLimits {
    pub fn validate(self) -> Result<(), ArchiveError> {
        if !(100..=2_592_000_000).contains(&self.duration_ms)
            || !(4 * 1024 * 1024..=1024 * 1024 * 1024 * 1024).contains(&self.max_bytes)
        {
            return Err(ArchiveError::Capacity);
        }
        Ok(())
    }
    pub(super) fn index_bytes(self) -> u64 {
        (self.max_bytes / 16).clamp(64 * 1024, 256 * 1024 * 1024)
    }
    pub(super) fn metadata_budget(self) -> u64 {
        // Reserve the database, a full-index WAL transaction, frame overhead and shared state.
        2 * self.index_bytes() + self.index_bytes() / 64 + 1024 * 1024
    }
}

#[derive(Clone, Copy, Debug, Serialize, schemars::JsonSchema)]
pub struct ArchiveStats {
    pub accounted_bytes: u64,
    pub metadata_budget_bytes: u64,
    pub payloads: u64,
    pub intervals: u64,
    pub retention_floor_ns: u64,
}

impl ArchiveIndex {
    pub fn stats(&self) -> Result<ArchiveStats, ArchiveError> {
        let (payloads, bytes): (u64, u64) = self.database.query_row(
            "SELECT COUNT(*),COALESCE(SUM(((length+4095)/4096)*4096+4096),0) FROM payloads",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let intervals = self
            .database
            .query_row("SELECT COUNT(*) FROM images", [], |row| row.get(0))?;
        let floor: Option<String> = self
            .database
            .query_row(
                "SELECT value FROM meta WHERE key='retention_floor'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let retention_floor_ns = floor
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| ArchiveError::InvalidPayload)
            })
            .transpose()?
            .unwrap_or(0);
        Ok(ArchiveStats {
            accounted_bytes: bytes + self.limits.metadata_budget(),
            metadata_budget_bytes: self.limits.metadata_budget(),
            payloads,
            intervals,
            retention_floor_ns,
        })
    }

    pub fn enforce_limits(&mut self, now: u64) -> Result<ArchiveStats, ArchiveError> {
        self.make_room(now, 0)?;
        self.stats()
    }

    pub(super) fn make_room(&mut self, now: u64, reserve: u64) -> Result<(), ArchiveError> {
        if now > i64::MAX as u64 || reserve + self.limits.metadata_budget() > self.limits.max_bytes
        {
            return Err(ArchiveError::Capacity);
        }
        let floor = now
            .saturating_sub(self.limits.duration_ms * 1_000_000)
            .max(self.stats()?.retention_floor_ns);
        let tx = self.database.transaction()?;
        tx.execute("INSERT INTO meta (key,value) VALUES ('retention_floor',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [floor.to_string()])?;
        tx.execute("DELETE FROM images WHERE last_ns<?1", [floor])?;
        tx.execute("DELETE FROM gaps WHERE end_ns<?1", [floor])?;
        tx.execute("DELETE FROM global_gaps WHERE end_ns<?1", [floor])?;
        tx.execute(
            "DELETE FROM transcripts WHERE COALESCE(latest_ns,received_ns)<?1",
            [floor],
        )?;
        tx.commit()?;
        self.remove_unused()?;
        for _ in 0..256 {
            if self.stats()?.accounted_bytes + reserve <= self.limits.max_bytes {
                return Ok(());
            }
            let oldest: Option<(i64, u64)> = self
                .database
                .query_row(
                    "SELECT id,last_ns FROM images ORDER BY last_ns LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((row, end)) = oldest else {
                return Err(ArchiveError::Capacity);
            };
            let tx = self.database.transaction()?;
            tx.execute("DELETE FROM images WHERE id=?1", [row])?;
            tx.execute("INSERT INTO meta (key,value) VALUES ('capacity_before',?1) ON CONFLICT(key) DO UPDATE SET value=CAST(MAX(CAST(value AS INTEGER),?1) AS TEXT)", [end])?;
            tx.commit()?;
            self.remove_unused()?;
        }
        Err(ArchiveError::Capacity)
    }

    fn remove_unused(&mut self) -> Result<(), ArchiveError> {
        let unused = self.database.prepare("SELECT id FROM payloads WHERE NOT EXISTS (SELECT 1 FROM images WHERE images.payload=payloads.id)")?
            .query_map([], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
        for id in unused {
            let uuid: Uuid = id.parse().map_err(|_| ArchiveError::InvalidPayload)?;
            // Remove bytes first while the unused index row still accounts for them on failure.
            self.payloads.remove(uuid)?;
            self.database
                .execute("DELETE FROM payloads WHERE id=?1", [id])?;
        }
        Ok(())
    }
}
