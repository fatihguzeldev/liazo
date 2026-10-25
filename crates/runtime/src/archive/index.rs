use super::{ArchiveError, PayloadDescriptor, PayloadStore};
use crate::history::{GapReason, HistoricalImage, HistoryState};
use bytes::Bytes;
use liazo_core::{
    protocol::SourceKey,
    wire::{Frame, Header, ImageMeta},
};
use liazo_transport::{Fingerprint, SessionBinding};
use rusqlite::{Connection, OptionalExtension, params};
use std::{collections::HashSet, path::Path, time::Duration};
use uuid::Uuid;

pub struct ArchiveIndex {
    database: Connection,
    payloads: PayloadStore,
}

#[derive(Clone, Copy)]
pub struct ArchiveIdentity {
    pub binding: SessionBinding,
    pub provider: Fingerprint,
}

struct StoredImage {
    row: i64,
    first_ns: u64,
    last_ns: u64,
    first: ImageMeta,
    last: ImageMeta,
    payload: Uuid,
    max_gap_ns: u64,
}

impl ArchiveIndex {
    pub fn open(root: &Path, identity: ArchiveIdentity) -> Result<Self, ArchiveError> {
        if identity.binding.session.is_nil() || identity.binding.epoch.is_nil() {
            return Err(ArchiveError::SessionMismatch);
        }
        let payloads = PayloadStore::open(&root.join("payloads"))?;
        let database = Connection::open(root.join("index.sqlite3"))?;
        let version: i64 = database.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version > 1 {
            return Err(ArchiveError::SessionMismatch);
        }
        database.busy_timeout(Duration::from_secs(2))?;
        database.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH, 128 * 1024)?;
        database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
            PRAGMA trusted_schema=OFF; PRAGMA cache_size=-2048; PRAGMA max_page_count=65536;
            PRAGMA wal_autocheckpoint=64; PRAGMA journal_size_limit=1048576;
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS payloads (id TEXT PRIMARY KEY, length INTEGER NOT NULL, hash BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS images (id INTEGER PRIMARY KEY, source TEXT NOT NULL, generation TEXT NOT NULL,
                payload TEXT NOT NULL REFERENCES payloads(id), first_ns INTEGER NOT NULL, last_ns INTEGER NOT NULL,
                first_meta TEXT NOT NULL, last_meta TEXT NOT NULL, max_gap_ns INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS images_time ON images(source, generation, first_ns DESC);
            CREATE TABLE IF NOT EXISTS gaps (id INTEGER PRIMARY KEY, source TEXT NOT NULL, generation TEXT NOT NULL,
                first_ns INTEGER NOT NULL, end_ns INTEGER, reason TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS gaps_time ON gaps(source, generation, first_ns DESC);")?;
        let identity = serde_json::to_string(
            &serde_json::json!({ "session": identity.binding.session, "epoch": identity.binding.epoch, "provider": identity.provider.to_string() }),
        )?;
        database.execute(
            "INSERT OR IGNORE INTO meta (key,value) VALUES ('binding',?1)",
            [&identity],
        )?;
        let existing: String =
            database.query_row("SELECT value FROM meta WHERE key='binding'", [], |row| {
                row.get(0)
            })?;
        if existing != identity {
            return Err(ArchiveError::SessionMismatch);
        }
        database.pragma_update(None, "user_version", 1)?;
        let check: String = database.pragma_query_value(None, "quick_check", |row| row.get(0))?;
        if check != "ok" {
            return Err(ArchiveError::InvalidPayload);
        }
        let referenced = database
            .prepare("SELECT id FROM payloads")?
            .query_map([], |row| row.get::<_, String>(0))?
            .map(|id| {
                id.map_err(ArchiveError::from)
                    .and_then(|id| id.parse::<Uuid>().map_err(|_| ArchiveError::InvalidPayload))
            })
            .collect::<Result<HashSet<_>, _>>()?;
        payloads.recover(&referenced)?;
        Ok(Self { database, payloads })
    }

    pub fn record_image(
        &mut self,
        metadata: ImageMeta,
        id: Uuid,
        bytes: Bytes,
        arrived_ns: u64,
        max_gap_ns: u64,
    ) -> Result<(), ArchiveError> {
        if arrived_ns > i64::MAX as u64
            || max_gap_ns > 181_000_000_000
            || id.is_nil()
            || Frame::new(Header::Image(metadata.clone()), bytes.clone()).is_err()
        {
            return Err(ArchiveError::InvalidPayload);
        }
        let old = self.last(&metadata.source, arrived_ns)?;
        if old.as_ref().is_some_and(|old| old.last_ns > arrived_ns) {
            return Err(ArchiveError::InvalidPayload);
        }
        let latest_gap: Option<u64> = self.database.query_row(
            "SELECT MAX(first_ns) FROM gaps WHERE source=?1 AND generation=?2",
            params![metadata.source.id, metadata.source.generation.to_string()],
            |row| row.get(0),
        )?;
        if latest_gap.is_some_and(|started| started > arrived_ns) {
            return Err(ArchiveError::InvalidPayload);
        }
        let descriptor = self.payloads.put(id, &bytes)?;
        let source = &metadata.source;
        let generation = source.generation.to_string();
        let json = serde_json::to_string(&metadata)?;
        let tx = self.database.transaction()?;
        let closed = tx.execute(
            "UPDATE gaps SET end_ns=?3 WHERE source=?1 AND generation=?2 AND end_ns IS NULL",
            params![source.id, generation, arrived_ns],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO payloads (id,length,hash) VALUES (?1,?2,?3)",
            params![
                id.to_string(),
                descriptor.length,
                descriptor.sha256.as_slice()
            ],
        )?;
        let mut coalesce = false;
        if let Some(old) = &old {
            let skipped = old.last.configuration_revision == metadata.configuration_revision
                && old.last.sequence.saturating_add(1) != metadata.sequence;
            let stale = arrived_ns.saturating_sub(old.last_ns) > old.max_gap_ns;
            if closed == 0 && (skipped || stale) {
                let since = if skipped {
                    old.last_ns.saturating_add(1)
                } else {
                    old.last_ns.saturating_add(old.max_gap_ns)
                };
                let reason = serde_json::to_string(&if skipped {
                    GapReason::SkippedFrames
                } else {
                    GapReason::NoData
                })?;
                tx.execute("INSERT INTO gaps (source,generation,first_ns,end_ns,reason) VALUES (?1,?2,?3,?4,?5)", params![source.id, generation, since, arrived_ns, reason])?;
            } else {
                coalesce = closed == 0
                    && old.payload == id
                    && old.last.configuration_revision == metadata.configuration_revision
                    && old.last.fingerprint == metadata.fingerprint
                    && old.last.format == metadata.format
                    && old.last.width == metadata.width
                    && old.last.height == metadata.height;
            }
        }
        if coalesce {
            tx.execute(
                "UPDATE images SET last_ns=?2,last_meta=?3,max_gap_ns=?4 WHERE id=?1",
                params![old.unwrap().row, arrived_ns, json, max_gap_ns],
            )?;
        } else {
            tx.execute("INSERT INTO images (source,generation,payload,first_ns,last_ns,first_meta,last_meta,max_gap_ns) VALUES (?1,?2,?3,?4,?4,?5,?5,?6)", params![source.id, generation, id.to_string(), arrived_ns, json, max_gap_ns])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn record_gap(
        &mut self,
        source: &SourceKey,
        at: u64,
        reason: GapReason,
    ) -> Result<(), ArchiveError> {
        source
            .validate()
            .map_err(|_| ArchiveError::InvalidPayload)?;
        if at > i64::MAX as u64 {
            return Err(ArchiveError::InvalidPayload);
        }
        let generation = source.generation.to_string();
        let reason = serde_json::to_string(&reason)?;
        let tx = self.database.transaction()?;
        let existing: Option<(String, u64)> = tx.query_row("SELECT reason,first_ns FROM gaps WHERE source=?1 AND generation=?2 AND end_ns IS NULL LIMIT 1", params![source.id, generation], |row| Ok((row.get(0)?, row.get(1)?))).optional()?;
        if existing.as_ref().is_some_and(|(_, started)| *started > at) {
            return Err(ArchiveError::InvalidPayload);
        }
        if existing
            .as_ref()
            .is_some_and(|(previous, _)| previous == &reason)
        {
            return Ok(());
        }
        tx.execute(
            "UPDATE gaps SET end_ns=?3 WHERE source=?1 AND generation=?2 AND end_ns IS NULL",
            params![source.id, generation, at],
        )?;
        tx.execute(
            "INSERT INTO gaps (source,generation,first_ns,reason) VALUES (?1,?2,?3,?4)",
            params![source.id, generation, at, reason],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn at(&self, source: &SourceKey, at: u64) -> Result<HistoricalImage, ArchiveError> {
        source
            .validate()
            .map_err(|_| ArchiveError::InvalidPayload)?;
        if at > i64::MAX as u64 {
            return Err(ArchiveError::InvalidPayload);
        }
        let mut result = HistoricalImage {
            source: source.clone(),
            state: HistoryState::NotObserved,
            first_observed_ns: None,
            last_confirmed_ns: None,
            first_capture: None,
            last_capture: None,
            payload: None,
        };
        let gap: Option<String> = self.database.query_row("SELECT reason FROM gaps WHERE source=?1 AND generation=?2 AND first_ns<=?3 AND (end_ns IS NULL OR end_ns>?3) ORDER BY first_ns DESC LIMIT 1", params![source.id, source.generation.to_string(), at], |row| row.get(0)).optional()?;
        if let Some(reason) = gap {
            result.state = HistoryState::Gap(serde_json::from_str(&reason)?);
            return Ok(result);
        }
        let Some(row) = self.last(source, at)? else {
            return Ok(result);
        };
        if at.saturating_sub(row.last_ns) > row.max_gap_ns {
            result.state = HistoryState::Gap(GapReason::NoData);
            return Ok(result);
        }
        let descriptor = self.database.query_row(
            "SELECT length,hash FROM payloads WHERE id=?1",
            [row.payload.to_string()],
            |data| Ok((data.get::<_, u64>(0)?, data.get::<_, Vec<u8>>(1)?)),
        )?;
        let descriptor = PayloadDescriptor {
            id: row.payload,
            length: descriptor.0,
            sha256: descriptor
                .1
                .try_into()
                .map_err(|_| ArchiveError::CorruptPayload)?,
        };
        let bytes: Bytes = self.payloads.read(&descriptor)?.into();
        if row.first_ns > row.last_ns
            || row.max_gap_ns > 181_000_000_000
            || row.last.source != *source
            || row.first.source != *source
            || Frame::new(Header::Image(row.last.clone()), bytes.clone()).is_err()
            || Frame::new(Header::Image(row.first.clone()), bytes.clone()).is_err()
        {
            return Err(ArchiveError::CorruptPayload);
        }
        result.state = if at <= row.last_ns {
            HistoryState::Available
        } else {
            HistoryState::Sampled
        };
        result.first_observed_ns = Some(row.first_ns);
        result.last_confirmed_ns = Some(row.last_ns);
        result.first_capture = Some(row.first);
        result.last_capture = Some(row.last);
        result.payload = Some(bytes);
        Ok(result)
    }

    fn last(&self, source: &SourceKey, at: u64) -> Result<Option<StoredImage>, ArchiveError> {
        let raw = self.database.query_row("SELECT id,first_ns,last_ns,first_meta,last_meta,payload,max_gap_ns FROM images WHERE source=?1 AND generation=?2 AND first_ns<=?3 ORDER BY first_ns DESC LIMIT 1",
            params![source.id, source.generation.to_string(), at], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, u64>(1)?, row.get::<_, u64>(2)?, row.get::<_, String>(3)?, row.get::<_, String>(4)?, row.get::<_, String>(5)?, row.get::<_, u64>(6)?))).optional()?;
        raw.map(
            |(row, first_ns, last_ns, first, last, payload, max_gap_ns)| {
                Ok(StoredImage {
                    row,
                    first_ns,
                    last_ns,
                    first: serde_json::from_str(&first)?,
                    last: serde_json::from_str(&last)?,
                    payload: payload.parse().map_err(|_| ArchiveError::InvalidPayload)?,
                    max_gap_ns,
                })
            },
        )
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liazo_core::protocol::ImageFormat;
    use liazo_transport::Identity;
    fn binding() -> ArchiveIdentity {
        ArchiveIdentity {
            provider: Identity::generate().unwrap().fingerprint(),
            binding: SessionBinding {
                session: Uuid::new_v4(),
                epoch: Uuid::new_v4(),
            },
        }
    }
    fn meta(sequence: u64) -> ImageMeta {
        ImageMeta {
            source: SourceKey {
                id: "display:fixture".into(),
                generation: Uuid::from_u128(1),
            },
            sequence,
            captured_ns: sequence,
            capture_started_ns: sequence,
            configuration_revision: 1,
            width: 1,
            height: 1,
            format: ImageFormat::Png,
            fingerprint: [1; 32],
        }
    }
    #[test]
    fn indexed_images_survive_restart_and_duplicate_confirmations_cannot_bridge_a_gap() {
        let root = tempfile::tempdir().unwrap();
        let binding = binding();
        let id = Uuid::new_v4();
        let mut archive = ArchiveIndex::open(root.path(), binding).unwrap();
        archive
            .record_image(meta(1), id, Bytes::from_static(b"fixture"), 100, 300)
            .unwrap();
        archive
            .record_image(meta(2), id, Bytes::from_static(b"fixture"), 200, 300)
            .unwrap();
        archive
            .record_gap(&meta(1).source, 300, GapReason::Disconnected)
            .unwrap();
        archive
            .record_image(meta(3), id, Bytes::from_static(b"fixture"), 600, 300)
            .unwrap();
        drop(archive);
        let archive = ArchiveIndex::open(root.path(), binding).unwrap();
        assert_eq!(
            archive
                .at(&meta(1).source, 150)
                .unwrap()
                .payload
                .unwrap()
                .as_ref(),
            b"fixture"
        );
        assert_eq!(
            archive.at(&meta(1).source, 400).unwrap().state,
            HistoryState::Gap(GapReason::Disconnected)
        );
        assert_eq!(
            archive
                .at(&meta(1).source, 600)
                .unwrap()
                .payload
                .unwrap()
                .as_ref(),
            b"fixture"
        );
        let count: u64 = archive
            .database
            .query_row("SELECT COUNT(*) FROM payloads", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
    #[test]
    fn unindexed_publication_is_recovered_and_missing_media_is_never_returned_as_complete() {
        let root = tempfile::tempdir().unwrap();
        let binding = binding();
        let mut archive = ArchiveIndex::open(root.path(), binding).unwrap();
        let id = Uuid::new_v4();
        archive
            .record_image(meta(1), id, Bytes::from_static(b"fixture"), 100, 300)
            .unwrap();
        let orphan = archive
            .payloads
            .put(Uuid::new_v4(), b"not committed")
            .unwrap();
        drop(archive);
        let archive = ArchiveIndex::open(root.path(), binding).unwrap();
        assert!(!archive.payloads.path(orphan.id).exists());
        archive.payloads.remove(id).unwrap();
        assert!(matches!(
            archive.at(&meta(1).source, 100),
            Err(ArchiveError::CorruptPayload)
        ));
        drop(archive);
        assert!(matches!(
            ArchiveIndex::open(root.path(), self::binding()),
            Err(ArchiveError::SessionMismatch)
        ));
    }
}
