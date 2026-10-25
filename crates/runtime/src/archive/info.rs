use super::{ArchiveError, ArchiveIdentity, ArchiveLimits};
use liazo_transport::SessionBinding;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, schemars::JsonSchema)]
pub struct ArchiveInfo {
    pub binding: SessionBinding,
    pub provider: String,
    pub wall_anchor_unix_ms: u64,
    pub latest_ns: u64,
    pub retention_floor_ns: u64,
    pub closed_at_ns: Option<u64>,
    pub writer_active: bool,
    pub limits: ArchiveLimits,
    pub accounted_bytes: u64,
}
#[derive(Deserialize)]
struct Binding {
    session: Uuid,
    epoch: Uuid,
    provider: String,
    wall_anchor_unix_ms: u64,
}

impl ArchiveInfo {
    pub fn identity(&self) -> Result<ArchiveIdentity, ArchiveError> {
        let provider = self
            .provider
            .parse()
            .map_err(|_| ArchiveError::SessionMismatch)?;
        let identity = ArchiveIdentity {
            binding: self.binding,
            provider,
            wall_anchor_unix_ms: self.wall_anchor_unix_ms,
        };
        identity.validate()?;
        Ok(identity)
    }
    pub fn read(root: &Path) -> Result<Self, ArchiveError> {
        for path in [
            root.to_path_buf(),
            root.join("index.sqlite3"),
            root.join("payloads"),
        ] {
            let metadata = std::fs::symlink_metadata(path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    ArchiveError::NotFound
                } else {
                    ArchiveError::Io(error)
                }
            })?;
            if metadata.file_type().is_symlink() {
                return Err(ArchiveError::InvalidPayload);
            }
        }
        let database = Connection::open_with_flags(
            root.join("index.sqlite3"),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        database.busy_timeout(Duration::from_millis(250))?;
        database.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA query_only=ON;")?;
        database.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH, 128 * 1024)?;
        let version: u32 = database.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if !(1..=2).contains(&version) {
            return Err(ArchiveError::SessionMismatch);
        }
        let meta = |name: &str| -> Result<Option<String>, ArchiveError> {
            Ok(database
                .query_row("SELECT value FROM meta WHERE key=?1", [name], |row| {
                    row.get(0)
                })
                .optional()?)
        };
        let binding: Binding =
            serde_json::from_str(&meta("binding")?.ok_or(ArchiveError::SessionMismatch)?)?;
        let closed_at_ns = meta("closed_at")?
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| ArchiveError::InvalidPayload)?;
        let limits = meta("limits")?
            .map(|value| serde_json::from_str::<ArchiveLimits>(&value))
            .transpose()?
            .unwrap_or_default();
        limits.validate()?;
        let mut latest_ns = closed_at_ns.unwrap_or(0);
        for (table, column) in [
            ("images", "last_ns"),
            ("gaps", "first_ns"),
            ("transcripts", "received_ns"),
            ("audio", "received_ns"),
            ("audio_policy", "first_ns"),
        ] {
            let exists: bool = database.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
                [table],
                |row| row.get(0),
            )?;
            if exists {
                let latest: Option<u64> = database.query_row(
                    &format!("SELECT MAX({column}) FROM {table}"),
                    [],
                    |row| row.get(0),
                )?;
                latest_ns = latest_ns.max(latest.unwrap_or(0));
            }
        }
        if latest_ns > i64::MAX as u64 || closed_at_ns.is_some_and(|closed| closed < latest_ns) {
            return Err(ArchiveError::InvalidPayload);
        }
        let payloads: u64 = database.query_row(
            "SELECT COALESCE(SUM(((length+4095)/4096)*4096+4096),0) FROM payloads",
            [],
            |row| row.get(0),
        )?;
        let mut accounted_bytes = payloads.saturating_add(16 * 1024);
        for name in ["index.sqlite3", "index.sqlite3-wal", "index.sqlite3-shm"] {
            match std::fs::symlink_metadata(root.join(name)) {
                Ok(metadata) if metadata.is_file() => {
                    accounted_bytes =
                        accounted_bytes.saturating_add(metadata.len().div_ceil(4096) * 4096 + 4096)
                }
                Ok(_) => return Err(ArchiveError::InvalidPayload),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let info = Self {
            binding: SessionBinding {
                session: binding.session,
                epoch: binding.epoch,
            },
            provider: binding.provider,
            wall_anchor_unix_ms: binding.wall_anchor_unix_ms,
            latest_ns,
            retention_floor_ns: meta("retention_floor")?
                .map(|value| value.parse::<u64>())
                .transpose()
                .map_err(|_| ArchiveError::InvalidPayload)?
                .unwrap_or(0),
            closed_at_ns,
            writer_active: meta("writer_active")?.is_some_and(|value| value == "1"),
            limits,
            accounted_bytes,
        };
        info.identity()?;
        Ok(info)
    }
}
