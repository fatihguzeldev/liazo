use super::{ArchiveError, ArchiveIndex, ArchiveInfo, AudioPage, TranscriptPage};
use crate::history::HistoricalImage;
use liazo_core::protocol::SourceKey;
use liazo_transport::SessionBinding;
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;
use uuid::Uuid;

const MAX_EPOCHS: usize = 65536;
#[derive(Clone, Serialize, schemars::JsonSchema)]
pub struct CatalogPage {
    pub entries: Vec<ArchiveInfo>,
    pub truncated: bool,
    pub unavailable: usize,
}
#[derive(Clone, Serialize, schemars::JsonSchema)]
pub struct ArchiveSource {
    pub key: SourceKey,
    pub image: bool,
    pub audio: bool,
}
#[derive(Clone, Serialize, schemars::JsonSchema)]
pub struct ArchiveSources {
    pub entries: Vec<ArchiveSource>,
    pub truncated: bool,
    pub next_after: Option<SourceKey>,
}

enum Command {
    BudgetClaim {
        root: PathBuf,
        lease: Arc<super::budget::LeaseState>,
        metadata: u64,
        reply: mpsc::Sender<Result<(), ArchiveError>>,
    },
    BudgetAdmit {
        token: Uuid,
        bytes: u64,
        reply: mpsc::Sender<Result<(), ArchiveError>>,
    },
    List {
        offset: usize,
        reply: oneshot::Sender<Result<CatalogPage, ArchiveError>>,
    },
    Sources {
        binding: SessionBinding,
        after: Option<SourceKey>,
        reply: oneshot::Sender<Result<ArchiveSources, ArchiveError>>,
    },
    Image {
        binding: SessionBinding,
        source: SourceKey,
        at: u64,
        reply: oneshot::Sender<Result<HistoricalImage, ArchiveError>>,
    },
    Transcripts {
        binding: SessionBinding,
        first: u64,
        last: u64,
        sources: Vec<SourceKey>,
        reply: oneshot::Sender<Result<TranscriptPage, ArchiveError>>,
    },
    Audio {
        binding: SessionBinding,
        first: u64,
        last: u64,
        sources: Vec<SourceKey>,
        reply: oneshot::Sender<Result<AudioPage, ArchiveError>>,
    },
    Wake,
}
struct Owner {
    sender: SyncSender<Command>,
    closing: Arc<AtomicBool>,
    start_gate: super::budget::ArchiveStartGate,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.closing.store(true, Ordering::Release);
        let _ = self.sender.try_send(Command::Wake);
    }
}
#[derive(Clone)]
pub struct ArchiveCatalog(Arc<Owner>);
impl ArchiveCatalog {
    pub fn spawn(root: PathBuf) -> Result<Self, ArchiveError> {
        let (sender, receiver) = mpsc::sync_channel(16);
        let closing = Arc::new(AtomicBool::new(false));
        let cancelled = closing.clone();
        std::thread::Builder::new()
            .name("liazo-history".into())
            .spawn(move || {
                let mut deadline = Instant::now() + Duration::from_secs(10);
                let mut maintenance_errors = 0;
                let mut budget = super::budget::Manager::new(root.clone());
                while !cancelled.load(Ordering::Acquire) {
                    if Instant::now() >= deadline {
                        maintenance_errors = budget.maintain(wall_ms()).unwrap_or(1);
                        deadline = Instant::now() + Duration::from_secs(10);
                    }
                    let command = match receiver
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    {
                        Ok(command) => command,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            continue;
                        }
                        Err(_) => break,
                    };
                    match command {
                        Command::BudgetClaim {
                            root,
                            lease,
                            metadata,
                            reply,
                        } => {
                            let _ = reply.send(budget.claim(root, lease, metadata));
                        }
                        Command::BudgetAdmit {
                            token,
                            bytes,
                            reply,
                        } => {
                            let _ = reply.send(budget.admit(token, bytes));
                        }
                        Command::List { offset, reply } => {
                            if !reply.is_closed() {
                                let _ = reply.send(scan(&root).map(
                                    |(mut entries, unavailable, truncated)| {
                                        entries.sort_by_key(|entry| {
                                            std::cmp::Reverse((
                                                entry.wall_anchor_unix_ms,
                                                entry.binding.epoch,
                                            ))
                                        });
                                        let more = entries.len() > offset.saturating_add(128);
                                        CatalogPage {
                                            entries: entries
                                                .into_iter()
                                                .skip(offset)
                                                .take(128)
                                                .collect(),
                                            truncated: truncated || more,
                                            unavailable: unavailable.max(maintenance_errors),
                                        }
                                    },
                                ));
                            }
                        }
                        Command::Sources {
                            binding,
                            after,
                            reply,
                        } => {
                            if !reply.is_closed() {
                                let _ = reply.send(
                                    budget.read(binding, |index| index.retained_sources(after)),
                                );
                            }
                        }
                        Command::Image {
                            binding,
                            source,
                            at,
                            reply,
                        } => {
                            if !reply.is_closed() {
                                let _ =
                                    reply.send(budget.read(binding, |index| index.at(&source, at)));
                            }
                        }
                        Command::Transcripts {
                            binding,
                            first,
                            last,
                            sources,
                            reply,
                        } => {
                            if !reply.is_closed() {
                                let _ = reply.send(budget.read(binding, |index| {
                                    index.transcripts_between(first, last, &sources)
                                }));
                            }
                        }
                        Command::Audio {
                            binding,
                            first,
                            last,
                            sources,
                            reply,
                        } => {
                            if !reply.is_closed() {
                                let _ = reply.send(budget.read(binding, |index| {
                                    index.audio_between(first, last, &sources)
                                }));
                            }
                        }
                        Command::Wake => {}
                    }
                }
            })?;
        Ok(Self(Arc::new(Owner {
            sender,
            closing,
            start_gate: super::budget::ArchiveStartGate::default(),
        })))
    }
    pub(crate) fn start_gate(&self) -> super::budget::ArchiveStartGate {
        self.0.start_gate.clone()
    }
    pub(super) fn claim_budget(
        &self,
        root: PathBuf,
        lease: Arc<super::budget::LeaseState>,
        metadata: u64,
    ) -> Result<(), ArchiveError> {
        self.blocking_request(|reply| Command::BudgetClaim {
            root,
            lease,
            metadata,
            reply,
        })
    }
    pub(super) fn admit_budget(&self, token: Uuid, bytes: u64) -> Result<(), ArchiveError> {
        self.blocking_request(|reply| Command::BudgetAdmit {
            token,
            bytes,
            reply,
        })
    }
    fn blocking_request(
        &self,
        command: impl FnOnce(mpsc::Sender<Result<(), ArchiveError>>) -> Command,
    ) -> Result<(), ArchiveError> {
        if self.0.closing.load(Ordering::Acquire) {
            return Err(ArchiveError::Stopped);
        }
        let (reply, result) = mpsc::channel();
        self.0
            .sender
            .try_send(command(reply))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ArchiveError::Busy,
                mpsc::TrySendError::Disconnected(_) => ArchiveError::Stopped,
            })?;
        result
            .recv_timeout(Duration::from_secs(5))
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => ArchiveError::Busy,
                mpsc::RecvTimeoutError::Disconnected => ArchiveError::Stopped,
            })?
    }
    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, ArchiveError>>) -> Command,
    ) -> Result<T, ArchiveError> {
        if self.0.closing.load(Ordering::Acquire) {
            return Err(ArchiveError::Stopped);
        }
        let (reply, result) = oneshot::channel();
        self.0
            .sender
            .try_send(command(reply))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ArchiveError::Busy,
                mpsc::TrySendError::Disconnected(_) => ArchiveError::Stopped,
            })?;
        tokio::time::timeout(Duration::from_secs(5), result)
            .await
            .map_err(|_| ArchiveError::Busy)?
            .map_err(|_| ArchiveError::Stopped)?
    }
    pub async fn list(&self, offset: usize) -> Result<CatalogPage, ArchiveError> {
        if offset > MAX_EPOCHS {
            return Err(ArchiveError::InvalidPayload);
        }
        self.request(|reply| Command::List { offset, reply }).await
    }
    pub async fn sources(
        &self,
        binding: SessionBinding,
        after: Option<SourceKey>,
    ) -> Result<ArchiveSources, ArchiveError> {
        self.request(|reply| Command::Sources {
            binding,
            after,
            reply,
        })
        .await
    }
    pub async fn image(
        &self,
        binding: SessionBinding,
        source: SourceKey,
        at: u64,
    ) -> Result<HistoricalImage, ArchiveError> {
        self.request(|reply| Command::Image {
            binding,
            source,
            at,
            reply,
        })
        .await
    }
    pub async fn transcripts(
        &self,
        binding: SessionBinding,
        first: u64,
        last: u64,
        sources: Vec<SourceKey>,
    ) -> Result<TranscriptPage, ArchiveError> {
        self.request(|reply| Command::Transcripts {
            binding,
            first,
            last,
            sources,
            reply,
        })
        .await
    }
    pub async fn audio(
        &self,
        binding: SessionBinding,
        first: u64,
        last: u64,
        sources: Vec<SourceKey>,
    ) -> Result<AudioPage, ArchiveError> {
        self.request(|reply| Command::Audio {
            binding,
            first,
            last,
            sources,
            reply,
        })
        .await
    }
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_millis().min(u128::from(u64::MAX)) as u64)
}
pub(super) fn relative(info: &ArchiveInfo, now: u64) -> u64 {
    now.saturating_sub(info.wall_anchor_unix_ms)
        .saturating_mul(1_000_000)
        .max(info.latest_ns)
        .min(i64::MAX as u64)
}
pub(super) fn path(root: &Path, binding: SessionBinding) -> Result<PathBuf, ArchiveError> {
    if binding.session.is_nil() || binding.epoch.is_nil() {
        return Err(ArchiveError::InvalidPayload);
    }
    let session = root.join(binding.session.to_string());
    let epoch = session.join(binding.epoch.to_string());
    for directory in [root, &session, &epoch] {
        let metadata = std::fs::symlink_metadata(directory).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ArchiveError::NotFound
            } else {
                error.into()
            }
        })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ArchiveError::InvalidPayload);
        }
    }
    Ok(epoch)
}
pub(super) fn open(root: &Path, binding: SessionBinding) -> Result<ArchiveIndex, ArchiveError> {
    let root = path(root, binding)?;
    let info = ArchiveInfo::read(&root)?;
    if info.binding != binding {
        return Err(ArchiveError::SessionMismatch);
    }
    let mut index = ArchiveIndex::open_existing(&root)?;
    if info.writer_active || info.closed_at_ns.is_none() {
        index.begin()?;
        index.finish(info.latest_ns)?;
    }
    index.enforce_limits(relative(&info, wall_ms()))?;
    Ok(index)
}
pub(super) fn scan(root: &Path) -> Result<(Vec<ArchiveInfo>, usize, bool), ArchiveError> {
    scan_except(root, None)
}
pub(super) fn scan_except(
    root: &Path,
    skip: Option<SessionBinding>,
) -> Result<(Vec<ArchiveInfo>, usize, bool), ArchiveError> {
    if !root.exists() {
        return Ok((vec![], 0, false));
    }
    if std::fs::symlink_metadata(root)?.file_type().is_symlink() {
        return Err(ArchiveError::InvalidPayload);
    }
    let mut entries = vec![];
    let mut unavailable = 0;
    for session in std::fs::read_dir(root)? {
        let session = session?;
        let Some(session_id) = canonical(&session.file_name()) else {
            continue;
        };
        if !session.file_type()?.is_dir() {
            continue;
        }
        for epoch in std::fs::read_dir(session.path())? {
            let epoch = epoch?;
            let Some(epoch_id) = canonical(&epoch.file_name()) else {
                continue;
            };
            if !epoch.file_type()?.is_dir() {
                continue;
            }
            if skip
                .is_some_and(|binding| binding.session == session_id && binding.epoch == epoch_id)
            {
                continue;
            }
            match ArchiveInfo::read(&epoch.path()) {
                Ok(info)
                    if info.binding
                        == SessionBinding {
                            session: session_id,
                            epoch: epoch_id,
                        } =>
                {
                    entries.push(info)
                }
                _ => unavailable += 1,
            }
            if entries.len() == MAX_EPOCHS {
                return Ok((entries, unavailable, true));
            }
        }
    }
    Ok((entries, unavailable, false))
}
fn canonical(name: &std::ffi::OsStr) -> Option<Uuid> {
    let text = name.to_str()?;
    let id = text.parse::<Uuid>().ok()?;
    (id.to_string() == text).then_some(id)
}
impl ArchiveIndex {
    pub fn retained_sources(
        &self,
        after: Option<SourceKey>,
    ) -> Result<ArchiveSources, ArchiveError> {
        let mut query=self.database.prepare("SELECT source,generation,MAX(image),MAX(audio) FROM (
            SELECT source,generation,1 AS image,0 AS audio FROM images UNION ALL
            SELECT source,generation,0,1 FROM audio UNION ALL SELECT source,generation,0,1 FROM transcripts)
            WHERE source>?1 OR (source=?1 AND generation>?2) GROUP BY source,generation ORDER BY source,generation LIMIT 257")?;
        let mut rows = query.query(rusqlite::params![
            after.as_ref().map_or("", |key| key.id.as_str()),
            after
                .as_ref()
                .map(|key| key.generation.to_string())
                .unwrap_or_default()
        ])?;
        let mut entries = vec![];
        while let Some(row) = rows.next()? {
            if entries.len() == 256 {
                let next_after = entries
                    .last()
                    .map(|entry: &ArchiveSource| entry.key.clone());
                return Ok(ArchiveSources {
                    entries,
                    truncated: true,
                    next_after,
                });
            }
            let key = SourceKey {
                id: row.get(0)?,
                generation: row
                    .get::<_, String>(1)?
                    .parse()
                    .map_err(|_| ArchiveError::InvalidPayload)?,
            };
            key.validate().map_err(|_| ArchiveError::InvalidPayload)?;
            entries.push(ArchiveSource {
                key,
                image: row.get(2)?,
                audio: row.get(3)?,
            });
        }
        Ok(ArchiveSources {
            entries,
            truncated: false,
            next_after: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{ArchiveIdentity, ArchiveLimits};
    use liazo_core::{protocol::ImageFormat, wire::ImageMeta};
    #[test]
    fn maintenance_removes_an_idle_closed_epoch_after_its_saved_duration() {
        let root = tempfile::tempdir().unwrap();
        let now = wall_ms();
        let binding = SessionBinding {
            session: Uuid::new_v4(),
            epoch: Uuid::new_v4(),
        };
        let identity = ArchiveIdentity {
            binding,
            provider: liazo_transport::Identity::generate().unwrap().fingerprint(),
            wall_anchor_unix_ms: now - 60_000,
        };
        let path = root
            .path()
            .join(binding.session.to_string())
            .join(binding.epoch.to_string());
        let mut index = ArchiveIndex::open(
            &path,
            identity,
            ArchiveLimits {
                duration_ms: 100,
                max_bytes: 4 * 1024 * 1024,
            },
        )
        .unwrap();
        let source = SourceKey {
            id: "display:idle".into(),
            generation: Uuid::new_v4(),
        };
        index
            .record_image(
                ImageMeta {
                    source: source.clone(),
                    sequence: 1,
                    configuration_revision: 1,
                    capture_started_ns: 1,
                    captured_ns: 2,
                    width: 1,
                    height: 1,
                    format: ImageFormat::Png,
                    fingerprint: [1; 32],
                },
                Uuid::new_v4(),
                bytes::Bytes::from_static(b"fixture"),
                1_000_000,
                1_000_000_000,
            )
            .unwrap();
        index.finish(2_000_000).unwrap();
        drop(index);
        assert_eq!(
            super::super::budget::Manager::new(root.path().to_owned())
                .maintain(now)
                .unwrap(),
            0
        );
        assert!(!path.exists());
        assert!(!root.path().join(binding.session.to_string()).exists());
    }
}
