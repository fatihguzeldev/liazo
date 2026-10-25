use super::{ArchiveCatalog, ArchiveError, ArchiveInfo, ArchiveLimits};
use liazo_transport::SessionBinding;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use uuid::Uuid;

const ROOT_RESERVE: u64 = 64 * 1024;
#[derive(Serialize, Deserialize)]
struct ProfilePolicy {
    max_bytes: u64,
}

fn read_policy(root: &Path) -> Result<u64, ArchiveError> {
    let path = root.join(".budget.json");
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ArchiveLimits::default().max_bytes);
        }
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ArchiveError::InvalidPayload);
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(ArchiveError::InvalidPayload);
    }
    let policy: ProfilePolicy = serde_json::from_slice(&bytes)?;
    ArchiveLimits {
        max_bytes: policy.max_bytes,
        ..ArchiveLimits::default()
    }
    .validate()?;
    Ok(policy.max_bytes)
}

fn write_policy(root: &Path, max_bytes: u64) -> Result<(), ArchiveError> {
    let temporary = root.join(".budget.part");
    match std::fs::symlink_metadata(&temporary) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            std::fs::remove_file(&temporary)?
        }
        Ok(_) => return Err(ArchiveError::InvalidPayload),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        _ => {}
    }
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(&serde_json::to_vec(&ProfilePolicy { max_bytes })?)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temporary, root.join(".budget.json"))?;
    #[cfg(unix)]
    File::open(root)?.sync_all()?;
    Ok(())
}

/// One profile-wide admission policy, shared by its catalogue and archive writer.
#[derive(Clone)]
pub struct ArchiveBudget {
    pub(super) catalog: ArchiveCatalog,
    pub(super) max_bytes: u64,
}

pub(super) struct LeaseState {
    pub token: Uuid,
    pub binding: SessionBinding,
    pub max_bytes: u64,
    pub reserved: AtomicU64,
}

pub(super) struct BudgetLease {
    catalog: ArchiveCatalog,
    state: Arc<LeaseState>,
}

impl ArchiveCatalog {
    pub fn budget(&self, max_bytes: u64) -> Result<ArchiveBudget, ArchiveError> {
        ArchiveLimits {
            max_bytes,
            ..ArchiveLimits::default()
        }
        .validate()?;
        Ok(ArchiveBudget {
            catalog: self.clone(),
            max_bytes,
        })
    }
}

impl ArchiveBudget {
    pub(super) fn claim(
        &self,
        root: &Path,
        binding: SessionBinding,
        metadata: u64,
    ) -> Result<BudgetLease, ArchiveError> {
        let state = Arc::new(LeaseState {
            token: Uuid::new_v4(),
            binding,
            max_bytes: self.max_bytes,
            reserved: AtomicU64::new(metadata),
        });
        self.catalog
            .claim_budget(root.to_owned(), state.clone(), metadata)?;
        Ok(BudgetLease {
            catalog: self.catalog.clone(),
            state,
        })
    }
}

impl BudgetLease {
    pub(super) fn admit(&self, bytes: u64) -> Result<(), ArchiveError> {
        self.catalog.admit_budget(self.state.token, bytes)
    }
}

pub(super) fn expected(root: &Path, binding: SessionBinding) -> PathBuf {
    root.join(binding.session.to_string())
        .join(binding.epoch.to_string())
}

pub(super) fn active_bytes(active: &Option<Arc<LeaseState>>) -> u64 {
    active
        .as_ref()
        .map_or(0, |lease| lease.reserved.load(Ordering::Acquire))
}

pub(super) fn closed_bytes(info: &ArchiveInfo) -> u64 {
    // Precharge the durable deletion marker, so starting cleanup cannot exceed the cap.
    info.accounted_bytes.saturating_add(8192)
}

pub(super) struct Manager {
    root: PathBuf,
    owner: Option<File>,
    active: Weak<LeaseState>,
    entries: Option<Vec<ArchiveInfo>>,
    trash: u64,
    max_bytes: u64,
}
impl Manager {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            owner: None,
            active: Weak::new(),
            entries: None,
            trash: 0,
            max_bytes: ArchiveLimits::default().max_bytes,
        }
    }
    fn own(&mut self) -> Result<(), ArchiveError> {
        if self.owner.is_some() {
            return Ok(());
        }
        let mut directory = std::fs::DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory.create(&self.root)?;
        let metadata = std::fs::symlink_metadata(&self.root)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ArchiveError::InvalidPayload);
        }
        let lock = self.root.join(".catalog.lock");
        match std::fs::symlink_metadata(&lock) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(ArchiveError::InvalidPayload);
            }
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let owner = options.open(lock)?;
        // Dropping the last catalogue handle wakes its thread without blocking
        // the caller on a join. Give that old owner a short bounded exit window.
        let until = std::time::Instant::now() + std::time::Duration::from_millis(250);
        loop {
            match owner.try_lock() {
                Ok(()) => break,
                Err(_) if std::time::Instant::now() < until => {
                    std::thread::sleep(std::time::Duration::from_millis(5))
                }
                Err(_) => return Err(ArchiveError::Busy),
            }
        }
        self.max_bytes = read_policy(&self.root)?;
        self.owner = Some(owner);
        Ok(())
    }
    fn refresh(&mut self) -> Result<(), ArchiveError> {
        let active = self.active.upgrade();
        self.trash =
            super::cleanup::recover_except(&self.root, active.as_ref().map(|lease| lease.binding))?
                .0;
        let (mut entries, unavailable, truncated) =
            super::catalog::scan_except(&self.root, active.as_ref().map(|lease| lease.binding))?;
        if unavailable != 0 || truncated {
            return Err(ArchiveError::InvalidPayload);
        }
        for entry in &mut entries {
            if active
                .as_ref()
                .is_some_and(|lease| lease.binding == entry.binding)
            {
                continue;
            }
            entry.accounted_bytes =
                entry
                    .accounted_bytes
                    .max(super::cleanup::validate(&expected(
                        &self.root,
                        entry.binding,
                    ))?);
        }
        entries.sort_by_key(|entry| (entry.wall_anchor_unix_ms, entry.binding.epoch));
        self.entries = Some(entries);
        Ok(())
    }
    pub fn claim(
        &mut self,
        root: PathBuf,
        lease: Arc<LeaseState>,
        metadata: u64,
    ) -> Result<(), ArchiveError> {
        if root != expected(&self.root, lease.binding)
            || lease.binding.session.is_nil()
            || lease.binding.epoch.is_nil()
        {
            return Err(ArchiveError::SessionMismatch);
        }
        if self.active.upgrade().is_some() {
            return Err(ArchiveError::Busy);
        }
        self.own()?;
        self.entries = None;
        self.refresh()?;
        for entry in self.entries.as_ref().unwrap() {
            // A profile has one active writer. Do not budget another owner's
            // changing files as if they were a fixed closed-epoch allocation.
            drop(super::PayloadStore::open(
                &expected(&self.root, entry.binding).join("payloads"),
            )?);
        }
        let previous = self
            .entries
            .as_ref()
            .unwrap()
            .iter()
            .find(|entry| entry.binding == lease.binding)
            .map_or(0, |entry| entry.accounted_bytes);
        lease
            .reserved
            .store(metadata.saturating_add(previous), Ordering::Release);
        self.max_bytes = lease.max_bytes;
        self.active = Arc::downgrade(&lease);
        self.fit(0, None)?;
        write_policy(&self.root, self.max_bytes)
    }
    pub fn admit(&mut self, token: Uuid, bytes: u64) -> Result<(), ArchiveError> {
        let lease = self
            .active
            .upgrade()
            .filter(|lease| lease.token == token)
            .ok_or(ArchiveError::Stopped)?;
        if self.entries.is_none() {
            self.refresh()?;
        }
        let previous = lease.reserved.swap(bytes, Ordering::AcqRel);
        if let Err(error) = self.fit(0, None) {
            lease.reserved.store(previous, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }
    fn total(&self) -> u64 {
        let active = self.active.upgrade();
        self.entries
            .as_ref()
            .into_iter()
            .flatten()
            .filter(|entry| {
                !active
                    .as_ref()
                    .is_some_and(|lease| lease.binding == entry.binding)
            })
            .fold(
                ROOT_RESERVE
                    .saturating_add(self.trash)
                    .saturating_add(active_bytes(&active)),
                |total, entry| total.saturating_add(closed_bytes(entry)),
            )
    }
    fn fit(&mut self, extra: u64, protect: Option<SessionBinding>) -> Result<(), ArchiveError> {
        let active = self.active.upgrade();
        let mut attempted = HashSet::new();
        while self.total().saturating_add(extra) > self.max_bytes {
            let candidate = self
                .entries
                .as_ref()
                .into_iter()
                .flatten()
                .find(|entry| {
                    Some(entry.binding) != protect
                        && !active
                            .as_ref()
                            .is_some_and(|lease| lease.binding == entry.binding)
                        && !attempted.contains(&(entry.binding.session, entry.binding.epoch))
                })
                .cloned();
            let Some(info) = candidate else {
                return Err(ArchiveError::Capacity);
            };
            attempted.insert((info.binding.session, info.binding.epoch));
            let _ = super::cleanup::erase(&self.root, &info);
            self.refresh()?;
        }
        Ok(())
    }
    pub fn invalidate(&mut self) {
        self.entries = None;
    }
    pub fn prepare_read(&mut self, binding: SessionBinding) -> Result<(), ArchiveError> {
        self.own()?;
        let info = ArchiveInfo::read(&super::catalog::path(&self.root, binding)?)?;
        if info.binding != binding {
            return Err(ArchiveError::SessionMismatch);
        }
        self.refresh()?;
        if self
            .active
            .upgrade()
            .is_some_and(|lease| lease.binding == binding)
        {
            return Err(ArchiveError::Busy);
        }
        let info = self
            .entries
            .as_ref()
            .unwrap()
            .iter()
            .find(|entry| entry.binding == binding)
            .ok_or(ArchiveError::NotFound)?;
        // A closed SQLite index can temporarily grow a WAL during recovery/cleanup.
        // Reserve that work before reopening it alongside the live writer.
        self.fit(info.limits.metadata_budget(), Some(binding))
    }
    pub fn read<T>(
        &mut self,
        binding: SessionBinding,
        operation: impl FnOnce(super::ArchiveIndex) -> Result<T, ArchiveError>,
    ) -> Result<T, ArchiveError> {
        self.prepare_read(binding)?;
        let result = super::catalog::open(&self.root, binding).and_then(operation);
        self.invalidate();
        result
    }
    pub fn maintain(&mut self, now: u64) -> Result<usize, ArchiveError> {
        if !self.root.try_exists()? {
            return Ok(0);
        }
        self.own()?;
        self.refresh()?;
        let entries = self.entries.clone().unwrap();
        let mut failed = 0;
        for info in entries {
            if self
                .active
                .upgrade()
                .is_some_and(|lease| lease.binding == info.binding)
            {
                continue;
            }
            let result = (|| {
                let now = super::catalog::relative(&info, now);
                if !info.writer_active
                    && info.closed_at_ns.is_some()
                    && now.saturating_sub(info.limits.duration_ms * 1_000_000) > info.latest_ns
                {
                    return super::cleanup::erase(&self.root, &info);
                }
                self.prepare_read(info.binding)?;
                let root = expected(&self.root, info.binding);
                let mut index = super::ArchiveIndex::open_existing(&root)?;
                if info.writer_active || info.closed_at_ns.is_none() {
                    index.begin()?;
                    index.finish(info.latest_ns)?;
                }
                index.enforce_limits(now)?;
                drop(index);
                if now.saturating_sub(info.limits.duration_ms * 1_000_000) > info.latest_ns {
                    super::cleanup::erase(&self.root, &info)?;
                }
                Ok::<_, ArchiveError>(())
            })();
            if !matches!(
                result,
                Ok(()) | Err(ArchiveError::Busy) | Err(ArchiveError::NotFound)
            ) {
                failed += 1;
            }
            self.invalidate();
        }
        self.refresh()?;
        if self.fit(0, None).is_err() {
            failed += 1;
        }
        Ok(failed)
    }
}
