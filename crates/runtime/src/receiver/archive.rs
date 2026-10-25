use super::{ReceiverSession, failure};
use crate::{
    RuntimeError,
    archive::{
        ArchiveBudget, ArchiveClient, ArchiveError, ArchiveLimits, ArchiveSources, ArchiveStatus,
        ArchiveWorker, AudioPage, TranscriptPage,
    },
    live::ConnectionState,
};
use liazo_core::{control::FailureCode, protocol::SourceKey};
use std::{path::PathBuf, time::Duration};
use tokio::sync::oneshot;

impl ReceiverSession {
    pub async fn start_archive(
        &self,
        root: PathBuf,
        limits: ArchiveLimits,
    ) -> Result<(), RuntimeError> {
        self.start_archive_with_budget(root, limits, None).await
    }

    pub async fn start_archive_with_budget(
        &self,
        root: PathBuf,
        limits: ArchiveLimits,
        budget: impl Into<Option<ArchiveBudget>>,
    ) -> Result<(), RuntimeError> {
        limits.validate()?;
        let budget = budget.into();
        let permit = self
            .0
            .operations
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Closed)?;
        let receiver = self.clone();
        let (reply, result) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = receiver.install_archive(root, limits, budget).await;
            let _ = reply.send(result);
        });
        result.await.map_err(|_| RuntimeError::Closed)?
    }

    async fn install_archive(
        &self,
        root: PathBuf,
        limits: ArchiveLimits,
        budget: Option<ArchiveBudget>,
    ) -> Result<(), RuntimeError> {
        let mut owner = self.0.archive.lock().await;
        {
            let mut state = self
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.connection != ConnectionState::Connected {
                return Err(RuntimeError::Closed);
            }
            state.archive = None;
        }
        if let Some(previous) = owner.as_mut()
            && !previous.stop().await
        {
            return Err(ArchiveError::Busy.into());
        }
        owner.take();
        let worker = ArchiveWorker::start_with_budget(
            root,
            self.archive_identity(),
            limits,
            self.0.shared.clock,
            budget,
        )
        .await?;
        let client = worker.client();
        *owner = Some(worker);
        let attached = {
            let mut state = self
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.connection == ConnectionState::Connected {
                state.attach_archive(client, self.0.shared.now_ns());
                true
            } else {
                false
            }
        };
        if !attached {
            if let Some(worker) = owner.as_mut() {
                worker.stop().await;
            }
            return Err(RuntimeError::Closed);
        }
        self.0.shared.signal();
        Ok(())
    }

    fn archive_client(&self) -> Option<ArchiveClient> {
        self.0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .archive
            .clone()
    }

    pub fn archive_status(&self) -> Option<ArchiveStatus> {
        self.archive_client().map(|archive| archive.status())
    }

    pub async fn flush_archive(&self) -> Result<(), RuntimeError> {
        if let Some(archive) = self.archive_client() {
            archive.flush().await?;
        }
        Ok(())
    }
    pub async fn configure_raw_audio(&self, sources: Vec<SourceKey>) -> Result<(), RuntimeError> {
        {
            let state = self
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.connection != ConnectionState::Connected {
                return Err(RuntimeError::Closed);
            }
            if sources.len() > 8
                || sources
                    .iter()
                    .any(|source| !state.settings.audio.contains(source))
                || sources
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != sources.len()
            {
                return Err(failure(
                    FailureCode::InvalidRequest,
                    "raw audio must use selected audio sources",
                ));
            }
        }
        let archive = self
            .archive_client()
            .ok_or_else(|| failure(FailureCode::Unavailable, "disk archive is not enabled"))?;
        let permit = self
            .0
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure(FailureCode::Busy, "session is busy"))?;
        let receiver = self.clone();
        let (reply, result) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = archive.configure_audio(sources).await;
            receiver
                .0
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .changed();
            receiver.0.shared.signal();
            let _ = reply.send(result);
        });
        result.await.map_err(|_| RuntimeError::Closed)??;
        Ok(())
    }

    pub async fn history_audio(
        &self,
        older: Duration,
        newer: Duration,
        selected: &[SourceKey],
    ) -> Result<AudioPage, RuntimeError> {
        let (first, last) = self.archive_range(older, newer, selected)?;
        let archive = self
            .archive_client()
            .ok_or_else(|| failure(FailureCode::Unavailable, "disk archive is not enabled"))?;
        Ok(archive.audio(first, last, selected.to_vec()).await?)
    }

    pub(super) async fn seal_archive(&self) -> Result<(), RuntimeError> {
        let _owner = self.0.archive.lock().await;
        if let Some(archive) = self.archive_client() {
            archive.seal().await?;
        }
        Ok(())
    }

    pub async fn retire_archive(&self) -> Result<(), RuntimeError> {
        let mut owner = self.0.archive.lock().await;
        if let Some(worker) = owner.as_mut()
            && !worker.stop().await
        {
            return Err(ArchiveError::Busy.into());
        }
        owner.take();
        self.0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .archive = None;
        self.0.shared.signal();
        Ok(())
    }

    pub async fn history_transcripts(
        &self,
        older: Duration,
        newer: Duration,
        selected: &[SourceKey],
    ) -> Result<TranscriptPage, RuntimeError> {
        let (first, last) = self.archive_range(older, newer, selected)?;
        let archive = self
            .archive_client()
            .ok_or_else(|| failure(FailureCode::Unavailable, "disk archive is not enabled"))?;
        Ok(archive.transcripts(first, last, selected.to_vec()).await?)
    }

    pub async fn retained_sources(
        &self,
        after: Option<SourceKey>,
    ) -> Result<ArchiveSources, RuntimeError> {
        let archive = self
            .archive_client()
            .ok_or_else(|| failure(FailureCode::Unavailable, "disk archive is not enabled"))?;
        Ok(archive.sources(after).await?)
    }

    fn archive_range(
        &self,
        older: Duration,
        newer: Duration,
        selected: &[SourceKey],
    ) -> Result<(u64, u64), RuntimeError> {
        if older < newer || selected.len() > 8 {
            return Err(failure(
                FailureCode::InvalidRequest,
                "invalid archive history range",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for source in selected {
            source
                .validate()
                .map_err(|error| failure(FailureCode::InvalidRequest, &error.to_string()))?;
            if !seen.insert(source) {
                return Err(failure(
                    FailureCode::InvalidRequest,
                    "duplicate archive source",
                ));
            }
        }
        let now = self.0.shared.now_ns();
        let relative = |age: Duration| {
            u64::try_from(age.as_nanos())
                .ok()
                .and_then(|age| now.checked_sub(age))
                .ok_or_else(|| {
                    failure(
                        FailureCode::InvalidRequest,
                        "requested time predates this session",
                    )
                })
        };
        Ok((relative(older)?, relative(newer)?))
    }
}
