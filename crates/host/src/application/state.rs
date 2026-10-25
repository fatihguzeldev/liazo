use super::{
    Active, HostError, RecoveryOperation, ResumeIntent, SessionHost, SpeechIntent,
    preferences::SpeechSettings, recovery::issue,
};
use liazo_core::protocol::{CaptureSettings, SourceKey};
use liazo_transport::SessionBinding;
use std::sync::{Arc, atomic::Ordering};

impl SessionHost {
    pub fn resume(&self) {
        if self.0.resume_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let owner = Arc::downgrade(&self.0);
        let task = tokio::spawn(async move {
            let Some(inner) = owner.upgrade() else {
                return;
            };
            let host = SessionHost(inner);
            let revision = host.0.revision.load(Ordering::Acquire);
            let result = match &host.0.preferences {
                Some(store) => store.load().await,
                None => Err(super::preferences::PreferenceError::Storage),
            };
            let intent = match result {
                Ok(Some(intent)) => intent,
                Ok(None) => return,
                Err(error) => {
                    eprintln!("liazo: {error}");
                    *host
                        .0
                        .startup_issue
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) = Some(error.to_string());
                    return;
                }
            };
            let trusted = host.0.peers.read().await;
            if !trusted
                .iter()
                .any(|peer| peer.fingerprint().to_string() == intent.provider)
            {
                return;
            }
            let active = host.0.active.read().await;
            if active.is_some() || host.0.revision.load(Ordering::Acquire) != revision {
                return;
            }
            *host.0.pending.write().await = Some(intent);
            drop(active);
            drop(trusted);
            host.ensure_monitor();
        });
        *self
            .0
            .startup
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(task);
    }

    pub async fn configure(
        &self,
        binding: SessionBinding,
        settings: CaptureSettings,
    ) -> Result<u64, HostError> {
        let host = self.clone();
        self.owned(async move {
            let receiver = host.receiver(binding).await?;
            let revision = receiver.configure(settings).await?;
            let selected = receiver.settings().audio;
            if let Some(status) = receiver.archive_status() {
                let retained: Vec<_> = status
                    .raw_audio_sources
                    .iter()
                    .filter(|key| selected.contains(key))
                    .cloned()
                    .collect();
                if retained != status.raw_audio_sources {
                    let _ = receiver.configure_raw_audio(retained).await;
                }
            }
            let speech_off = {
                let mut active = host.0.active.write().await;
                let active = active.as_mut().ok_or(HostError::NoSession)?;
                active.raw_audio.retain(|key| selected.contains(key));
                if let Some(speech) = active.speech.as_mut() {
                    speech.settings.sources.retain(|key| selected.contains(key));
                    if speech.settings.sources.is_empty() {
                        active.speech = None;
                        active.speech_revision = active.speech_revision.saturating_add(1);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            if speech_off {
                receiver.stop_transcription().await;
            }
            let _ = host.persist_current().await;
            Ok(revision)
        })
        .await
    }

    pub async fn configure_raw_audio(
        &self,
        binding: SessionBinding,
        sources: Vec<SourceKey>,
    ) -> Result<(), HostError> {
        let host = self.clone();
        self.owned(async move {
            host.receiver(binding)
                .await?
                .configure_raw_audio(sources)
                .await?;
            let _ = host.persist_current().await;
            Ok(())
        })
        .await
    }

    pub(super) fn next_preferences_revision(&self) -> u64 {
        self.0.preferences_revision.fetch_add(1, Ordering::AcqRel)
    }

    pub(super) async fn persist_current(&self) -> Result<(), HostError> {
        let (version, intent) = {
            let active = self.0.active.read().await;
            let intent = match active.as_ref() {
                Some(active) if active.reconnect => Some(self.intent(active)),
                Some(_) => None,
                None => self.0.pending.read().await.clone(),
            };
            (self.next_preferences_revision(), intent)
        };
        self.persist(version, intent).await
    }

    pub(super) async fn persist(
        &self,
        version: u64,
        intent: Option<ResumeIntent>,
    ) -> Result<(), HostError> {
        let result = match &self.0.preferences {
            Some(store) => store
                .save(version, intent)
                .await
                .map_err(|_| HostError::Settings),
            None => Err(HostError::Settings),
        };
        let mut active = self.0.active.write().await;
        if let Some(active) = active.as_mut() {
            active
                .recovery
                .retain(|issue| issue.operation != RecoveryOperation::Settings);
            if result.is_err() {
                active.recovery.push(issue(
                    RecoveryOperation::Settings,
                    None,
                    "session settings could not be saved",
                ));
            }
        }
        result
    }

    fn intent(&self, active: &Active) -> ResumeIntent {
        let capture = active.receiver.settings();
        let mut raw_audio = active
            .receiver
            .archive_status()
            .map(|archive| archive.raw_audio_sources)
            .unwrap_or_else(|| active.raw_audio.clone());
        raw_audio.retain(|key| capture.audio.contains(key));
        let speech = active
            .speech
            .as_ref()
            .map(|speech| {
                let mut settings = speech.settings.clone();
                settings.sources.retain(|key| capture.audio.contains(key));
                settings
            })
            .filter(|speech| !speech.sources.is_empty());
        ResumeIntent {
            version: 1,
            owner: self.0.identity.fingerprint().to_string(),
            provider: active.peer.to_string(),
            address: active.address,
            binding: active.receiver.archive_identity().binding,
            capture,
            raw_audio,
            speech,
        }
    }
}
impl From<SpeechSettings> for SpeechIntent {
    fn from(settings: SpeechSettings) -> Self {
        Self {
            model: None,
            settings,
        }
    }
}
