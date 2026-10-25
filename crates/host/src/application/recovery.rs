use super::{
    Active, HostError, RecoveryIssue, RecoveryOperation, SessionHost, SessionSummary, SpeechIntent,
};
use liazo_core::protocol::{CaptureSettings, SourceAvailability, SourceKey, SourceKind};
use liazo_runtime::{
    ConnectionState, RuntimeError, archive::ArchiveLimits, receiver::ReceiverSession,
};
use liazo_transport::{Fingerprint, PeerClient, SessionBinding};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

impl SessionHost {
    pub(super) async fn connect_inner(
        &self,
        address: SocketAddr,
        peer: Fingerprint,
        expected: Option<SessionBinding>,
    ) -> Result<SessionSummary, HostError> {
        if let Some(current) = self.current().await
            && current.connection == ConnectionState::Connected
        {
            return if current.provider == peer.to_string() && current.address == address {
                Ok(current)
            } else {
                Err(HostError::ActiveSession)
            };
        }
        let (revision, transition) = {
            let active = self.0.active.read().await;
            if let Some(expected) = expected {
                let active = active.as_ref().ok_or(HostError::Interrupted)?;
                if !active.reconnect
                    || active.receiver.archive_identity().binding != expected
                    || active.receiver.connection() != ConnectionState::Disconnected
                {
                    return Err(HostError::Interrupted);
                }
            }
            let revision = self.0.revision.load(Ordering::Acquire);
            let mut current = self.0.transition.lock().unwrap_or_else(|e| e.into_inner());
            current.cancel();
            *current = CancellationToken::new();
            (revision, current.clone())
        };
        // Resolve trust before touching the retained connection or its configuration.
        let certificate = self
            .0
            .peers
            .read()
            .await
            .iter()
            .find(|candidate| candidate.fingerprint() == peer)
            .map(|peer| peer.certificate.clone())
            .ok_or(HostError::Unpaired)?;
        let previous = {
            let mut active = self.0.active.write().await;
            active.as_mut().map(|active| {
                if let Some(archive) = active.receiver.archive_status() {
                    active.raw_audio = archive.raw_audio_sources;
                }
                (
                    active.receiver.clone(),
                    active.peer,
                    active.reconnect,
                    active.raw_audio.clone(),
                    active.speech.clone(),
                )
            })
        };
        let mut session = Uuid::new_v4();
        let mut restore = None;
        if let Some((previous, previous_peer, reconnect, raw_audio, speech)) = previous {
            if reconnect
                && previous_peer == peer
                && previous.connection() == ConnectionState::Disconnected
            {
                session = previous.archive_identity().binding.session;
                restore = Some((previous.settings(), raw_audio, speech));
            }
            let _ = previous.disconnect().await;
            previous.retire_archive().await?;
        }
        // Keep the old active handle until a new authenticated epoch is ready. Failed attempts
        // retain the logical session and settings, and old epochs remain queryable.
        let client = PeerClient::new(&self.0.identity, &certificate)?;
        let receiver = tokio::select! {
            biased;
            _ = transition.cancelled() => return Err(HostError::Interrupted),
            receiver = ReceiverSession::connect(&client, address, session) => receiver?,
        };
        if transition.is_cancelled() {
            return Err(HostError::Interrupted);
        }
        let mut recovery = vec![];
        let archive = self.initialize_archive(&receiver).await;
        if archive.is_err() {
            recovery.push(issue(
                RecoveryOperation::Archive,
                None,
                "history recording is unavailable; live observation continues",
            ));
        }
        if transition.is_cancelled() {
            let _ = receiver.disconnect().await;
            let _ = receiver.retire_archive().await;
            return Err(HostError::Interrupted);
        }
        let mut raw_audio = vec![];
        let mut desired_speech = None;
        if let Some((settings, desired_raw, speech)) = restore {
            let (settings, missing) = restore_settings(settings, &receiver.sources());
            recovery.extend(missing);
            if let Err(error) = receiver.configure(settings).await {
                recovery.push(issue(RecoveryOperation::Capture, None, &error.to_string()));
            }
            if transition.is_cancelled() {
                let _ = receiver.disconnect().await;
                let _ = receiver.retire_archive().await;
                return Err(HostError::Interrupted);
            }
            raw_audio = desired_raw;
            let selected = receiver.settings().audio;
            let available_raw: Vec<_> = raw_audio
                .iter()
                .filter(|key| selected.contains(key))
                .cloned()
                .collect();
            if !available_raw.is_empty()
                && let Err(error) = receiver.configure_raw_audio(available_raw).await
            {
                recovery.push(issue(RecoveryOperation::RawAudio, None, &error.to_string()));
            }
            if let Some(speech) = speech {
                let _speech = self.0.speech_operation.lock().await;
                if !transition.is_cancelled()
                    && let Err(error) = start_speech(&receiver, &speech).await
                {
                    recovery.push(issue(
                        RecoveryOperation::Transcription,
                        None,
                        &error.to_string(),
                    ));
                }
                if transition.is_cancelled() {
                    receiver.stop_transcription().await;
                }
                desired_speech = Some(speech);
            }
        }
        let trusted = self.0.peers.read().await;
        let mut active = self.0.active.write().await;
        if !trusted
            .iter()
            .any(|candidate| candidate.fingerprint() == peer)
            || self.0.revision.load(Ordering::Acquire) != revision
        {
            drop(active);
            drop(trusted);
            let _ = receiver.disconnect().await;
            let _ = receiver.retire_archive().await;
            return Err(HostError::Interrupted);
        }
        *active = Some(Active {
            peer,
            address,
            receiver,
            reconnect: true,
            raw_audio,
            speech: desired_speech,
            speech_revision: 0,
            recovery,
        });
        drop(active);
        drop(trusted);
        self.ensure_monitor();
        self.current().await.ok_or(HostError::NoSession)
    }

    fn ensure_monitor(&self) {
        let mut monitor = self.0.monitor.lock().unwrap_or_else(|e| e.into_inner());
        if monitor.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        let owner = Arc::downgrade(&self.0);
        *monitor = Some(tokio::spawn(async move {
            let mut delay = Duration::from_secs(1);
            loop {
                tokio::time::sleep(delay).await;
                let Some(inner) = owner.upgrade() else {
                    break;
                };
                let host = SessionHost(inner);
                let attempt = {
                    let active = host.0.active.read().await;
                    let Some(active) = active.as_ref() else {
                        continue;
                    };
                    if !active.reconnect {
                        continue;
                    }
                    if active.receiver.connection() == ConnectionState::Disconnected {
                        Some((
                            active.address,
                            active.peer,
                            active.receiver.archive_identity().binding,
                        ))
                    } else {
                        None
                    }
                };
                if let Some((address, peer, binding)) = attempt {
                    let next = host.clone();
                    let result = host
                        .owned(
                            async move { next.connect_inner(address, peer, Some(binding)).await },
                        )
                        .await;
                    delay = if result.is_ok() {
                        Duration::from_secs(1)
                    } else {
                        (delay * 2).min(Duration::from_secs(30))
                    };
                } else {
                    let recovered = host.recover_services().await;
                    delay = if recovered {
                        Duration::from_secs(1)
                    } else {
                        (delay * 2).min(Duration::from_secs(30))
                    };
                }
            }
        }));
    }

    async fn initialize_archive(&self, receiver: &ReceiverSession) -> Result<(), HostError> {
        let binding = receiver.archive_identity().binding;
        let limits = ArchiveLimits::default();
        let budget = self.catalog()?.budget(limits.max_bytes)?;
        Ok(receiver
            .start_archive_with_budget(
                self.0
                    .archive_root
                    .join(binding.session.to_string())
                    .join(binding.epoch.to_string()),
                limits,
                budget,
            )
            .await?)
    }

    async fn recover_services(&self) -> bool {
        let pending = {
            let active = self.0.active.read().await;
            let Some(active) = active.as_ref() else {
                return true;
            };
            if !active.reconnect || active.receiver.connection() != ConnectionState::Connected {
                return true;
            }
            (
                active.receiver.archive_identity().binding,
                active.speech_revision,
                active.raw_audio.clone(),
                (!active.receiver.transcription_status().active)
                    .then(|| active.speech.clone())
                    .flatten(),
                active.receiver.archive_status().is_none(),
            )
        };
        let (binding, speech_revision, raw_audio, speech, archive_missing) = pending;
        if !archive_missing && speech.is_none() {
            return true;
        }
        let Ok(_operation) = self.0.operation.try_lock() else {
            return true;
        };
        let Ok(receiver) = self.receiver(binding).await else {
            return true;
        };
        let mut outcomes = vec![];
        if archive_missing {
            let archive = self.initialize_archive(&receiver).await;
            outcomes.push((
                RecoveryOperation::Archive,
                archive.err().map(|_| {
                    "history recording is unavailable; live observation continues".to_owned()
                }),
            ));
            if receiver.archive_status().is_some() && !raw_audio.is_empty() {
                let selected = receiver.settings().audio;
                let sources: Vec<_> = raw_audio
                    .into_iter()
                    .filter(|key| selected.contains(key))
                    .collect();
                if !sources.is_empty() {
                    outcomes.push((
                        RecoveryOperation::RawAudio,
                        receiver
                            .configure_raw_audio(sources)
                            .await
                            .err()
                            .map(|error| error.to_string()),
                    ));
                }
            }
        }
        if let Some(speech) = speech {
            let _speech = self.0.speech_operation.lock().await;
            let still_desired = {
                let active = self.0.active.read().await;
                active.as_ref().is_some_and(|active| {
                    active.reconnect
                        && active.speech_revision == speech_revision
                        && active.speech.is_some()
                })
            };
            if !still_desired {
                return true;
            }
            outcomes.push((
                RecoveryOperation::Transcription,
                start_speech(&receiver, &speech)
                    .await
                    .err()
                    .map(|error| error.to_string()),
            ));
        }
        let succeeded = outcomes.iter().all(|(_, error)| error.is_none());
        let mut active = self.0.active.write().await;
        if let Some(active) = active.as_mut()
            && active.receiver.archive_identity().binding == binding
            && active.speech_revision == speech_revision
            && active.reconnect
        {
            for (operation, error) in outcomes {
                active.recovery.retain(|issue| issue.operation != operation);
                if let Some(error) = error {
                    active.recovery.push(issue(operation, None, &error));
                }
            }
        } else {
            drop(active);
            receiver.stop_transcription().await;
        }
        succeeded
    }
}

pub(super) fn issue(
    operation: RecoveryOperation,
    source: Option<SourceKey>,
    message: &str,
) -> RecoveryIssue {
    RecoveryIssue {
        operation,
        source,
        message: message.chars().take(256).collect(),
    }
}

pub(super) async fn start_speech(
    receiver: &ReceiverSession,
    intent: &SpeechIntent,
) -> Result<(), RuntimeError> {
    receiver
        .start_transcription(
            intent.model.clone(),
            intent.threads,
            intent.sources.clone(),
            liazo_transcription::streaming::StreamingSettings::default(),
        )
        .await
}

fn restore_settings(
    mut settings: CaptureSettings,
    catalog: &liazo_core::catalog::SourceSnapshot,
) -> (CaptureSettings, Vec<RecoveryIssue>) {
    let mut issues = vec![];
    let mut keep = |key: &SourceKey, image: Option<&liazo_core::protocol::ImageSubscription>| {
        let source = catalog.sources.iter().find(|source| {
            source.key == *key
                && source.availability == SourceAvailability::Available
                && image.is_some()
                    == matches!(source.kind, SourceKind::Display | SourceKind::Window)
        });
        let message = match (source, image.and_then(|image| image.crop)) {
            (Some(source), Some(crop))
                if crop
                    .validate_within(
                        source.pixel_width.unwrap_or(0),
                        source.pixel_height.unwrap_or(0),
                    )
                    .is_err() =>
            {
                Some("saved crop does not fit the source; select its capture area again")
            }
            (Some(_), _) => None,
            _ => Some("selected source is unavailable or changed; select it again"),
        };
        if let Some(message) = message {
            issues.push(issue(
                RecoveryOperation::Capture,
                Some(key.clone()),
                message,
            ));
            false
        } else {
            true
        }
    };
    settings
        .images
        .retain(|image| keep(&image.source, Some(image)));
    settings.audio.retain(|source| keep(source, None));
    (settings, issues)
}
