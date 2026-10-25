use crate::{
    HostError, SessionHost,
    requests::{ArchiveImages, ArchiveRange, ArchiveSources},
};
use liazo_runtime::{
    archive::{ArchiveSource, AudioPage, CatalogPage, TranscriptPage},
    history::HistoricalImage,
    receiver::ReceiverSession,
};
use liazo_transport::SessionBinding;
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize, schemars::JsonSchema)]
pub struct ArchivedImages {
    pub binding: SessionBinding,
    pub at_ns: u64,
    pub images: Vec<HistoricalImage>,
    pub selection_truncated: bool,
    pub media_omitted: usize,
}

impl SessionHost {
    pub async fn archives(&self, offset: usize) -> Result<CatalogPage, HostError> {
        Ok(self.catalog()?.list(offset).await?)
    }
    async fn active_archive(&self, binding: SessionBinding) -> Option<ReceiverSession> {
        let receiver = self.receiver(binding).await.ok()?;
        if receiver.archive_status().is_none()
            && receiver.connection() != liazo_runtime::ConnectionState::Connected
        {
            return None;
        }
        Some(receiver)
    }
    pub async fn archive_sources(
        &self,
        args: ArchiveSources,
    ) -> Result<liazo_runtime::archive::ArchiveSources, HostError> {
        if let Some(after) = &args.after {
            after.validate().map_err(|_| HostError::Invalid)?;
        }
        if let Some(receiver) = self.active_archive(args.binding).await {
            use liazo_core::protocol::SourceKind;
            let sources: Vec<_> = receiver
                .sources()
                .sources
                .into_iter()
                .map(|source| ArchiveSource {
                    key: source.key,
                    image: matches!(source.kind, SourceKind::Display | SourceKind::Window),
                    audio: !matches!(source.kind, SourceKind::Display | SourceKind::Window),
                })
                .collect();
            let retained = if receiver.archive_status().is_some() {
                receiver.retained_sources(args.after.clone()).await?
            } else {
                liazo_runtime::archive::ArchiveSources {
                    entries: vec![],
                    truncated: false,
                    next_after: None,
                }
            };
            return Ok(merge_sources(sources, retained, args.after.as_ref()));
        }
        Ok(self.catalog()?.sources(args.binding, args.after).await?)
    }
    pub async fn archive_images(&self, args: ArchiveImages) -> Result<ArchivedImages, HostError> {
        super::application::validate_sources(&args.sources)?;
        let at_ns = args
            .at_ms
            .checked_mul(1_000_000)
            .filter(|at| *at <= i64::MAX as u64)
            .ok_or(HostError::Invalid)?;
        if let Some(receiver) = self.active_archive(args.binding).await {
            let age = receiver
                .elapsed()
                .checked_sub(Duration::from_nanos(at_ns))
                .ok_or(HostError::Invalid)?;
            let mut snapshot = receiver.history_at(age, &args.sources).await?;
            let mut budget = 32 * 1024 * 1024;
            let mut omitted = 0;
            for image in &mut snapshot.images {
                if let Some(payload) = &image.payload {
                    if payload.len() > budget {
                        image.payload = None;
                        omitted += 1;
                    } else {
                        budget -= payload.len();
                    }
                }
            }
            return Ok(ArchivedImages {
                binding: args.binding,
                at_ns,
                images: snapshot.images,
                selection_truncated: snapshot.selection_truncated,
                media_omitted: omitted,
            });
        }
        let mut selected = args.sources;
        let mut truncated = false;
        if selected.is_empty() {
            let page = self.catalog()?.sources(args.binding, None).await?;
            selected = page
                .entries
                .into_iter()
                .filter(|source| source.image)
                .map(|source| source.key)
                .collect();
            truncated = page.truncated || selected.len() > 8;
            selected.truncate(8);
        }
        let mut images = vec![];
        let mut budget = 32 * 1024 * 1024;
        let mut omitted = 0;
        for source in selected {
            let mut image = self.catalog()?.image(args.binding, source, at_ns).await?;
            if let Some(payload) = &image.payload {
                if payload.len() > budget {
                    image.payload = None;
                    omitted += 1;
                } else {
                    budget -= payload.len();
                }
            }
            images.push(image);
        }
        Ok(ArchivedImages {
            binding: args.binding,
            at_ns,
            images,
            selection_truncated: truncated,
            media_omitted: omitted,
        })
    }
    pub async fn archive_transcripts(
        &self,
        args: ArchiveRange,
    ) -> Result<TranscriptPage, HostError> {
        let (first, last) = range(&args)?;
        if let Some(receiver) = self.active_archive(args.binding).await {
            let now = receiver.elapsed();
            return Ok(receiver
                .history_transcripts(
                    now.checked_sub(first).ok_or(HostError::Invalid)?,
                    now.checked_sub(last).ok_or(HostError::Invalid)?,
                    &args.sources,
                )
                .await?);
        }
        Ok(self
            .catalog()?
            .transcripts(
                args.binding,
                first.as_nanos() as u64,
                last.as_nanos() as u64,
                args.sources,
            )
            .await?)
    }
    pub async fn archive_audio(&self, args: ArchiveRange) -> Result<AudioPage, HostError> {
        let (first, last) = range(&args)?;
        if let Some(receiver) = self.active_archive(args.binding).await {
            let now = receiver.elapsed();
            return Ok(receiver
                .history_audio(
                    now.checked_sub(first).ok_or(HostError::Invalid)?,
                    now.checked_sub(last).ok_or(HostError::Invalid)?,
                    &args.sources,
                )
                .await?);
        }
        Ok(self
            .catalog()?
            .audio(
                args.binding,
                first.as_nanos() as u64,
                last.as_nanos() as u64,
                args.sources,
            )
            .await?)
    }
}
fn range(args: &ArchiveRange) -> Result<(Duration, Duration), HostError> {
    super::application::validate_sources(&args.sources)?;
    if args.from_ms > args.to_ms || args.to_ms > i64::MAX as u64 / 1_000_000 {
        return Err(HostError::Invalid);
    }
    Ok((
        Duration::from_millis(args.from_ms),
        Duration::from_millis(args.to_ms),
    ))
}

fn merge_sources(
    mut current: Vec<ArchiveSource>,
    retained: liazo_runtime::archive::ArchiveSources,
    after: Option<&liazo_core::protocol::SourceKey>,
) -> liazo_runtime::archive::ArchiveSources {
    current.extend(retained.entries);
    if let Some(after) = after {
        current.retain(|source| {
            (&source.key.id, source.key.generation) > (&after.id, after.generation)
        });
    }
    current.sort_by(|a, b| (&a.key.id, a.key.generation).cmp(&(&b.key.id, b.key.generation)));
    current.dedup_by(|later, first| {
        if later.key != first.key {
            return false;
        }
        first.image |= later.image;
        first.audio |= later.audio;
        true
    });
    let truncated = retained.truncated || current.len() > 256;
    current.truncate(256);
    let next_after = if truncated {
        current.last().map(|source| source.key.clone())
    } else {
        None
    };
    liazo_runtime::archive::ArchiveSources {
        entries: current,
        truncated,
        next_after,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liazo_core::protocol::SourceKey;
    use liazo_runtime::archive::ArchiveSources;
    use uuid::Uuid;

    fn source(id: &str, generation: u128) -> ArchiveSource {
        ArchiveSource {
            key: SourceKey {
                id: id.into(),
                generation: Uuid::from_u128(generation),
            },
            image: true,
            audio: false,
        }
    }
    #[test]
    fn an_active_catalog_keeps_retained_generations_and_combines_source_kinds() {
        let mut retained = source("display:closed-window", 1);
        retained.audio = true;
        retained.image = false;
        let page = merge_sources(
            vec![
                source("display:closed-window", 2),
                source("display:closed-window", 1),
            ],
            ArchiveSources {
                entries: vec![retained],
                truncated: false,
                next_after: None,
            },
            None,
        );
        assert_eq!(page.entries.len(), 2);
        assert!(page.entries[0].image && page.entries[0].audio);
        assert_eq!(page.entries[0].key.generation, Uuid::from_u128(1));
        assert_eq!(page.entries[1].key.generation, Uuid::from_u128(2));
    }
    #[test]
    fn interleaved_live_and_retained_pages_do_not_skip_or_repeat_sources() {
        let live: Vec<_> = (0..350)
            .map(|n| source(&format!("display:{:04}", n * 2), 1))
            .collect();
        let retained: Vec<_> = (0..350)
            .map(|n| source(&format!("display:{:04}", n * 2 + 1), 1))
            .collect();
        let mut after: Option<SourceKey> = None;
        let mut all = vec![];
        loop {
            let candidates: Vec<_> = retained
                .iter()
                .filter(|entry| {
                    after.as_ref().is_none_or(|key| {
                        (&entry.key.id, entry.key.generation) > (&key.id, key.generation)
                    })
                })
                .cloned()
                .collect();
            let page = merge_sources(
                live.clone(),
                ArchiveSources {
                    entries: candidates.iter().take(256).cloned().collect(),
                    truncated: candidates.len() > 256,
                    next_after: None,
                },
                after.as_ref(),
            );
            after = page.next_after;
            all.extend(page.entries);
            if !page.truncated {
                break;
            }
            assert!(after.is_some());
            assert!(all.len() <= 700);
        }
        assert_eq!(all.len(), 700);
        for (number, entry) in all.iter().enumerate() {
            assert_eq!(entry.key.id, format!("display:{number:04}"));
        }
    }
}
