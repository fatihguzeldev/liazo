use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::protocol::{ProtocolError, Source, SourceAvailability, SourceKey, SourceKind};

const MAX_DISCOVERED_SOURCES: usize = 512;

pub struct SourceDescriptor {
    pub id: String,
    // Native lifetime identity, including process birth time for process-owned handles.
    pub instance: String,
    pub label: String,
    pub kind: SourceKind,
    pub availability: SourceAvailability,
    pub pixel_width: Option<u32>,
    pub pixel_height: Option<u32>,
    pub scale: Option<f64>,
    pub process_id: Option<u32>,
}

impl SourceDescriptor {
    fn validate(&self) -> Result<(), ProtocolError> {
        SourceKey {
            id: self.id.clone(),
            generation: Uuid::from_u128(1),
        }
        .validate()?;
        let image = matches!(self.kind, SourceKind::Display | SourceKind::Window);
        if self.instance.is_empty()
            || self.instance.len() > 1024
            || self.label.len() > 2048
            || self.pixel_width.is_some_and(|x| x == 0 || x > 65_536)
            || self.pixel_height.is_some_and(|x| x == 0 || x > 65_536)
            || self.pixel_width.is_some() != self.pixel_height.is_some()
            || self
                .scale
                .is_some_and(|x| !x.is_finite() || x <= 0.0 || x > 16.0)
            || (image
                && self.availability == SourceAvailability::Available
                && (self.pixel_width.is_none() || self.scale.is_none()))
            || (!image && (self.pixel_width.is_some() || self.scale.is_some()))
            || self.process_id == Some(0)
        {
            return Err(ProtocolError("invalid source metadata"));
        }
        Ok(())
    }

    fn into_source(self, generation: Uuid) -> (String, Source) {
        let source = Source {
            key: SourceKey {
                id: self.id,
                generation,
            },
            label: self.label,
            kind: self.kind,
            availability: self.availability,
            pixel_width: self.pixel_width,
            pixel_height: self.pixel_height,
            scale: self.scale,
            process_id: self.process_id,
        };
        (self.instance, source)
    }
}

#[derive(Clone)]
struct Entry {
    instance: String,
    source: Source,
}

#[derive(Clone, Default)]
pub struct SourceRegistry {
    entries: HashMap<String, Entry>,
    revision: u64,
}

pub struct CatalogUpdate {
    pub revision: u64,
    pub removed: Vec<SourceKey>,
    pub changed: Vec<Source>,
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SourceIssue {
    pub kind: SourceKind,
    pub availability: SourceAvailability,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SourceSnapshot {
    pub revision: u64,
    pub sources: Vec<Source>,
    pub issues: Vec<SourceIssue>,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceResolutionError {
    #[error("source is missing")]
    Missing,
    #[error("source belongs to an older generation")]
    StaleGeneration,
    #[error("source permission is required")]
    PermissionRequired,
    #[error("source is unavailable")]
    Unavailable,
}

impl SourceRegistry {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn sources(&self) -> Vec<Source> {
        let mut sources: Vec<_> = self
            .entries
            .values()
            .map(|entry| entry.source.clone())
            .collect();
        sources.sort_by(|a, b| a.key.id.cmp(&b.key.id));
        sources
    }

    pub fn resolve(&self, key: &SourceKey) -> Result<&Source, SourceResolutionError> {
        let source = &self
            .entries
            .get(&key.id)
            .ok_or(SourceResolutionError::Missing)?
            .source;
        if source.key != *key {
            return Err(SourceResolutionError::StaleGeneration);
        }
        match source.availability {
            SourceAvailability::Available => Ok(source),
            SourceAvailability::PermissionRequired => {
                Err(SourceResolutionError::PermissionRequired)
            }
            SourceAvailability::Unavailable => Err(SourceResolutionError::Unavailable),
        }
    }

    pub fn replace_kind(
        &mut self,
        kind: SourceKind,
        descriptors: Vec<SourceDescriptor>,
    ) -> Result<CatalogUpdate, ProtocolError> {
        let next_revision = self.next_revision()?;
        let old_count = self
            .entries
            .values()
            .filter(|entry| entry.source.kind == kind)
            .count();
        if self.entries.len() - old_count + descriptors.len() > MAX_DISCOVERED_SOURCES {
            return Err(ProtocolError("too many discovered sources"));
        }
        let mut ids = HashSet::new();
        for descriptor in &descriptors {
            descriptor.validate()?;
            if descriptor.kind != kind
                || !ids.insert(descriptor.id.as_str())
                || self
                    .entries
                    .get(&descriptor.id)
                    .is_some_and(|entry| entry.source.kind != kind)
            {
                return Err(ProtocolError(
                    "source snapshot contains conflicting identities",
                ));
            }
        }
        let mut removed = Vec::new();
        self.entries.retain(|id, entry| {
            if entry.source.kind == kind && !ids.contains(id.as_str()) {
                removed.push(entry.source.key.clone());
                false
            } else {
                true
            }
        });
        let mut changed = Vec::new();
        for descriptor in descriptors {
            let old = self.entries.get(&descriptor.id);
            let generation =
                if let Some(entry) = old.filter(|entry| entry.instance == descriptor.instance) {
                    entry.source.key.generation
                } else {
                    if let Some(entry) = old {
                        removed.push(entry.source.key.clone());
                    }
                    Uuid::new_v4()
                };
            let (instance, source) = descriptor.into_source(generation);
            if old.is_none_or(|entry| entry.source != source) {
                changed.push(source.clone());
            }
            self.entries
                .insert(source.key.id.clone(), Entry { instance, source });
        }
        if !removed.is_empty() || !changed.is_empty() {
            self.revision = next_revision;
        }
        Ok(CatalogUpdate {
            revision: self.revision,
            removed,
            changed,
        })
    }

    pub fn mark_unavailable(
        &mut self,
        kind: SourceKind,
        availability: SourceAvailability,
    ) -> Result<CatalogUpdate, ProtocolError> {
        if availability == SourceAvailability::Available {
            return Err(ProtocolError(
                "availability must come from a source snapshot",
            ));
        }
        let next_revision = self.next_revision()?;
        let mut changed = Vec::new();
        for entry in self
            .entries
            .values_mut()
            .filter(|entry| entry.source.kind == kind)
        {
            if entry.source.availability != availability {
                entry.source.availability = availability;
                changed.push(entry.source.clone());
            }
        }
        if !changed.is_empty() {
            self.revision = next_revision;
        }
        Ok(CatalogUpdate {
            revision: self.revision,
            removed: Vec::new(),
            changed,
        })
    }

    fn next_revision(&self) -> Result<u64, ProtocolError> {
        self.revision
            .checked_add(1)
            .ok_or(ProtocolError("source revision exhausted"))
    }
}
