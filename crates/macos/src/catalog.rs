use crate::NativeError;
use liazo_core::{
    catalog::{SourceDescriptor, SourceIssue, SourceRegistry, SourceSnapshot},
    protocol::{SourceAvailability, SourceKind},
};
use serde::Deserialize;

#[derive(Deserialize)]
struct RawSource {
    id: String,
    instance: String,
    kind: SourceKind,
    title: String,
    width: Option<u32>,
    height: Option<u32>,
    scale: Option<f64>,
    #[serde(rename = "processID")]
    process_id: Option<u32>,
}

#[derive(Deserialize)]
pub struct NativeDiscovery {
    screens: Vec<RawSource>,
    audio: Vec<RawSource>,
    screen_error: Option<String>,
    screen_allowed: bool,
    microphone_allowed: bool,
}

fn invalid() -> NativeError {
    NativeError::Failure("invalid native source catalog".into())
}

pub async fn discover_sources() -> Result<NativeDiscovery, NativeError> {
    serde_json::from_value(crate::sources().await?).map_err(|_| invalid())
}

impl RawSource {
    fn descriptor(self, microphone_allowed: bool) -> SourceDescriptor {
        SourceDescriptor {
            label: if self.title.is_empty() {
                self.id.clone()
            } else {
                self.title
            },
            id: self.id,
            instance: self.instance,
            kind: self.kind,
            availability: if self.kind == SourceKind::Microphone && !microphone_allowed {
                SourceAvailability::PermissionRequired
            } else {
                SourceAvailability::Available
            },
            pixel_width: self.width,
            pixel_height: self.height,
            scale: self.scale,
            process_id: self.process_id,
        }
    }
}

impl NativeDiscovery {
    // Discovery runs independently; the provider applies its complete result synchronously.
    pub fn apply(self, registry: &mut SourceRegistry) -> Result<SourceSnapshot, NativeError> {
        if self.screens.len() + self.audio.len() > 512
            || self
                .screens
                .iter()
                .any(|source| !matches!(source.kind, SourceKind::Display | SourceKind::Window))
            || self
                .audio
                .iter()
                .any(|source| matches!(source.kind, SourceKind::Display | SourceKind::Window))
        {
            return Err(invalid());
        }
        let has_display = self
            .screens
            .iter()
            .any(|source| source.kind == SourceKind::Display);
        let screen_problem = !self.screen_allowed || self.screen_error.is_some() || !has_display;
        let mut candidate = registry.clone();
        let mut issues = Vec::new();
        let mut groups = [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for source in self.screens.into_iter().chain(self.audio) {
            let index = match source.kind {
                SourceKind::Display => 0,
                SourceKind::Window => 1,
                SourceKind::SystemAudio => 2,
                SourceKind::ProcessAudio => 3,
                SourceKind::Microphone => 4,
            };
            groups[index].push(source.descriptor(self.microphone_allowed));
        }
        for (kind, sources) in [
            SourceKind::Display,
            SourceKind::Window,
            SourceKind::SystemAudio,
            SourceKind::ProcessAudio,
            SourceKind::Microphone,
        ]
        .into_iter()
        .zip(groups)
        {
            if screen_problem && matches!(kind, SourceKind::Display | SourceKind::Window) {
                let availability = if self.screen_allowed {
                    SourceAvailability::Unavailable
                } else {
                    SourceAvailability::PermissionRequired
                };
                candidate
                    .mark_unavailable(kind, availability)
                    .map_err(|_| invalid())?;
                issues.push(SourceIssue {
                    kind,
                    availability,
                    message: self
                        .screen_error
                        .clone()
                        .unwrap_or_else(|| "no display is available".into()),
                });
            } else {
                candidate
                    .replace_kind(kind, sources)
                    .map_err(|_| invalid())?;
            }
        }
        if !self.microphone_allowed {
            issues.push(SourceIssue {
                kind: SourceKind::Microphone,
                availability: SourceAvailability::PermissionRequired,
                message: "microphone permission is required".into(),
            });
        }
        let snapshot = SourceSnapshot {
            revision: candidate.revision(),
            sources: candidate.sources(),
            issues,
        };
        *registry = candidate;
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn catalog(display: bool) -> serde_json::Value {
        json!({"screens": if display { vec![json!({"id":"display:1","instance":"physical-display", "kind":"display", "title":"display", "width":1200,"height":800,"scale":2.0})] } else { vec![] },
            "audio":[{"id":"audio:system","instance":"system","kind":"system_audio","title":"system audio"}],
            "screen_allowed":true,"microphone_allowed":false})
    }

    #[test]
    fn missing_displays_preserve_known_identity_as_unavailable() {
        let mut registry = SourceRegistry::default();
        let discovery: NativeDiscovery = serde_json::from_value(catalog(true)).unwrap();
        let initial = discovery.apply(&mut registry).unwrap();
        let key = initial
            .sources
            .iter()
            .find(|source| source.kind == SourceKind::Display)
            .unwrap()
            .key
            .clone();
        let missing: NativeDiscovery = serde_json::from_value(catalog(false)).unwrap();
        let snapshot = missing.apply(&mut registry).unwrap();
        assert!(
            snapshot.sources.iter().any(|source| source.key == key
                && source.availability == SourceAvailability::Unavailable)
        );
        assert!(registry.resolve(&key).is_err());
        let returned: NativeDiscovery = serde_json::from_value(catalog(true)).unwrap();
        returned.apply(&mut registry).unwrap();
        assert!(registry.resolve(&key).is_ok());
    }

    #[test]
    fn a_late_invalid_audio_record_cannot_partially_publish_new_display_sources() {
        let mut registry = SourceRegistry::default();
        let mut malformed = catalog(true);
        malformed["audio"][0]["instance"] = json!("");
        let discovery: NativeDiscovery = serde_json::from_value(malformed).unwrap();
        assert!(discovery.apply(&mut registry).is_err());
        assert!(registry.sources().is_empty());
    }
}
