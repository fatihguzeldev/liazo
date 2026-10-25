use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_IMAGE_PIXELS: u64 = 16_777_216;
pub const MAX_MEDIA_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_SOURCES_PER_KIND: usize = 8;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ProtocolError(pub &'static str);

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SourceKey {
    pub id: String,
    pub generation: Uuid,
}

impl SourceKey {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.id.is_empty()
            || self.id.len() > 256
            || self.id.chars().any(char::is_control)
            || self.generation.is_nil()
        {
            return Err(ProtocolError("invalid source identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Display,
    Window,
    SystemAudio,
    ProcessAudio,
    Microphone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceAvailability {
    Available,
    PermissionRequired,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Source {
    pub key: SourceKey,
    pub label: String,
    pub kind: SourceKind,
    pub availability: SourceAvailability,
    pub pixel_width: Option<u32>,
    pub pixel_height: Option<u32>,
    pub scale: Option<f64>,
    pub process_id: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImageFormat {
    Png,
    Jpeg,
}

impl ImageFormat {
    pub fn mime_type(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CropPixels {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

impl CropPixels {
    pub fn validate_within(self, width: u32, height: u32) -> Result<(), ProtocolError> {
        if self.width == 0
            || self.height == 0
            || self.x.checked_add(self.width).is_none_or(|x| x > width)
            || self.y.checked_add(self.height).is_none_or(|y| y > height)
        {
            return Err(ProtocolError("crop must fit inside the selected source"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ImageSubscription {
    pub source: SourceKey,
    pub interval_ms: u32,
    pub max_width: Option<u32>,
    pub crop: Option<CropPixels>,
    pub format: ImageFormat,
    pub quality: u8,
    pub include_cursor: bool,
}

impl ImageSubscription {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.source.validate()?;
        if !(50..=60_000).contains(&self.interval_ms)
            || self
                .max_width
                .is_some_and(|width| !(1..=8192).contains(&width))
            || self.quality > 100
        {
            return Err(ProtocolError("invalid image capture settings"));
        }
        if let Some(crop) = self.crop {
            crop.validate_within(u32::MAX, u32::MAX)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CaptureSettings {
    pub images: Vec<ImageSubscription>,
    pub audio: Vec<SourceKey>,
}

impl CaptureSettings {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.images.len() > MAX_SOURCES_PER_KIND || self.audio.len() > MAX_SOURCES_PER_KIND {
            return Err(ProtocolError("too many selected sources"));
        }
        let mut identities = HashSet::new();
        for image in &self.images {
            image.validate()?;
            if !identities.insert(image.source.id.as_str()) {
                return Err(ProtocolError("a source was selected more than once"));
            }
        }
        for source in &self.audio {
            source.validate()?;
            if !identities.insert(source.id.as_str()) {
                return Err(ProtocolError("a source was selected more than once"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AudioMeta {
    pub source: SourceKey,
    pub stream_id: Uuid,
    pub sequence: u64,
    pub captured_ns: u64,
    pub sample_position: u64,
    pub sample_rate: u32,
    pub channels: u8,
    pub frames: u32,
    pub discontinuity: bool,
}

impl AudioMeta {
    pub fn validate_payload(&self, bytes: usize) -> Result<(), ProtocolError> {
        self.source.validate()?;
        if !(8000..=384_000).contains(&self.sample_rate)
            || self.stream_id.is_nil()
            || !(1..=8).contains(&self.channels)
            || !(1..=8192).contains(&self.frames)
            || self
                .sample_position
                .checked_add(u64::from(self.frames))
                .is_none()
            || bytes != self.frames as usize * usize::from(self.channels) * 4
        {
            return Err(ProtocolError("invalid float32 audio payload"));
        }
        Ok(())
    }
}
