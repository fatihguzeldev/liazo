use liazo_core::{
    protocol::{AudioMeta, ImageFormat, SourceKey},
    wire::{Frame, Header, ImageMeta},
};
use serde_json::Value;

use crate::{NativeError, NativePacket};

fn invalid() -> NativeError {
    NativeError::Failure("invalid native media metadata".into())
}

fn number(metadata: &Value, key: &str) -> Result<u64, NativeError> {
    metadata
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(invalid)
}

fn boolean(metadata: &Value, key: &str) -> Result<bool, NativeError> {
    metadata
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(invalid)
}

fn narrow<T: TryFrom<u64>>(value: u64) -> Result<T, NativeError> {
    value.try_into().map_err(|_| invalid())
}

impl NativePacket {
    pub fn into_image_frame(
        self,
        source: SourceKey,
        sequence: u64,
        revision: u64,
    ) -> Result<Frame, NativeError> {
        let format = match self.metadata.get("format").and_then(Value::as_str) {
            Some("png") => ImageFormat::Png,
            Some("jpeg") => ImageFormat::Jpeg,
            _ => return Err(invalid()),
        };
        let hash = self
            .metadata
            .get("fingerprint")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        if hash.len() != 64 || !hash.is_ascii() {
            return Err(invalid());
        }
        let mut fingerprint = [0_u8; 32];
        for (i, byte) in fingerprint.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hash[i * 2..i * 2 + 2], 16).map_err(|_| invalid())?;
        }
        let image = ImageMeta {
            source,
            sequence,
            configuration_revision: revision,
            width: narrow(number(&self.metadata, "width")?)?,
            height: narrow(number(&self.metadata, "height")?)?,
            format,
            fingerprint,
            capture_started_ns: number(&self.metadata, "requested_ns")?,
            captured_ns: number(&self.metadata, "completed_ns")?,
        };
        let header = if boolean(&self.metadata, "unchanged")? {
            Header::Unchanged(image)
        } else {
            Header::Image(image)
        };
        Frame::new(header, self.payload).map_err(|e| NativeError::Failure(e.to_string()))
    }

    pub fn into_audio_frame(self, source: SourceKey) -> Result<Frame, NativeError> {
        let rate = self
            .metadata
            .get("sample_rate")
            .and_then(Value::as_f64)
            .ok_or_else(invalid)?;
        if !rate.is_finite() || rate.fract() != 0.0 || !(8000.0..=384000.0).contains(&rate) {
            return Err(invalid());
        }
        let audio = AudioMeta {
            source,
            sequence: number(&self.metadata, "sequence")?,
            captured_ns: number(&self.metadata, "host_time_ns")?,
            sample_position: number(&self.metadata, "sample_position")?,
            sample_rate: rate as u32,
            channels: narrow(number(&self.metadata, "channels")?)?,
            frames: narrow(number(&self.metadata, "frames")?)?,
            discontinuity: boolean(&self.metadata, "discontinuity")?,
        };
        Frame::new(Header::Audio(audio), self.payload)
            .map_err(|e| NativeError::Failure(e.to_string()))
    }
}
