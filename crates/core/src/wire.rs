use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::control::{Event, Reply, Request};
use crate::protocol::{
    AudioMeta, ImageFormat, MAX_IMAGE_PIXELS, MAX_MEDIA_BYTES, ProtocolError, SourceKey,
};

const MAGIC: &[u8; 4] = b"LZ01";
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("peer message exceeds the size limit")]
    TooLarge,
    #[error("unsupported peer protocol")]
    InvalidMagic,
    #[error(transparent)]
    Invalid(#[from] ProtocolError),
    #[error("invalid peer message: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImageMeta {
    pub source: SourceKey,
    pub sequence: u64,
    pub captured_ns: u64,
    #[serde(default)]
    pub capture_started_ns: u64,
    pub configuration_revision: u64,
    pub width: u32,
    pub height: u32,
    pub format: ImageFormat,
    pub fingerprint: [u8; 32],
}

impl ImageMeta {
    fn validate(&self) -> Result<(), ProtocolError> {
        self.source.validate()?;
        if self.width == 0
            || self.height == 0
            || u64::from(self.width) * u64::from(self.height) > MAX_IMAGE_PIXELS
        {
            return Err(ProtocolError("invalid image dimensions"));
        }
        if self.capture_started_ns > self.captured_ns {
            return Err(ProtocolError("invalid capture time range"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Header {
    Image(ImageMeta),
    Unchanged(ImageMeta),
    Audio(AudioMeta),
    Request(Request),
    Reply(Reply),
    Event(Event),
    Ping { nonce: u64 },
    Pong { nonce: u64 },
}

impl Header {
    fn validate_payload(&self, len: usize) -> Result<(), WireError> {
        if len > MAX_MEDIA_BYTES {
            return Err(WireError::TooLarge);
        }
        match self {
            Self::Image(meta) => {
                meta.validate()?;
                if len == 0 {
                    return Err(ProtocolError("new image has no payload").into());
                }
            }
            Self::Audio(meta) => meta.validate_payload(len)?,
            Self::Unchanged(meta) => {
                meta.validate()?;
                require_empty(len)?;
            }
            Self::Ping { .. } | Self::Pong { .. } => require_empty(len)?,
            Self::Request(request) => {
                request.validate()?;
                require_empty(len)?;
            }
            Self::Reply(reply) => {
                reply.validate()?;
                require_empty(len)?;
            }
            Self::Event(event) => {
                event.validate()?;
                require_empty(len)?;
            }
        }
        Ok(())
    }
}

fn require_empty(len: usize) -> Result<(), ProtocolError> {
    if len != 0 {
        return Err(ProtocolError("unexpected binary payload"));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct Frame {
    pub header: Header,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(header: Header, payload: Bytes) -> Result<Self, WireError> {
        header.validate_payload(payload.len())?;
        Ok(Self { header, payload })
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), WireError> {
        self.header.validate_payload(self.payload.len())?;
        let metadata = serde_json::to_vec(&self.header)?;
        if metadata.is_empty() || metadata.len() > MAX_HEADER_BYTES {
            return Err(WireError::TooLarge);
        }
        let mut prefix = [0_u8; 12];
        prefix[..4].copy_from_slice(MAGIC);
        prefix[4..8].copy_from_slice(&(metadata.len() as u32).to_be_bytes());
        prefix[8..12].copy_from_slice(&(self.payload.len() as u32).to_be_bytes());
        writer.write_all(&prefix).await?;
        writer.write_all(&metadata).await?;
        writer.write_all(&self.payload).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn read_from<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<Option<Self>, WireError> {
        let mut prefix = [0_u8; 12];
        if reader.read(&mut prefix[..1]).await? == 0 {
            return Ok(None);
        }
        reader.read_exact(&mut prefix[1..]).await?;
        if &prefix[..4] != MAGIC {
            return Err(WireError::InvalidMagic);
        }
        let metadata_len = u32::from_be_bytes(prefix[4..8].try_into().unwrap()) as usize;
        let payload_len = u32::from_be_bytes(prefix[8..12].try_into().unwrap()) as usize;
        if metadata_len == 0 || metadata_len > MAX_HEADER_BYTES || payload_len > MAX_MEDIA_BYTES {
            return Err(WireError::TooLarge);
        }
        let mut metadata = vec![0_u8; metadata_len];
        reader.read_exact(&mut metadata).await?;
        let header: Header = serde_json::from_slice(&metadata)?;
        header.validate_payload(payload_len)?;
        let mut payload = vec![0_u8; payload_len];
        reader.read_exact(&mut payload).await?;
        Self::new(header, Bytes::from(payload)).map(Some)
    }
}
