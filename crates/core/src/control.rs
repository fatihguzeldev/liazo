use crate::{
    catalog::SourceIssue,
    protocol::{CaptureSettings, ProtocolError, Source, SourceKey},
};
use serde::{Deserialize, Serialize};

pub const SOURCE_PAGE_SIZE: usize = 16;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub command: Command,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", content = "arguments", rename_all = "snake_case")]
pub enum Command {
    Hello { version: u16 },
    Sources { after: Option<String> },
    Configure(CaptureSettings),
    CaptureNow { sources: Vec<SourceKey> },
    Clock { client_send_ns: u64 },
    Stop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reply {
    pub id: u64,
    pub result: ReplyValue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ReplyValue {
    Hello {
        version: u16,
        platform: String,
        max_images: u8,
        max_audio: u8,
    },
    Sources(SourcePage),
    Applied {
        revision: u64,
        settings: CaptureSettings,
    },
    CaptureQueued {
        revision: u64,
        not_before_ns: u64,
    },
    Clock {
        client_send_ns: u64,
        received_ns: u64,
        sent_ns: u64,
    },
    Stopped,
    Failed(Failure),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourcePage {
    pub revision: u64,
    pub sources: Vec<Source>,
    pub issues: Vec<SourceIssue>,
    pub next_after: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    InvalidRequest,
    UnsupportedVersion,
    PermissionRequired,
    SourceMissing,
    SourceChanged,
    Unavailable,
    Busy,
    Timeout,
    Internal,
}

#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error, schemars::JsonSchema)]
#[error("{message}")]
pub struct Failure {
    pub code: FailureCode,
    pub message: String,
}

impl Failure {
    pub fn new(code: FailureCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum Event {
    CatalogChanged {
        revision: u64,
    },
    SourceStatus {
        source: SourceKey,
        revision: u64,
        observed_ns: u64,
        #[serde(default)]
        capture_started_ns: Option<u64>,
        failure: Option<Failure>,
    },
}

impl Request {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.id == 0 {
            return Err(ProtocolError("request id is required"));
        }
        match &self.command {
            Command::Sources { after } => validate_after(after)?,
            Command::Configure(settings) => settings.validate()?,
            Command::CaptureNow { sources } => {
                if sources.is_empty() || sources.len() > 8 {
                    return Err(ProtocolError("invalid capture selection"));
                }
                let mut ids = std::collections::HashSet::new();
                for source in sources {
                    source.validate()?;
                    if !ids.insert(&source.id) {
                        return Err(ProtocolError("duplicate capture selection"));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl Reply {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.id == 0 {
            return Err(ProtocolError("reply id is required"));
        }
        match &self.result {
            ReplyValue::Hello {
                platform,
                max_images,
                max_audio,
                ..
            } => {
                if platform.is_empty() || platform.len() > 32 || *max_images > 8 || *max_audio > 8 {
                    return Err(ProtocolError("invalid provider capabilities"));
                }
            }
            ReplyValue::Sources(page) => {
                if page.sources.len() > SOURCE_PAGE_SIZE || page.issues.len() > 5 {
                    return Err(ProtocolError("source page is too large"));
                }
                validate_after(&page.next_after)?;
                for source in &page.sources {
                    source.key.validate()?;
                    if source.label.len() > 2048
                        || source
                            .scale
                            .is_some_and(|scale| !scale.is_finite() || scale <= 0.0)
                    {
                        return Err(ProtocolError("invalid source metadata"));
                    }
                }
                for issue in &page.issues {
                    if issue.message.len() > 2048 {
                        return Err(ProtocolError("invalid source issue"));
                    }
                }
            }
            ReplyValue::Applied { settings, .. } => settings.validate()?,
            ReplyValue::Clock {
                received_ns,
                sent_ns,
                ..
            } if received_ns > sent_ns => return Err(ProtocolError("invalid clock response")),
            ReplyValue::Failed(failure) => failure.validate()?,
            _ => {}
        }
        Ok(())
    }
}

impl Event {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if let Self::SourceStatus {
            source,
            failure,
            observed_ns,
            capture_started_ns,
            ..
        } = self
        {
            source.validate()?;
            if capture_started_ns.is_some_and(|started| started > *observed_ns) {
                return Err(ProtocolError("invalid capture failure timing"));
            }
            if let Some(failure) = failure {
                failure.validate()?;
            }
        }
        Ok(())
    }
}

impl Failure {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.message.len() > 2048 {
            return Err(ProtocolError("failure message is too large"));
        }
        Ok(())
    }
}

fn validate_after(after: &Option<String>) -> Result<(), ProtocolError> {
    if after.as_ref().is_some_and(|after| {
        after.is_empty() || after.len() > 256 || after.chars().any(char::is_control)
    }) {
        return Err(ProtocolError("invalid source cursor"));
    }
    Ok(())
}
