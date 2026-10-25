use liazo_core::protocol::{CaptureSettings, SourceKey};
use liazo_runtime::history::HistoryLimits;
use liazo_transport::SessionBinding;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Empty {}

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub binding: SessionBinding,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Connect {
    pub address: SocketAddr,
    pub peer: String,
}

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Pair {
    pub code: String,
    pub name: String,
    pub label: String,
}

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Forget {
    pub peer: String,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Observe {
    pub binding: SessionBinding,
    #[serde(default)]
    pub after: u64,
    #[serde(default)]
    pub current: bool,
    #[serde(default)]
    pub sources: Vec<SourceKey>,
    #[serde(default)]
    pub wait_ms: u64,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Configure {
    pub binding: SessionBinding,
    pub settings: CaptureSettings,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub binding: SessionBinding,
    pub sources: Vec<SourceKey>,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct History {
    pub binding: SessionBinding,
    pub age_ms: u64,
    #[serde(default)]
    pub sources: Vec<SourceKey>,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Transcripts {
    pub binding: SessionBinding,
    pub older_ms: u64,
    #[serde(default)]
    pub newer_ms: u64,
    #[serde(default)]
    pub sources: Vec<SourceKey>,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub binding: SessionBinding,
    pub limits: HistoryLimits,
}
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Transcribe {
    pub binding: SessionBinding,
    pub model: PathBuf,
    pub threads: u8,
    pub sources: Vec<SourceKey>,
}
