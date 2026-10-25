use crate::{
    SessionSummary,
    http::{HistoryResponse, ObservationResponse},
    requests,
};
use liazo_core::catalog::SourceSnapshot;
use liazo_runtime::archive::TranscriptPage;
use schemars::JsonSchema;

#[allow(dead_code)]
#[derive(JsonSchema)]
struct DesktopApi {
    session: Option<SessionSummary>,
    discovery: Vec<liazo_transport::discovery::DiscoveredProvider>,
    pair_request: requests::Pair,
    forget_request: requests::Forget,
    models: Vec<crate::models::ModelStatus>,
    model_install_request: requests::ModelInstall,
    sources: SourceSnapshot,
    observation: ObservationResponse,
    history: HistoryResponse,
    archives: liazo_runtime::archive::CatalogPage,
    archive_sources: liazo_runtime::archive::ArchiveSources,
    archive_images: crate::http::ArchiveImagesResponse,
    archives_request: requests::Archives,
    archive_sources_request: requests::ArchiveSources,
    archive_images_request: requests::ArchiveImages,
    archive_range_request: requests::ArchiveRange,
    transcripts: TranscriptPage,
    audio_history: crate::http::AudioHistoryResponse,
    raw_audio_request: requests::RawAudio,
    observe_request: requests::Observe,
    connect_request: requests::Connect,
    configure_request: requests::Configure,
    capture_request: requests::Capture,
    history_request: requests::History,
    transcripts_request: requests::Transcripts,
    transcribe_request: requests::Transcribe,
    limits_request: requests::Limits,
    scope: requests::Scope,
}

pub fn schema() -> serde_json::Value {
    schemars::generate::SchemaSettings::draft07()
        .for_serialize()
        .into_generator()
        .into_root_schema_for::<DesktopApi>()
        .to_value()
}
