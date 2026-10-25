use crate::requests::*;
use crate::{
    HostError, SessionHost,
    media::{Media, MediaRef},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Request, State},
    http::{Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use liazo_runtime::{
    Observation,
    history::{HistoryLimits, HistorySnapshot},
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MAX_REQUEST: usize = 64 * 1024;

pub fn client_builder() -> reqwest::ClientBuilder {
    static CRYPTO: std::sync::Once = std::sync::Once::new();
    CRYPTO.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    reqwest::Client::builder()
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub version: u16,
    pub address: SocketAddr,
    pub token: String,
}
impl Endpoint {
    pub fn validate(&self) -> std::io::Result<()> {
        if self.version != 1
            || !self.address.ip().is_loopback()
            || self.address.port() == 0
            || self.token.len() != 43
            || !URL_SAFE_NO_PAD
                .decode(&self.token)
                .is_ok_and(|token| token.len() == 32)
        {
            return Err(std::io::Error::other("invalid local host endpoint"));
        }
        Ok(())
    }
    pub fn new(address: SocketAddr) -> std::io::Result<Self> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(std::io::Error::other(
                "local host requires a bound loopback address",
            ));
        }
        let mut token = [0; 32];
        getrandom::fill(&mut token).map_err(std::io::Error::other)?;
        Ok(Self {
            version: 1,
            address,
            token: URL_SAFE_NO_PAD.encode(token),
        })
    }
}

#[derive(Clone)]
struct HttpState {
    host: SessionHost,
    authorization: Arc<Vec<u8>>,
    authority: String,
    media: Media,
    requests: Arc<Semaphore>,
    controls: Arc<Semaphore>,
}

pub async fn serve(
    listener: TcpListener,
    host: SessionHost,
    endpoint: Endpoint,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    endpoint.validate()?;
    if listener.local_addr()? != endpoint.address || !endpoint.address.ip().is_loopback() {
        return Err(std::io::Error::other(
            "local endpoint does not match listener",
        ));
    }
    let state = HttpState {
        host,
        authorization: Arc::new(format!("Bearer {}", endpoint.token).into_bytes()),
        authority: endpoint.address.to_string(),
        media: Media::default(),
        requests: Arc::new(Semaphore::new(8)),
        controls: Arc::new(Semaphore::new(2)),
    };
    let cleanup_media = state.media.clone();
    let cleanup_cancel = cancel.child_token();
    let stopping = cleanup_cancel.clone();
    let cleanup = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! { _ = stopping.cancelled() => break, _ = tick.tick() => cleanup_media.trim() }
        }
    });
    struct Cleanup(tokio::task::JoinHandle<()>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _cleanup = Cleanup(cleanup);
    let router = Router::new()
        .nest_service(
            "/mcp",
            crate::mcp::service(state.host.clone(), cancel.child_token()),
        )
        .route("/v1/status", get(status))
        .route("/v1/peers", get(peers))
        .route("/v1/discover", get(discover))
        .route("/v1/pair", post(pair))
        .route("/v1/forget", post(forget))
        .route("/v1/models", get(models))
        .route("/v1/model-install", post(model_install))
        .route("/v1/model-cancel", post(model_cancel))
        .route("/v1/connect", post(connect))
        .route("/v1/sources", post(sources))
        .route("/v1/observe", post(observe))
        .route("/v1/configure", post(configure))
        .route("/v1/capture", post(capture))
        .route("/v1/history", post(history))
        .route("/v1/archives", post(archives))
        .route("/v1/archive-sources", post(archive_sources))
        .route("/v1/archive-images", post(archive_images))
        .route("/v1/archive-transcripts", post(archive_transcripts))
        .route("/v1/archive-audio", post(archive_audio))
        .route("/v1/transcripts", post(transcripts))
        .route("/v1/raw-audio", post(raw_audio))
        .route("/v1/audio-history", post(audio_history))
        .route("/v1/history-limits", post(history_limits))
        .route("/v1/transcribe", post(transcribe))
        .route("/v1/transcription-stop", post(transcription_stop))
        .route("/v1/stop", post(stop))
        .route("/v1/media/{id}", get(media).delete(release_media))
        .layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state);
    axum::serve(listener, router)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await
}

async fn authorize(State(state): State<HttpState>, request: Request, next: Next) -> Response {
    if request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        != Some(state.authority.as_str())
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if request
        .headers()
        .get(header::ORIGIN)
        .is_some_and(|origin| origin.as_bytes() != format!("http://{}", state.authority).as_bytes())
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .map_or(&[][..], |value| value.as_bytes());
    if !bool::from(supplied.ct_eq(state.authorization.as_slice())) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (permit, reserved) = match state.requests.clone().try_acquire_owned() {
        Ok(permit) => (permit, false),
        Err(_) => match state.controls.clone().try_acquire_owned() {
            Ok(permit) => (permit, true),
            Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
        },
    };
    let (parts, body) = request.into_parts();
    let bytes = match tokio::time::timeout(
        Duration::from_secs(5),
        axum::body::to_bytes(body, MAX_REQUEST),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    if reserved {
        let control = parts.method == Method::POST
            && (matches!(
                parts.uri.path(),
                "/v1/stop" | "/v1/transcription-stop" | "/v1/model-cancel"
            ) || (parts.uri.path() == "/mcp"
                && serde_json::from_slice::<serde_json::Value>(&bytes).is_ok_and(|body| {
                    match body["method"].as_str() {
                        Some("notifications/cancelled") => true,
                        Some("tools/call") => matches!(
                            body["params"]["name"].as_str(),
                            Some("stop" | "transcription_stop" | "model_cancel")
                        ),
                        _ => false,
                    }
                })));
        if !control {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
    }
    let response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    let (mut parts, body) = response.into_parts();
    parts
        .headers
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    parts
        .headers
        .insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    let stream = body.into_data_stream().map(move |chunk| {
        let _held = &permit;
        chunk
    });
    Response::from_parts(parts, Body::from_stream(stream))
}

impl IntoResponse for HostError {
    fn into_response(self) -> Response {
        let code = match self {
            Self::NoSession => StatusCode::NOT_FOUND,
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::Archive(liazo_runtime::archive::ArchiveError::NotFound) => StatusCode::NOT_FOUND,
            Self::Archive(liazo_runtime::archive::ArchiveError::Busy) => StatusCode::CONFLICT,
            Self::Archive(liazo_runtime::archive::ArchiveError::InvalidPayload) => {
                StatusCode::BAD_REQUEST
            }
            Self::Unpaired => StatusCode::FORBIDDEN,
            Self::ActiveSession | Self::Interrupted | Self::Busy => StatusCode::CONFLICT,
            _ => StatusCode::UNPROCESSABLE_ENTITY,
        };
        (code, Json(serde_json::json!({"error":self.to_string()}))).into_response()
    }
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct ObservationResponse {
    observation: Observation,
    media: Vec<MediaRef>,
    media_omitted: usize,
}
#[derive(Serialize, schemars::JsonSchema)]
pub struct HistoryResponse {
    history: HistorySnapshot,
    media: Vec<MediaRef>,
    media_omitted: usize,
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct ArchiveImagesResponse {
    pub history: crate::ArchivedImages,
    pub media: Vec<MediaRef>,
}

async fn archives(
    State(state): State<HttpState>,
    Json(args): Json<Archives>,
) -> Result<Json<liazo_runtime::archive::CatalogPage>, HostError> {
    Ok(Json(state.host.archives(args.offset).await?))
}
async fn archive_sources(
    State(state): State<HttpState>,
    Json(args): Json<ArchiveSources>,
) -> Result<Json<liazo_runtime::archive::ArchiveSources>, HostError> {
    Ok(Json(state.host.archive_sources(args).await?))
}
async fn archive_images(
    State(state): State<HttpState>,
    Json(args): Json<ArchiveImages>,
) -> Result<Json<ArchiveImagesResponse>, HostError> {
    let mut history = state.host.archive_images(args).await?;
    let mut media = vec![];
    for image in &mut history.images {
        if let (Some(metadata), Some(bytes)) = (&image.last_capture, image.payload.take()) {
            match state.media.publish(metadata, bytes) {
                Some(reference) => media.push(reference),
                None => history.media_omitted += 1,
            }
        }
    }
    Ok(Json(ArchiveImagesResponse { history, media }))
}
async fn archive_transcripts(
    State(state): State<HttpState>,
    Json(args): Json<ArchiveRange>,
) -> Result<Json<liazo_runtime::archive::TranscriptPage>, HostError> {
    Ok(Json(state.host.archive_transcripts(args).await?))
}
async fn archive_audio(
    State(state): State<HttpState>,
    Json(args): Json<ArchiveRange>,
) -> Result<Json<AudioHistoryResponse>, HostError> {
    Ok(Json(audio_response(
        &state.media,
        state.host.archive_audio(args).await?,
    )))
}

fn image_response(state: &HttpState, mut observation: Observation) -> ObservationResponse {
    let mut media = Vec::new();
    let mut omitted = 0;
    for image in &mut observation.images {
        if let Some(bytes) = image.payload.take() {
            match state.media.publish(&image.metadata, bytes) {
                Some(reference) => media.push(reference),
                None => omitted += 1,
            }
        }
    }
    ObservationResponse {
        observation,
        media,
        media_omitted: omitted,
    }
}
async fn status(State(state): State<HttpState>) -> Json<Option<crate::SessionSummary>> {
    Json(state.host.current().await)
}
async fn peers(State(state): State<HttpState>) -> Json<Vec<(String, String)>> {
    Json(state.host.peers().await)
}
async fn models(
    State(state): State<HttpState>,
) -> Result<Json<Vec<crate::models::ModelStatus>>, HostError> {
    Ok(Json(state.host.models().status().await))
}
async fn model_install(
    State(state): State<HttpState>,
    Json(args): Json<ModelInstall>,
) -> Result<Json<serde_json::Value>, HostError> {
    state.host.models().install(&args.id).await?;
    Ok(Json(serde_json::json!({"accepted":true})))
}
async fn model_cancel(State(state): State<HttpState>) -> Json<serde_json::Value> {
    state.host.models().cancel().await;
    Json(serde_json::json!({"requested":true}))
}
async fn discover(
    State(state): State<HttpState>,
) -> Result<Json<Vec<liazo_transport::discovery::DiscoveredProvider>>, HostError> {
    Ok(Json(state.host.discover().await?))
}
async fn pair(
    State(state): State<HttpState>,
    Json(args): Json<Pair>,
) -> Result<Json<(String, String)>, HostError> {
    Ok(Json(
        state.host.pair(args.code, args.name, args.label).await?,
    ))
}
async fn forget(
    State(state): State<HttpState>,
    Json(args): Json<Forget>,
) -> Result<Json<serde_json::Value>, HostError> {
    Ok(Json(
        serde_json::json!({"removed":state.host.forget(args.peer.parse()?).await?}),
    ))
}
async fn connect(
    State(state): State<HttpState>,
    Json(args): Json<Connect>,
) -> Result<Json<crate::SessionSummary>, HostError> {
    Ok(Json(
        state.host.connect(args.address, args.peer.parse()?).await?,
    ))
}
async fn sources(
    State(state): State<HttpState>,
    Json(args): Json<Scope>,
) -> Result<Json<liazo_core::catalog::SourceSnapshot>, HostError> {
    Ok(Json(state.host.receiver(args.binding).await?.sources()))
}
async fn observe(
    State(state): State<HttpState>,
    Json(args): Json<Observe>,
) -> Result<Json<ObservationResponse>, HostError> {
    Ok(Json(image_response(
        &state,
        state.host.observe(args).await?,
    )))
}
async fn configure(
    State(state): State<HttpState>,
    Json(args): Json<Configure>,
) -> Result<Json<serde_json::Value>, HostError> {
    let revision = state.host.configure(args.binding, args.settings).await?;
    Ok(Json(serde_json::json!({"revision":revision})))
}
async fn capture(
    State(state): State<HttpState>,
    Json(args): Json<Capture>,
) -> Result<Json<ObservationResponse>, HostError> {
    Ok(Json(image_response(
        &state,
        state.host.capture(args).await?,
    )))
}
async fn history(
    State(state): State<HttpState>,
    Json(args): Json<History>,
) -> Result<Json<HistoryResponse>, HostError> {
    let mut history = state
        .host
        .receiver(args.binding)
        .await?
        .history_at(Duration::from_millis(args.age_ms), &args.sources)
        .await?;
    let mut media = Vec::new();
    let mut omitted = 0;
    for image in &mut history.images {
        if let (Some(metadata), Some(bytes)) = (&image.last_capture, image.payload.take()) {
            match state.media.publish(metadata, bytes) {
                Some(reference) => media.push(reference),
                None => omitted += 1,
            }
        }
    }
    Ok(Json(HistoryResponse {
        history,
        media,
        media_omitted: omitted,
    }))
}
async fn transcripts(
    State(state): State<HttpState>,
    Json(args): Json<Transcripts>,
) -> Result<Json<liazo_runtime::archive::TranscriptPage>, HostError> {
    Ok(Json(
        state
            .host
            .receiver(args.binding)
            .await?
            .history_transcripts(
                Duration::from_millis(args.older_ms),
                Duration::from_millis(args.newer_ms),
                &args.sources,
            )
            .await?,
    ))
}
async fn history_limits(
    State(state): State<HttpState>,
    Json(args): Json<Limits>,
) -> Result<Json<HistoryLimits>, HostError> {
    Ok(Json(
        state
            .host
            .receiver(args.binding)
            .await?
            .configure_history(args.limits)?,
    ))
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct AudioMedia {
    pub segment: Uuid,
    pub reference: MediaRef,
}
#[derive(Serialize, schemars::JsonSchema)]
pub struct AudioHistoryResponse {
    pub audio: liazo_runtime::archive::AudioPage,
    pub media: Vec<AudioMedia>,
    pub media_omitted: usize,
}
async fn raw_audio(
    State(state): State<HttpState>,
    Json(args): Json<RawAudio>,
) -> Result<Json<serde_json::Value>, HostError> {
    state
        .host
        .configure_raw_audio(args.binding, args.sources)
        .await?;
    Ok(Json(serde_json::json!({"applied":true})))
}
async fn audio_history(
    State(state): State<HttpState>,
    Json(args): Json<Transcripts>,
) -> Result<Json<AudioHistoryResponse>, HostError> {
    let audio = state
        .host
        .receiver(args.binding)
        .await?
        .history_audio(
            Duration::from_millis(args.older_ms),
            Duration::from_millis(args.newer_ms),
            &args.sources,
        )
        .await?;
    Ok(Json(audio_response(&state.media, audio)))
}
fn audio_response(
    store: &Media,
    mut audio: liazo_runtime::archive::AudioPage,
) -> AudioHistoryResponse {
    let mut media = vec![];
    let mut omitted = 0;
    for clip in &mut audio.clips {
        if let Some(reference) = store.publish_audio(&clip.segment, std::mem::take(&mut clip.wav)) {
            media.push(AudioMedia {
                segment: clip.segment.id,
                reference,
            });
        } else {
            omitted += 1;
        }
    }
    AudioHistoryResponse {
        audio,
        media,
        media_omitted: omitted,
    }
}
async fn transcribe(
    State(state): State<HttpState>,
    Json(args): Json<Transcribe>,
) -> Result<Json<serde_json::Value>, HostError> {
    state
        .host
        .transcribe(args.binding, args.model, args.threads, args.sources)
        .await?;
    Ok(Json(serde_json::json!({"started":true})))
}
async fn transcription_stop(
    State(state): State<HttpState>,
    Json(args): Json<Scope>,
) -> Result<Json<serde_json::Value>, HostError> {
    state.host.stop_transcription(args.binding).await?;
    Ok(Json(serde_json::json!({"stopped":true})))
}
async fn stop(
    State(state): State<HttpState>,
    Json(args): Json<Scope>,
) -> Result<Json<serde_json::Value>, HostError> {
    state.host.stop(args.binding).await?;
    Ok(Json(serde_json::json!({"stopped":true})))
}
async fn media(State(state): State<HttpState>, Path(id): Path<Uuid>) -> Response {
    match state.media.get(id) {
        Some((bytes, mime)) => ([(header::CONTENT_TYPE, mime)], bytes).into_response(),
        None => StatusCode::GONE.into_response(),
    }
}
async fn release_media(State(state): State<HttpState>, Path(id): Path<Uuid>) -> StatusCode {
    state.media.release(id);
    StatusCode::NO_CONTENT
}
