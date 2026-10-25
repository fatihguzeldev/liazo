use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use liazo_core::{
    protocol::{ImageFormat, SourceKey},
    transcript::{EndReason, Transcript},
    wire::ImageMeta,
};
use liazo_host::{
    SessionHost,
    http::{Endpoint, serve},
};
use liazo_runtime::archive::{
    ArchiveIdentity, ArchiveIndex, ArchiveLimits, AudioPacket, AudioSegment, ReceiverTimeRange,
};
use liazo_transport::{Identity, SessionBinding};
use reqwest::{Client, StatusCode};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn source(id: &str) -> SourceKey {
    SourceKey {
        id: id.into(),
        generation: Uuid::from_u128(1),
    }
}
fn write_epoch(root: &std::path::Path, identity: ArchiveIdentity, image: &'static [u8]) {
    let mut archive = ArchiveIndex::open(
        &root
            .join(identity.binding.session.to_string())
            .join(identity.binding.epoch.to_string()),
        identity,
        ArchiveLimits::default(),
    )
    .unwrap();
    archive.begin().unwrap();
    archive
        .record_image(
            ImageMeta {
                source: source("display:retained"),
                sequence: 1,
                configuration_revision: 1,
                capture_started_ns: 1,
                captured_ns: 2,
                width: 1,
                height: 1,
                format: ImageFormat::Png,
                fingerprint: [7; 32],
            },
            Uuid::new_v4(),
            Bytes::from_static(image),
            100_000_000,
            1_000_000_000,
        )
        .unwrap();
    let stream = Uuid::new_v4();
    let audio = source("audio:retained");
    archive
        .record_audio_policy(std::slice::from_ref(&audio), 100_000_000)
        .unwrap();
    let range = ReceiverTimeRange {
        earliest_ns: 100_000_000,
        latest_ns: 110_000_000,
    };
    archive
        .record_audio(
            AudioSegment {
                id: Uuid::new_v4(),
                source: audio.clone(),
                stream_id: stream,
                sample_rate: 16000,
                channels: 1,
                packets: vec![AudioPacket {
                    sequence: 1,
                    sample_position: 0,
                    frames: 160,
                    captured_ns: 5_000_000,
                    received_ns: 110_000_000,
                    receiver_range: Some(range),
                    discontinuity: true,
                }],
            },
            &[0; 640],
        )
        .unwrap();
    archive
        .record_transcript(
            Transcript {
                id: Uuid::new_v4(),
                source: audio,
                stream_id: stream,
                revision: 1,
                start_ns: 5_000_000,
                end_ns: 15_000_000,
                text: "retained speech".into(),
                ending: Some(EndReason::Silence),
            },
            120_000_000,
            Some(range),
        )
        .unwrap();
    archive.finish(200_000_000).unwrap();
}
async fn post(
    client: &Client,
    base: &str,
    token: &str,
    path: &str,
    args: Value,
) -> reqwest::Response {
    client
        .post(format!("{base}/v1/{path}"))
        .bearer_auth(token)
        .json(&args)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn http_and_mcp_read_closed_epochs_without_an_active_connection() {
    let root = tempfile::tempdir().unwrap();
    let provider = Identity::generate().unwrap();
    let session = Uuid::new_v4();
    let identity = |epoch| ArchiveIdentity {
        provider: provider.fingerprint(),
        binding: SessionBinding { session, epoch },
        wall_anchor_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
    };
    let first = identity(Uuid::new_v4());
    let second = identity(Uuid::new_v4());
    write_epoch(root.path(), first, b"old epoch image");
    write_epoch(root.path(), second, b"new epoch image");
    let host = SessionHost::new(
        Identity::generate().unwrap(),
        vec![],
        root.path().to_owned(),
    );
    assert!(host.current().await.is_none());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Endpoint::new(listener.local_addr().unwrap()).unwrap();
    let base = format!("http://{}", endpoint.address);
    let token = &endpoint.token;
    let cancel = CancellationToken::new();
    let _guard = cancel.clone().drop_guard();
    let running = tokio::spawn(serve(
        listener,
        host.clone(),
        endpoint.clone(),
        cancel.clone(),
    ));
    let client = liazo_host::http::client_builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let page: Value = post(&client, &base, token, "archives", json!({}))
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["entries"].as_array().unwrap().len(), 2);
    assert_eq!(page["unavailable"], 0);
    let sources: Value = post(
        &client,
        &base,
        token,
        "archive-sources",
        json!({"binding":first.binding}),
    )
    .await
    .error_for_status()
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(sources["entries"].as_array().unwrap().len(), 2);
    let args = json!({"binding":first.binding,"at_ms":150});
    let image: Value = post(&client, &base, token, "archive-images", args.clone())
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(image["history"]["binding"], json!(first.binding));
    assert_eq!(image["history"]["at_ns"], 150_000_000);
    let reference = &image["media"][0];
    let bytes = client
        .get(format!(
            "{base}/v1/media/{}",
            reference["id"].as_str().unwrap()
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"old epoch image");
    let range = json!({"binding":first.binding,"from_ms":90,"to_ms":130});
    let text: Value = post(&client, &base, token, "archive-transcripts", range.clone())
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(text["entries"][0]["transcript"]["text"], "retained speech");
    let audio: Value = post(&client, &base, token, "archive-audio", range.clone())
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(audio["audio"]["clips"].as_array().unwrap().len(), 1);
    assert_eq!(audio["media"][0]["reference"]["content_type"], "audio/wav");
    assert_eq!(
        post(
            &client,
            &base,
            token,
            "archive-audio",
            json!({"binding":first.binding,"from_ms":200,"to_ms":100})
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(
            &client,
            &base,
            token,
            "archive-images",
            json!({"binding":{"session":session,"epoch":Uuid::new_v4()},"at_ms":100})
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    let transport = StreamableHttpClientTransport::with_client(
        client,
        StreamableHttpClientTransportConfig::with_uri(format!("{base}/mcp"))
            .auth_header(token.clone()),
    );
    let mcp = ClientConfig::default().serve(transport).await.unwrap();
    let tools = mcp.list_tools(None).await.unwrap();
    for name in [
        "archives",
        "archive_sources",
        "archive_images",
        "archive_transcripts",
        "archive_audio",
    ] {
        assert!(tools.tools.iter().any(|tool| tool.name == name));
    }
    let image = mcp
        .call_tool(
            CallToolRequestParams::new("archive_images")
                .with_arguments(args.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(image.is_error, Some(false));
    let data = image
        .content
        .iter()
        .find_map(|content| content.as_image())
        .unwrap();
    assert_eq!(STANDARD.decode(&data.data).unwrap(), b"old epoch image");
    let wave = mcp
        .call_tool(
            CallToolRequestParams::new("archive_audio")
                .with_arguments(range.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_eq!(wave.is_error, Some(false));
    let data = wave
        .content
        .iter()
        .find_map(|content| content.as_audio())
        .unwrap();
    assert_eq!(&STANDARD.decode(&data.data).unwrap()[..4], b"RIFF");
    assert!(host.current().await.is_none());
    mcp.cancel().await.unwrap();
    cancel.cancel();
    running.await.unwrap().unwrap();
}
