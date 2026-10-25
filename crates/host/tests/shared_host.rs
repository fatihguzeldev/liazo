use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use liazo_core::{
    catalog::SourceSnapshot,
    control::{Failure, FailureCode},
    protocol::{
        AudioMeta, CaptureSettings, ImageFormat, ImageSubscription, Source, SourceAvailability,
        SourceKey, SourceKind,
    },
    wire::{Frame, Header, ImageMeta},
};
use liazo_host::{
    SessionHost,
    http::{Endpoint, serve},
};
use liazo_runtime::{
    backend::{AudioTrack, CaptureBackend},
    provider,
};
use liazo_transport::{Identity, PeerServer, PeerSession, SessionServer, StoredPeer};
use reqwest::StatusCode;
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn source() -> SourceKey {
    SourceKey {
        id: "display:fixture".into(),
        generation: Uuid::from_u128(1),
    }
}
fn audio_source() -> SourceKey {
    SourceKey {
        id: "audio:fixture".into(),
        generation: Uuid::from_u128(2),
    }
}
struct Audio {
    clock: Instant,
    sequence: u64,
    stream: Uuid,
}
#[async_trait]
impl AudioTrack for Audio {
    async fn next(&mut self) -> Result<Option<Frame>, Failure> {
        tokio::time::sleep(Duration::from_millis(10)).await;
        self.sequence += 1;
        Ok(Some(
            Frame::new(
                Header::Audio(AudioMeta {
                    source: audio_source(),
                    stream_id: self.stream,
                    sequence: self.sequence,
                    sample_position: (self.sequence - 1) * 160,
                    sample_rate: 16000,
                    channels: 1,
                    frames: 160,
                    captured_ns: self.clock.elapsed().as_nanos() as u64,
                    discontinuity: self.sequence == 1,
                }),
                Bytes::from(vec![0; 640]),
            )
            .unwrap(),
        ))
    }
    async fn stop(self: Box<Self>) {}
}
struct Capture {
    clock: Instant,
    calls: AtomicUsize,
}
#[async_trait]
impl CaptureBackend for Capture {
    fn platform(&self) -> &'static str {
        "fixture"
    }
    fn clock_ns(&self) -> u64 {
        self.clock.elapsed().as_nanos() as u64
    }
    async fn discover(&self) -> Result<SourceSnapshot, Failure> {
        Ok(SourceSnapshot {
            revision: 1,
            sources: vec![
                Source {
                    key: source(),
                    label: "fixture".into(),
                    kind: SourceKind::Display,
                    availability: SourceAvailability::Available,
                    pixel_width: Some(1),
                    pixel_height: Some(1),
                    scale: Some(1.),
                    process_id: None,
                },
                Source {
                    key: audio_source(),
                    label: "fixture audio".into(),
                    kind: SourceKind::SystemAudio,
                    availability: SourceAvailability::Available,
                    pixel_width: None,
                    pixel_height: None,
                    scale: None,
                    process_id: None,
                },
            ],
            issues: vec![],
        })
    }
    async fn image(
        &self,
        settings: &ImageSubscription,
        previous: Option<[u8; 32]>,
        sequence: u64,
        revision: u64,
    ) -> Result<Frame, Failure> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let meta = ImageMeta {
            source: settings.source.clone(),
            sequence,
            configuration_revision: revision,
            capture_started_ns: self.clock_ns(),
            captured_ns: self.clock_ns(),
            width: 1,
            height: 1,
            format: ImageFormat::Png,
            fingerprint: [7; 32],
        };
        Ok(if previous.is_some() {
            Frame::new(Header::Unchanged(meta), Bytes::new()).unwrap()
        } else {
            Frame::new(Header::Image(meta), Bytes::from_static(b"fixture-image")).unwrap()
        })
    }
    async fn audio(
        &self,
        source: &SourceKey,
        stream: Uuid,
    ) -> Result<Box<dyn AudioTrack>, Failure> {
        if *source != audio_source() {
            return Err(Failure::new(
                FailureCode::Unavailable,
                "unknown audio source",
            ));
        }
        Ok(Box::new(Audio {
            clock: self.clock,
            sequence: 0,
            stream,
        }))
    }
}

#[tokio::test]
async fn authenticated_consumers_share_one_session_and_binary_media_without_a_gui() {
    let provider_identity = Identity::generate().unwrap();
    let receiver_identity = Identity::generate().unwrap();
    let peer = StoredPeer {
        label: "fixture".into(),
        certificate: provider_identity.certificate().clone(),
    };
    let server = SessionServer::new(
        PeerServer::new(
            &provider_identity,
            &[receiver_identity.certificate().clone()],
        )
        .unwrap(),
    );
    let provider_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_address = provider_listener.local_addr().unwrap();
    let capture = Arc::new(Capture {
        clock: Instant::now(),
        calls: AtomicUsize::new(0),
    });
    let fixture = capture.clone();
    let provider = tokio::spawn(async move {
        let mut lanes = Vec::new();
        let mut binding = None;
        for _ in 0..3 {
            let (socket, _) = provider_listener.accept().await.unwrap();
            let lane = server.accept(socket).await.unwrap();
            binding = Some(lane.binding);
            lanes.push(lane.connection);
        }
        let audio = lanes.pop().unwrap();
        let images = lanes.pop().unwrap();
        let control = lanes.pop().unwrap();
        provider::serve(
            fixture,
            PeerSession {
                binding: binding.unwrap(),
                control,
                images,
                audio,
            },
            CancellationToken::new(),
        )
        .await
    });
    let root = tempfile::tempdir().unwrap();
    let host = SessionHost::new(
        receiver_identity,
        vec![peer.clone()],
        root.path().to_owned(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Endpoint::new(listener.local_addr().unwrap()).unwrap();
    let base = format!("http://{}", endpoint.address);
    let cancel = CancellationToken::new();
    let _guard = cancel.clone().drop_guard();
    let running = tokio::spawn(serve(
        listener,
        host.clone(),
        endpoint.clone(),
        cancel.clone(),
    ));
    let client = liazo_host::http::client_builder()
        .timeout(Duration::from_secs(4))
        .build()
        .unwrap();
    assert_eq!(
        client
            .get(format!("{base}/v1/status"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let token = &endpoint.token;
    assert_eq!(
        client
            .get(format!("{base}/v1/status"))
            .bearer_auth(token)
            .header("origin", "https://example.invalid")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .get(format!("{base}/v1/status"))
            .bearer_auth(token)
            .header("host", "rebind.invalid")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/connect"))
            .bearer_auth(token)
            .body(vec![b' '; 65_537])
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let args = json!({"address":provider_address,"peer":peer.fingerprint().to_string()});
    let connected: Value = client
        .post(format!("{base}/v1/connect"))
        .bearer_auth(token)
        .json(&args)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let second: Value = client
        .post(format!("{base}/v1/connect"))
        .bearer_auth(token)
        .json(&args)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(connected["binding"], second["binding"]);
    let binding = connected["binding"].clone();
    let settings = CaptureSettings {
        images: vec![ImageSubscription {
            source: source(),
            interval_ms: 60_000,
            max_width: None,
            crop: None,
            format: ImageFormat::Png,
            quality: 90,
            include_cursor: false,
        }],
        audio: vec![],
    };
    client
        .post(format!("{base}/v1/configure"))
        .bearer_auth(token)
        .json(&json!({"binding":binding,"settings":settings}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let request = json!({"binding":binding,"current":true,"wait_ms":100});
    let first = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let observed: Value = client
                .post(format!("{base}/v1/observe"))
                .bearer_auth(token)
                .json(&request)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            if !observed["media"].as_array().unwrap().is_empty() {
                break observed;
            }
        }
    })
    .await
    .unwrap();
    let second: Value = client
        .post(format!("{base}/v1/observe"))
        .bearer_auth(token)
        .json(&request)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        first["observation"]["cursor"],
        second["observation"]["cursor"]
    );
    assert_eq!(capture.calls.load(Ordering::Relaxed), 1);
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("{base}/mcp")).auth_header(token),
    );
    let mcp = ClientConfig::default().serve(transport).await.unwrap();
    let tools = mcp.list_tools(None).await.unwrap();
    assert!(tools.tools.iter().any(|tool| tool.name == "capture_now"));
    assert!(
        tools
            .tools
            .iter()
            .any(|tool| tool.name == "observe" && tool.input_schema.contains_key("$defs"))
    );
    let call = CallToolRequestParams::new("observe").with_arguments(
        json!({"binding":binding,"current":true})
            .as_object()
            .unwrap()
            .clone(),
    );
    let observed = mcp.call_tool(call).await.unwrap();
    assert_eq!(observed.is_error, Some(false));
    let image = observed
        .content
        .iter()
        .find_map(|content| content.as_image())
        .expect("native MCP image content");
    assert_eq!(STANDARD.decode(&image.data).unwrap(), b"fixture-image");
    assert_eq!(
        observed.structured_content.as_ref().unwrap()["observation"]["binding"],
        binding
    );
    assert_eq!(capture.calls.load(Ordering::Relaxed), 1);
    let id = first["media"][0]["id"].as_str().unwrap();
    let after = observed.structured_content.as_ref().unwrap()["observation"]["cursor"]
        .as_u64()
        .unwrap();
    let quiet = mcp
        .call_tool(
            CallToolRequestParams::new("observe").with_arguments(
                json!({"binding":binding,"after":after,"wait_ms":180})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert!(
        quiet
            .content
            .iter()
            .all(|content| content.as_image().is_none())
    );
    let fresh = mcp
        .call_tool(
            CallToolRequestParams::new("capture_now").with_arguments(
                json!({"binding":binding,"sources":[source()]})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(fresh.is_error, Some(false));
    assert!(
        fresh
            .content
            .iter()
            .any(|content| content.as_image().is_some())
    );
    let retained = mcp
        .call_tool(
            CallToolRequestParams::new("history").with_arguments(
                json!({"binding":binding,"age_ms":0,"sources":[source()]})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert!(
        retained
            .content
            .iter()
            .any(|content| content.as_image().is_some())
    );
    assert_eq!(
        client
            .get(format!("{base}/v1/media/{id}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let image = client
        .get(format!("{base}/v1/media/{id}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(image.headers()["cache-control"], "no-store");
    assert_eq!(image.bytes().await.unwrap().as_ref(), b"fixture-image");
    std::fs::write(
        root.path().join("host.json"),
        serde_json::to_vec(&endpoint).unwrap(),
    )
    .unwrap();
    let attached = liazo_host::launcher::Launcher {
        executable: root.path().join("does-not-exist"),
        profile: "fixture".into(),
        root: root.path().to_owned(),
    }
    .ensure()
    .await
    .unwrap();
    assert_eq!(
        attached.address, endpoint.address,
        "an existing host is reused without starting a process"
    );
    let proxy = liazo_host::proxy::McpProxy::connect(&attached)
        .await
        .unwrap();
    let (proxy_io, consumer_io) = tokio::io::duplex(64 * 1024);
    let proxy_server = tokio::spawn(async move {
        proxy
            .serve(proxy_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap()
    });
    let consumer = ClientConfig::default().serve(consumer_io).await.unwrap();
    let through_proxy = consumer
        .call_tool(
            CallToolRequestParams::new("observe").with_arguments(
                json!({"binding":binding,"current":true})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert!(
        through_proxy
            .content
            .iter()
            .any(|content| content.as_image().is_some())
    );
    consumer.cancel().await.unwrap();
    proxy_server.await.unwrap();
    assert_eq!(
        host.current().await.unwrap().connection,
        liazo_runtime::ConnectionState::Connected,
        "closing an MCP consumer leaves shared capture running"
    );
    let scope: liazo_transport::SessionBinding = serde_json::from_value(binding.clone()).unwrap();
    let held = host.receiver(scope).await.unwrap();
    let mut settings = held.settings();
    settings.audio = vec![audio_source()];
    held.configure(settings).await.unwrap();
    let raw = mcp
        .call_tool(
            CallToolRequestParams::new("raw_audio").with_arguments(
                json!({"binding":binding,"sources":[audio_source()]})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(raw.is_error, Some(false));
    // Accumulate a real-time fixture interval before requesting its most recent 100 ms.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let audio = mcp
        .call_tool(
            CallToolRequestParams::new("audio_history").with_arguments(
                json!({"binding":binding,"older_ms":100,"sources":[audio_source()]})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(audio.is_error, Some(false));
    let data = audio
        .content
        .iter()
        .find_map(|content| content.as_audio())
        .expect("native MCP audio content");
    assert_eq!(data.mime_type, "audio/wav");
    assert_eq!(&STANDARD.decode(&data.data).unwrap()[..4], b"RIFF");
    let audio: Value = client
        .post(format!("{base}/v1/audio-history"))
        .bearer_auth(token)
        .json(&json!({"binding":binding,"older_ms":100,"sources":[audio_source()]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let reference = &audio["media"][0]["reference"];
    assert_eq!(reference["content_type"], "audio/wav");
    let wave = client
        .get(format!(
            "{base}/v1/media/{}",
            reference["id"].as_str().unwrap()
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(wave.headers()["content-type"], "audio/wav");
    assert_eq!(&wave.bytes().await.unwrap()[..4], b"RIFF");
    held.configure_raw_audio(vec![]).await.unwrap();
    let cursor = held.observe(0, false, &[], Duration::ZERO).await.cursor;
    let mut waiters = Vec::new();
    for _ in 0..8 {
        let (client, base, token, binding) =
            (client.clone(), base.clone(), token.clone(), binding.clone());
        waiters.push(tokio::spawn(async move {
            client
                .post(format!("{base}/v1/observe"))
                .bearer_auth(token)
                .json(&json!({"binding":binding,"after":cursor,"wait_ms":5000}))
                .send()
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if client
                .get(format!("{base}/v1/status"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status()
                == StatusCode::TOO_MANY_REQUESTS
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let stopped = client
        .post(format!("{base}/v1/transcription-stop"))
        .bearer_auth(token)
        .json(&json!({"binding":binding}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        stopped.status(),
        StatusCode::OK,
        "control stays available while observation slots are full"
    );
    drop(stopped);
    host.replace_peers(vec![]).await.unwrap();
    for waiter in waiters {
        let _ = waiter.await.unwrap();
    }
    assert!(host.current().await.is_none());
    assert_eq!(
        held.observe(0, false, &[], Duration::ZERO).await.connection,
        liazo_runtime::ConnectionState::Disconnected
    );
    assert!(held.configure(CaptureSettings::default()).await.is_err());
    let retained = host
        .archive_sources(liazo_host::requests::ArchiveSources {
            binding: held.archive_identity().binding,
            after: None,
        })
        .await
        .unwrap();
    assert!(retained.entries.iter().any(|entry| entry.key == source()));
    mcp.cancel().await.unwrap();
    cancel.cancel();
    running.await.unwrap().unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), provider)
        .await
        .unwrap()
        .unwrap();
}
