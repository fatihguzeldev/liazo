use async_trait::async_trait;
use bytes::Bytes;
use liazo_core::{
    catalog::SourceSnapshot,
    control::{Failure, FailureCode},
    protocol::{
        CaptureSettings, ImageFormat, ImageSubscription, Source, SourceAvailability, SourceKey,
        SourceKind,
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
use reqwest::{Client, StatusCode};
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
            sources: vec![Source {
                key: source(),
                label: "fixture".into(),
                kind: SourceKind::Display,
                availability: SourceAvailability::Available,
                pixel_width: Some(1),
                pixel_height: Some(1),
                scale: Some(1.),
                process_id: None,
            }],
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
    async fn audio(&self, _: &SourceKey, _: Uuid) -> Result<Box<dyn AudioTrack>, Failure> {
        Err(Failure::new(
            FailureCode::Unavailable,
            "fixture has no audio",
        ))
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
    let client = Client::builder()
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
    let id = first["media"][0]["id"].as_str().unwrap();
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
    let scope: liazo_transport::SessionBinding = serde_json::from_value(binding.clone()).unwrap();
    let held = host.receiver(scope).await.unwrap();
    host.replace_peers(vec![]).await.unwrap();
    assert!(host.current().await.is_none());
    assert_eq!(
        held.observe(0, false, &[], Duration::ZERO).await.connection,
        liazo_runtime::ConnectionState::Disconnected
    );
    assert!(held.configure(CaptureSettings::default()).await.is_err());
    cancel.cancel();
    running.await.unwrap().unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), provider)
        .await
        .unwrap();
}
