use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use liazo_core::{
    catalog::SourceSnapshot,
    control::Failure,
    protocol::{
        AudioMeta, CaptureSettings, ImageFormat, ImageSubscription, Source, SourceAvailability,
        SourceKey, SourceKind,
    },
    wire::{Frame, Header, ImageMeta},
};
use liazo_host::{
    SessionHost,
    http::{Endpoint, serve},
    requests,
};
use liazo_runtime::{
    backend::{AudioTrack, CaptureBackend},
    provider,
};
use liazo_transport::{Identity, PeerServer, PeerSession, SessionServer, StoredPeer};
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    io::{Cursor, Read},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, time::timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn file_sha256(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut block = [0; 64 * 1024];
    loop {
        let bytes = file.read(&mut block)?;
        if bytes == 0 {
            break;
        }
        hash.update(&block[..bytes]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
struct Image {
    bytes: Bytes,
    width: u32,
    height: u32,
    fingerprint: [u8; 32],
}
impl Image {
    fn read(path: &Path) -> Result<Self> {
        let length = std::fs::metadata(path)?.len();
        if length == 0 || length > 32 * 1024 * 1024 {
            return Err("PNG input must be between 1 byte and 32 MiB".into());
        }
        let bytes = std::fs::read(path)?;
        let mut reader = png::Decoder::new(Cursor::new(&bytes)).read_info()?;
        let size = reader
            .output_buffer_size()
            .filter(|size| *size <= 64 * 1024 * 1024)
            .ok_or("decoded PNG exceeds 64 MiB")?;
        let mut pixels = vec![0; size];
        let decoded = reader.next_frame(&mut pixels)?;
        let fingerprint = Sha256::digest(&pixels[..decoded.buffer_size()]).into();
        Ok(Self {
            bytes: bytes.into(),
            width: decoded.width,
            height: decoded.height,
            fingerprint,
        })
    }
}
fn source(audio: bool) -> SourceKey {
    SourceKey {
        id: if audio {
            "audio:benchmark"
        } else {
            "display:benchmark"
        }
        .into(),
        generation: Uuid::from_u128(if audio { 1 } else { 2 }),
    }
}
struct Fixture {
    clock: Instant,
    images: [Image; 2],
    calls: AtomicU64,
    produced_image_bytes: AtomicU64,
    produced_audio_bytes: Arc<AtomicU64>,
}
struct Track {
    start: Instant,
    start_ns: u64,
    sequence: u64,
    stream: Uuid,
    produced: Arc<AtomicU64>,
}
#[async_trait]
impl AudioTrack for Track {
    async fn next(&mut self) -> std::result::Result<Option<Frame>, Failure> {
        self.sequence += 1;
        tokio::time::sleep_until((self.start + Duration::from_millis(self.sequence * 20)).into())
            .await;
        self.produced.fetch_add(960 * 4, Ordering::Relaxed);
        Ok(Some(
            Frame::new(
                Header::Audio(AudioMeta {
                    source: source(true),
                    stream_id: self.stream,
                    sequence: self.sequence,
                    sample_position: (self.sequence - 1) * 960,
                    sample_rate: 48000,
                    channels: 1,
                    frames: 960,
                    captured_ns: self.start_ns + (self.sequence - 1) * 20_000_000,
                    discontinuity: false,
                }),
                Bytes::from_static(&[0; 960 * 4]),
            )
            .unwrap(),
        ))
    }
    async fn stop(self: Box<Self>) {}
}
#[async_trait]
impl CaptureBackend for Fixture {
    fn platform(&self) -> &'static str {
        "preencoded-png-benchmark"
    }
    fn clock_ns(&self) -> u64 {
        self.clock.elapsed().as_nanos() as u64
    }
    async fn discover(&self) -> std::result::Result<SourceSnapshot, Failure> {
        Ok(SourceSnapshot {
            revision: 1,
            issues: vec![],
            sources: [false, true]
                .into_iter()
                .map(|audio| Source {
                    key: source(audio),
                    label: "benchmark".into(),
                    kind: if audio {
                        SourceKind::SystemAudio
                    } else {
                        SourceKind::Display
                    },
                    availability: SourceAvailability::Available,
                    pixel_width: (!audio).then_some(self.images[0].width),
                    pixel_height: (!audio).then_some(self.images[0].height),
                    scale: (!audio).then_some(1.),
                    process_id: None,
                })
                .collect(),
        })
    }
    async fn image(
        &self,
        settings: &ImageSubscription,
        previous: Option<[u8; 32]>,
        sequence: u64,
        revision: u64,
    ) -> std::result::Result<Frame, Failure> {
        let started = self.clock_ns();
        self.calls.fetch_add(1, Ordering::Relaxed);
        let image = &self.images[(sequence as usize) % 2];
        let metadata = ImageMeta {
            source: settings.source.clone(),
            sequence,
            configuration_revision: revision,
            capture_started_ns: started,
            captured_ns: self.clock_ns(),
            width: image.width,
            height: image.height,
            format: ImageFormat::Png,
            fingerprint: image.fingerprint,
        };
        Ok(if previous == Some(image.fingerprint) {
            Frame::new(Header::Unchanged(metadata), Bytes::new()).unwrap()
        } else {
            self.produced_image_bytes
                .fetch_add(image.bytes.len() as u64, Ordering::Relaxed);
            Frame::new(Header::Image(metadata), image.bytes.clone()).unwrap()
        })
    }
    async fn audio(
        &self,
        _: &SourceKey,
        stream: Uuid,
    ) -> std::result::Result<Box<dyn AudioTrack>, Failure> {
        Ok(Box::new(Track {
            start: Instant::now(),
            start_ns: self.clock_ns(),
            sequence: 0,
            stream,
            produced: self.produced_audio_bytes.clone(),
        }))
    }
}
#[derive(Default)]
struct Timings(Vec<f64>);
impl Timings {
    fn push(&mut self, elapsed: Duration) {
        self.0.push(elapsed.as_secs_f64() * 1000.);
    }
    fn json(&self) -> Value {
        if self.0.is_empty() {
            return json!({"samples":0,"p50_ms":null,"p95_ms":null,"maximum_ms":null});
        }
        let mut sorted = self.0.clone();
        sorted.sort_by(f64::total_cmp);
        let percentile =
            |p: f64| sorted[((p * sorted.len() as f64).ceil() as usize).saturating_sub(1)];
        json!({"samples":sorted.len(),"p50_ms":percentile(0.50),"p95_ms":percentile(0.95),"maximum_ms":sorted.last()})
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if !(2..=4).contains(&arguments.len()) {
        return Err(
            "usage: live_path <first.png> <edited.png> [samples=40] [interval-ms=500]".into(),
        );
    }
    let count: usize = arguments
        .get(2)
        .map(|value| value.to_string_lossy().parse())
        .transpose()?
        .unwrap_or(40);
    let interval_ms: u64 = arguments
        .get(3)
        .map(|value| value.to_string_lossy().parse())
        .transpose()?
        .unwrap_or(500);
    if !(20..=300).contains(&count) || !(50..=1000).contains(&interval_ms) {
        return Err("samples must be 20..300 and interval-ms must be 50..1000".into());
    }
    let binary_sha256 = file_sha256(&std::env::current_exe()?)?;
    let git = |args: &[&str]| {
        std::process::Command::new("git")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    let git_head = git(&["rev-parse", "HEAD"]);
    let dirty = git(&["status", "--porcelain", "--untracked-files=normal"])
        .map(|output| !output.is_empty());
    let images = [
        Image::read(Path::new(&arguments[0]))?,
        Image::read(Path::new(&arguments[1]))?,
    ];
    if (images[0].width, images[0].height) != (images[1].width, images[1].height)
        || images[0].fingerprint == images[1].fingerprint
    {
        return Err("inputs must have equal dimensions and different decoded pixels".into());
    }
    let image_lengths = [images[0].bytes.len(), images[1].bytes.len()];
    let dimensions = [images[0].width, images[0].height];
    let fixture = Arc::new(Fixture {
        clock: Instant::now(),
        images,
        calls: AtomicU64::new(0),
        produced_image_bytes: AtomicU64::new(0),
        produced_audio_bytes: Arc::new(AtomicU64::new(0)),
    });
    let provider_identity = Identity::generate()?;
    let receiver_identity = Identity::generate()?;
    let peer = StoredPeer {
        label: "benchmark".into(),
        certificate: provider_identity.certificate().clone(),
    };
    let server = SessionServer::new(PeerServer::new(
        &provider_identity,
        &[receiver_identity.certificate().clone()],
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let provider_address = listener.local_addr()?;
    let capture = fixture.clone();
    let provider = tokio::spawn(async move {
        let mut lanes = Vec::new();
        let mut binding = None;
        for _ in 0..3 {
            let (socket, _) = listener
                .accept()
                .await
                .map_err(liazo_transport::TransportError::from)?;
            let lane = server.accept(socket).await?;
            binding = Some(lane.binding);
            lanes.push(lane.connection);
        }
        let audio = lanes.pop().unwrap();
        let images = lanes.pop().unwrap();
        let control = lanes.pop().unwrap();
        provider::serve(
            capture,
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
    let profile = tempfile::tempdir()?;
    let host = SessionHost::new(
        receiver_identity,
        vec![peer.clone()],
        profile.path().to_owned(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = Endpoint::new(listener.local_addr()?)?;
    let base = format!("http://{}", endpoint.address);
    let cancel = CancellationToken::new();
    let _cleanup = cancel.clone().drop_guard();
    let running = tokio::spawn(serve(
        listener,
        host.clone(),
        endpoint.clone(),
        cancel.clone(),
    ));
    let summary = host.connect(provider_address, peer.fingerprint()).await?;
    let binding = summary.binding;
    let receiver = host.receiver(binding).await?;
    host.configure(
        binding,
        CaptureSettings {
            images: vec![ImageSubscription {
                source: source(false),
                interval_ms: 60_000,
                max_width: None,
                crop: None,
                format: ImageFormat::Png,
                quality: 90,
                include_cursor: false,
            }],
            audio: vec![source(true)],
        },
    )
    .await?;
    host.configure_raw_audio(binding, vec![source(true)])
        .await?;
    let client = liazo_host::http::client_builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?;
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("{base}/mcp"))
            .auth_header(&endpoint.token),
    );
    let mcp = ClientConfig::default().serve(transport).await?;
    for _ in 0..5 {
        host.capture(requests::Capture {
            binding,
            sources: vec![source(false)],
        })
        .await?;
    }
    receiver.flush_archive().await?;
    let before_calls = fixture.calls.load(Ordering::Relaxed);
    let before_images = fixture.produced_image_bytes.load(Ordering::Relaxed);
    let before_audio = fixture.produced_audio_bytes.load(Ordering::Relaxed);
    let before_audio_sequence = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(audio) = receiver
                .observe(0, false, &[], Duration::ZERO)
                .await
                .audio
                .first()
            {
                break audio.sequence;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let mut fresh = Timings::default();
    let mut provider_to_api = Timings::default();
    let mut memory = Timings::default();
    let mut http = Timings::default();
    let mut mcp_time = Timings::default();
    let mut mcp_json_bytes = 0_usize;
    let mut mcp_base64_bytes = 0_usize;
    let mut http_json_bytes = 0_usize;
    let mut http_image_bytes = 0_usize;
    let mut age_min = Timings::default();
    let mut age_max = Timings::default();
    let mut late = 0;
    let mut storage_timeline = Vec::new();
    let started = Instant::now();
    for sample in 0..count {
        let due = started + Duration::from_millis(sample as u64 * interval_ms);
        if Instant::now() > due + Duration::from_millis(interval_ms) {
            late += 1;
        }
        tokio::time::sleep_until(due.into()).await;
        let timer = Instant::now();
        let captured = host
            .capture(requests::Capture {
                binding,
                sources: vec![source(false)],
            })
            .await?;
        fresh.push(timer.elapsed());
        let image = captured
            .images
            .first()
            .ok_or("fresh capture omitted image")?;
        let payload = image
            .payload
            .as_ref()
            .ok_or("fresh capture omitted payload")?;
        provider_to_api.push(Duration::from_nanos(
            fixture
                .clock_ns()
                .checked_sub(image.metadata.captured_ns)
                .ok_or("invalid shared monotonic timestamp")?,
        ));
        if let Some(age) = image.age {
            age_min.push(Duration::from_millis(age.minimum_ms));
            age_max.push(Duration::from_millis(age.maximum_ms));
        }
        let timer = Instant::now();
        let cached = receiver
            .observe(0, true, &[source(false)], Duration::ZERO)
            .await;
        memory.push(timer.elapsed());
        if cached.images[0].metadata.sequence != image.metadata.sequence
            || cached.images[0].payload.as_ref() != Some(payload)
        {
            return Err("warm observation differs from the fresh image".into());
        }
        let timer = Instant::now();
        let metadata = client
            .post(format!("{base}/v1/observe"))
            .bearer_auth(&endpoint.token)
            .json(&requests::Observe {
                binding,
                after: 0,
                current: true,
                sources: vec![source(false)],
                wait_ms: 0,
            })
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let metadata_value: Value = serde_json::from_slice(&metadata)?;
        let media = metadata_value["media"]
            .as_array()
            .and_then(|media| media.first())
            .ok_or("HTTP omitted image")?;
        let id = media["id"].as_str().ok_or("invalid HTTP media id")?;
        let binary = client
            .get(format!("{base}/v1/media/{id}"))
            .bearer_auth(&endpoint.token)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        http.push(timer.elapsed());
        if binary != *payload {
            return Err("HTTP binary bytes differ from decoded-verified PNG input".into());
        }
        http_json_bytes += metadata.len();
        http_image_bytes += binary.len();
        client
            .delete(format!("{base}/v1/media/{id}"))
            .bearer_auth(&endpoint.token)
            .send()
            .await?
            .error_for_status()?;
        let timer = Instant::now();
        let response = mcp
            .call_tool(
                CallToolRequestParams::new("observe").with_arguments(
                    json!({"binding":binding,"current":true,"sources":[source(false)]})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await?;
        mcp_time.push(timer.elapsed());
        if response.is_error == Some(true) {
            return Err("MCP returned an operational error".into());
        }
        let encoded = response
            .content
            .iter()
            .find_map(|content| content.as_image())
            .ok_or("MCP omitted native image content")?;
        if STANDARD.decode(&encoded.data)? != *payload {
            return Err("MCP image bytes differ from verified PNG input".into());
        }
        mcp_base64_bytes += encoded.data.len();
        mcp_json_bytes += serde_json::to_vec(&response)?.len();
        if (sample + 1) % 10 == 0 {
            storage_timeline.push(json!({"after_sample":sample + 1,"elapsed_seconds":started.elapsed().as_secs_f64(),"ram_history":receiver.history_stats(),"archive":receiver.archive_status()}));
        }
    }
    tokio::time::sleep_until((started + Duration::from_millis(count as u64 * interval_ms)).into())
        .await;
    let image_bytes = fixture.produced_image_bytes.load(Ordering::Relaxed) - before_images;
    let audio_bytes = fixture.produced_audio_bytes.load(Ordering::Relaxed) - before_audio;
    let elapsed = started.elapsed();
    let snapshot = receiver.observe(0, false, &[], Duration::ZERO).await;
    let last_audio_sequence = snapshot
        .audio
        .first()
        .map(|audio| audio.sequence)
        .ok_or("live receiver audio disappeared")?;
    if last_audio_sequence <= before_audio_sequence {
        return Err("live receiver audio did not advance during measurement".into());
    }
    let calls = fixture.calls.load(Ordering::Relaxed) - before_calls;
    if calls != count as u64 || snapshot.gaps != 0 {
        return Err("unexpected extra capture or live media discontinuity".into());
    }
    receiver.flush_archive().await?;
    let report = json!({
        "version":1,"profile":if cfg!(debug_assertions) { "debug" } else { "release" },"platform":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"version":env!("CARGO_PKG_VERSION"),"tokio_workers":4},
        "source":{"git_head_at_run":git_head,"checkout_dirty_at_run":dirty,"binary_sha256":binary_sha256,"example_source_sha256":format!("{:x}",Sha256::digest(include_bytes!("live_path.rs"))),"rust_toolchain_pin":include_str!("../../../rust-toolchain.toml")},
        "workload":{"dimensions":dimensions,"png_bytes":image_lengths,"sample_interval_ms":interval_ms,"samples":count,"elapsed_seconds":elapsed.as_secs_f64(),"missed_interval_deadlines":late,"audio":{"sample_rate":48000,"channels":1,"frames_per_packet":960,"kind":"synthetic silence"},"raw_audio_archive":true,"stt":false,"consumers":["RAM observe","authenticated HTTP + binary image","official MCP over HTTP"]},
        "timings":{"fresh_request_to_receiver_api_return":fresh.json(),"provider_completion_to_receiver_api_return":provider_to_api.json(),"warm_ram_observe":memory.json(),"http_observe_and_binary_image":http.json(),"mcp_observe_including_json_and_base64":mcp_time.json(),"mapped_frame_age_lower_bound":age_min.json(),"mapped_frame_age_upper_bound":age_max.json()},
        "application_bytes":{"provider_image_payloads_produced":image_bytes,"provider_pcm_payloads_produced":audio_bytes,"provider_payload_mbit_per_second":((image_bytes+audio_bytes) as f64)*8./elapsed.as_secs_f64()/1_000_000.,"http_metadata_json":http_json_bytes,"http_binary_png":http_image_bytes,"mcp_base64_characters":mcp_base64_bytes,"mcp_result_reserialized_json_excludes_rpc_http":mcp_json_bytes,"peer_tls_tcp_wire_bytes":null},
        "resources":{"ram_history":receiver.history_stats(),"archive":receiver.archive_status(),"first_audio_sequence":before_audio_sequence,"last_audio_sequence":last_audio_sequence,"storage_timeline":storage_timeline},
        "limits":["provider, receiver, host, HTTP and MCP share one process on loopback","provider timing uses the same known monotonic origin; it is not a cross-device clock estimate","input PNGs are decoded and byte-verified after both client paths; text readability and codec quality are not scored","native capture, native encode, LAN, Windows, STT, model upload and TLS/TCP overhead are not measured","CPU/RSS require an external measurement of this built binary; startup, input decode, validation and accounting are included in those process totals","this bounded component run is not six-hour acceptance"]
    });
    host.stop(binding).await?;
    receiver.retire_archive().await?;
    timeout(Duration::from_secs(5), provider).await???;
    mcp.cancel().await?;
    cancel.cancel();
    timeout(Duration::from_secs(5), running).await???;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
