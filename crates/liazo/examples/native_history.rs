#[cfg(target_os = "macos")]
#[path = "../src/macos.rs"]
mod macos;

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("native_history requires macos");
    std::process::exit(1);
}

#[cfg(target_os = "macos")]
fn main() {
    let worker = std::thread::spawn(|| {
        struct MainLoop;
        impl Drop for MainLoop {
            fn drop(&mut self) {
                liazo_macos::stop_main_loop();
            }
        }
        let _main_loop = MainLoop;
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()?
            .block_on(run::execute())
    });
    liazo_macos::run_main_loop();
    match worker.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("native history acceptance: {error}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("native history acceptance worker failed");
            std::process::exit(1);
        }
    }
}

#[cfg(target_os = "macos")]
mod run {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use liazo_core::{
        protocol::{
            CaptureSettings, ImageFormat, ImageSubscription, SourceAvailability, SourceKey,
            SourceKind,
        },
        wire::ImageMeta,
    };
    use liazo_host::{
        SessionHost,
        http::{Endpoint, serve},
        requests,
    };
    use liazo_runtime::{
        ConnectionState,
        archive::{ArchiveLimits, ArchivePhase},
        backend::CaptureBackend,
        history::{HistoricalImage, HistoryLimits, HistoryState},
        provider,
    };
    use liazo_transport::{
        Identity, PeerServer, PeerSession, SessionBinding, SessionServer, StoredPeer,
    };
    use rmcp::{
        ServiceExt,
        model::{CallToolRequestParams, ClientConfig},
        transport::{
            StreamableHttpClientTransport,
            streamable_http_client::StreamableHttpClientTransportConfig,
        },
    };
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use std::{
        error::Error,
        io::Read,
        path::Path,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use tokio::{net::TcpListener, time::timeout};
    use tokio_util::sync::CancellationToken;

    type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
    struct Proof {
        source: SourceKey,
        metadata: ImageMeta,
        bytes: Vec<u8>,
        at_ms: u64,
    }
    fn check(value: bool, message: &'static str) -> Result<()> {
        if value { Ok(()) } else { Err(message.into()) }
    }
    fn digest_file(path: &Path) -> Result<String> {
        let mut file = std::fs::File::open(path)?;
        let mut hash = Sha256::new();
        let mut chunk = [0; 64 * 1024];
        loop {
            let count = file.read(&mut chunk)?;
            if count == 0 {
                break;
            }
            hash.update(&chunk[..count]);
        }
        Ok(format!("{:x}", hash.finalize()))
    }
    fn rss_kib() -> Option<u64> {
        std::process::Command::new("/bin/ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok()?.trim().parse().ok())
    }
    fn git(args: &[&str]) -> Option<String> {
        std::process::Command::new("git")
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }
    fn args(binding: SessionBinding, proof: &Proof) -> requests::ArchiveImages {
        requests::ArchiveImages {
            binding,
            at_ms: proof.at_ms,
            sources: vec![proof.source.clone()],
        }
    }
    fn json_args(binding: SessionBinding, proof: &Proof) -> Value {
        json!({"binding":binding,"at_ms":proof.at_ms,"sources":[proof.source.clone()]})
    }
    fn retained(image: &HistoricalImage, proof: &Proof) -> Result<()> {
        check(image.source == proof.source, "historical source changed")?;
        check(
            matches!(image.state, HistoryState::Available | HistoryState::Sampled),
            "historical image is not retained",
        )?;
        check(
            image
                .last_capture
                .as_ref()
                .is_some_and(|metadata| metadata.fingerprint == proof.metadata.fingerprint),
            "historical native fingerprint changed",
        )?;
        check(
            image
                .payload
                .as_ref()
                .is_some_and(|bytes| bytes.as_ref() == proof.bytes),
            "historical encoded bytes changed",
        )
    }
    async fn http_retained(
        client: &reqwest::Client,
        endpoint: &Endpoint,
        binding: SessionBinding,
        proof: &Proof,
    ) -> Result<()> {
        let base = format!("http://{}", endpoint.address);
        let result: Value = client
            .post(format!("{base}/v1/archive-images"))
            .bearer_auth(&endpoint.token)
            .json(&json_args(binding, proof))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        check(
            result["history"]["binding"] == json!(binding),
            "http history changed epoch",
        )?;
        check(
            result["history"]["at_ns"] == json!(proof.at_ms * 1_000_000),
            "http history changed requested time",
        )?;
        check(
            result["history"]["images"][0]["last_capture"]["fingerprint"]
                == json!(proof.metadata.fingerprint),
            "http history native fingerprint changed",
        )?;
        let id = result["media"][0]["id"]
            .as_str()
            .ok_or("http history omitted image bytes")?
            .parse::<uuid::Uuid>()?;
        let media_url = format!("{base}/v1/media/{id}");
        let response = client
            .get(&media_url)
            .bearer_auth(&endpoint.token)
            .send()
            .await?
            .error_for_status()?;
        check(
            response
                .headers()
                .get("content-type")
                .is_some_and(|mime| mime == "image/png"),
            "http image type changed",
        )?;
        let bytes = response.bytes().await?;
        check(
            bytes.as_ref() == proof.bytes,
            "http historical image bytes changed",
        )?;
        client
            .delete(media_url)
            .bearer_auth(&endpoint.token)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
    async fn mcp_retained(
        client: &rmcp::Peer<rmcp::RoleClient>,
        binding: SessionBinding,
        proof: &Proof,
    ) -> Result<()> {
        let result = client
            .call_tool(
                CallToolRequestParams::new("archive_images").with_arguments(
                    json_args(binding, proof)
                        .as_object()
                        .ok_or("invalid history arguments")?
                        .clone(),
                ),
            )
            .await?;
        check(result.is_error != Some(true), "mcp history failed")?;
        let image = result
            .content
            .iter()
            .find_map(|content| content.as_image())
            .ok_or("mcp history omitted native image content")?;
        check(image.mime_type == "image/png", "mcp image type changed")?;
        check(
            STANDARD.decode(&image.data)? == proof.bytes,
            "mcp historical image bytes changed",
        )
    }
    async fn all_retained(
        host: &SessionHost,
        http: &reqwest::Client,
        endpoint: &Endpoint,
        mcp: &rmcp::Peer<rmcp::RoleClient>,
        binding: SessionBinding,
        proof: &Proof,
    ) -> Result<()> {
        let result = host.archive_images(args(binding, proof)).await?;
        check(
            result.binding == binding && result.at_ns == proof.at_ms * 1_000_000,
            "host history changed epoch or requested time",
        )?;
        retained(
            result.images.first().ok_or("host history omitted source")?,
            proof,
        )?;
        http_retained(http, endpoint, binding, proof).await?;
        mcp_retained(mcp, binding, proof).await
    }

    pub async fn execute() -> Result<()> {
        let seconds = std::env::args()
            .nth(1)
            .map(|value| value.parse::<u64>())
            .transpose()?
            .unwrap_or(780);
        check(
            (65..=900).contains(&seconds),
            "duration must be 65..900 seconds; full acceptance requires at least 725",
        )?;
        let binary_sha256 = digest_file(&std::env::current_exe()?)?;
        let source_sha256 = format!("{:x}", Sha256::digest(include_bytes!("native_history.rs")));
        let adapter_sha256 = format!("{:x}", Sha256::digest(include_bytes!("../src/macos.rs")));
        let root = tempfile::Builder::new()
            .prefix("liazo-native-history-")
            .tempdir()?;
        let result = session(root.path(), seconds, json!({
            "binary_sha256":binary_sha256,"example_sha256":source_sha256,"adapter_sha256":adapter_sha256,
            "git_head":git(&["rev-parse", "HEAD"]),"source_dirty":git(&["status", "--porcelain", "--untracked-files=normal"]).map(|status| !status.is_empty()),
            "started_unix_ms":SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        })).await;
        // Media remain in this private, task-owned temporary directory only for the run.
        root.close()?;
        println!(
            "{}",
            json!({"event":"temporary_archive_removed","success":result.is_ok()})
        );
        result
    }

    async fn session(root: &Path, seconds: u64, build: Value) -> Result<()> {
        let cancel = CancellationToken::new();
        let _cancel = cancel.clone().drop_guard();
        let capture = Arc::new(super::macos::MacCapture::default());
        let native = timeout(Duration::from_secs(10), capture.discover()).await??;
        let source = native
            .sources
            .iter()
            .find(|source| {
                source.kind == SourceKind::Display
                    && source.availability == SourceAvailability::Available
            })
            .ok_or("no permitted native display is available; no permission request was made")?
            .key
            .clone();
        let provider_identity = Identity::generate()?;
        let receiver_identity = Identity::generate()?;
        let peer = StoredPeer {
            label: "native acceptance".into(),
            certificate: provider_identity.certificate().clone(),
        };
        let server = SessionServer::new(PeerServer::new(
            &provider_identity,
            &[receiver_identity.certificate().clone()],
        )?);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let provider_cancel = cancel.child_token();
        let provider = tokio::spawn(async move {
            let mut lanes = vec![];
            let mut binding = None;
            for _ in 0..3 {
                let (socket, _) = listener.accept().await?;
                let lane = server.accept(socket).await?;
                if binding.is_some_and(|binding| binding != lane.binding) {
                    return Err("provider lane epoch mismatch".into());
                }
                binding = Some(lane.binding);
                lanes.push(lane.connection);
            }
            let audio = lanes.pop().ok_or("missing audio lane")?;
            let images = lanes.pop().ok_or("missing image lane")?;
            let control = lanes.pop().ok_or("missing control lane")?;
            provider::serve(
                capture,
                PeerSession {
                    binding: binding.ok_or("missing epoch")?,
                    control,
                    images,
                    audio,
                },
                provider_cancel,
            )
            .await?;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
        let host = SessionHost::new(receiver_identity, vec![peer.clone()], root.to_owned());
        let current = host.connect(address, peer.fingerprint()).await?;
        let binding = current.binding;
        let receiver = host.receiver(binding).await?;
        receiver.configure_history(HistoryLimits::default())?;
        host.configure(
            binding,
            CaptureSettings {
                images: vec![ImageSubscription {
                    source: source.clone(),
                    interval_ms: 1000,
                    max_width: Some(1200),
                    crop: None,
                    format: ImageFormat::Png,
                    quality: 100,
                    include_cursor: false,
                }],
                audio: vec![],
            },
        )
        .await?;
        host.capture(requests::Capture {
            binding,
            sources: vec![source.clone()],
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let at_ms = receiver.elapsed().as_millis() as u64;
        let initial = host
            .archive_images(requests::ArchiveImages {
                binding,
                at_ms,
                sources: vec![source.clone()],
            })
            .await?;
        let image = initial
            .images
            .first()
            .ok_or("initial native image is missing")?;
        let proof = Proof {
            source,
            metadata: image
                .last_capture
                .clone()
                .ok_or("initial image metadata is missing")?,
            bytes: image
                .payload
                .as_ref()
                .ok_or("initial native image bytes are missing")?
                .to_vec(),
            at_ms,
        };
        check(
            proof.bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
            "initial capture is not an actual PNG",
        )?;
        receiver.flush_archive().await?;
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = Endpoint::new(http_listener.local_addr()?)?;
        let http_server = tokio::spawn(serve(
            http_listener,
            host.clone(),
            endpoint.clone(),
            cancel.child_token(),
        ));
        let http = liazo_host::http::client_builder()
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()?;
        let transport = StreamableHttpClientTransport::with_client(
            http.clone(),
            StreamableHttpClientTransportConfig::with_uri(format!(
                "http://{}/mcp",
                endpoint.address
            ))
            .auth_header(endpoint.token.clone()),
        );
        let mcp = ClientConfig::default().serve(transport).await?;
        all_retained(&host, &http, &endpoint, &mcp, binding, &proof).await?;
        println!(
            "{}",
            json!({"event":"native_history_started","build":build,"binding":binding,"duration_seconds":seconds,
            "available_displays":native.sources.iter().filter(|source|source.kind==SourceKind::Display && source.availability==SourceAvailability::Available).count(),
            "three_tls_lanes":true,"audio_subscriptions":0,"width":proof.metadata.width,"height":proof.metadata.height,
            "initial_encoded_bytes":proof.bytes.len(),"ram_limits":HistoryLimits::default(),"archive_limits":ArchiveLimits::default()})
        );
        let point = Duration::from_millis(proof.at_ms);
        let mut ram_checkpoint = false;
        let mut twelve_minute_checkpoint = false;
        let mut reported = 0;
        let mut peak_rss = rss_kib().unwrap_or(0);
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => return Err("native history acceptance interrupted".into()),
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
            }
            let age = receiver
                .elapsed()
                .checked_sub(point)
                .ok_or("native history clock reversed")?;
            let observation = receiver
                .observe(
                    0,
                    false,
                    std::slice::from_ref(&proof.source),
                    Duration::ZERO,
                )
                .await;
            check(
                observation.connection == ConnectionState::Connected,
                "native provider disconnected",
            )?;
            check(
                observation.failures.is_empty(),
                "native screenshot source failed",
            )?;
            let archive = receiver
                .archive_status()
                .ok_or("native archive disappeared")?;
            check(
                archive.phase == ArchivePhase::Recording,
                "native archive is not recording",
            )?;
            check(
                archive.dropped_records == 0 && archive.queued_bytes <= 64 * 1024 * 1024,
                "native archive dropped or exceeded its queue bound",
            )?;
            let ram = receiver.history_stats();
            check(
                ram.accounted_bytes <= HistoryLimits::default().max_bytes,
                "native RAM history exceeded its byte bound",
            )?;
            if let Some(stats) = archive.stats {
                check(
                    stats.accounted_bytes <= ArchiveLimits::default().max_bytes,
                    "native archive exceeded its byte bound",
                )?;
            }
            if !ram_checkpoint && age >= Duration::from_secs(65) {
                all_retained(&host, &http, &endpoint, &mcp, binding, &proof).await?;
                ram_checkpoint = true;
                println!(
                    "{}",
                    json!({"event":"past_ram_window","age_seconds":age.as_secs_f64(),"host_http_mcp_byte_match":true})
                );
            }
            if !twelve_minute_checkpoint && age >= Duration::from_secs(725) {
                all_retained(&host, &http, &endpoint, &mcp, binding, &proof).await?;
                twelve_minute_checkpoint = true;
                println!(
                    "{}",
                    json!({"event":"twelve_minute_retained","age_seconds":age.as_secs_f64(),"host_http_mcp_byte_match":true})
                );
            }
            if age.as_secs() / 60 > reported {
                reported = age.as_secs() / 60;
                peak_rss = peak_rss.max(rss_kib().unwrap_or(0));
                println!(
                    "{}",
                    json!({"event":"native_history_progress","age_seconds":age.as_secs_f64(),"last_native_sequence":observation.images.first().map(|image|image.metadata.sequence),
                    "ram":ram,"archive":archive,"sampled_peak_rss_kib":peak_rss})
                );
            }
            if age >= Duration::from_secs(seconds) {
                break;
            }
        }
        host.stop(binding).await?;
        receiver.retire_archive().await?;
        let age = receiver
            .elapsed()
            .checked_sub(point)
            .ok_or("native history clock reversed")?;
        let ram_only = receiver
            .history_at(age, std::slice::from_ref(&proof.source))
            .await?;
        check(
            ram_only.retention_floor_ns > proof.at_ms * 1_000_000,
            "original point is still inside the RAM history window",
        )?;
        check(
            ram_only.images.first().is_some_and(|image| {
                image.state == HistoryState::Expired && image.payload.is_none()
            }),
            "original point remains available from RAM",
        )?;
        all_retained(&host, &http, &endpoint, &mcp, binding, &proof).await?;
        let archives = host.archives(0).await?;
        let closed = archives
            .entries
            .iter()
            .find(|archive| archive.binding == binding)
            .ok_or("closed native epoch is not indexed")?;
        check(
            closed.closed_at_ns.is_some() && !closed.writer_active,
            "native archive was not sealed",
        )?;
        println!(
            "{}",
            json!({"event":"native_history_complete","binding":binding,"age_seconds":age.as_secs_f64(),"twelve_minute_verified":twelve_minute_checkpoint,
            "ram_proven_expired_without_archive":true,"closed_indexed_host_http_mcp_byte_match":true,"archive_accounted_bytes":closed.accounted_bytes,
            "initial_encoded_bytes":proof.bytes.len(),"sampled_peak_rss_kib":peak_rss,"audio_or_stt_tested":false,"windows_or_six_hour_acceptance":false})
        );
        mcp.cancel().await?;
        cancel.cancel();
        timeout(Duration::from_secs(5), provider).await???;
        timeout(Duration::from_secs(5), http_server).await???;
        drop(receiver);
        drop(host);
        Ok(())
    }
}
