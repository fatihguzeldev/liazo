#[cfg(target_os = "macos")]
mod native_peer {
    use bytes::Bytes;
    use liazo_core::{
        protocol::{ImageFormat, ImageSubscription, SourceKey},
        wire::{Frame, Header},
    };
    use liazo_transport::{Identity, LaneKind, PeerClient, PeerServer, PeerSession, SessionServer};
    use std::{error::Error, sync::mpsc, time::Duration};
    use tokio::net::TcpListener;

    type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
    const AUDIO_CHUNKS: usize = 100;

    pub fn run() -> Result<()> {
        let provider = Identity::generate()?;
        let receiver = Identity::generate()?;
        let server = SessionServer::new(PeerServer::new(
            &provider,
            &[receiver.certificate().clone()],
        )?);
        let client = PeerClient::new(&receiver, provider.certificate())?;
        let (ready, address) = mpsc::channel();
        let source = std::thread::spawn(move || -> Result<()> {
            tokio::runtime::Runtime::new()?
                .block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await?;
                    ready.send(listener.local_addr()?)?;
                    let catalog = liazo_macos::sources().await?;
                    if let Some(error) = catalog["screen_error"].as_str() {
                        return Err(format!("screen sources: {error}").into());
                    }
                    let id = catalog["screens"]
                        .as_array()
                        .and_then(|list| list.iter().find(|source| source["kind"] == "display"))
                        .and_then(|source| source["id"].as_str())
                        .ok_or("no display available")?;
                    let generation = uuid::Uuid::new_v4();
                    let image_key = SourceKey {
                        id: id.into(),
                        generation,
                    };
                    let settings = ImageSubscription {
                        source: image_key.clone(),
                        interval_ms: 500,
                        max_width: Some(800),
                        crop: None,
                        format: ImageFormat::Png,
                        quality: 90,
                        include_cursor: false,
                    };
                    let mut lanes = Vec::new();
                    for expected in [LaneKind::Control, LaneKind::Images, LaneKind::Audio] {
                        let (socket, _) = listener.accept().await?;
                        let lane = server.accept(socket).await?;
                        if lane.kind != expected {
                            return Err("wrong lane".into());
                        }
                        lanes.push(lane.connection);
                    }
                    let mut audio_lane = lanes.pop().ok_or("missing audio lane")?;
                    let mut image_lane = lanes.pop().ok_or("missing image lane")?;
                    let mut control = lanes.pop().ok_or("missing control lane")?;
                    let packet = liazo_macos::screenshot(&settings, None).await?;
                    packet
                        .into_image_frame(image_key, 1, 1)?
                        .write_to(&mut image_lane)
                        .await?;
                    let audio_key = SourceKey {
                        id: "audio:system".into(),
                        generation,
                    };
                    let mut stream = liazo_macos::AudioStream::start(&audio_key.id).await?;
                    tokio::time::timeout(Duration::from_secs(3), async {
                        for _ in 0..AUDIO_CHUNKS {
                            stream
                                .next()
                                .await
                                .ok_or("audio ended")??
                                .into_audio_frame(audio_key.clone())?
                                .write_to(&mut audio_lane)
                                .await?;
                        }
                        Ok::<_, Box<dyn Error + Send + Sync>>(())
                    })
                    .await??;
                    stream.stop().await;
                    let ping = Frame::read_from(&mut control)
                        .await?
                        .ok_or("missing control ping")?;
                    if !matches!(ping.header, Header::Ping { nonce: 42 }) {
                        return Err("wrong ping".into());
                    }
                    Frame::new(Header::Pong { nonce: 42 }, Bytes::new())?
                        .write_to(&mut control)
                        .await?;
                    let completed = Frame::read_from(&mut control)
                        .await?
                        .ok_or("missing receiver completion")?;
                    if !matches!(completed.header, Header::Ping { nonce: 43 }) {
                        return Err("wrong receiver completion".into());
                    }
                    Ok(())
                })
                .inspect_err(|error| eprintln!("provider: {error}"))
        });
        let destination = std::thread::spawn(move || -> Result<()> {
            struct Stop;
            impl Drop for Stop {
                fn drop(&mut self) {
                    liazo_macos::stop_main_loop();
                }
            }
            let _stop = Stop;
            let address = address.recv()?;
            tokio::runtime::Runtime::new()?
                .block_on(async {
                    let mut session =
                        PeerSession::connect(&client, address, uuid::Uuid::new_v4()).await?;
                    let image = Frame::read_from(&mut session.images)
                        .await?
                        .ok_or("missing image")?;
                    let Header::Image(metadata) = image.header else {
                        return Err("wrong image message".into());
                    };
                    if !image
                        .payload
                        .starts_with(&[137, 80, 78, 71, 13, 10, 26, 10])
                    {
                        return Err("invalid png received".into());
                    }
                    println!(
                        "verified tls image: {} bytes, {}×{}",
                        image.payload.len(),
                        metadata.width,
                        metadata.height
                    );
                    let mut chunks = 0;
                    let mut payload = 0;
                    for _ in 0..AUDIO_CHUNKS {
                        let frame = Frame::read_from(&mut session.audio)
                            .await?
                            .ok_or("audio ended")?;
                        if !matches!(frame.header, Header::Audio(_)) {
                            return Err("wrong audio message".into());
                        }
                        chunks += 1;
                        payload += frame.payload.len();
                    }
                    println!("verified tls audio: {chunks} chunks, {payload} bytes");
                    Frame::new(Header::Ping { nonce: 42 }, Bytes::new())?
                        .write_to(&mut session.control)
                        .await?;
                    let pong = Frame::read_from(&mut session.control)
                        .await?
                        .ok_or("missing pong")?;
                    if !matches!(pong.header, Header::Pong { nonce: 42 }) {
                        return Err("wrong pong".into());
                    }
                    Frame::new(Header::Ping { nonce: 43 }, Bytes::new())?
                        .write_to(&mut session.control)
                        .await?;
                    println!("verified control, image, and audio lanes in one epoch");
                    Ok(())
                })
                .inspect_err(|error| eprintln!("receiver: {error}"))
        });
        liazo_macos::run_main_loop();
        destination
            .join()
            .map_err(|_| "receiver runtime panicked")??;
        source.join().map_err(|_| "provider runtime panicked")??;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn main() {
    if let Err(error) = native_peer::run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("this integration check requires macos capture");
}
