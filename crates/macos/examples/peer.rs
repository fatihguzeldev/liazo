#[cfg(target_os = "macos")]
mod native_peer {
    use liazo_core::{
        protocol::{ImageFormat, ImageSubscription, SourceKey},
        wire::{Frame, Header},
    };
    use liazo_transport::{Identity, PeerClient, PeerServer};
    use std::{error::Error, sync::mpsc, time::Duration};
    use tokio::{io::AsyncWriteExt, net::TcpListener};

    type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

    pub fn run() -> Result<()> {
        let provider = Identity::generate()?;
        let receiver = Identity::generate()?;
        let server = PeerServer::new(&provider, &[receiver.certificate().clone()])?;
        let client = PeerClient::new(&receiver, provider.certificate())?;
        let (ready, address) = mpsc::channel();
        let source = std::thread::spawn(move || -> Result<()> {
            tokio::runtime::Runtime::new()?.block_on(async {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                ready.send(listener.local_addr()?)?;
                let catalog = liazo_macos::sources().await?;
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
                let (socket, _) = listener.accept().await?;
                let mut image_lane = server.accept(socket).await?;
                let packet = liazo_macos::screenshot(&settings, None).await?;
                packet
                    .into_image_frame(image_key, 1, 1)?
                    .write_to(&mut image_lane)
                    .await?;
                image_lane.shutdown().await?;

                let (socket, _) = listener.accept().await?;
                let mut audio_lane = server.accept(socket).await?;
                let audio_key = SourceKey {
                    id: "audio:system".into(),
                    generation,
                };
                let mut stream = liazo_macos::AudioStream::start(&audio_key.id).await?;
                let deadline = tokio::time::sleep(Duration::from_secs(2));
                tokio::pin!(deadline);
                loop {
                    tokio::select! {
                        _ = &mut deadline => break,
                        packet = stream.next() => {
                            let Some(packet) = packet else { break };
                            packet?.into_audio_frame(audio_key.clone())?
                                .write_to(&mut audio_lane).await?;
                        }
                    }
                }
                stream.stop().await;
                audio_lane.shutdown().await?;
                Ok(())
            })
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
            tokio::runtime::Runtime::new()?.block_on(async {
                let mut image_lane = client.connect(address).await?;
                let image = Frame::read_from(&mut image_lane)
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
                let mut audio_lane = client.connect(address).await?;
                let mut chunks = 0;
                let mut payload = 0;
                while let Some(frame) = Frame::read_from(&mut audio_lane).await? {
                    if !matches!(frame.header, Header::Audio(_)) {
                        return Err("wrong audio message".into());
                    }
                    chunks += 1;
                    payload += frame.payload.len();
                }
                println!("verified tls audio: {chunks} chunks, {payload} bytes");
                Ok(())
            })
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
