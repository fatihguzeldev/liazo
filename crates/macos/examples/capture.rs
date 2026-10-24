#[cfg(target_os = "macos")]
fn main() {
    let worker = std::thread::spawn(|| {
        struct StopLoop;
        impl Drop for StopLoop {
            fn drop(&mut self) {
                liazo_macos::stop_main_loop();
            }
        }
        let _stop = StopLoop;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let catalog = liazo_macos::sources().await?;
            let missing = || {
                liazo_macos::NativeError::Failure(
                    "no display is available; check screen recording permission".into(),
                )
            };
            let screens = catalog["screens"].as_array().ok_or_else(missing)?;
            let source = screens
                .iter()
                .find(|s| s["kind"] == "display")
                .ok_or_else(missing)?;
            let settings = liazo_core::protocol::ImageSubscription {
                source: liazo_core::protocol::SourceKey {
                    id: source["id"].as_str().ok_or_else(missing)?.into(),
                    generation: uuid_for_example(),
                },
                interval_ms: 500,
                max_width: Some(1200),
                crop: std::env::var_os("LIAZO_TEST_CROP").map(|_| {
                    liazo_core::protocol::CropPixels {
                        x: 0,
                        y: 0,
                        width: 200,
                        height: 100,
                    }
                }),
                format: liazo_core::protocol::ImageFormat::Png,
                quality: 90,
                include_cursor: false,
            };
            let image = liazo_macos::screenshot(&settings, None).await?;
            println!(
                "native image: {} bytes, {}×{}",
                image.payload.len(),
                image.metadata["width"],
                image.metadata["height"]
            );
            if let Some(source) = std::env::args().nth(1) {
                let mut stream = liazo_macos::AudioStream::start(&source).await?;
                let mut chunks = 0;
                let deadline = tokio::time::sleep(std::time::Duration::from_secs(2));
                tokio::pin!(deadline);
                loop {
                    tokio::select! {
                        _ = &mut deadline => break,
                        packet = stream.next() => {
                            let Some(packet) = packet else { break };
                            packet?;
                            chunks += 1;
                        }
                    }
                }
                stream.stop().await;
                println!("native audio: {chunks} chunks");
            }
            Ok::<_, liazo_macos::NativeError>(())
        })
    });
    liazo_macos::run_main_loop();
    if let Err(error) = worker.join().unwrap() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(target_os = "macos")]
fn uuid_for_example() -> uuid::Uuid {
    uuid::Uuid::from_u128(1)
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("native capture is only available on macos");
}
