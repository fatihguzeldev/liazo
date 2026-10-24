#[cfg(target_os = "macos")]
fn main() {
    let worker = std::thread::spawn(|| {
        struct Stop;
        impl Drop for Stop {
            fn drop(&mut self) {
                liazo_macos::stop_main_loop();
            }
        }
        let _stop = Stop;
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mut registry = liazo_core::catalog::SourceRegistry::default();
            let snapshot = liazo_macos::discover_sources()
                .await?
                .apply(&mut registry)?;
            for kind in [
                liazo_core::protocol::SourceKind::Display,
                liazo_core::protocol::SourceKind::Window,
                liazo_core::protocol::SourceKind::SystemAudio,
                liazo_core::protocol::SourceKind::ProcessAudio,
                liazo_core::protocol::SourceKind::Microphone,
            ] {
                println!(
                    "{kind:?}: {} sources",
                    snapshot
                        .sources
                        .iter()
                        .filter(|source| source.kind == kind)
                        .count()
                );
            }
            for issue in snapshot.issues {
                println!("{:?}: {:?}", issue.kind, issue.availability);
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

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("native source discovery requires macos");
}
