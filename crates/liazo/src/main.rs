use clap::{Parser, Subcommand};
use liazo_core::protocol::{CaptureSettings, ImageFormat, ImageSubscription};
use liazo_runtime::receiver::ReceiverSession;
use liazo_transport::{Fingerprint, PairingInvitation, PeerClient, request_pairing};
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Duration,
};
use uuid::Uuid;

mod host;
#[cfg(target_os = "macos")]
mod macos;
mod profile;
#[cfg(target_os = "macos")]
mod provide;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(name = "liazo", version, about = "your computers, on speaking terms.")]
struct Cli {
    #[arg(long, global = true, default_value = "default")]
    profile: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Host {
        #[arg(long, default_value = "127.0.0.1:45811")]
        listen: SocketAddr,
    },
    Provide {
        #[arg(long, default_value = "0.0.0.0:45810")]
        listen: SocketAddr,
        #[arg(long)]
        advertise: Option<IpAddr>,
        #[arg(long)]
        pair: bool,
    },
    Pair {
        #[arg(long, default_value = "client")]
        name: String,
    },
    Connect {
        address: SocketAddr,
        #[arg(long)]
        peer: Fingerprint,
        #[arg(long)]
        image: Vec<String>,
        #[arg(long)]
        audio: Vec<String>,
        #[arg(long, default_value_t = 500)]
        interval: u32,
        #[arg(long, default_value_t = 1920)]
        width: u32,
        #[arg(long)]
        seconds: Option<u64>,
        #[arg(long)]
        image_out: Option<PathBuf>,
        #[arg(long, requires = "model")]
        transcribe: Vec<String>,
        #[arg(long, requires = "transcribe")]
        model: Option<PathBuf>,
        #[arg(long, default_value_t = 4)]
        threads: u8,
        #[arg(long)]
        no_archive: bool,
    },
    Peers,
    Discover,
    Unlock,
    Revoke {
        peer: Fingerprint,
    },
}

fn main() {
    let cli = Cli::parse();
    #[cfg(target_os = "macos")]
    if matches!(cli.command, Command::Provide { .. }) {
        let worker = std::thread::spawn(move || {
            struct Stop;
            impl Drop for Stop {
                fn drop(&mut self) {
                    liazo_macos::stop_main_loop();
                }
            }
            let _stop = Stop;
            execute(cli)
        });
        liazo_macos::run_main_loop();
        finish(
            worker
                .join()
                .unwrap_or_else(|_| Err("provider thread failed".into())),
        );
        return;
    }
    finish(execute(cli));
}

fn finish(result: Result<()>) {
    if let Err(error) = result {
        eprintln!("{error}");
        #[cfg(target_os = "macos")]
        if matches!(
            error.downcast_ref::<liazo_transport::CredentialError>(),
            Some(
                liazo_transport::CredentialError::AccessDenied
                    | liazo_transport::CredentialError::Storage
            )
        ) {
            eprintln!("allow credential access with: liazo --profile <name> unlock");
        }
        std::process::exit(1);
    }
}
fn execute(cli: Cli) -> Result<()> {
    #[cfg(target_os = "macos")]
    let _keychain_ui = if matches!(cli.command, Command::Unlock) {
        None
    } else {
        Some(security_framework::os::macos::keychain::SecKeychain::disable_user_interaction()?)
    };
    tokio::runtime::Runtime::new()?.block_on(run(cli))
}

async fn run(cli: Cli) -> Result<()> {
    let profile = profile::Profile::open(cli.profile)?;
    let credentials = profile.credentials()?;
    match cli.command {
        Command::Host { listen } => host::run(profile, credentials, listen).await,
        Command::Unlock => {
            let _ = profile.identity(&credentials)?;
            let _ = credentials.peers()?;
            println!("credentials ready");
            Ok(())
        }
        #[cfg(target_os = "macos")]
        Command::Provide {
            listen,
            advertise,
            pair,
        } => provide::run(profile, credentials, listen, advertise, pair).await,
        #[cfg(not(target_os = "macos"))]
        Command::Provide { .. } => Err("capture provider requires macos".into()),
        Command::Peers => {
            for peer in credentials.peers()? {
                println!("{} {}", peer.fingerprint(), peer.label);
            }
            Ok(())
        }
        Command::Discover => {
            for provider in liazo_transport::discovery::discover(Duration::from_secs(3)).await? {
                println!("{}", serde_json::to_string(&provider)?);
            }
            Ok(())
        }
        Command::Revoke { peer } => {
            let _lock = profile.lock("credentials")?;
            println!("removed: {}", credentials.remove_peer(peer)?);
            Ok(())
        }
        Command::Pair { name } => {
            let identity = profile.identity(&credentials)?;
            eprintln!("paste pair code:");
            let invitation = tokio::task::spawn_blocking(|| -> Result<_> {
                let mut line = String::new();
                std::io::stdin().read_line(&mut line)?;
                Ok(PairingInvitation::decode(line.trim())?)
            })
            .await??;
            request_pairing(&invitation, &identity, &name).await?;
            let _lock = profile.lock("credentials")?;
            credentials.save_peer(invitation.provider_certificate(), "provider")?;
            println!(
                "paired: {}",
                liazo_transport::fingerprint(invitation.provider_certificate())
            );
            Ok(())
        }
        Command::Connect {
            address,
            peer,
            image,
            audio,
            interval,
            width,
            seconds,
            image_out,
            transcribe,
            model,
            threads,
            no_archive,
        } => {
            let _owner = profile.lock("receiver")?;
            let identity = profile.identity(&credentials)?;
            let provider = credentials
                .peers()?
                .into_iter()
                .find(|provider| provider.fingerprint() == peer)
                .ok_or("provider is not paired")?;
            let session = ReceiverSession::connect(
                &PeerClient::new(&identity, &provider.certificate)?,
                address,
                Uuid::new_v4(),
            )
            .await?;
            if !no_archive {
                let binding = session.archive_identity().binding;
                let root = profile
                    .root
                    .join("sessions")
                    .join(binding.session.to_string())
                    .join(binding.epoch.to_string());
                session
                    .start_archive(root, liazo_runtime::archive::ArchiveLimits::default())
                    .await?;
            }
            let sources = session.sources();
            let resolve = |id: &str| {
                sources
                    .sources
                    .iter()
                    .find(|source| source.key.id == id)
                    .map(|source| source.key.clone())
                    .ok_or_else(|| format!("source is unavailable: {id}"))
            };
            let settings = CaptureSettings {
                images: image
                    .iter()
                    .map(|id| {
                        Ok(ImageSubscription {
                            source: resolve(id)?,
                            interval_ms: interval,
                            max_width: Some(width),
                            crop: None,
                            format: ImageFormat::Png,
                            quality: 90,
                            include_cursor: false,
                        })
                    })
                    .collect::<std::result::Result<_, String>>()?,
                audio: audio
                    .iter()
                    .map(|id| resolve(id))
                    .collect::<std::result::Result<_, _>>()?,
            };
            session.configure(settings).await?;
            if let Some(path) = model {
                let selected = transcribe
                    .iter()
                    .map(|id| resolve(id))
                    .collect::<std::result::Result<_, _>>()?;
                let model = tokio::task::spawn_blocking(move || {
                    liazo_transcription::WhisperModel::load(&path)
                })
                .await??;
                session
                    .start_transcription(
                        model,
                        threads,
                        selected,
                        liazo_transcription::streaming::StreamingSettings::default(),
                    )
                    .await?;
            }
            println!(
                "{}",
                serde_json::json!({"event":"connected", "sources":sources.sources.len(), "issues":sources.issues})
            );
            let deadline = tokio::time::sleep(Duration::from_secs(seconds.unwrap_or(86400 * 365)));
            tokio::pin!(deadline);
            let mut ticks = tokio::time::interval(Duration::from_secs(1));
            let mut cursor = 0;
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = &mut deadline => break,
                    _ = ticks.tick() => {
                        let observation = session.observe(cursor, image_out.is_some(), &[], Duration::ZERO).await;
                        cursor = observation.cursor;
                        if let Some(path) = &image_out && let Some(bytes) = observation.images.first().and_then(|image| image.payload.as_ref()) { std::fs::write(path, bytes)?; }
                        println!("{}", serde_json::json!({"event":"observation", "cursor":cursor, "connection":observation.connection,
                            "images":observation.images.len(), "audio":observation.audio.len(), "gaps":observation.gaps,
                            "transcript_updates":observation.transcripts.len(), "transcription":observation.transcription}));
                        if matches!(observation.connection, liazo_runtime::ConnectionState::Disconnected) { return Err("provider disconnected".into()); }
                    }
                }
            }
            session.stop().await?;
            Ok(())
        }
    }
}
