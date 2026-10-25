use crate::{Result, profile::Profile};
use liazo_host::{SessionHost, http::Endpoint};
use liazo_transport::DeviceCredentials;
use std::{io::Write, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub async fn run(
    profile: Profile,
    credentials: DeviceCredentials,
    listen: SocketAddr,
) -> Result<()> {
    if !listen.ip().is_loopback() {
        return Err("local host requires a loopback address".into());
    }
    let _owner = profile.lock("receiver")?;
    let identity = profile.identity(&credentials)?;
    let peers = credentials.peers()?;
    let host = SessionHost::new(identity, peers, profile.root.join("sessions"));
    let listener = TcpListener::bind(listen).await?;
    let endpoint = Endpoint::new(listener.local_addr()?)?;
    let descriptor = profile.root.join("host.json");
    let temporary = profile.root.join(format!("host-{}.tmp", Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    {
        let mut file = options.open(&temporary)?;
        file.write_all(&serde_json::to_vec(&endpoint)?)?;
        file.sync_all()?;
    }
    if descriptor.exists() {
        std::fs::remove_file(&descriptor)?;
    }
    std::fs::rename(&temporary, &descriptor)?;
    struct Descriptor(std::path::PathBuf);
    impl Drop for Descriptor {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _descriptor = Descriptor(descriptor);
    println!(
        "{}",
        serde_json::json!({"event":"host_ready","address":endpoint.address})
    );
    let cancel = CancellationToken::new();
    let _cancel = cancel.clone().drop_guard();
    let mut server = tokio::spawn(liazo_host::http::serve(
        listener,
        host.clone(),
        endpoint,
        cancel.clone(),
    ));
    let credentials = Arc::new(credentials);
    let mut trust_tick = tokio::time::interval(Duration::from_secs(1));
    trust_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break Ok(()),
            ended = &mut server => break ended.map_err(|error| error.into()).and_then(|result| result.map_err(Into::into)),
            _ = trust_tick.tick() => {
                let credentials = credentials.clone();
                // Native credential access stays off the media and HTTP runtime threads.
                match tokio::task::spawn_blocking(move || credentials.peers()).await {
                    Ok(Ok(peers)) => host.replace_peers(peers).await?,
                    _ => { host.replace_peers(vec![]).await?; break Err("paired trust is unavailable".into()); }
                }
            }
        }
    };
    if let Some(current) = host.current().await {
        let receiver = host.receiver(current.binding).await?;
        if receiver.stop().await.is_err() {
            let _ = receiver.disconnect().await;
        }
    }
    cancel.cancel();
    if !server.is_finished() && timeout(Duration::from_secs(5), &mut server).await.is_err() {
        server.abort();
    }
    result
}
