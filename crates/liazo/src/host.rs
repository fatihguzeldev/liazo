use crate::{Result, profile::Profile};
use liazo_host::{SessionHost, http::Endpoint};
use liazo_transport::{CredentialError, DeviceCredentials, Fingerprint, StoredPeer};
use std::{io::Write, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct NativeTrust {
    profile: Arc<Profile>,
    credentials: Arc<DeviceCredentials>,
}
impl liazo_host::TrustStore for NativeTrust {
    fn peers(&self) -> std::result::Result<Vec<StoredPeer>, CredentialError> {
        self.credentials.peers()
    }
    fn save(&self, peer: StoredPeer) -> std::result::Result<(), CredentialError> {
        let _lock = self
            .profile
            .lock("credentials")
            .map_err(|_| CredentialError::Storage)?;
        self.credentials.save_peer(&peer.certificate, &peer.label)
    }
    fn remove(&self, peer: Fingerprint) -> std::result::Result<bool, CredentialError> {
        let _lock = self
            .profile
            .lock("credentials")
            .map_err(|_| CredentialError::Storage)?;
        self.credentials.remove_peer(peer)
    }
}

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
    let profile = Arc::new(profile);
    let store = Arc::new(NativeTrust {
        profile: profile.clone(),
        credentials: Arc::new(credentials),
    });
    let host = SessionHost::with_trust_store(
        identity,
        peers,
        profile.root.join("sessions"),
        profile.root.join("models"),
        store,
    );
    host.resume();
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
    let mut trust_tick = tokio::time::interval(Duration::from_secs(1));
    trust_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break Ok(()),
            ended = &mut server => break ended.map_err(|error| error.into()).and_then(|result| result.map_err(Into::into)),
            _ = trust_tick.tick() => {
                match host.refresh_trust().await {
                    Ok(()) => {},
                    _ => { host.replace_peers(vec![]).await?; break Err("paired trust is unavailable".into()); }
                }
            }
        }
    };
    host.models().cancel().await;
    if let Some(current) = host.current().await
        && let Ok(receiver) = host.receiver(current.binding).await
        && receiver.stop().await.is_err()
    {
        let _ = receiver.disconnect().await;
    }
    cancel.cancel();
    if !server.is_finished() && timeout(Duration::from_secs(5), &mut server).await.is_err() {
        server.abort();
    }
    result
}
