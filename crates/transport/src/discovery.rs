use crate::{Fingerprint, TransportError};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use serde::Serialize;
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

const SERVICE: &str = "_liazo._tcp.local.";

struct RunningDaemon(ServiceDaemon);
impl Drop for RunningDaemon {
    fn drop(&mut self) {
        let _ = self.0.shutdown();
    }
}

pub struct Advertisement {
    daemon: RunningDaemon,
    name: String,
}

impl Advertisement {
    pub fn start(identity: Fingerprint, port: u16) -> Result<Self, TransportError> {
        let daemon = RunningDaemon(ServiceDaemon::new().map_err(error)?);
        let pin = identity.to_string();
        let label = format!("liazo-{}", &pin[..12]);
        let info = ServiceInfo::new(
            SERVICE,
            &label,
            &format!("{label}.local."),
            "",
            port,
            &[("version", "1"), ("fingerprint", pin.as_str())][..],
        )
        .map_err(error)?
        .enable_addr_auto();
        let name = info.get_fullname().to_owned();
        daemon.0.register(info).map_err(error)?;
        Ok(Self { daemon, name })
    }
}

impl Drop for Advertisement {
    fn drop(&mut self) {
        let _ = self.daemon.0.unregister(&self.name);
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DiscoveredProvider {
    pub name: String,
    pub addresses: Vec<SocketAddr>,
    // Discovery is an untrusted routing hint. Only the credential store grants trust.
    pub advertised_fingerprint: String,
}

pub async fn discover(duration: Duration) -> Result<Vec<DiscoveredProvider>, TransportError> {
    let daemon = RunningDaemon(ServiceDaemon::new().map_err(error)?);
    let receiver = daemon.0.browse(SERVICE).map_err(error)?;
    let deadline = tokio::time::Instant::now() + duration.min(Duration::from_secs(10));
    let mut found = HashMap::new();
    while let Ok(Ok(event)) = tokio::time::timeout_at(deadline, receiver.recv_async()).await {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                if info.get_port() == 0 || info.get_property_val_str("version") != Some("1") {
                    continue;
                }
                let Some(pin) = info
                    .get_property_val_str("fingerprint")
                    .and_then(|pin| pin.parse::<Fingerprint>().ok())
                else {
                    continue;
                };
                let mut addresses: Vec<_> = info
                    .get_addresses_v4()
                    .into_iter()
                    .take(16)
                    .map(|ip| SocketAddr::new(IpAddr::V4(ip), info.get_port()))
                    .collect();
                addresses.sort_unstable();
                if addresses.is_empty() || info.fullname.len() > 255 || found.len() >= 64 {
                    continue;
                }
                found.insert(
                    info.fullname.clone(),
                    DiscoveredProvider {
                        name: info.fullname,
                        addresses,
                        advertised_fingerprint: pin.to_string(),
                    },
                );
            }
            ServiceEvent::ServiceRemoved(_, name) => {
                found.remove(&name);
            }
            _ => {}
        }
    }
    let mut results: Vec<_> = found.into_values().collect();
    results.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(results)
}

fn error(error: mdns_sd::Error) -> TransportError {
    TransportError::Discovery(error.to_string())
}
