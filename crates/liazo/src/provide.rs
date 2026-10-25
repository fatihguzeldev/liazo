use crate::{Result, profile::Profile};
use liazo_transport::{
    AcceptedLane, DeviceCredentials, LaneKind, PairingServer, PeerConnection, PeerServer,
    PeerSession, SessionBinding, SessionServer,
};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct Partial {
    binding: SessionBinding,
    created: Instant,
    control: Option<PeerConnection>,
    images: Option<PeerConnection>,
    audio: Option<PeerConnection>,
}

impl Partial {
    fn new(binding: SessionBinding) -> Self {
        Self {
            binding,
            created: Instant::now(),
            control: None,
            images: None,
            audio: None,
        }
    }
    fn push(&mut self, lane: AcceptedLane) {
        match lane.kind {
            LaneKind::Control => self.control = Some(lane.connection),
            LaneKind::Images => self.images = Some(lane.connection),
            LaneKind::Audio => self.audio = Some(lane.connection),
        }
    }
    fn complete(&self) -> bool {
        self.control.is_some() && self.images.is_some() && self.audio.is_some()
    }
    fn finish(mut self) -> PeerSession {
        PeerSession {
            binding: self.binding,
            control: self.control.take().unwrap(),
            images: self.images.take().unwrap(),
            audio: self.audio.take().unwrap(),
        }
    }
}

#[cfg(target_os = "macos")]
pub async fn run(
    profile: Profile,
    credentials: DeviceCredentials,
    listen: SocketAddr,
    advertise: Option<IpAddr>,
    pair: bool,
) -> Result<()> {
    let _owner = profile.lock("provider")?;
    let identity = profile.identity(&credentials)?;
    let peers = credentials.peers()?;
    let server = PeerServer::new(
        &identity,
        &peers
            .iter()
            .map(|peer| peer.certificate.clone())
            .collect::<Vec<_>>(),
    )?;
    let sessions = SessionServer::new(server.clone());
    let listener = TcpListener::bind(listen).await?;
    let bound = listener.local_addr()?;
    let address = SocketAddr::new(advertised_ip(bound.ip(), advertise)?, bound.port());
    let _advertisement = match liazo_transport::discovery::Advertisement::start(
        identity.fingerprint(),
        bound.port(),
    ) {
        Ok(advertisement) => Some(advertisement),
        Err(error) => {
            eprintln!("{error}");
            None
        }
    };
    let pairing_listener = TcpListener::bind(SocketAddr::new(bound.ip(), 0)).await?;
    let invitation = pair
        .then(|| {
            PairingServer::open(
                &identity,
                SocketAddr::new(address.ip(), pairing_listener.local_addr()?.port()),
            )
        })
        .transpose()?
        .map(Arc::new);
    println!(
        "{}",
        serde_json::json!({"event":"ready","address":address.to_string(),"fingerprint":identity.fingerprint().to_string()})
    );
    if let Some(pairing) = &invitation {
        // This is the explicitly requested setup code, never part of operational media logs.
        println!(
            "{}",
            serde_json::json!({"event":"pair_code","code":pairing.invitation().encode()?})
        );
    }
    let backend = Arc::new(crate::macos::MacCapture::default());
    let cancel = CancellationToken::new();
    let mut handshakes = JoinSet::new();
    let mut pending: HashMap<Uuid, Partial> = HashMap::new();
    let mut active = JoinSet::new();
    let mut active_cancel: Option<CancellationToken> = None;
    let mut pairings = JoinSet::new();
    let mut pairing_handshakes = JoinSet::new();
    let mut trust = tokio::time::interval(Duration::from_secs(1));
    let mut previous: Vec<_> = peers.iter().map(|peer| peer.fingerprint()).collect();
    previous.sort_by_key(ToString::to_string);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            accepted = listener.accept(), if handshakes.len() < 16 => {
                let (socket, _) = accepted?;
                let sessions = sessions.clone();
                handshakes.spawn(async move { sessions.accept(socket).await });
            }
            accepted = handshakes.join_next(), if !handshakes.is_empty() => {
                if let Some(Ok(Ok(lane))) = accepted {
                    pending.retain(|_, partial| partial.created.elapsed() < Duration::from_secs(10));
                    let epoch = lane.binding.epoch;
                    if pending.len() >= 16 && !pending.contains_key(&epoch) { continue; }
                    let entry = pending.entry(epoch).or_insert_with(|| Partial::new(lane.binding));
                    entry.push(lane);
                    if entry.complete() {
                        let peer = pending.remove(&epoch).unwrap().finish();
                        if let Some(cancel) = active_cancel.take() { cancel.cancel(); }
                        while active.join_next().await.is_some() {}
                        let stopped = cancel.child_token();
                        active_cancel = Some(stopped.clone());
                        let backend = backend.clone();
                        active.spawn(async move { liazo_runtime::provider::serve(backend, peer, stopped).await });
                    }
                }
            }
            _ = active.join_next(), if !active.is_empty() => { active_cancel = None; }
            accepted = pairing_listener.accept(), if invitation.is_some() && pairing_handshakes.len() < 4 => {
                let (socket, _) = accepted?;
                let invitation = invitation.as_ref().unwrap().clone();
                pairing_handshakes.spawn(async move { invitation.accept(socket).await });
            }
            request = pairing_handshakes.join_next(), if !pairing_handshakes.is_empty() => {
                if let Some(Ok(Ok(request))) = request {
                    eprintln!("pair request: {} {}", request.label(), request.fingerprint());
                    let (answer, answered) = oneshot::channel();
                    // One invitation is single-use, so at most one terminal approval reader exists.
                    std::thread::spawn(move || {
                        eprintln!("allow this computer? [y/n]");
                        let mut line = String::new();
                        let allowed = std::io::stdin().read_line(&mut line).is_ok() && line.trim() == "y";
                        let _ = answer.send(allowed);
                    });
                    pairings.spawn(async move {
                        let allowed = tokio::time::timeout(Duration::from_secs(120), answered).await.ok().and_then(|answer| answer.ok()).unwrap_or(false);
                        (request, allowed)
                    });
                }
            }
            answer = pairings.join_next(), if !pairings.is_empty() => {
                if let Some(Ok((request, allowed))) = answer {
                    if allowed {
                        {
                            let _lock = profile.lock("credentials")?;
                            credentials.save_peer(request.certificate(), request.label())?;
                            let peers = credentials.peers()?;
                            server.replace_trusted(&identity, &peers.into_iter().map(|peer| peer.certificate).collect::<Vec<_>>())?;
                        }
                        let peer = request.fingerprint();
                        request.approve().await?;
                        println!("{}", serde_json::json!({"event":"paired","fingerprint":peer.to_string()}));
                    } else { request.deny().await?; }
                }
            }
            _ = trust.tick() => {
                pending.retain(|_, partial| partial.created.elapsed() < Duration::from_secs(10));
                if let Ok(_lock) = profile.lock("credentials") && let Ok(peers) = credentials.peers() {
                    let mut pins: Vec<_> = peers.iter().map(|peer| peer.fingerprint()).collect();
                    pins.sort_by_key(ToString::to_string);
                    if pins != previous {
                        server.replace_trusted(&identity, &peers.into_iter().map(|peer| peer.certificate).collect::<Vec<_>>())?;
                        previous = pins;
                    }
                }
            }
        }
    }
    cancel.cancel();
    handshakes.abort_all();
    pairings.abort_all();
    pairing_handshakes.abort_all();
    drop(pending);
    while active.join_next().await.is_some() {}
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub async fn run(
    _: Profile,
    _: DeviceCredentials,
    _: SocketAddr,
    _: Option<IpAddr>,
    _: bool,
) -> Result<()> {
    Err("capture provider requires macos".into())
}

fn advertised_ip(bound: IpAddr, explicit: Option<IpAddr>) -> Result<IpAddr> {
    if let Some(ip) = explicit {
        if ip.is_unspecified() || ip.is_multicast() {
            return Err("invalid advertised address".into());
        }
        return Ok(ip);
    }
    if !bound.is_unspecified() {
        return Ok(bound);
    }
    let mut interfaces = if_addrs::get_if_addrs()?;
    interfaces.sort_by_key(|interface| interface.name != "en0");
    interfaces
        .into_iter()
        .map(|interface| interface.ip())
        .find(|ip| !ip.is_loopback() && !ip.is_unspecified() && ip.is_ipv4())
        .ok_or_else(|| "no local address is available; specify --advertise".into())
}
