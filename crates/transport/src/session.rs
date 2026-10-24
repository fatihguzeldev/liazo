use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{Fingerprint, PeerClient, PeerConnection, PeerServer, TransportError};

const MAGIC: &[u8; 4] = b"LS01";
const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SESSIONS: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionBinding {
    pub session: Uuid,
    pub epoch: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneKind {
    Control,
    Images,
    Audio,
}

impl LaneKind {
    fn number(self) -> u8 {
        match self {
            Self::Control => 0,
            Self::Images => 1,
            Self::Audio => 2,
        }
    }
}

struct Ticket {
    binding: SessionBinding,
    token: [u8; 32],
}

pub(crate) struct SessionState {
    ticket: Ticket,
    peer: Fingerprint,
    claims: AtomicU8,
    expires: Instant,
    pub(crate) cancelled: CancellationToken,
}

pub(crate) struct SessionLease(pub(crate) Arc<SessionState>);

impl SessionLease {
    pub(crate) fn close(&self) {
        self.0.cancelled.cancel();
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Clone)]
pub struct SessionServer {
    peer: PeerServer,
    sessions: Arc<Mutex<HashMap<Uuid, Weak<SessionState>>>>,
}

pub struct AcceptedLane {
    pub binding: SessionBinding,
    pub kind: LaneKind,
    pub connection: PeerConnection,
}

impl SessionServer {
    pub fn new(peer: PeerServer) -> Self {
        Self {
            peer,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn revoke(&self, peer: Fingerprint) -> bool {
        self.peer.revoke(peer)
    }

    pub async fn accept(&self, socket: TcpStream) -> Result<AcceptedLane, TransportError> {
        timeout(SETUP_TIMEOUT, self.accept_inner(socket))
            .await
            .map_err(|_| TransportError::Timeout)?
    }

    async fn accept_inner(&self, socket: TcpStream) -> Result<AcceptedLane, TransportError> {
        let mut connection = self.peer.accept(socket).await?;
        let (kind, ticket) = read_ticket(&mut connection).await?;
        if ticket.binding.session.is_nil() {
            return Err(TransportError::Protocol);
        }
        let state = {
            let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            sessions.retain(|_, entry| {
                entry.upgrade().is_some_and(|state| {
                    if state.cancelled.is_cancelled() {
                        return false;
                    }
                    if state.claims.load(Ordering::Acquire) != 7 && Instant::now() >= state.expires
                    {
                        state.cancelled.cancel();
                        return false;
                    }
                    true
                })
            });
            if kind == LaneKind::Control {
                if !ticket.binding.epoch.is_nil() || ticket.token != [0; 32] {
                    return Err(TransportError::Protocol);
                }
                sessions.retain(|_, entry| {
                    if let Some(state) = entry.upgrade()
                        && state.peer == connection.fingerprint()
                        && state.ticket.binding.session == ticket.binding.session
                    {
                        state.cancelled.cancel();
                        return false;
                    }
                    true
                });
                if sessions.len() >= MAX_SESSIONS {
                    return Err(TransportError::Protocol);
                }
                let mut token = [0; 32];
                SystemRandom::new()
                    .fill(&mut token)
                    .map_err(|_| TransportError::InvalidIdentity)?;
                let state = Arc::new(SessionState {
                    ticket: Ticket {
                        binding: SessionBinding {
                            session: ticket.binding.session,
                            epoch: Uuid::new_v4(),
                        },
                        token,
                    },
                    peer: connection.fingerprint(),
                    claims: AtomicU8::new(1),
                    expires: Instant::now() + SETUP_TIMEOUT,
                    cancelled: CancellationToken::new(),
                });
                sessions.insert(state.ticket.binding.epoch, Arc::downgrade(&state));
                state
            } else {
                let state = sessions
                    .get(&ticket.binding.epoch)
                    .and_then(Weak::upgrade)
                    .ok_or(TransportError::Protocol)?;
                if state.peer != connection.fingerprint()
                    || state.ticket.binding != ticket.binding
                    || !bool::from(state.ticket.token.ct_eq(&ticket.token))
                {
                    return Err(TransportError::UntrustedPeer);
                }
                let bit = 1 << kind.number();
                if state.claims.fetch_or(bit, Ordering::AcqRel) & bit != 0 {
                    return Err(TransportError::Protocol);
                }
                state
            }
        };
        connection.bind_session(state.clone());
        if kind == LaneKind::Control {
            write_ticket(&mut connection, kind, &state.ticket).await?;
        } else {
            connection.write_u8(1).await?;
            connection.flush().await?;
        }
        Ok(AcceptedLane {
            binding: state.ticket.binding,
            kind,
            connection,
        })
    }
}

pub struct PeerSession {
    pub binding: SessionBinding,
    pub control: PeerConnection,
    pub images: PeerConnection,
    pub audio: PeerConnection,
}

impl PeerSession {
    pub async fn connect(
        client: &PeerClient,
        address: SocketAddr,
        session: Uuid,
    ) -> Result<Self, TransportError> {
        if session.is_nil() {
            return Err(TransportError::Protocol);
        }
        timeout(SETUP_TIMEOUT, async {
            let mut control = client.connect(address).await?;
            write_ticket(
                &mut control,
                LaneKind::Control,
                &Ticket {
                    binding: SessionBinding {
                        session,
                        epoch: Uuid::nil(),
                    },
                    token: [0; 32],
                },
            )
            .await?;
            let (kind, ticket) = read_ticket(&mut control).await?;
            if kind != LaneKind::Control
                || ticket.binding.session != session
                || ticket.binding.epoch.is_nil()
            {
                return Err(TransportError::Protocol);
            }
            let state = Arc::new(SessionState {
                ticket,
                peer: control.fingerprint(),
                claims: AtomicU8::new(7),
                expires: Instant::now() + SETUP_TIMEOUT,
                cancelled: CancellationToken::new(),
            });
            control.bind_session(state.clone());
            let images = attach(client, address, LaneKind::Images, state.clone()).await?;
            let audio = attach(client, address, LaneKind::Audio, state.clone()).await?;
            Ok(Self {
                binding: state.ticket.binding,
                control,
                images,
                audio,
            })
        })
        .await
        .map_err(|_| TransportError::Timeout)?
    }
}

async fn attach(
    client: &PeerClient,
    address: SocketAddr,
    kind: LaneKind,
    state: Arc<SessionState>,
) -> Result<PeerConnection, TransportError> {
    let mut connection = client.connect(address).await?;
    connection.bind_session(state.clone());
    write_ticket(&mut connection, kind, &state.ticket).await?;
    if connection.read_u8().await? != 1 {
        return Err(TransportError::Protocol);
    }
    Ok(connection)
}

async fn read_ticket(io: &mut PeerConnection) -> Result<(LaneKind, Ticket), TransportError> {
    let mut message = [0; 69];
    io.read_exact(&mut message).await?;
    if &message[..4] != MAGIC {
        return Err(TransportError::Protocol);
    }
    let kind = match message[4] {
        0 => LaneKind::Control,
        1 => LaneKind::Images,
        2 => LaneKind::Audio,
        _ => return Err(TransportError::Protocol),
    };
    Ok((
        kind,
        Ticket {
            binding: SessionBinding {
                session: Uuid::from_bytes(message[5..21].try_into().unwrap()),
                epoch: Uuid::from_bytes(message[21..37].try_into().unwrap()),
            },
            token: message[37..].try_into().unwrap(),
        },
    ))
}

async fn write_ticket(
    io: &mut PeerConnection,
    kind: LaneKind,
    ticket: &Ticket,
) -> Result<(), TransportError> {
    let mut message = [0; 69];
    message[..4].copy_from_slice(MAGIC);
    message[4] = kind.number();
    message[5..21].copy_from_slice(ticket.binding.session.as_bytes());
    message[21..37].copy_from_slice(ticket.binding.epoch.as_bytes());
    message[37..].copy_from_slice(&ticket.token);
    io.write_all(&message).await?;
    io.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Identity;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn lanes_reject_wrong_tokens_other_paired_peers_and_duplicate_attachments() {
        let provider = Identity::generate().unwrap();
        let receiver = Identity::generate().unwrap();
        let other = Identity::generate().unwrap();
        let server = SessionServer::new(
            PeerServer::new(
                &provider,
                &[receiver.certificate().clone(), other.certificate().clone()],
            )
            .unwrap(),
        );
        let client = PeerClient::new(&receiver, provider.certificate()).unwrap();
        let other_client = PeerClient::new(&other, provider.certificate()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let session = Uuid::new_v4();
        let send = async {
            let mut connection = client.connect(address).await.unwrap();
            write_ticket(
                &mut connection,
                LaneKind::Control,
                &Ticket {
                    binding: SessionBinding {
                        session,
                        epoch: Uuid::nil(),
                    },
                    token: [0; 32],
                },
            )
            .await
            .unwrap();
            let (_, ticket) = read_ticket(&mut connection).await.unwrap();
            (connection, ticket)
        };
        let accept = async {
            let (socket, _) = listener.accept().await.unwrap();
            server.accept(socket).await.unwrap()
        };
        let ((_control, ticket), _accepted) = tokio::join!(send, accept);

        async fn attempt(
            server: &SessionServer,
            listener: &TcpListener,
            client: &PeerClient,
            ticket: &Ticket,
        ) -> (
            Result<AcceptedLane, TransportError>,
            Result<PeerConnection, TransportError>,
        ) {
            tokio::join!(
                async {
                    let (socket, _) = listener.accept().await.unwrap();
                    server.accept(socket).await
                },
                async {
                    let mut connection = client.connect(listener.local_addr().unwrap()).await?;
                    write_ticket(&mut connection, LaneKind::Images, ticket).await?;
                    if connection.read_u8().await? != 1 {
                        return Err(TransportError::Protocol);
                    }
                    Ok(connection)
                }
            )
        }

        let wrong = Ticket {
            binding: ticket.binding,
            token: [0; 32],
        };
        let (accepted, attempted) = attempt(&server, &listener, &client, &wrong).await;
        assert!(accepted.is_err() && attempted.is_err());
        let (accepted, attempted) = attempt(&server, &listener, &other_client, &ticket).await;
        assert!(accepted.is_err() && attempted.is_err());
        let (accepted, attempted) = attempt(&server, &listener, &client, &ticket).await;
        let mut accepted = accepted.unwrap();
        let mut original = attempted.unwrap();
        let (duplicate, attempted) = attempt(&server, &listener, &client, &ticket).await;
        assert!(duplicate.is_err() && attempted.is_err());
        original.write_u8(5).await.unwrap();
        original.flush().await.unwrap();
        assert_eq!(accepted.connection.read_u8().await.unwrap(), 5);
    }
}
