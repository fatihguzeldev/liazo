use liazo_host::{HostError, SessionHost, TrustStore};
use liazo_transport::{CredentialError, Fingerprint, Identity, PairingServer, StoredPeer};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};

#[derive(Default)]
struct Store {
    peers: Mutex<Vec<StoredPeer>>,
    fail: AtomicBool,
}
impl TrustStore for Store {
    fn peers(&self) -> Result<Vec<StoredPeer>, CredentialError> {
        Ok(self.peers.lock().unwrap().clone())
    }
    fn save(&self, peer: StoredPeer) -> Result<(), CredentialError> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(CredentialError::Storage);
        }
        let mut peers = self.peers.lock().unwrap();
        peers.retain(|old| old.fingerprint() != peer.fingerprint());
        peers.push(peer);
        Ok(())
    }
    fn remove(&self, peer: Fingerprint) -> Result<bool, CredentialError> {
        let mut peers = self.peers.lock().unwrap();
        let before = peers.len();
        peers.retain(|old| old.fingerprint() != peer);
        Ok(before != peers.len())
    }
}

#[tokio::test]
async fn approved_pairing_persists_after_its_consumer_leaves_and_forgetting_removes_trust() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::default());
    let host = SessionHost::with_trust_store(
        Identity::generate().unwrap(),
        vec![],
        root.path().to_owned(),
        store.clone(),
    );
    let provider = Identity::generate().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = PairingServer::open(&provider, listener.local_addr().unwrap()).unwrap();
    let invitation = server.invitation().encode().unwrap();
    let (approve, answer) = oneshot::channel();
    let (seen, requested) = oneshot::channel();
    let serving = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let pending = server.accept(socket).await.unwrap();
        seen.send(()).unwrap();
        answer.await.unwrap();
        pending.approve().await.unwrap();
    });
    let pairing = host.clone();
    let code = invitation.clone();
    let consumer =
        tokio::spawn(async move { pairing.pair(code, "windows".into(), "mac".into()).await });
    requested.await.unwrap();
    assert!(host.peers().await.is_empty());
    assert!(store.peers().unwrap().is_empty());
    assert!(matches!(
        host.pair(invitation, "windows".into(), "mac".into()).await,
        Err(HostError::Busy)
    ));
    host.refresh_trust().await.unwrap();
    consumer.abort();
    let _ = consumer.await;
    approve.send(()).unwrap();
    serving.await.unwrap();
    timeout(Duration::from_secs(2), async {
        while host.peers().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store.peers().unwrap()[0].fingerprint(),
        provider.fingerprint()
    );
    assert_eq!(host.peers().await[0].1, "mac");
    assert!(host.forget(provider.fingerprint()).await.unwrap());
    assert!(store.peers().unwrap().is_empty());
    assert!(host.peers().await.is_empty());
}

#[tokio::test]
async fn a_persistence_failure_cannot_be_reported_as_a_paired_computer() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::default());
    store.fail.store(true, Ordering::Relaxed);
    let host = SessionHost::with_trust_store(
        Identity::generate().unwrap(),
        vec![],
        root.path().to_owned(),
        store.clone(),
    );
    let provider = Identity::generate().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = PairingServer::open(&provider, listener.local_addr().unwrap()).unwrap();
    let code = server.invitation().encode().unwrap();
    let serving = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        server
            .accept(socket)
            .await
            .unwrap()
            .approve()
            .await
            .unwrap();
    });
    assert!(matches!(
        host.pair(code, "windows".into(), "mac".into()).await,
        Err(HostError::Credential(_))
    ));
    serving.await.unwrap();
    assert!(host.peers().await.is_empty());
    assert!(store.peers().unwrap().is_empty());
}
