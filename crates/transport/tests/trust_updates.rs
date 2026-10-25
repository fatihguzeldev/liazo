use liazo_transport::{Identity, PeerClient, PeerServer};
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    sync::oneshot,
    time::{Duration, timeout},
};

#[tokio::test]
async fn shared_acceptors_apply_pairing_and_revoke_existing_connections_without_recreation() {
    let identity = Identity::generate().unwrap();
    let receiver = Identity::generate().unwrap();
    let server = PeerServer::new(&identity, &[]).unwrap();
    let existing_acceptor = server.clone();
    let client = PeerClient::new(&receiver, identity.certificate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let rejected = tokio::spawn(async move { client.connect(address).await });
    let (socket, _) = listener.accept().await.unwrap();
    assert!(existing_acceptor.accept(socket).await.is_err());
    assert!(rejected.await.unwrap().is_err());
    server
        .replace_trusted(&identity, &[receiver.certificate().clone()])
        .unwrap();
    let (ready, accepted) = oneshot::channel();
    let worker = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = existing_acceptor.accept(socket).await.unwrap();
        ready.send(()).unwrap();
        connection.read_u8().await
    });
    let client = PeerClient::new(&receiver, identity.certificate()).unwrap();
    let _connection = client.connect(address).await.unwrap();
    accepted.await.unwrap();
    server.replace_trusted(&identity, &[]).unwrap();
    assert!(
        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
}
