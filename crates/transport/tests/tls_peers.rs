use bytes::Bytes;
use liazo_core::wire::{Frame, Header};
use liazo_transport::{Identity, PeerClient, PeerServer};
use tokio::net::TcpListener;

#[test]
fn persisted_identity_preserves_its_pin_and_rejects_a_mismatched_key() {
    let identity = Identity::generate().unwrap();
    let restored = Identity::from_der(
        identity.certificate().to_vec(),
        identity.secret_key_der().to_vec(),
    )
    .unwrap();
    assert_eq!(restored.fingerprint(), identity.fingerprint());
    let other = Identity::generate().unwrap();
    assert!(
        Identity::from_der(
            identity.certificate().to_vec(),
            other.secret_key_der().to_vec(),
        )
        .is_err()
    );
    let pin = identity.fingerprint().to_string();
    assert_eq!(
        pin.parse::<liazo_transport::Fingerprint>().unwrap(),
        identity.fingerprint()
    );
    assert!("not a pin".parse::<liazo_transport::Fingerprint>().is_err());
}

#[tokio::test]
async fn paired_peers_exchange_frames_and_expose_verified_identities() {
    let provider = Identity::generate().unwrap();
    let client = Identity::generate().unwrap();
    let provider_id = provider.fingerprint();
    let client_id = client.fingerprint();
    let server = PeerServer::new(&provider, &[client.certificate().clone()]).unwrap();
    let connector = PeerClient::new(&client, provider.certificate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = server.accept(socket).await.unwrap();
        assert_eq!(connection.fingerprint(), client_id);
        let frame = Frame::read_from(&mut connection).await.unwrap().unwrap();
        assert!(matches!(frame.header, Header::Ping { nonce: 42 }));
        Frame::new(Header::Pong { nonce: 42 }, Bytes::new())
            .unwrap()
            .write_to(&mut connection)
            .await
            .unwrap();
    });
    let mut connection = connector.connect(address).await.unwrap();
    assert_eq!(connection.fingerprint(), provider_id);
    Frame::new(Header::Ping { nonce: 42 }, Bytes::new())
        .unwrap()
        .write_to(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        Frame::read_from(&mut connection)
            .await
            .unwrap()
            .unwrap()
            .header,
        Header::Pong { nonce: 42 }
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn an_unpaired_client_never_receives_a_verified_connection() {
    let provider = Identity::generate().unwrap();
    let paired = Identity::generate().unwrap();
    let stranger = Identity::generate().unwrap();
    let server = PeerServer::new(&provider, &[paired.certificate().clone()]).unwrap();
    let connector = PeerClient::new(&stranger, provider.certificate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        assert!(server.accept(socket).await.is_err());
    });
    let _ = connector.connect(address).await;
    task.await.unwrap();
}

#[tokio::test]
async fn a_provider_with_the_wrong_certificate_is_rejected() {
    let provider = Identity::generate().unwrap();
    let impostor = Identity::generate().unwrap();
    let client = Identity::generate().unwrap();
    let server = PeerServer::new(&impostor, &[client.certificate().clone()]).unwrap();
    let connector = PeerClient::new(&client, provider.certificate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = server.accept(socket).await;
    });
    assert!(connector.connect(address).await.is_err());
    task.await.unwrap();
}
