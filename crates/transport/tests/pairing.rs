use liazo_transport::{
    Identity, PairingInvitation, PairingServer, PeerClient, PeerServer, request_pairing,
};
use tokio::net::TcpListener;

#[tokio::test]
async fn pairing_waits_for_explicit_approval_and_then_supports_mutual_tls() {
    let provider = Identity::generate().unwrap();
    let receiver = Identity::generate().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let pairing = PairingServer::open(&provider, address).unwrap();
    let invitation = PairingInvitation::decode(&pairing.invitation().encode().unwrap()).unwrap();
    let expected = receiver.fingerprint();
    let client = tokio::spawn(async move {
        request_pairing(&invitation, &receiver, "windows")
            .await
            .unwrap();
        (receiver, invitation)
    });
    let (socket, _) = listener.accept().await.unwrap();
    let pending = pairing.accept(socket).await.unwrap();
    assert_eq!(pending.fingerprint(), expected);
    assert_eq!(pending.label(), "windows");
    assert!(
        !client.is_finished(),
        "receiving a request must not approve it"
    );
    let approved = pending.approve().await.unwrap();
    let (receiver, invitation) = client.await.unwrap();
    let server = PeerServer::new(&provider, &[approved.certificate().clone()]).unwrap();
    let connector = PeerClient::new(&receiver, invitation.provider_certificate()).unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        server.accept(socket).await.unwrap().fingerprint()
    });
    connector.connect(address).await.unwrap();
    assert_eq!(task.await.unwrap(), expected);
}

#[tokio::test]
async fn denied_and_used_invitations_do_not_approve_a_peer() {
    let provider = Identity::generate().unwrap();
    let receiver = Identity::generate().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pairing = PairingServer::open(&provider, listener.local_addr().unwrap()).unwrap();
    let invitation = pairing.invitation();
    let client = tokio::spawn(async move {
        assert!(
            request_pairing(&invitation, &receiver, "windows")
                .await
                .is_err()
        );
        (receiver, invitation)
    });
    let (socket, _) = listener.accept().await.unwrap();
    pairing.accept(socket).await.unwrap().deny().await.unwrap();
    let (receiver, invitation) = client.await.unwrap();
    let client =
        tokio::spawn(async move { request_pairing(&invitation, &receiver, "windows").await });
    let (socket, _) = listener.accept().await.unwrap();
    assert!(pairing.accept(socket).await.is_err());
    assert!(client.await.unwrap().is_err());
}

#[tokio::test]
async fn an_invitation_does_not_trust_a_different_provider() {
    let provider = Identity::generate().unwrap();
    let impostor = Identity::generate().unwrap();
    let receiver = Identity::generate().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let original = PairingServer::open(&provider, address).unwrap();
    let wrong = PairingServer::open(&impostor, address).unwrap();
    let invitation = original.invitation();
    let client =
        tokio::spawn(async move { request_pairing(&invitation, &receiver, "windows").await });
    let (socket, _) = listener.accept().await.unwrap();
    assert!(wrong.accept(socket).await.is_err());
    assert!(client.await.unwrap().is_err());
}

#[test]
fn malformed_or_oversized_invitations_are_rejected_before_connecting() {
    assert!(PairingInvitation::decode("not an invitation").is_err());
    assert!(PairingInvitation::decode(&"a".repeat(32_769)).is_err());
}
