use liazo_transport::{Identity, LaneKind, PeerClient, PeerServer, PeerSession, SessionServer};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use uuid::Uuid;

#[tokio::test]
async fn reconnect_replaces_the_old_epoch_without_closing_the_new_one() {
    let provider = Identity::generate().unwrap();
    let receiver = Identity::generate().unwrap();
    let server =
        SessionServer::new(PeerServer::new(&provider, &[receiver.certificate().clone()]).unwrap());
    let client = PeerClient::new(&receiver, provider.certificate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let session = Uuid::new_v4();
    async fn accept_three(
        server: &SessionServer,
        listener: &TcpListener,
    ) -> Vec<liazo_transport::AcceptedLane> {
        let mut lanes = Vec::new();
        for _ in 0..3 {
            let (socket, _) = listener.accept().await.unwrap();
            lanes.push(server.accept(socket).await.unwrap());
        }
        lanes
    }
    let (old, mut old_lanes) = tokio::join!(
        PeerSession::connect(&client, address, session),
        accept_three(&server, &listener)
    );
    let old = old.unwrap();
    let (new, mut new_lanes) = tokio::join!(
        PeerSession::connect(&client, address, session),
        accept_three(&server, &listener)
    );
    let mut new = new.unwrap();
    assert_ne!(old.binding.epoch, new.binding.epoch);
    assert_eq!(
        old_lanes[0].connection.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::ConnectionAborted
    );
    drop(old_lanes);
    drop(old);
    new_lanes[0].connection.write_u8(42).await.unwrap();
    new_lanes[0].connection.flush().await.unwrap();
    assert_eq!(new.control.read_u8().await.unwrap(), 42);
}

#[tokio::test]
async fn three_independent_lanes_share_one_session_and_epoch() {
    let provider = Identity::generate().unwrap();
    let receiver = Identity::generate().unwrap();
    let server =
        SessionServer::new(PeerServer::new(&provider, &[receiver.certificate().clone()]).unwrap());
    let client = PeerClient::new(&receiver, provider.certificate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let session = Uuid::new_v4();
    let task = tokio::spawn(async move {
        let mut lanes = Vec::new();
        for _ in 0..3 {
            let (socket, _) = listener.accept().await.unwrap();
            lanes.push(server.accept(socket).await.unwrap());
        }
        assert_eq!(lanes[0].kind, LaneKind::Control);
        assert_eq!(lanes[1].kind, LaneKind::Images);
        assert_eq!(lanes[2].kind, LaneKind::Audio);
        assert!(lanes.iter().all(|lane| lane.binding == lanes[0].binding));
        assert_eq!(lanes[0].binding.session, session);
        assert!(!lanes[0].binding.epoch.is_nil());
        lanes[0].connection.write_u8(7).await.unwrap();
        lanes[0].connection.flush().await.unwrap();
        assert_eq!(lanes[2].connection.read_u8().await.unwrap(), 9);
        lanes[0].binding
    });
    let mut connected = PeerSession::connect(&client, address, session)
        .await
        .unwrap();
    assert_eq!(connected.control.read_u8().await.unwrap(), 7);
    connected.audio.write_u8(9).await.unwrap();
    connected.audio.flush().await.unwrap();
    assert_eq!(task.await.unwrap(), connected.binding);
}

#[tokio::test]
async fn closing_a_control_lane_wakes_waiting_media_readers() {
    let provider = Identity::generate().unwrap();
    let receiver = Identity::generate().unwrap();
    let server =
        SessionServer::new(PeerServer::new(&provider, &[receiver.certificate().clone()]).unwrap());
    let client = PeerClient::new(&receiver, provider.certificate()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (ready, accepted) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut lanes = Vec::new();
        for _ in 0..3 {
            let (socket, _) = listener.accept().await.unwrap();
            lanes.push(server.accept(socket).await.unwrap());
        }
        ready.send(()).unwrap();
        let control = lanes.remove(0);
        let audio = lanes.pop().unwrap();
        let waiting = tokio::spawn(async move {
            let mut io = audio.connection;
            io.read_u8().await
        });
        drop(control);
        assert!(
            timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    });
    let _connected = PeerSession::connect(&client, address, Uuid::new_v4())
        .await
        .unwrap();
    accepted.await.unwrap();
    task.await.unwrap();
}
