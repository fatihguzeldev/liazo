use bytes::Bytes;
use liazo_core::{
    control::{Command, Request},
    protocol::CaptureSettings,
    wire::{Frame, Header},
};

#[test]
fn control_requests_cannot_carry_hidden_media_or_unbounded_source_selections() {
    let request = Request {
        id: 1,
        command: Command::Configure(CaptureSettings::default()),
    };
    assert!(
        Frame::new(
            Header::Request(request.clone()),
            Bytes::from_static(b"hidden")
        )
        .is_err()
    );
    assert!(Frame::new(Header::Request(request), Bytes::new()).is_ok());
    let invalid = Request {
        id: 0,
        command: Command::Sources { after: None },
    };
    assert!(Frame::new(Header::Request(invalid), Bytes::new()).is_err());
    let invalid = Request {
        id: 2,
        command: Command::Sources {
            after: Some("x".repeat(257)),
        },
    };
    assert!(Frame::new(Header::Request(invalid), Bytes::new()).is_err());
}
