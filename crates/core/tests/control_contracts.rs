use bytes::Bytes;
use liazo_core::{
    control::{Command, Event, Request},
    protocol::{CaptureSettings, SourceKey},
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

#[test]
fn capture_failure_timing_cannot_claim_a_start_after_completion() {
    let mut event = Event::SourceStatus {
        source: SourceKey {
            id: "display:fixture".into(),
            generation: uuid::Uuid::new_v4(),
        },
        revision: 1,
        observed_ns: 200,
        capture_started_ns: Some(300),
        failure: None,
    };
    assert!(Frame::new(Header::Event(event.clone()), Bytes::new()).is_err());
    if let Event::SourceStatus {
        capture_started_ns, ..
    } = &mut event
    {
        *capture_started_ns = Some(100);
    }
    assert!(Frame::new(Header::Event(event), Bytes::new()).is_ok());
}
