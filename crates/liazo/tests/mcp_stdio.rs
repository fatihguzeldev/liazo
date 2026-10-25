use liazo_host::{
    SessionHost,
    http::{Endpoint, serve},
};
use liazo_transport::Identity;
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ClientConfig},
    transport::TokioChildProcess,
};
use std::{path::PathBuf, time::Duration};
use tokio::{net::TcpListener, process::Command};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn the_actual_stdio_command_attaches_without_a_gui_or_native_credential_access() {
    let name = format!("mcp-test-{}", Uuid::new_v4().simple());
    let profiles = directories::BaseDirs::new()
        .unwrap()
        .data_local_dir()
        .join("liazo/profiles");
    std::fs::create_dir_all(&profiles).unwrap();
    let root = profiles.join(&name);
    std::fs::create_dir(&root).unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let host = SessionHost::new(Identity::generate().unwrap(), vec![], root.join("sessions"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Endpoint::new(listener.local_addr().unwrap()).unwrap();
    std::fs::write(
        root.join("host.json"),
        serde_json::to_vec(&endpoint).unwrap(),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let _cancel = cancel.clone().drop_guard();
    let server = tokio::spawn(serve(listener, host, endpoint, cancel.clone()));
    let mut command = Command::new(env!("CARGO_BIN_EXE_liazo"));
    command.arg("--profile").arg(name).arg("mcp");
    let transport = TokioChildProcess::new(command).unwrap();
    let client = tokio::time::timeout(
        Duration::from_secs(10),
        ClientConfig::default().serve(transport),
    )
    .await
    .unwrap()
    .unwrap();
    let tools = client.list_tools(None).await.unwrap();
    assert!(tools.tools.iter().any(|tool| tool.name == "observe"));
    let status = client
        .call_tool(CallToolRequestParams::new("status"))
        .await
        .unwrap();
    assert_eq!(status.is_error, Some(false));
    assert_eq!(status.structured_content, Some(serde_json::Value::Null));
    client.cancel().await.unwrap();
    cancel.cancel();
    server.await.unwrap().unwrap();
}
