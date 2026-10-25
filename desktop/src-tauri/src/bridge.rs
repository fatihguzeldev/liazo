use liazo_host::{http::Endpoint, launcher::Launcher};
use reqwest::{Client, Method, Response};
use serde_json::Value;
use std::{path::PathBuf, time::Duration};
use tauri::State;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct Bridge {
    launcher: Launcher,
    endpoint: Mutex<Option<Endpoint>>,
    client: Client,
}
impl Bridge {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let profile = std::env::var("LIAZO_PROFILE").unwrap_or_else(|_| "default".into());
        if profile.is_empty()
            || profile.len() > 64
            || !profile
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err("invalid profile".into());
        }
        let root = directories::BaseDirs::new()
            .ok_or("application data unavailable")?
            .data_local_dir()
            .join("liazo/profiles")
            .join(&profile);
        std::fs::create_dir_all(&root)?;
        let executable = companion(std::env::current_exe()?)?;
        Ok(Self {
            launcher: Launcher {
                executable,
                profile,
                root,
            },
            endpoint: Mutex::new(None),
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(30))
                .build()?,
        })
    }
    async fn endpoint(&self) -> Result<Endpoint, String> {
        let mut endpoint = self.endpoint.lock().await;
        if endpoint.is_none() {
            *endpoint = Some(
                self.launcher
                    .ensure()
                    .await
                    .map_err(|error| error.to_string())?,
            );
        }
        Ok(endpoint.as_ref().unwrap().clone())
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        args: Option<Value>,
    ) -> Result<Response, String> {
        let endpoint = self.endpoint().await?;
        let mut request = self
            .client
            .request(method, format!("http://{}/v1/{path}", endpoint.address))
            .bearer_auth(endpoint.token);
        if path == "pair" {
            request = request.timeout(Duration::from_secs(125));
        }
        if let Some(args) = args {
            let bytes = serde_json::to_vec(&args).map_err(|_| "invalid request")?;
            if bytes.len() > 64 * 1024 {
                return Err("request is too large".into());
            }
            request = request
                .header("content-type", "application/json")
                .body(bytes);
        }
        match request.send().await {
            Ok(response) if response.status() == reqwest::StatusCode::UNAUTHORIZED => {
                *self.endpoint.lock().await = None;
                Err("local host changed; retry".into())
            }
            Ok(response) if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                Err("local host is busy".into())
            }
            Ok(response) => Ok(response),
            Err(_) => {
                *self.endpoint.lock().await = None;
                Err("local host is unavailable".into())
            }
        }
    }
}

fn companion(executable: PathBuf) -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(executable
        .parent()
        .ok_or("application path unavailable")?
        .join(format!("liazo{}", std::env::consts::EXE_SUFFIX)))
}
async fn bytes(mut response: Response, limit: usize) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err("response is too large".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "local host disconnected")?
    {
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err("response is too large".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[tauri::command]
pub async fn operation(
    state: State<'_, Bridge>,
    name: String,
    args: Value,
) -> Result<Value, String> {
    let method = match name.as_str() {
        "status" | "peers" | "discover" => Method::GET,
        "connect" | "pair" | "forget" | "sources" | "observe" | "configure" | "capture"
        | "history" | "transcripts" | "history-limits" | "transcribe" | "transcription-stop"
        | "stop" => Method::POST,
        _ => return Err("unknown operation".into()),
    };
    let body = (method == Method::POST).then_some(args);
    let response = state.request(method, &name, body).await?;
    let ok = response.status().is_success();
    let value: Value = serde_json::from_slice(&bytes(response, 16 * 1024 * 1024).await?)
        .map_err(|_| "local host returned invalid data")?;
    if !ok {
        return Err(value["error"]
            .as_str()
            .unwrap_or("operation failed")
            .to_owned());
    }
    Ok(value)
}

#[tauri::command]
pub async fn media(state: State<'_, Bridge>, id: Uuid) -> Result<tauri::ipc::Response, String> {
    let path = format!("media/{id}");
    let response = state.request(Method::GET, &path, None).await?;
    if !response.status().is_success() {
        return Err("preview expired".into());
    }
    let data = bytes(response, 32 * 1024 * 1024).await?;
    let _ = state.request(Method::DELETE, &path, None).await;
    Ok(tauri::ipc::Response::new(data))
}

#[tauri::command]
pub async fn release_media(state: State<'_, Bridge>, ids: Vec<Uuid>) -> Result<(), String> {
    if ids.len() > 64 {
        return Err("too many previews".into());
    }
    for id in ids {
        let _ = state
            .request(Method::DELETE, &format!("media/{id}"), None)
            .await;
    }
    Ok(())
}
