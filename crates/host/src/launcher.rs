use crate::http::Endpoint;
use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::{Instant, sleep};

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::spawn_host;

#[cfg(all(test, windows))]
mod tests;

#[cfg(not(windows))]
fn spawn_host(executable: &Path, args: &[&OsStr]) -> std::io::Result<std::process::Child> {
    use std::process::{Command, Stdio};
    let mut command = Command::new(executable);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.spawn()
}

#[derive(Debug, thiserror::Error)]
pub enum LaunchError {
    #[error("local host setup is unavailable")]
    Storage(#[from] std::io::Error),
    #[error("invalid local host endpoint")]
    Endpoint,
    #[error("local host did not start; check credential access with liazo unlock")]
    Startup,
}

pub struct Launcher {
    pub executable: PathBuf,
    pub profile: String,
    pub root: PathBuf,
}

impl Launcher {
    pub async fn ensure(&self) -> Result<Endpoint, LaunchError> {
        if self.profile.is_empty()
            || self.profile.len() > 64
            || !self
                .profile
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(LaunchError::Endpoint);
        }
        let client = crate::http::client_builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(250))
            .timeout(Duration::from_millis(750))
            .build()
            .map_err(|_| LaunchError::Startup)?;
        if let Some(endpoint) = self.running(&client).await? {
            return Ok(endpoint);
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let startup = options.open(self.root.join("host-start.lock"))?;
        let deadline = Instant::now() + Duration::from_secs(10);
        let _startup: File = loop {
            match startup.try_lock() {
                Ok(()) => break startup,
                Err(std::fs::TryLockError::WouldBlock) => {
                    if let Some(endpoint) = self.running(&client).await? {
                        return Ok(endpoint);
                    }
                    if Instant::now() >= deadline {
                        return Err(LaunchError::Startup);
                    }
                    sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(std::io::Error::other(error).into()),
            }
        };
        if let Some(endpoint) = self.running(&client).await? {
            return Ok(endpoint);
        }
        let mut child = spawn_host(
            &self.executable,
            &[
                OsStr::new("--profile"),
                OsStr::new(&self.profile),
                OsStr::new("host"),
                OsStr::new("--listen"),
                OsStr::new("127.0.0.1:0"),
            ],
        )?;
        loop {
            if let Some(endpoint) = self.running(&client).await? {
                return Ok(endpoint);
            }
            if child.try_wait()?.is_some() {
                return Err(LaunchError::Startup);
            }
            if Instant::now() >= deadline {
                // Only this launcher's unsuccessful child is terminated.
                let _ = child.kill();
                let _ = child.wait();
                return Err(LaunchError::Startup);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn running(&self, client: &reqwest::Client) -> Result<Option<Endpoint>, LaunchError> {
        let Some(endpoint) = read_endpoint(&self.root.join("host.json"))? else {
            return Ok(None);
        };
        let ready = client
            .get(format!("http://{}/v1/status", endpoint.address))
            .bearer_auth(&endpoint.token)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        Ok(ready.then_some(endpoint))
    }
}

pub fn read_endpoint(path: &Path) -> Result<Option<Endpoint>, LaunchError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.len() > 4096 {
        return Err(LaunchError::Endpoint);
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(LaunchError::Endpoint);
    }
    let endpoint: Endpoint = serde_json::from_slice(&bytes).map_err(|_| LaunchError::Endpoint)?;
    endpoint.validate().map_err(|_| LaunchError::Endpoint)?;
    Ok(Some(endpoint))
}
