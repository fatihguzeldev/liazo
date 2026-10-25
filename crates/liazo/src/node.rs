use crate::{Result, profile::Profile};
use serde_json::Value;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::{IpAddr, SocketAddr},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::Duration,
};
use tokio::time::{Instant, sleep};

const WAIT: Duration = Duration::from_secs(10);
const STOP_WAIT: Duration = Duration::from_secs(20);

struct Service {
    domain: String,
    target: String,
    directory: PathBuf,
}

impl Service {
    fn new(profile: &Profile) -> Result<Self> {
        let user = Command::new("/usr/bin/id").arg("-u").output()?;
        let uid: u32 = std::str::from_utf8(&user.stdout)?.trim().parse()?;
        if !user.status.success() {
            return Err("cannot determine the logged-in user".into());
        }
        let domain = format!("gui/{uid}");
        Ok(Self {
            target: format!("{domain}/app.liazo.provider.{}", profile.name),
            domain,
            directory: profile.root.join("node"),
        })
    }

    fn inspect(&self) -> Result<Option<String>> {
        let result = launchctl(&["print", &self.target])?;
        if result.status.success() {
            return Ok(Some(String::from_utf8(result.stdout)?));
        }
        if result.status.code() == Some(113) {
            return Ok(None);
        }
        Err("cannot inspect the Liazo background service".into())
    }

    fn ready(&self, service: &str) -> Result<Option<Value>> {
        Ok(read_ready(&self.directory.join("ready.json"))?
            .and_then(|ready| matching_ready(ready, running_pid(service))))
    }

    fn unload(&self) -> Result<()> {
        if !launchctl(&["bootout", &self.target])?.status.success() {
            return Err("cannot stop the Liazo background service".into());
        }
        Ok(())
    }
}

pub async fn start(
    profile: Profile,
    listen: SocketAddr,
    advertise: Option<IpAddr>,
    pair: bool,
) -> Result<()> {
    let _control = profile.lock("node-control")?;
    let service = Service::new(&profile)?;
    if let Some(existing) = service.inspect()? {
        if let Some(ready) = service.ready(&existing)? {
            print_running(&ready);
            if pair {
                println!("already running; use stop, then start --pair for a new pairing code");
            }
            return Ok(());
        }
        if running_pid(&existing).is_some() {
            return Err("Liazo is starting or unavailable; check status before retrying".into());
        }
        if !stopped_service(&existing) {
            return Err(
                "Liazo service state is unavailable; use stop before starting it again".into(),
            );
        }
        service.unload()?;
    }
    // A foreground provider must not be replaced by a second background instance.
    let provider = profile.lock("provider")?;
    private_directory(&service.directory)?;
    let ready_path = service.directory.join("ready.json");
    remove_if_present(&ready_path)?;
    let executable = std::env::current_exe()?;
    let mut arguments = vec![
        path_text(&executable)?.to_owned(),
        "--profile".to_owned(),
        profile.name.clone(),
        "provide".to_owned(),
        "--listen".to_owned(),
        listen.to_string(),
        "--ready-file".to_owned(),
        path_text(&ready_path)?.to_owned(),
    ];
    if let Some(address) = advertise {
        arguments.extend(["--advertise".to_owned(), address.to_string()]);
    }
    if pair {
        arguments.push("--pair".to_owned());
    }
    let log = service.directory.join("provider.log");
    atomic_private_write(&log, b"")?;
    let plist = service.directory.join("provider.plist");
    atomic_private_write(
        &plist,
        launch_plist(&profile.name, &arguments, path_text(&log)?).as_bytes(),
    )?;
    drop(provider);
    if !launchctl(&["bootstrap", &service.domain, path_text(&plist)?])?
        .status
        .success()
    {
        return Err("cannot start Liazo in the logged-in macOS session".into());
    }
    let deadline = Instant::now() + WAIT;
    loop {
        let readiness = match service.inspect() {
            Ok(Some(current)) => service.ready(&current),
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        };
        match readiness {
            Ok(Some(mut ready)) => {
                // Pairing credentials are only handed to the invoking terminal, never kept in logs.
                let code = ready.as_object_mut().unwrap().remove("pair_code");
                if let Err(error) = atomic_private_write(&ready_path, &serde_json::to_vec(&ready)?)
                {
                    service.unload()?;
                    wait_stopped(&profile).await?;
                    return Err(error);
                }
                print_running(&ready);
                if let Some(Value::String(code)) = code {
                    println!("{code}");
                }
                return Ok(());
            }
            Ok(None) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            failure => {
                let cleanup = service.unload();
                if let Err(error) = failure {
                    cleanup?;
                    wait_stopped(&profile).await?;
                    return Err(error);
                }
                cleanup?;
                wait_stopped(&profile).await?;
                return Err(format!(
                    "Liazo did not become ready; see {}. If credential access is blocked, run liazo --profile {} unlock",
                    log.display(), profile.name
                ).into());
            }
        }
    }
}

pub async fn stop(profile: Profile) -> Result<()> {
    let _control = profile.lock("node-control")?;
    let service = Service::new(&profile)?;
    if service.inspect()?.is_some() {
        service.unload()?;
    } else if provider_busy(&profile)? {
        return Err("a foreground provider is running; stop it in its original terminal".into());
    }
    wait_stopped(&profile).await?;
    remove_if_present(&service.directory.join("ready.json"))?;
    println!("liazo stopped");
    Ok(())
}

async fn wait_stopped(profile: &Profile) -> Result<()> {
    let deadline = Instant::now() + STOP_WAIT;
    loop {
        if !provider_busy(profile)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("Liazo has not finished stopping; check status".into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

pub async fn status(profile: Profile) -> Result<()> {
    let service = Service::new(&profile)?;
    if let Some(current) = service.inspect()? {
        if let Some(ready) = service.ready(&current)? {
            print_running(&ready);
        } else if !stopped_service(&current) {
            println!("liazo starting or unavailable");
        } else {
            println!(
                "liazo stopped; last startup log: {}",
                service.directory.join("provider.log").display()
            );
        }
    } else if provider_busy(&profile)? {
        println!("liazo running in a foreground terminal");
    } else {
        println!("liazo stopped");
    }
    Ok(())
}

fn launchctl(arguments: &[&str]) -> Result<Output> {
    Ok(Command::new("/bin/launchctl")
        .args(arguments)
        .stdin(Stdio::null())
        .output()?)
}

fn launch_plist(profile: &str, arguments: &[String], log: &str) -> String {
    let arguments = arguments
        .iter()
        .map(|value| format!("<string>{}</string>", xml(value)))
        .collect::<String>();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\"><dict>\
         <key>Label</key><string>app.liazo.provider.{}</string>\
         <key>ProgramArguments</key><array>{arguments}</array>\
         <key>RunAtLoad</key><true/><key>KeepAlive</key><false/>\
         <key>ExitTimeOut</key><integer>15</integer><key>Umask</key><integer>63</integer>\
         <key>StandardInPath</key><string>/dev/null</string>\
         <key>StandardOutPath</key><string>{}</string>\
         <key>StandardErrorPath</key><string>{}</string>\
         </dict></plist>\n",
        xml(profile),
        xml(log),
        xml(log),
    )
}

fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn running_pid(service: &str) -> Option<u32> {
    let field = |name: &str| {
        service
            .lines()
            .find_map(|line| line.trim().strip_prefix(name))
    };
    if field("state = ")? != "running" {
        return None;
    }
    field("pid = ")?.parse::<u32>().ok().filter(|pid| *pid != 0)
}

fn stopped_service(service: &str) -> bool {
    service
        .lines()
        .any(|line| matches!(line.trim(), "state = not running" | "state = exited"))
}

fn provider_busy(profile: &Profile) -> Result<bool> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(profile.root.join("provider.lock"))?;
    match file.try_lock() {
        Ok(()) => Ok(false),
        Err(std::fs::TryLockError::WouldBlock) => Ok(true),
        Err(error) => Err(std::io::Error::other(error).into()),
    }
}

fn matching_ready(ready: Value, pid: Option<u32>) -> Option<Value> {
    let pid = u64::from(pid?);
    (ready.get("pid")?.as_u64()? == pid
        && ready
            .get("address")?
            .as_str()?
            .parse::<SocketAddr>()
            .ok()?
            .port()
            != 0
        && ready.get("fingerprint")?.as_str().is_some())
    .then_some(ready)
}

fn print_running(ready: &Value) {
    println!(
        "liazo running (pid {}, {})",
        ready["pid"],
        ready["address"].as_str().unwrap()
    );
}

fn path_text(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| "Liazo requires UTF-8 application and profile paths".into())
}

fn private_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    if !std::fs::symlink_metadata(path)?.is_dir() {
        return Err("invalid Liazo service directory".into());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn atomic_private_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    let _ = std::fs::remove_file(temporary);
    result
}

fn read_ready(path: &Path) -> Result<Option<Value>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.len() > 65536 {
        return Err("invalid Liazo service readiness".into());
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err("invalid Liazo service readiness".into());
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn remove_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn xml_arguments_cannot_add_plist_elements() {
        assert_eq!(
            xml("/Users/a & b/<liazo>/\"it's\""),
            "/Users/a &amp; b/&lt;liazo&gt;/&quot;it&apos;s&quot;"
        );
    }

    #[test]
    fn only_a_running_launchd_process_has_a_pid() {
        assert_eq!(running_pid("\tstate = running\n\tpid = 123\n"), Some(123));
        assert_eq!(running_pid("\tstate = exited\n\tpid = 123\n"), None);
        assert_eq!(running_pid("\tstate = running\n\tpid = 0\n"), None);
        assert!(!stopped_service("\tstate = spawn scheduled\n"));
        assert!(stopped_service("\tstate = not running\n"));
    }

    #[test]
    fn stale_readiness_cannot_report_a_running_provider() {
        let ready = json!({"pid": 123, "address": "127.0.0.1:45810", "fingerprint": "peer"});
        assert!(matching_ready(ready.clone(), None).is_none());
        assert!(matching_ready(ready.clone(), Some(456)).is_none());
        assert!(matching_ready(ready, Some(123)).is_some());
    }

    #[test]
    fn service_state_is_private_and_symlink_readiness_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("node");
        private_directory(&directory).unwrap();
        let ready = directory.join("ready.json");
        atomic_private_write(&ready, br#"{"pid":123}"#).unwrap();
        assert_eq!(
            std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&ready).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(read_ready(&ready).unwrap().unwrap()["pid"], 123);
        let link = directory.join("linked.json");
        std::os::unix::fs::symlink(ready, &link).unwrap();
        assert!(read_ready(&link).is_err());
    }

    #[test]
    fn a_foreground_provider_lock_prevents_duplicate_startup() {
        let directory = tempfile::tempdir().unwrap();
        let profile = Profile {
            name: "test".into(),
            root: directory.path().to_owned(),
        };
        assert!(!provider_busy(&profile).unwrap());
        let foreground = profile.lock("provider").unwrap();
        assert!(provider_busy(&profile).unwrap());
        drop(foreground);
        assert!(!provider_busy(&profile).unwrap());
    }

    #[test]
    fn storage_failure_is_not_reported_as_a_running_provider() {
        let directory = tempfile::tempdir().unwrap();
        let profile = Profile {
            name: "test".into(),
            root: directory.path().to_owned(),
        };
        std::fs::create_dir(directory.path().join("provider.lock")).unwrap();
        assert!(provider_busy(&profile).is_err());
    }

    #[test]
    fn plist_keeps_arguments_literal_and_does_not_enable_login_restart() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("provider.plist");
        let executable = "/Users/test & work/liazo.app/Contents/MacOS/liazo";
        let plist = launch_plist(
            "test",
            &[executable.to_owned(), "--pair".to_owned()],
            "/tmp/lz.log",
        );
        atomic_private_write(&path, plist.as_bytes()).unwrap();
        let parsed = Command::new("/usr/bin/plutil")
            .args(["-convert", "json", "-o", "-"])
            .arg(path)
            .output()
            .unwrap();
        assert!(parsed.status.success());
        let value: Value = serde_json::from_slice(&parsed.stdout).unwrap();
        assert_eq!(value["ProgramArguments"][0], executable);
        assert_eq!(value["KeepAlive"], false);
        assert_eq!(value["Umask"], 63);
    }
}
