use super::spawn_host;
use std::{
    ffi::OsStr,
    fs,
    io::{self, Read},
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    ptr,
    thread::sleep,
    time::{Duration, Instant},
};
use windows_sys::Win32::{
    Foundation::{DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_BROKEN_PIPE, WAIT_TIMEOUT},
    System::{
        Pipes::PeekNamedPipe,
        Threading::{
            GetCurrentProcess, OpenProcess, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
            TerminateProcess, WaitForSingleObject,
        },
    },
};

const MARKER: &str = "liazo-owned-pipe-fixture-v1";
const SENTINEL: &str = "value 語 \"quoted\" ending \\";

#[test]
fn spawn_rejects_invalid_windows_command_lines() {
    let executable = std::env::current_exe().unwrap();
    let oversized = "x".repeat(32_768);
    for (label, result) in [
        ("executable NUL", spawn_host(Path::new("bad\0path"), &[])),
        (
            "argument NUL",
            spawn_host(&executable, &[OsStr::new("bad\0arg")]),
        ),
        (
            "command line length",
            spawn_host(&executable, &[OsStr::new(&oversized)]),
        ),
    ] {
        let error = result.err().expect("invalid input must not spawn a child");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{label}");
    }
}

#[test]
fn detached_host_does_not_keep_consumer_pipes_open() {
    let directory = tempfile::Builder::new()
        .prefix("liazo-launcher-")
        .tempdir()
        .unwrap();
    let root = directory.path().join("Liazo doğrulama 語");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("owner"), MARKER).unwrap();
    let executable = root.join("pipe fixture.exe");
    fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
    let proxy = Command::new(&executable)
        .args([
            "--ignored",
            "--exact",
            "launcher::tests::proxy_fixture",
            "--test-threads=1",
            "--nocapture",
        ])
        .current_dir(&root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut processes = FixtureProcesses {
        proxy,
        host: None,
        root,
    };
    let mut stdout = processes.proxy.stdout.take().unwrap();
    let mut stderr = processes.proxy.stderr.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let exit = loop {
        if let Some(exit) = processes.proxy.try_wait().unwrap() {
            break exit;
        }
        assert!(Instant::now() < deadline, "proxy did not exit");
        sleep(Duration::from_millis(20));
    };
    assert!(exit.success(), "proxy fixture failed: {exit}");
    processes.host = Some(open_host(&processes.root).unwrap());
    let host = processes.host.as_ref().unwrap();
    let initial_heartbeat = fs::read(processes.root.join("heartbeat")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let (mut stdout_eof, mut stderr_eof, mut advanced) = (false, false, false);
    loop {
        stdout_eof |= pipe_eof(&mut stdout).unwrap();
        stderr_eof |= pipe_eof(&mut stderr).unwrap();
        advanced |= fs::read(processes.root.join("heartbeat"))
            .is_ok_and(|value| !value.is_empty() && value != initial_heartbeat);
        // EOF is only valid evidence while the separately owned host is still alive.
        assert_eq!(
            unsafe { WaitForSingleObject(host.as_raw_handle(), 0) },
            WAIT_TIMEOUT
        );
        if stdout_eof && stderr_eof && advanced {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "consumer pipes retained after proxy exit: stdout_eof={stdout_eof}, \
             stderr_eof={stderr_eof}, host_heartbeat_advanced={advanced}"
        );
        sleep(Duration::from_millis(20));
    }
}

// Only the isolated libtest children execute these fixtures; no app CLI backdoor is needed.
#[test]
#[ignore = "controlled subprocess fixture"]
fn proxy_fixture() {
    let Some(root) = fixture_root() else {
        return;
    };
    // Consumer libraries may own inheritable copies beyond the standard handle slots.
    // Redirecting only a child's three stdio handles must not leak these duplicates.
    let _stdout = inheritable_copy(&io::stdout()).unwrap();
    let _stderr = inheritable_copy(&io::stderr()).unwrap();
    let executable = std::env::current_exe().unwrap();
    let mut host = spawn_host(
        &executable,
        &[
            OsStr::new("--ignored"),
            OsStr::new("--exact"),
            OsStr::new("launcher::tests::host_fixture"),
            OsStr::new("--test-threads=1"),
            OsStr::new("--nocapture"),
            OsStr::new("--skip"),
            OsStr::new(SENTINEL),
        ],
    )
    .unwrap();
    fs::write(root.join("host.pid"), host.id().to_string()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !root.join("ready").exists() {
        assert!(
            host.try_wait().unwrap().is_none(),
            "host exited before ready"
        );
        assert!(Instant::now() < deadline, "host did not become ready");
        sleep(Duration::from_millis(20));
    }
    // Dropping the process handle leaves the detached host running after the proxy exits.
}

#[test]
#[ignore = "controlled subprocess fixture"]
fn host_fixture() {
    let Some(root) = fixture_root() else {
        return;
    };
    let args: Vec<_> = std::env::args_os().collect();
    assert!(
        args.windows(2)
            .any(|pair| pair[0] == "--skip" && pair[1] == SENTINEL)
    );
    fs::write(root.join("heartbeat"), "0").unwrap();
    fs::write(root.join("ready"), "ready").unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut heartbeat = 0_u32;
    while !root.join("stop").exists() && Instant::now() < deadline {
        heartbeat += 1;
        fs::write(root.join("heartbeat"), heartbeat.to_string()).unwrap();
        sleep(Duration::from_millis(20));
    }
}

fn fixture_root() -> Option<PathBuf> {
    let root = std::env::current_dir().ok()?;
    (fs::read(root.join("owner")).ok()?.as_slice() == MARKER.as_bytes()).then_some(root)
}

fn inheritable_copy(handle: &impl AsRawHandle) -> io::Result<OwnedHandle> {
    let mut copy = ptr::null_mut();
    let process = unsafe { GetCurrentProcess() };
    if unsafe {
        DuplicateHandle(
            process,
            handle.as_raw_handle(),
            process,
            &mut copy,
            0,
            1,
            DUPLICATE_SAME_ACCESS,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedHandle::from_raw_handle(copy) })
}

fn pipe_eof(pipe: &mut (impl Read + AsRawHandle)) -> io::Result<bool> {
    let mut available = 0;
    if unsafe {
        PeekNamedPipe(
            pipe.as_raw_handle(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            &mut available,
            ptr::null_mut(),
        )
    } == 0
    {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32) {
            Ok(true)
        } else {
            Err(error)
        };
    }
    if available > 0 {
        let mut discard = [0; 4096];
        let count = available.min(discard.len() as u32) as usize;
        return pipe.read(&mut discard[..count]).map(|count| count == 0);
    }
    Ok(false)
}

fn open_host(root: &Path) -> io::Result<OwnedHandle> {
    let pid = fs::read_to_string(root.join("host.pid"))?
        .parse::<u32>()
        .map_err(io::Error::other)?;
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
    if handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

struct FixtureProcesses {
    proxy: Child,
    host: Option<OwnedHandle>,
    root: PathBuf,
}

impl Drop for FixtureProcesses {
    fn drop(&mut self) {
        let _ = fs::write(self.root.join("stop"), "stop");
        if self.proxy.try_wait().ok().flatten().is_none() {
            let _ = self.proxy.kill();
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.proxy.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            sleep(Duration::from_millis(20));
        }
        if self.host.is_none() {
            self.host = open_host(&self.root).ok();
        }
        if let Some(host) = &self.host
            && unsafe { WaitForSingleObject(host.as_raw_handle(), 2_000) } == WAIT_TIMEOUT
        {
            unsafe {
                TerminateProcess(host.as_raw_handle(), 1);
                WaitForSingleObject(host.as_raw_handle(), 2_000);
            }
        }
    }
}
