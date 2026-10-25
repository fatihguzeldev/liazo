use crate::Result;
use liazo_transport::{DeviceCredentials, Identity};
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

pub struct Profile {
    pub name: String,
    pub root: PathBuf,
}

impl Profile {
    pub fn open(name: String) -> Result<Self> {
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err("invalid profile name".into());
        }
        let root = directories::BaseDirs::new()
            .ok_or("application data directory unavailable")?
            .data_local_dir()
            .join("liazo")
            .join("profiles")
            .join(&name);
        std::fs::create_dir_all(&root)?;
        Ok(Self { name, root })
    }
    pub fn lock(&self, role: &str) -> Result<File> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(self.root.join(format!("{role}.lock")))?;
        file.try_lock()
            .map_err(|_| format!("{role} is already running or busy"))?;
        Ok(file)
    }
    pub fn credentials(&self) -> Result<DeviceCredentials> {
        Ok(DeviceCredentials::open_profile(&self.name)?)
    }
    pub fn identity(&self, credentials: &DeviceCredentials) -> Result<Identity> {
        let _lock = self.lock("credentials")?;
        Ok(credentials.load_or_create_identity()?)
    }
}
