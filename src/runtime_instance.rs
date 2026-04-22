use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RuntimeInstanceMetadata {
    pid: u32,
    started_at: String,
    cwd: String,
    web_port: u16,
}

pub struct RuntimeInstanceGuard {
    _file: File,
    path: PathBuf,
}

impl RuntimeInstanceGuard {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn acquire_runtime_instance_guard(
    root_dir: &Path,
    web_port: u16,
) -> Result<RuntimeInstanceGuard> {
    let runtime_dir = root_dir.join("workspace").join("runtime");
    fs::create_dir_all(&runtime_dir).with_context(|| {
        format!(
            "failed to create runtime instance directory {}",
            runtime_dir.display()
        )
    })?;
    let lock_path = runtime_dir.join("nyx.lock");
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open runtime lock {}", lock_path.display()))?;

    let lock_result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if lock_result != 0 {
        let err = std::io::Error::last_os_error();
        if matches!(err.raw_os_error(), Some(libc::EWOULDBLOCK)) {
            let metadata = read_runtime_instance_metadata(&mut file).ok();
            if let Some(metadata) = metadata {
                bail!(
                    "another Nyx runtime is already active (pid {} on port {} since {} via {})",
                    metadata.pid,
                    metadata.web_port,
                    metadata.started_at,
                    metadata.cwd
                );
            }
            bail!(
                "another Nyx runtime is already active and holds {}",
                lock_path.display()
            );
        }
        return Err(err).with_context(|| {
            format!(
                "failed to acquire exclusive runtime lock {}",
                lock_path.display()
            )
        });
    }

    let metadata = RuntimeInstanceMetadata {
        pid: std::process::id(),
        started_at: chrono::Utc::now().to_rfc3339(),
        cwd: root_dir.display().to_string(),
        web_port,
    };
    write_runtime_instance_metadata(&mut file, &metadata).with_context(|| {
        format!(
            "failed to write runtime instance metadata to {}",
            lock_path.display()
        )
    })?;

    Ok(RuntimeInstanceGuard {
        _file: file,
        path: lock_path,
    })
}

fn read_runtime_instance_metadata(file: &mut File) -> Result<RuntimeInstanceMetadata> {
    let mut content = String::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_string(&mut content)?;
    serde_json::from_str(content.trim()).context("invalid runtime instance metadata")
}

fn write_runtime_instance_metadata(
    file: &mut File,
    metadata: &RuntimeInstanceMetadata,
) -> Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    serde_json::to_writer_pretty(&mut *file, metadata)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_instance_metadata_round_trip() {
        let path = std::env::temp_dir().join(format!(
            "nyx_runtime_guard_{}.json",
            uuid::Uuid::new_v4().simple()
        ));
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let metadata = RuntimeInstanceMetadata {
            pid: 4242,
            started_at: "2026-04-22T08:20:00Z".to_string(),
            cwd: "/tmp/nyx".to_string(),
            web_port: 8099,
        };

        write_runtime_instance_metadata(&mut file, &metadata).unwrap();
        let decoded = read_runtime_instance_metadata(&mut file).unwrap();
        assert_eq!(decoded.pid, metadata.pid);
        assert_eq!(decoded.started_at, metadata.started_at);
        assert_eq!(decoded.cwd, metadata.cwd);
        assert_eq!(decoded.web_port, metadata.web_port);

        std::fs::remove_file(&path).ok();
    }
}
