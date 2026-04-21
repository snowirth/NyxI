//! Sandbox abstraction for Python tool and self-edit execution.
//!
//! Phase 9. This module defines the `Sandbox` trait, a `HostSandbox` fallback
//! that preserves today's host-local execution exactly, and a `DockerSandbox`
//! backend that shells out to the `docker` CLI for container-isolated Python
//! execution.
//!
//! Callers inside the forge that previously invoked `python3` directly route
//! through this module so the runtime has a single choke point for tool
//! isolation.
//!
//! Scope is deliberately tight: there is no configuration DSL, no filesystem
//! policy beyond a bind-mounted tempdir, no network policy beyond `--network`.
//! Just enough to preserve existing behavior and provide a working container
//! backend configured by env vars.

use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

/// Identifier for the backend that actually executed a script.
///
/// Callers log this so we can tell after the fact which backend ran — useful
/// when Docker is requested but unavailable and we silently fall back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxKind {
    /// Host-local execution via `tokio::process::Command` or
    /// `std::process::Command`. Today's behavior.
    Host,
    /// Docker-backed container execution. Not yet implemented —
    /// requesting this variant falls through to `Host` with a warning.
    Docker,
}

impl SandboxKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SandboxKind::Host => "host",
            SandboxKind::Docker => "docker",
        }
    }

    /// Parse `NYX_SANDBOX_KIND` style strings. Unknown values default to
    /// `Host` so a malformed env var never blocks execution.
    pub fn from_str_lossy(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "docker" => SandboxKind::Docker,
            _ => SandboxKind::Host,
        }
    }

    /// Read the preferred sandbox from `NYX_SANDBOX_KIND`. Missing or empty
    /// env var means `Host`. Docker is opt-in via `NYX_SANDBOX_KIND=docker`.
    ///
    /// The default is Host (not "Docker when available") because Nyx's
    /// bundled tools (e.g. the browser tool's Playwright dependency) live on
    /// the host and a blanket Docker default would break them. Once forge-
    /// generated tools get a separate isolation path, flipping the default
    /// for *that* path makes sense — doing it globally today does not.
    pub fn from_env() -> Self {
        std::env::var("NYX_SANDBOX_KIND")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|v| Self::from_str_lossy(&v))
            .unwrap_or(SandboxKind::Host)
    }
}

/// A file that should be staged into the sandbox workdir before execution.
///
/// Only meaningful for container-style backends (e.g. `DockerSandbox`) that
/// do not share the host filesystem. `HostSandbox` ignores staged files
/// entirely — the source already exists on disk where the caller pointed.
///
/// Callers that want Docker-compatibility should list every host file their
/// script needs (typically just the entrypoint) here. The Docker backend
/// will copy the file into its bind-mounted workdir as `<container_name>`
/// and rewrite any arg in `SandboxSpec::args` equal to `host_path` into
/// `/work/<container_name>` so a single spec works for both backends.
#[derive(Debug, Clone)]
pub struct StagedFile {
    /// Absolute host path of the source file.
    pub host_path: PathBuf,
    /// Name to give the file inside the container workdir (e.g. `tool.py`).
    /// Callers reference it by this name in `args` only if they chose to
    /// write container-relative args; when they pass the host path, Docker
    /// rewrites it automatically.
    pub container_name: String,
}

/// Description of a single Python execution request.
///
/// Fields mirror the subset of `Command` surface the existing forge callers
/// actually use. If a Docker backend later needs more (mounts, memory limits),
/// extend this struct — do not bolt on a second spec type.
#[derive(Debug, Clone)]
pub struct SandboxSpec {
    /// Arguments passed to `python3`. Example: `["-I", "/path/to/script.py"]`
    /// or `["-c", "...", "arg1"]`.
    pub args: Vec<String>,
    /// Working directory for the child. `None` means inherit the caller's cwd.
    pub cwd: Option<PathBuf>,
    /// Extra environment overrides. The child inherits the parent env and
    /// these values are layered on top.
    pub env: Vec<(String, String)>,
    /// Optional payload written to the child's stdin. `None` means the child
    /// gets a null stdin.
    pub stdin: Option<Vec<u8>>,
    /// Hard wall-clock timeout. `None` means no enforced timeout — matches
    /// call sites that today run `.output()` blocking.
    pub timeout: Option<Duration>,
    /// Files to stage into the sandbox workdir. Only consumed by container
    /// backends — `HostSandbox` ignores this field. See [`StagedFile`].
    pub staged_files: Vec<StagedFile>,
}

impl SandboxSpec {
    pub fn new<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            args: args.into_iter().map(Into::into).collect(),
            cwd: None,
            env: Vec::new(),
            stdin: None,
            timeout: None,
            staged_files: Vec::new(),
        }
    }

    pub fn cwd(mut self, path: impl AsRef<Path>) -> Self {
        self.cwd = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn stdin_bytes(mut self, bytes: Vec<u8>) -> Self {
        self.stdin = Some(bytes);
        self
    }

    pub fn timeout(mut self, dur: Duration) -> Self {
        self.timeout = Some(dur);
        self
    }

    /// Append a [`StagedFile`] to the spec. Host backends ignore these;
    /// Docker copies them into `/work` before launching the container.
    pub fn stage(mut self, file: StagedFile) -> Self {
        self.staged_files.push(file);
        self
    }
}

/// Rewrite any argument that equals one of the `staged` files' `host_path`
/// into its container-visible `/work/<container_name>` form.
///
/// This is the core of the "specs are portable across backends" contract:
/// callers pass host paths in `args` (which Host runs verbatim), and Docker
/// uses this helper to translate them before handing argv to the container.
/// Pulled out as a pure function so it is testable without a daemon.
fn rewrite_args_for_staged_files(args: &[String], staged: &[StagedFile]) -> Vec<String> {
    if staged.is_empty() {
        return args.to_vec();
    }
    args.iter()
        .map(|arg| {
            for file in staged {
                // Compare as paths so trailing-slash / separator quirks do
                // not cause a miss. Equality on the canonical string form
                // is what the caller controls; we accept that exact match.
                if Path::new(arg) == file.host_path.as_path() {
                    return format!("/work/{}", file.container_name);
                }
            }
            arg.clone()
        })
        .collect()
}

/// Captured result of a sandboxed execution.
#[derive(Debug, Clone)]
pub struct SandboxOutput {
    pub status: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub success: bool,
    pub backend: SandboxKind,
}

impl SandboxOutput {
    pub fn stdout_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }

    pub fn stderr_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stderr)
    }
}

/// Trait every sandbox backend implements.
///
/// Kept intentionally small. Streaming callsites, if any ever emerge, can add
/// a second trait or method — today every caller does batch capture, so this
/// single async method is sufficient.
#[async_trait]
pub trait Sandbox: Send + Sync {
    /// Run a Python invocation described by `spec`. The implementation is
    /// responsible for wiring stdin, capturing stdout/stderr, and enforcing
    /// any timeout.
    async fn run_python(&self, spec: &SandboxSpec) -> Result<SandboxOutput>;

    /// Which backend produced a given output — mostly for log tagging.
    fn kind(&self) -> SandboxKind;
}

/// Host-local backend. Preserves today's behavior exactly.
#[derive(Debug, Clone, Default)]
pub struct HostSandbox;

impl HostSandbox {
    pub fn new() -> Self {
        Self
    }

    /// Synchronous variant used by the protected-evolve Python validators,
    /// which are called from sync code paths that previously used
    /// `std::process::Command` directly. Preserves those semantics verbatim.
    pub fn run_python_blocking(&self, spec: &SandboxSpec) -> Result<SandboxOutput> {
        let mut command = StdCommand::new("python3");
        command
            .args(&spec.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }
        for (key, value) in &spec.env {
            command.env(key, value);
        }

        if let Some(bytes) = spec.stdin.as_ref() {
            command.stdin(Stdio::piped());
            let mut child = command.spawn().context("failed to spawn python3")?;
            if let Some(mut stdin) = child.stdin.take() {
                use std::io::Write as _;
                stdin
                    .write_all(bytes)
                    .context("failed to write python3 stdin")?;
            }
            let output = child
                .wait_with_output()
                .context("failed to wait for python3")?;
            Ok(SandboxOutput {
                status: output.status.code(),
                success: output.status.success(),
                stdout: output.stdout,
                stderr: output.stderr,
                backend: SandboxKind::Host,
            })
        } else {
            let output = command.output().context("failed to run python3")?;
            Ok(SandboxOutput {
                status: output.status.code(),
                success: output.status.success(),
                stdout: output.stdout,
                stderr: output.stderr,
                backend: SandboxKind::Host,
            })
        }
    }
}

#[async_trait]
impl Sandbox for HostSandbox {
    async fn run_python(&self, spec: &SandboxSpec) -> Result<SandboxOutput> {
        let mut command = tokio::process::Command::new("python3");
        command
            .args(&spec.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(cwd) = &spec.cwd {
            command.current_dir(cwd);
        }
        for (key, value) in &spec.env {
            command.env(key, value);
        }

        if spec.stdin.is_some() {
            command.stdin(Stdio::piped());
        } else {
            command.stdin(Stdio::null());
        }

        let mut child = command.spawn().context("failed to spawn python3")?;

        if let Some(bytes) = spec.stdin.as_ref() {
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(bytes)
                    .await
                    .context("failed to write python3 stdin")?;
                drop(stdin);
            }
        }

        let wait_future = child.wait_with_output();
        let output = match spec.timeout {
            Some(dur) => tokio::time::timeout(dur, wait_future)
                .await
                .map_err(|_| anyhow::anyhow!("python3 timed out after {:?}", dur))?
                .context("failed to wait for python3")?,
            None => wait_future.await.context("failed to wait for python3")?,
        };

        Ok(SandboxOutput {
            status: output.status.code(),
            success: output.status.success(),
            stdout: output.stdout,
            stderr: output.stderr,
            backend: SandboxKind::Host,
        })
    }

    fn kind(&self) -> SandboxKind {
        SandboxKind::Host
    }
}

/// Environment variable controlling which Docker image `DockerSandbox` uses.
/// Missing or empty falls back to [`DOCKER_DEFAULT_IMAGE`].
pub const DOCKER_IMAGE_ENV: &str = "NYX_SANDBOX_DOCKER_IMAGE";
/// Default container image when `NYX_SANDBOX_DOCKER_IMAGE` is unset.
pub const DOCKER_DEFAULT_IMAGE: &str = "python:3.11-slim";
/// Environment variable controlling the container network mode. Accepted
/// values: `none` (default) and `bridge`. Anything else falls back to `none`.
pub const DOCKER_NETWORK_ENV: &str = "NYX_SANDBOX_DOCKER_NETWORK";
/// Default network mode: no network reachable from inside the container.
pub const DOCKER_DEFAULT_NETWORK: &str = "none";

/// Docker-backed sandbox. Shells out to the `docker` CLI via
/// `tokio::process::Command`. No Rust Docker SDK is used.
///
/// The backend is configured entirely by env vars (see
/// [`DOCKER_IMAGE_ENV`] and [`DOCKER_NETWORK_ENV`]); nothing here is promoted
/// into `runtime::Config` yet, matching the Phase 9 non-goal list.
#[derive(Debug, Clone)]
pub struct DockerSandbox {
    image: String,
    network: String,
}

impl DockerSandbox {
    /// Build a sandbox using values from env (or the documented defaults).
    /// Construction never fails — values are normalized, not validated against
    /// the daemon. Use [`DockerSandbox::available`] for daemon probing.
    pub fn new() -> Self {
        let image = std::env::var(DOCKER_IMAGE_ENV)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DOCKER_DEFAULT_IMAGE.to_string());
        let network = std::env::var(DOCKER_NETWORK_ENV)
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .map(|s| match s.as_str() {
                "none" | "bridge" => s,
                _ => DOCKER_DEFAULT_NETWORK.to_string(),
            })
            .unwrap_or_else(|| DOCKER_DEFAULT_NETWORK.to_string());
        Self { image, network }
    }

    /// Explicit constructor for tests. Bypasses env reading.
    pub fn with_config(image: impl Into<String>, network: impl Into<String>) -> Self {
        Self {
            image: image.into(),
            network: network.into(),
        }
    }

    pub fn image(&self) -> &str {
        &self.image
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    /// Probe: spawn `docker version` with a short timeout. Returns `true` only
    /// if the binary is on `PATH` and exits zero within the window. Any other
    /// outcome (missing binary, non-zero exit, timeout, daemon down) is
    /// treated as unavailable — callers route to fallback.
    pub fn available() -> bool {
        // Quick synchronous probe via `std::process::Command` so this method
        // can be called from sync contexts (e.g. the factory) without forcing
        // callers onto a runtime.
        let mut cmd = StdCommand::new("docker");
        cmd.arg("version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .stdin(Stdio::null());
        match cmd.spawn() {
            Ok(mut child) => {
                // Poll for up to ~2s before giving up. The docker CLI exits
                // fast when the daemon is down; this is only a backstop.
                let deadline = std::time::Instant::now() + Duration::from_millis(2000);
                loop {
                    match child.try_wait() {
                        Ok(Some(status)) => return status.success(),
                        Ok(None) => {
                            if std::time::Instant::now() >= deadline {
                                let _ = child.kill();
                                let _ = child.wait();
                                return false;
                            }
                            std::thread::sleep(Duration::from_millis(50));
                        }
                        Err(_) => {
                            let _ = child.kill();
                            let _ = child.wait();
                            return false;
                        }
                    }
                }
            }
            Err(_) => false,
        }
    }
}

impl Default for DockerSandbox {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sandbox for DockerSandbox {
    async fn run_python(&self, spec: &SandboxSpec) -> Result<SandboxOutput> {
        // Create a host workdir that will bind-mount as /work in the
        // container. We root it under `$HOME` rather than `$TMPDIR` because
        // Colima/Lima's virtiofs only shares `$HOME` by default — files
        // written under `/var/folders/.../T/` (macOS's per-user temp) are
        // invisible to the VM and therefore to the container. Rooting under
        // `$HOME` works on Docker Desktop and Colima both. Caller can
        // override by setting `NYX_SANDBOX_WORKDIR_ROOT`.
        let workdir_root = std::env::var("NYX_SANDBOX_WORKDIR_ROOT")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".nyx-sandbox")))
            .unwrap_or_else(std::env::temp_dir);
        let workdir_host = workdir_root.join(format!(
            "nyx_docker_sandbox_{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&workdir_host)
            .with_context(|| format!("failed to create sandbox workdir {:?}", workdir_host))?;
        // RAII guard: nuke the tempdir on the way out, success or failure.
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(workdir_host.clone());

        // Stage any caller-provided files into the bind-mounted workdir
        // BEFORE spawning the container. Copy (not symlink) so the
        // container sees a real file at the expected path regardless of
        // host mount semantics.
        for file in &spec.staged_files {
            let dest = workdir_host.join(&file.container_name);
            std::fs::copy(&file.host_path, &dest).with_context(|| {
                format!(
                    "failed to stage {:?} -> {:?} for docker sandbox",
                    file.host_path, dest
                )
            })?;
        }

        // Rewrite args so any arg matching a staged host_path becomes the
        // container-local `/work/<container_name>`. Keeps the same
        // `SandboxSpec` usable for Host (host paths) and Docker (rewritten
        // to container paths) without forcing callers to pick one dialect.
        let rewritten_args = rewrite_args_for_staged_files(&spec.args, &spec.staged_files);

        // Unique container name so we can `docker kill` on timeout without
        // racing any other sandbox invocation.
        let container_name = format!("nyx-sandbox-{}", uuid::Uuid::new_v4().simple());

        // Build the `docker run` argv. Order: `docker run <flags> <image> python3 <spec.args>`.
        let mut docker_args: Vec<String> =
            Vec::with_capacity(32 + spec.args.len() + spec.env.len() * 2);
        docker_args.push("run".into());
        docker_args.push("--rm".into());
        docker_args.push("--name".into());
        docker_args.push(container_name.clone());
        docker_args.push(format!("--network={}", self.network));
        docker_args.push("--memory=512m".into());
        docker_args.push("--pids-limit=256".into());
        docker_args.push("-w".into());
        docker_args.push("/work".into());
        // Bind-mount tempdir as /work. Read-write so scripts that write
        // artifacts (common for verifier flows) still work.
        docker_args.push("-v".into());
        docker_args.push(format!("{}:/work", workdir_host.display()));

        // Stdin attached? docker needs `-i` to plumb a pipe in.
        if spec.stdin.is_some() {
            docker_args.push("-i".into());
        }

        // Env overrides from the spec. Keys are not leaked by default — only
        // values the caller explicitly passed are forwarded.
        for (key, value) in &spec.env {
            docker_args.push("-e".into());
            docker_args.push(format!("{}={}", key, value));
        }

        docker_args.push(self.image.clone());
        docker_args.push("python3".into());
        for a in &rewritten_args {
            docker_args.push(a.clone());
        }

        tracing::debug!(
            container = %container_name,
            image = %self.image,
            network = %self.network,
            "docker sandbox: spawning container"
        );

        let mut command = tokio::process::Command::new("docker");
        command
            .args(&docker_args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if spec.stdin.is_some() {
            command.stdin(Stdio::piped());
        } else {
            command.stdin(Stdio::null());
        }
        if let Some(cwd) = &spec.cwd {
            // cwd for the *docker* process itself — the container's working
            // directory is always /work, set above.
            command.current_dir(cwd);
        }

        let mut child = command
            .spawn()
            .context("failed to spawn `docker run`; is the CLI on PATH?")?;

        if let Some(bytes) = spec.stdin.as_ref() {
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(bytes)
                    .await
                    .context("failed to write stdin to docker container")?;
                drop(stdin);
            }
        }

        let wait_future = child.wait_with_output();
        let output = match spec.timeout {
            Some(dur) => match tokio::time::timeout(dur, wait_future).await {
                Ok(res) => res.context("failed to wait for docker container")?,
                Err(_elapsed) => {
                    // The wait future moved `child`, so we can't kill it via
                    // the handle anymore. Kill by name — same effect.
                    tracing::warn!(
                        container = %container_name,
                        "docker sandbox: timeout after {:?}; issuing `docker kill`",
                        dur
                    );
                    let _ = tokio::process::Command::new("docker")
                        .arg("kill")
                        .arg(&container_name)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .await;
                    return Err(anyhow::anyhow!("docker sandbox timed out after {:?}", dur));
                }
            },
            None => wait_future
                .await
                .context("failed to wait for docker container")?,
        };

        Ok(SandboxOutput {
            status: output.status.code(),
            success: output.status.success(),
            stdout: output.stdout,
            stderr: output.stderr,
            backend: SandboxKind::Docker,
        })
    }

    fn kind(&self) -> SandboxKind {
        SandboxKind::Docker
    }
}

/// Factory: select a sandbox backend.
///
/// If the caller asks for `Docker` but it is not available, log a warning and
/// return a `HostSandbox` instead. This is the loud-fallback behavior called
/// out in the Phase 9 decision doc.
pub fn build_sandbox(kind: SandboxKind) -> Arc<dyn Sandbox> {
    match kind {
        SandboxKind::Host => Arc::new(HostSandbox::new()),
        SandboxKind::Docker => {
            if DockerSandbox::available() {
                let sandbox = DockerSandbox::new();
                tracing::info!(
                    "sandbox: using Docker backend (image={}, network={})",
                    sandbox.image(),
                    sandbox.network()
                );
                Arc::new(sandbox)
            } else {
                tracing::warn!(
                    "SandboxKind::Docker requested but Docker is unavailable on this host; falling back to HostSandbox"
                );
                Arc::new(HostSandbox::new())
            }
        }
    }
}

/// Convenience: build the sandbox indicated by `NYX_SANDBOX_KIND` (or `Host`).
pub fn build_sandbox_from_env() -> Arc<dyn Sandbox> {
    build_sandbox(SandboxKind::from_env())
}

/// Shared singleton used by forge call sites. Built lazily from
/// `NYX_SANDBOX_KIND` on first access. Tests can still construct their own
/// `HostSandbox` directly if they need isolation.
pub fn shared_sandbox() -> Arc<dyn Sandbox> {
    use std::sync::OnceLock;
    static INSTANCE: OnceLock<Arc<dyn Sandbox>> = OnceLock::new();
    INSTANCE.get_or_init(build_sandbox_from_env).clone()
}

/// Shared host-only sandbox for sync callers that cannot cross the async
/// boundary cleanly (see `protected_evolve::validate_python_*`). This is
/// deliberately a concrete `HostSandbox` rather than `dyn Sandbox`: the
/// blocking path is a `HostSandbox` feature, not a trait contract.
pub fn shared_host_sandbox() -> &'static HostSandbox {
    use std::sync::OnceLock;
    static INSTANCE: OnceLock<HostSandbox> = OnceLock::new();
    INSTANCE.get_or_init(HostSandbox::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn host_sandbox_runs_trivial_python() {
        let sandbox = HostSandbox::new();
        let spec = SandboxSpec::new(["-c", "print('hi')"]);
        let output = sandbox
            .run_python(&spec)
            .await
            .expect("host sandbox runs python3");
        assert!(
            output.success,
            "expected success, got status {:?}",
            output.status
        );
        assert_eq!(output.stdout_str().trim(), "hi");
        assert_eq!(output.backend, SandboxKind::Host);
        assert_eq!(sandbox.kind(), SandboxKind::Host);
    }

    #[tokio::test]
    async fn host_sandbox_forwards_stdin() {
        let sandbox = HostSandbox::new();
        let spec = SandboxSpec::new(["-c", "import sys; sys.stdout.write(sys.stdin.read())"])
            .stdin_bytes(b"hello-stdin".to_vec());
        let output = sandbox
            .run_python(&spec)
            .await
            .expect("host sandbox runs python3 with stdin");
        assert!(output.success);
        assert_eq!(output.stdout_str().as_ref(), "hello-stdin");
    }

    #[test]
    fn host_sandbox_blocking_captures_output() {
        let sandbox = HostSandbox::new();
        let spec = SandboxSpec::new(["-c", "print('sync-hi')"]);
        let output = sandbox
            .run_python_blocking(&spec)
            .expect("blocking sandbox runs python3");
        assert!(output.success);
        assert_eq!(output.stdout_str().trim(), "sync-hi");
    }

    #[tokio::test]
    async fn host_sandbox_ignores_staged_files() {
        // HostSandbox must pass through unchanged when a caller attaches a
        // StagedFile: the host already has the file on disk, and the argv
        // still references it by host path. The staged list should never
        // influence host execution.
        let tmp = std::env::temp_dir().join(format!(
            "nyx_host_stage_test_{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&tmp).expect("mk tmpdir");
        let script_path = tmp.join("tool.py");
        std::fs::write(&script_path, "print('host-staged-ok')").expect("write script");

        let sandbox = HostSandbox::new();
        let spec = SandboxSpec::new(["-I".to_string(), script_path.to_string_lossy().to_string()])
            .cwd(&tmp)
            .stage(StagedFile {
                // Point at a nonexistent path to prove HostSandbox never
                // touches the staging list — a copy would fail here.
                host_path: PathBuf::from("/nonexistent/host/source.py"),
                container_name: "tool.py".into(),
            });
        let output = sandbox
            .run_python(&spec)
            .await
            .expect("host sandbox runs script ignoring staged files");
        assert!(
            output.success,
            "expected success, got status {:?}",
            output.status
        );
        assert_eq!(output.stdout_str().trim(), "host-staged-ok");
        assert_eq!(output.backend, SandboxKind::Host);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn spec_stage_builder_appends() {
        let spec = SandboxSpec::new(["-c", "pass"])
            .stage(StagedFile {
                host_path: PathBuf::from("/a.py"),
                container_name: "a.py".into(),
            })
            .stage(StagedFile {
                host_path: PathBuf::from("/b.py"),
                container_name: "b.py".into(),
            });
        assert_eq!(spec.staged_files.len(), 2);
        assert_eq!(spec.staged_files[0].container_name, "a.py");
        assert_eq!(spec.staged_files[1].container_name, "b.py");
        assert_eq!(spec.staged_files[0].host_path, PathBuf::from("/a.py"));
        assert_eq!(spec.staged_files[1].host_path, PathBuf::from("/b.py"));
    }

    #[test]
    fn sandbox_spec_rewrite_arg_for_staged_file() {
        // Matched host path should rewrite to `/work/<container_name>`.
        let staged = vec![StagedFile {
            host_path: PathBuf::from("/abs/host/path/tool.py"),
            container_name: "tool.py".into(),
        }];
        let args = vec!["-I".to_string(), "/abs/host/path/tool.py".to_string()];
        let rewritten = rewrite_args_for_staged_files(&args, &staged);
        assert_eq!(
            rewritten,
            vec!["-I".to_string(), "/work/tool.py".to_string()]
        );
    }

    #[test]
    fn sandbox_spec_rewrite_arg_leaves_non_matching_unchanged() {
        // Non-matching args pass through untouched. Empty staged list is a
        // no-op (covers the hot path in HostSandbox where staged is empty).
        let staged = vec![StagedFile {
            host_path: PathBuf::from("/abs/host/path/tool.py"),
            container_name: "tool.py".into(),
        }];
        let args = vec![
            "-I".to_string(),
            "-c".to_string(),
            "print('hi')".to_string(),
        ];
        let rewritten = rewrite_args_for_staged_files(&args, &staged);
        assert_eq!(rewritten, args);

        let empty_staged: Vec<StagedFile> = Vec::new();
        let rewritten_empty = rewrite_args_for_staged_files(&args, &empty_staged);
        assert_eq!(rewritten_empty, args);
    }

    #[tokio::test]
    async fn docker_requested_without_backend_falls_back_to_host() {
        // Only meaningful when Docker is NOT installed on this host — on a
        // Docker-having machine the factory correctly returns the real
        // Docker backend, which is a separate (ignored) integration path.
        if DockerSandbox::available() {
            eprintln!(
                "skipping docker_requested_without_backend_falls_back_to_host: \
                 Docker is available on this host, fallback behavior cannot be exercised"
            );
            return;
        }
        let sandbox = build_sandbox(SandboxKind::Docker);
        assert_eq!(sandbox.kind(), SandboxKind::Host);
        let spec = SandboxSpec::new(["-c", "print('fallback-ok')"]);
        let output = sandbox
            .run_python(&spec)
            .await
            .expect("fallback sandbox runs python3");
        assert!(output.success);
        assert_eq!(output.stdout_str().trim(), "fallback-ok");
        assert_eq!(output.backend, SandboxKind::Host);
    }

    #[test]
    fn sandbox_kind_parses_env_values() {
        assert_eq!(SandboxKind::from_str_lossy("host"), SandboxKind::Host);
        assert_eq!(SandboxKind::from_str_lossy("HOST"), SandboxKind::Host);
        assert_eq!(SandboxKind::from_str_lossy("docker"), SandboxKind::Docker);
        // Unknown falls back to Host, never panics.
        assert_eq!(SandboxKind::from_str_lossy("k8s"), SandboxKind::Host);
        assert_eq!(SandboxKind::from_str_lossy(""), SandboxKind::Host);
    }

    // --- Docker backend tests ------------------------------------------------
    //
    // The tests in this block split into two groups:
    //
    //   1. Tests that run unconditionally on this host. They exercise the
    //      availability probe, the construction defaults, and the factory
    //      fallback path. They must pass on machines without Docker.
    //   2. Tests marked `#[ignore = "requires docker daemon"]`. They spawn
    //      real containers and must only be run on a machine with a working
    //      `docker` CLI + daemon. Run them with:
    //
    //          cargo test --test module_test -- --ignored
    //          # or, for these in-crate tests:
    //          cargo test --lib -- --ignored
    //
    //      Exact invocation depends on where the tests live after future
    //      reorganization; `-- --ignored` is the constant part.

    #[test]
    fn docker_sandbox_available_returns_false_when_cli_missing() {
        // This host has no `docker` binary on PATH; the probe must report
        // that honestly. If this ever flips to `true` on a dev machine with
        // docker installed, that's expected — the assertion below is gated
        // on the presence of the binary.
        let docker_on_path = StdCommand::new("docker")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .stdin(Stdio::null())
            .status()
            .is_ok();
        if !docker_on_path {
            assert!(
                !DockerSandbox::available(),
                "DockerSandbox::available() must be false when `docker` is not on PATH"
            );
        }
    }

    #[test]
    fn docker_sandbox_new_applies_defaults_when_env_unset() {
        // We cannot assume the env is clean (other tests may set vars),
        // so we build explicitly via `with_config` AND sanity-check that
        // the documented constants line up with what `new()` uses when env
        // is missing. This validates construction without touching a daemon.
        let explicit = DockerSandbox::with_config(DOCKER_DEFAULT_IMAGE, DOCKER_DEFAULT_NETWORK);
        assert_eq!(explicit.image(), "python:3.11-slim");
        assert_eq!(explicit.network(), "none");
        assert_eq!(explicit.kind(), SandboxKind::Docker);

        // Document the env var names by name so the build breaks if anyone
        // renames them without updating the decision doc.
        assert_eq!(DOCKER_IMAGE_ENV, "NYX_SANDBOX_DOCKER_IMAGE");
        assert_eq!(DOCKER_NETWORK_ENV, "NYX_SANDBOX_DOCKER_NETWORK");
    }

    #[test]
    fn docker_sandbox_with_config_roundtrips_values() {
        let s = DockerSandbox::with_config("my/image:tag", "bridge");
        assert_eq!(s.image(), "my/image:tag");
        assert_eq!(s.network(), "bridge");
    }

    // --- Ignored integration tests: require Docker daemon --------------------

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    // Run with: `cargo test -- --ignored docker_sandbox_runs_trivial_python`
    async fn docker_sandbox_runs_trivial_python() {
        let sandbox = DockerSandbox::new();
        assert_eq!(sandbox.kind(), SandboxKind::Docker);
        let spec = SandboxSpec::new(["-c", "print('hi')"]).timeout(Duration::from_secs(60));
        let output = sandbox
            .run_python(&spec)
            .await
            .expect("docker sandbox runs python3");
        assert!(
            output.success,
            "expected success, got status {:?}",
            output.status
        );
        assert!(
            output.stdout_str().contains("hi"),
            "expected stdout to contain 'hi', got {:?}",
            output.stdout_str()
        );
        assert_eq!(output.backend, SandboxKind::Docker);
    }

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    // Run with: `cargo test -- --ignored docker_sandbox_network_is_none_by_default`
    async fn docker_sandbox_network_is_none_by_default() {
        // Default construction must yield `--network=none`; a raw TCP
        // connect to 8.8.8.8:53 must therefore fail from inside the
        // container. This is the containment boundary Phase 9's acceptance
        // criteria call out.
        let sandbox = DockerSandbox::new();
        assert_eq!(sandbox.network(), "none");
        let script = r#"
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(2)
try:
    s.connect(('8.8.8.8', 53))
    print('REACHED_NETWORK')
    sys.exit(0)
except Exception as e:
    print('blocked: ' + type(e).__name__)
    sys.exit(3)
"#;
        let spec = SandboxSpec::new(["-c", script]).timeout(Duration::from_secs(30));
        let output = sandbox
            .run_python(&spec)
            .await
            .expect("docker sandbox runs network test");
        assert!(
            !output.stdout_str().contains("REACHED_NETWORK"),
            "network should be blocked by default, got stdout={:?}",
            output.stdout_str()
        );
        assert_ne!(output.status, Some(0), "script should have exited non-zero");
    }

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    // Run with: `cargo test -- --ignored docker_sandbox_memory_limit_enforced`
    async fn docker_sandbox_memory_limit_enforced() {
        // --memory=512m is set by default. A script that tries to allocate
        // well beyond that should be OOM-killed. We assert *some* failure;
        // the exact exit code depends on kernel/cgroup behavior.
        let sandbox = DockerSandbox::new();
        let script = r#"
# Try to allocate ~2 GiB of real bytes.
buf = bytearray(2 * 1024 * 1024 * 1024)
print(len(buf))
"#;
        let spec = SandboxSpec::new(["-c", script]).timeout(Duration::from_secs(60));
        let output = sandbox
            .run_python(&spec)
            .await
            .expect("docker sandbox runs memory test");
        assert!(
            !output.success,
            "expected OOM/failure under --memory=512m, got success"
        );
    }

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    // Run with: `cargo test -- --ignored docker_sandbox_timeout_kills_container`
    async fn docker_sandbox_timeout_kills_container() {
        // A script that sleeps longer than the wall-clock timeout must
        // trigger `docker kill` and return a timeout error.
        let sandbox = DockerSandbox::new();
        let spec =
            SandboxSpec::new(["-c", "import time; time.sleep(30)"]).timeout(Duration::from_secs(2));
        let result = sandbox.run_python(&spec).await;
        assert!(result.is_err(), "expected timeout error, got Ok");
    }

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    // Run with: `cargo test -- --ignored docker_sandbox_runs_staged_file`
    //
    // End-to-end check for the StagedFile mechanism: write a small script
    // to a host tempdir, hand it to the sandbox as a staged file whose
    // `host_path` also appears literally in argv, and confirm the Docker
    // backend both copies the file into /work and rewrites the argv entry
    // to `/work/<container_name>` so `python3 -I /work/tool.py` succeeds.
    async fn docker_sandbox_runs_staged_file() {
        let tmp = std::env::temp_dir().join(format!(
            "nyx_docker_stage_test_{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&tmp).expect("mk tmpdir");
        let script_path = tmp.join("tool.py");
        std::fs::write(&script_path, "print('docker-staged-ok')").expect("write script");

        let sandbox = DockerSandbox::new();
        let spec = SandboxSpec::new([
            "-I".to_string(),
            // Host path deliberately — the Docker backend is supposed to
            // rewrite this to `/work/tool.py` via the staged_files list.
            script_path.to_string_lossy().to_string(),
        ])
        .timeout(Duration::from_secs(60))
        .stage(StagedFile {
            host_path: script_path.clone(),
            container_name: "tool.py".into(),
        });

        let output = sandbox
            .run_python(&spec)
            .await
            .expect("docker sandbox runs staged python file");
        assert!(
            output.success,
            "expected success, got status {:?} stderr={:?}",
            output.status,
            output.stderr_str()
        );
        assert!(
            output.stdout_str().contains("docker-staged-ok"),
            "expected stdout to contain 'docker-staged-ok', got {:?}",
            output.stdout_str()
        );
        assert_eq!(output.backend, SandboxKind::Docker);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    #[ignore = "requires docker daemon"]
    // Run with: `cargo test --lib -- --ignored docker_sandbox_contains_malicious_tool`
    async fn docker_sandbox_contains_malicious_tool() {
        // A deliberately malicious script tries three escape vectors. Each
        // should be contained by the default sandbox flags (`--network=none`,
        // bind-mount-only-workdir, ephemeral container FS):
        //   1. Read a macOS host path (/Users/...) — container is Linux, path
        //      does not exist inside the container, must be absent.
        //   2. Write to the host filesystem outside the bind-mounted /work —
        //      container's /tmp is its own, not the host's. Host must remain
        //      untouched.
        //   3. List the host home directory — likewise must not be visible.
        let sandbox = DockerSandbox::new();
        let host_canary_path = "/tmp/nyx_sandbox_escape_canary";
        // Make sure the canary does not pre-exist on the host.
        let _ = std::fs::remove_file(host_canary_path);

        let script = r#"
import os, json
results = {}
# 1. Host path (macOS-style) should not exist inside a Linux container.
results['users_exists'] = os.path.exists('/Users')
# 2. Attempt to write a canary to /tmp. Inside the container this writes to
#    the container's own ephemeral tmp, not the host tmp. We verify on the
#    host side that the canary never appears there.
try:
    with open('/tmp/nyx_sandbox_escape_canary', 'w') as f:
        f.write('pwned')
    results['tmp_write'] = 'wrote'
except Exception as e:
    results['tmp_write'] = 'blocked:' + type(e).__name__
# 3. Try to list a macOS home dir — not mounted into the container.
try:
    os.listdir('/Users/vduox')
    results['home_list'] = 'leaked'
except Exception as e:
    results['home_list'] = 'blocked:' + type(e).__name__
print(json.dumps(results))
"#;
        let spec = SandboxSpec::new(["-c", script]).timeout(Duration::from_secs(30));
        let output = sandbox
            .run_python(&spec)
            .await
            .expect("sandbox runs malicious probe script");
        assert!(
            output.success,
            "probe script should execute to completion (and fail to escape); status={:?} stderr={:?}",
            output.status,
            output.stderr_str()
        );
        let results: serde_json::Value =
            serde_json::from_str(&output.stdout_str()).expect("probe script should emit JSON");

        assert_eq!(
            results["users_exists"].as_bool(),
            Some(false),
            "Linux container must not see a /Users directory"
        );
        assert_ne!(
            results["home_list"].as_str(),
            Some("leaked"),
            "Host home directory must not be listable from the container"
        );
        assert!(
            !std::path::Path::new(host_canary_path).exists(),
            "host /tmp was written to from inside the container — escape detected"
        );
    }
}
