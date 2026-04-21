//! Tool executor — runs Python scripts with OS sandboxing where it is reliable.
//!
//! Sandbox-preferred tools can run inside macOS sandbox-exec with:
//! - Filesystem: secret reads and protected core writes denied
//! - Network: denied for non-network tools
//! - Resources: 30s CPU, 512MB RAM, no forking via rlimit
//! - Python: --isolated mode (ignores PYTHONPATH, user site-packages)
//!
//! Some tools stay direct because they rely on their own tighter guardrails,
//! external CLIs, or site-packages. Sandbox profiles are generated at runtime
//! with the actual project path, so clones to any directory work without
//! modification.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Built-in tool descriptions used for self-model/capability awareness.
pub const BUILTIN_TOOL_CAPABILITIES: &[(&str, &str)] = &[
    ("weather", "Get current weather for a city"),
    ("web_search", "Search the web for current information"),
    (
        "git_info",
        "Inspect repository status, log, TODOs, and diffs",
    ),
    ("github", "Check GitHub notifications, PRs, and issues"),
    ("vision", "Capture and describe the user's screen"),
    ("gif", "Search for a GIF"),
    (
        "file_ops",
        "Read allowed files and perform bounded file operations",
    ),
    ("twitter", "Read or post Twitter/X activity"),
    ("image_gen", "Generate images from text prompts"),
    (
        "computer_use",
        "Drive the computer through the dedicated tool",
    ),
    ("transcribe", "Transcribe speech or audio"),
    (
        "browser",
        "Open a URL and read its content, links, or screenshot via Playwright",
    ),
];

/// Location of the `browser` builtin's Python entrypoint. Ships alongside
/// the other bundled Python tools in `tools/`. Requires a host-side
/// `pip install playwright && playwright install chromium` to actually run.
const BROWSER_TOOL_SCRIPT: &str = "tools/browser.py";

/// Commands accepted by `tools/browser.py`. Duplicated from the Python side
/// on purpose — the Rust handler validates the command field before touching
/// the sandbox so a typoed command fails fast with a structured error
/// instead of launching Playwright only to have it bail.
const BROWSER_COMMANDS: &[&str] = &[
    "navigate",
    "extract_text",
    "extract_links",
    "screenshot",
    "fetch_html",
    "interact",
    "session_open",
    "session_step",
    "session_close",
    "session_list",
    "session_list_saved",
    "session_restore",
    "jar_list",
];

/// Session-mode commands. These bypass the forge one-shot path and route to
/// the long-running `tools/browser_daemon.py` via `BrowserSessionManager`.
const BROWSER_SESSION_COMMANDS: &[&str] = &[
    "session_open",
    "session_step",
    "session_close",
    "session_list",
    "session_list_saved",
    "session_restore",
    "jar_list",
];

/// Commands that may carry a `jar` field. Only `session_open` actually uses it;
/// every other command should reject a stray `jar` so the wire surface stays
/// tight and typos on the caller side surface as validation errors instead of
/// silently-ignored fields.
const BROWSER_COMMANDS_ACCEPTING_JAR: &[&str] = &["session_open"];

/// Regex for jar names: lowercase alnum, underscore, dash; 1-40 chars.
/// Kept as a `OnceLock<Regex>` so we compile it once.
fn jar_name_regex() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^[a-z0-9_-]{1,40}$").expect("jar name regex compiles"))
}

pub(crate) fn is_browser_session_command(cmd: &str) -> bool {
    BROWSER_SESSION_COMMANDS.contains(&cmd)
}

/// Tools that need outbound HTTP access.
const NETWORK_TOOLS: &[&str] = &[
    "web_search",
    "weather",
    "github",
    "twitter",
    "gif",
    "image_gen",
    "browser",
];

/// Tools that should use the OS wrapper when the host can support it.
const SANDBOX_PREFERRED_TOOLS: &[&str] = &["file_ops", "weather", "web_search"];

/// Tools that always run directly because they need broader local process
/// access, external CLIs, or third-party Python packages that should not be
/// forced through the generated macOS profile.
const DIRECT_TOOLS: &[&str] = &[
    "git_info",
    "github",
    "twitter",
    "gif",
    "image_gen",
    "vision",
    "computer_use",
    "transcribe",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OsSandboxMode {
    Auto,
    Off,
    Force,
}

impl OsSandboxMode {
    fn current() -> Self {
        match std::env::var("NYX_OS_SANDBOX_MODE")
            .unwrap_or_else(|_| "auto".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "off" | "0" | "false" => Self::Off,
            "force" | "on" | "1" | "true" => Self::Force,
            _ => Self::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PythonIsolationMode {
    Standard,
    Isolated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ToolExecutionPolicy {
    sandboxed: bool,
    isolation: PythonIsolationMode,
}

#[derive(Debug, Clone)]
struct OsSandboxProbe {
    available: bool,
    detail: String,
}

impl OsSandboxProbe {
    fn available(detail: impl Into<String>) -> Self {
        Self {
            available: true,
            detail: detail.into(),
        }
    }

    fn unavailable(detail: impl Into<String>) -> Self {
        Self {
            available: false,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolRuntimeStatus {
    pub kind: String,
    pub name: String,
    pub description: String,
    pub ready: bool,
    pub status: String,
    pub issue: Option<String>,
    pub requires_network: bool,
    pub sandboxed: bool,
    pub filename: Option<String>,
    pub command: Option<String>,
    pub server_name: Option<String>,
    pub source: Option<String>,
    pub quarantined_until: Option<String>,
}

pub fn executable_available(program: &str) -> bool {
    let program = program.trim();
    if program.is_empty() {
        return false;
    }

    let path = Path::new(program);
    if path.is_absolute() {
        return path.is_file();
    }
    if program.contains(std::path::MAIN_SEPARATOR) {
        return path.is_file();
    }

    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    for entry in std::env::split_paths(&paths) {
        if entry.join(program).is_file() {
            return true;
        }
    }
    false
}

fn tool_prefers_os_sandbox(name: &str) -> bool {
    SANDBOX_PREFERRED_TOOLS.contains(&name)
}

fn tool_runs_direct(name: &str) -> bool {
    DIRECT_TOOLS.contains(&name)
}

fn preferred_tool_should_use_sandbox(mode: OsSandboxMode, sandbox_available: bool) -> bool {
    match mode {
        OsSandboxMode::Off => false,
        OsSandboxMode::Auto => sandbox_available,
        OsSandboxMode::Force => true,
    }
}

fn format_exit_status(status: std::process::ExitStatus) -> String {
    match status.code() {
        Some(code) => format!("exit code {code}"),
        None => "signal termination".to_string(),
    }
}

fn detect_os_sandbox_probe() -> OsSandboxProbe {
    if !cfg!(target_os = "macos") {
        return OsSandboxProbe::unavailable("macOS sandbox-exec is only available on macOS hosts");
    }
    if !executable_available("sandbox-exec") {
        return OsSandboxProbe::unavailable("sandbox-exec is not available in PATH");
    }
    let Ok(cwd) = std::env::current_dir() else {
        return OsSandboxProbe::unavailable("could not resolve current working directory");
    };
    let project_dir = cwd.to_str().unwrap_or(".");
    let home_dir = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let profile = generate_sandbox_profile(project_dir, &home_dir, false);
    let sandbox_dir = cwd.join("sandbox");
    if let Err(error) = std::fs::create_dir_all(&sandbox_dir) {
        return OsSandboxProbe::unavailable(format!(
            "could not create sandbox probe directory: {error}"
        ));
    }

    let profile_path = sandbox_dir.join(format!(
        ".sandbox_probe_{}_{}.sb",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    if let Err(error) = std::fs::write(&profile_path, profile) {
        return OsSandboxProbe::unavailable(format!(
            "could not write sandbox probe profile: {error}"
        ));
    }

    let status = std::process::Command::new("sandbox-exec")
        .args(["-f", profile_path.to_str().unwrap_or(""), "/usr/bin/true"])
        .status();
    std::fs::remove_file(&profile_path).ok();

    match status {
        Ok(status) if status.success() => {
            OsSandboxProbe::available("sandbox-exec probe passed with generated Nyx profile")
        }
        Ok(status) => OsSandboxProbe::unavailable(format!(
            "generated sandbox profile failed with {}",
            format_exit_status(status)
        )),
        Err(error) => {
            OsSandboxProbe::unavailable(format!("generated sandbox profile probe failed: {error}"))
        }
    }
}

fn os_sandbox_probe() -> &'static OsSandboxProbe {
    static OS_SANDBOX_PROBE: OnceLock<OsSandboxProbe> = OnceLock::new();
    OS_SANDBOX_PROBE.get_or_init(detect_os_sandbox_probe)
}

fn sandbox_force_issue(name: &str) -> Option<String> {
    if !tool_prefers_os_sandbox(name) || OsSandboxMode::current() != OsSandboxMode::Force {
        return None;
    }

    let probe = os_sandbox_probe();
    if probe.available {
        None
    } else {
        Some(format!(
            "NYX_OS_SANDBOX_MODE=force requested for {}, but {}",
            name, probe.detail
        ))
    }
}

fn tool_execution_policy_with_sandbox(
    name: &str,
    mode: OsSandboxMode,
    sandbox_available: bool,
) -> ToolExecutionPolicy {
    match name {
        "file_ops" | "weather" => ToolExecutionPolicy {
            sandboxed: preferred_tool_should_use_sandbox(mode, sandbox_available),
            isolation: PythonIsolationMode::Isolated,
        },
        "web_search" => ToolExecutionPolicy {
            sandboxed: preferred_tool_should_use_sandbox(mode, sandbox_available),
            isolation: PythonIsolationMode::Standard,
        },
        _ if tool_runs_direct(name) => ToolExecutionPolicy {
            sandboxed: false,
            isolation: PythonIsolationMode::Standard,
        },
        _ => ToolExecutionPolicy {
            sandboxed: false,
            isolation: PythonIsolationMode::Standard,
        },
    }
}

fn tool_execution_policy(name: &str) -> ToolExecutionPolicy {
    tool_execution_policy_with_sandbox(name, OsSandboxMode::current(), os_sandbox_probe().available)
}

fn command_description(script_path: &Path, policy: ToolExecutionPolicy) -> String {
    let base = if matches!(policy.isolation, PythonIsolationMode::Isolated) {
        format!("python3 -I {}", script_path.to_string_lossy())
    } else {
        format!("python3 {}", script_path.to_string_lossy())
    };
    if policy.sandboxed {
        format!("sandbox-exec -f <generated-profile> {base}")
    } else {
        base
    }
}

pub fn builtin_tool_runtime_statuses() -> Vec<ToolRuntimeStatus> {
    BUILTIN_TOOL_CAPABILITIES
        .iter()
        .map(|(name, description)| builtin_tool_runtime_status(name, description))
        .collect()
}

/// Absolute location on disk of a builtin tool's Python entrypoint.
/// Every builtin lives at `tools/<name>.py`.
fn builtin_tool_script_path(name: &str) -> PathBuf {
    PathBuf::from(format!("tools/{}.py", name))
}

fn builtin_tool_runtime_status(name: &str, description: &str) -> ToolRuntimeStatus {
    let script_path = builtin_tool_script_path(name);
    let policy = tool_execution_policy(name);
    let requires_network = NETWORK_TOOLS.contains(&name);
    let issue = builtin_tool_issue(name, &script_path);
    let status = if issue.is_some() { "blocked" } else { "ready" };

    ToolRuntimeStatus {
        kind: "builtin".to_string(),
        name: name.to_string(),
        description: description.to_string(),
        ready: issue.is_none(),
        status: status.to_string(),
        issue,
        requires_network,
        sandboxed: policy.sandboxed,
        filename: Some(script_path.to_string_lossy().to_string()),
        command: Some(command_description(&script_path, policy)),
        server_name: None,
        source: Some("nyx_builtin".to_string()),
        quarantined_until: None,
    }
}

fn builtin_tool_issue(name: &str, script_path: &Path) -> Option<String> {
    if !script_path.is_file() {
        return Some(format!("tool script missing at {}", script_path.display()));
    }
    if let Some(issue) = sandbox_force_issue(name) {
        return Some(issue);
    }

    match name {
        "github" if !executable_available("gh") => {
            Some("gh CLI not found in PATH".to_string())
        }
        "twitter" if !twitter_ready() => Some(
            "twitter needs workspace/twitter_cookies.json or NYX_TWITTER_USERNAME and NYX_TWITTER_PASSWORD"
                .to_string(),
        ),
        "image_gen" if env_or_dotenv_value("NYX_NIM_API_KEY").is_none() => {
            Some("NYX_NIM_API_KEY is not configured".to_string())
        }
        "vision" if env_or_dotenv_value("NYX_NIM_API_KEY").is_none() => {
            Some("NYX_NIM_API_KEY is not configured".to_string())
        }
        "computer_use" if env_or_dotenv_value("NYX_ANTHROPIC_API_KEY").is_none() => {
            Some("NYX_ANTHROPIC_API_KEY is not configured".to_string())
        }
        "transcribe" if !executable_available("whisper") && !executable_available("whisper-cpp") => {
            Some("no whisper CLI found in PATH".to_string())
        }
        _ => None,
    }
}

fn twitter_ready() -> bool {
    if Path::new("workspace/twitter_cookies.json").is_file() {
        return true;
    }
    env_or_dotenv_value("NYX_TWITTER_USERNAME").is_some()
        && env_or_dotenv_value("NYX_TWITTER_PASSWORD").is_some()
}

fn env_or_dotenv_value(name: &str) -> Option<String> {
    if let Ok(value) = std::env::var(name) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let dotenv_path = Path::new(".env");
    let contents = std::fs::read_to_string(dotenv_path).ok()?;
    contents.lines().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            return None;
        }
        let (key, value) = trimmed.split_once('=')?;
        if key.trim() != name {
            return None;
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
}

/// Generate a sandbox-exec profile at runtime with the actual paths.
fn generate_sandbox_profile(project_dir: &str, home_dir: &str, allow_network: bool) -> String {
    let mut profile = format!(
        r#"(version 3)
(allow default)

;; BLOCKED from reading (secrets)
(deny file-read* file-write*
    (subpath "{home_dir}/.ssh")
    (subpath "{home_dir}/.gnupg")
    (subpath "{home_dir}/.aws")
    (literal "{home_dir}/.env")
    (literal "{project_dir}/.env"))

;; BLOCKED from writing (Nyx core — protected identity)
(deny file-write*
    (subpath "{project_dir}/src")
    (subpath "{project_dir}/agents")
    (literal "{project_dir}/IDENTITY.md")
    (literal "{project_dir}/SOUL.md")
    (literal "{project_dir}/Cargo.toml")
    (literal "{project_dir}/Cargo.lock"))

;; Never let helper tools bind/listen locally.
(deny network-bind)
(deny network-inbound)
"#
    );

    if !allow_network {
        profile.push_str(
            r#"
;; Non-network tools must stay offline.
(deny network-outbound)
"#,
        );
    }

    profile
}

async fn execute_tool_process(
    script: &str,
    args_json: &str,
    cwd: &std::path::Path,
    profile_path: Option<&std::path::Path>,
    isolation: PythonIsolationMode,
) -> Result<std::process::Output> {
    use tokio::io::AsyncReadExt;

    let mut cmd = if let Some(path) = profile_path {
        let mut c = std::process::Command::new("sandbox-exec");
        c.args(["-f", path.to_str().unwrap_or(""), "python3"]);
        if matches!(isolation, PythonIsolationMode::Isolated) {
            c.arg("-I");
        }
        c.arg(script);
        c
    } else {
        let mut c = std::process::Command::new("python3");
        if matches!(isolation, PythonIsolationMode::Isolated) {
            c.arg("-I");
        }
        c.arg(script);
        c
    };

    cmd.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .current_dir(cwd);

    // Resource limits (CPU + memory always, NPROC only for sandboxed tools)
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let limit_nproc = profile_path.is_some();
        unsafe {
            cmd.pre_exec(move || {
                let cpu = libc::rlimit {
                    rlim_cur: 25,
                    rlim_max: 30,
                };
                libc::setrlimit(libc::RLIMIT_CPU, &cpu);
                // Note: RLIMIT_AS removed — macOS doesn't allow setting below current usage.
                // 30s tokio timeout is sufficient protection.
                if limit_nproc {
                    let nproc = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    libc::setrlimit(libc::RLIMIT_NPROC, &nproc);
                }
                Ok(())
            });
        }
    }

    let mut child = tokio::process::Command::from(cmd)
        .kill_on_drop(true)
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        stdin.write_all(args_json.as_bytes()).await?;
        drop(stdin);
    }

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_task = tokio::spawn(async move {
        let mut buffer = Vec::new();
        if let Some(mut stream) = stdout {
            stream.read_to_end(&mut buffer).await?;
        }
        Ok::<Vec<u8>, std::io::Error>(buffer)
    });
    let stderr_task = tokio::spawn(async move {
        let mut buffer = Vec::new();
        if let Some(mut stream) = stderr {
            stream.read_to_end(&mut buffer).await?;
        }
        Ok::<Vec<u8>, std::io::Error>(buffer)
    });

    let status = match tokio::time::timeout(std::time::Duration::from_secs(30), child.wait()).await
    {
        Ok(result) => result?,
        Err(_) => {
            child.start_kill().ok();
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            bail!("tool timed out after 30s");
        }
    };

    Ok(std::process::Output {
        status,
        stdout: stdout_task.await??,
        stderr: stderr_task.await??,
    })
}

/// Run a tool by name with JSON args. Returns the parsed JSON output.
pub async fn run(name: &str, args: &serde_json::Value) -> Result<serde_json::Value> {
    run_with_state(None, name, args).await
}

/// AppState-aware variant. Session-mode browser commands need the shared
/// [`BrowserSessionManager`] on [`AppState`]; other tools don't. Callers
/// coming through `AppState::execute_tool` should use this path so session
/// commands actually reach the daemon.
pub async fn run_with_state(
    state: Option<&crate::AppState>,
    name: &str,
    args: &serde_json::Value,
) -> Result<serde_json::Value> {
    // Phase 10: the `browser` builtin goes through the forge sandbox path so
    // its Python stdin/stdout contract is adapted into the runtime's
    // `{"success": ..., "output": ...}` envelope. Phase 11 adds the
    // session_* commands which route to a long-running daemon instead.
    if name == "browser" {
        return run_browser_with_state(state, args).await;
    }

    let script = format!("tools/{}.py", name);
    let args_json = serde_json::to_string(args)?;
    let cwd = std::env::current_dir()?;
    let project_dir = cwd.to_str().unwrap_or(".");
    let home_dir = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());

    let needs_network = NETWORK_TOOLS.contains(&name);
    let policy = tool_execution_policy(name);

    // Generate a sandbox profile only for tools that should actively use the
    // macOS wrapper on this host.
    let use_sandbox = policy.sandboxed;
    let mut profile_path = if use_sandbox {
        let profile = generate_sandbox_profile(project_dir, &home_dir, needs_network);
        let path = cwd.join("sandbox").join(format!(
            ".sandbox_{}_{}.sb",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(cwd.join("sandbox"))?;
        std::fs::write(&path, &profile)?;
        Some(path)
    } else {
        None
    };

    let mut output = execute_tool_process(
        &script,
        &args_json,
        &cwd,
        profile_path.as_deref(),
        policy.isolation,
    )
    .await?;
    let mut stderr = String::from_utf8_lossy(&output.stderr).to_string();

    // If a host claims sandbox support but launch still fails, sandbox-preferred
    // tools can fall back to their direct path in auto mode.
    let sandbox_failed_silently = !output.status.success()
        && output.status.code().is_none()
        && output.stdout.is_empty()
        && output.stderr.is_empty();
    let sandbox_launch_failed =
        (!output.status.success() && stderr.contains("sandbox-exec:")) || sandbox_failed_silently;
    if use_sandbox
        && tool_prefers_os_sandbox(name)
        && OsSandboxMode::current() == OsSandboxMode::Auto
        && sandbox_launch_failed
    {
        tracing::warn!(
            "tool {}: sandbox unavailable, retrying without sandbox",
            name
        );
        if let Some(path) = profile_path.take() {
            std::fs::remove_file(path).ok();
        }
        output = execute_tool_process(&script, &args_json, &cwd, None, policy.isolation).await?;
        stderr = String::from_utf8_lossy(&output.stderr).to_string();
    }

    // Clean up temp profile
    if let Some(path) = profile_path {
        std::fs::remove_file(path).ok();
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let result: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|_| {
        if !stderr.is_empty() {
            tracing::debug!("tool {} stderr: {}", name, crate::trunc(&stderr, 200));
            return serde_json::json!({
                "success": false,
                "error": format!("parse error: {}", crate::trunc(&stderr, 200)),
            });
        }
        if !stdout.trim().is_empty() {
            return serde_json::json!({
                "success": false,
                "error": format!("parse error: {}", crate::trunc(&stdout, 200)),
            });
        }
        serde_json::json!({
            "success": false,
            "error": format!("parse error (status: {:?})", output.status.code()),
        })
    });

    Ok(result)
}

/// Validated description of a browser-tool invocation that is safe to hand
/// to the Python entrypoint. Produced by [`prepare_browser_dispatch`] so the
/// Rust side catches missing/unknown commands before spawning Playwright.
///
/// `steps` carries the recipe-mode script for the `interact` command. It is
/// `None` for the read-only single-shot commands and a non-empty array of
/// typed step objects for `interact`. We keep it as `Vec<serde_json::Value>`
/// rather than a typed enum because the step schema is deliberately thin —
/// Python validates selectors/values, Rust just ensures each element is an
/// object with a string `type` field so the envelope is well-formed before
/// we spawn Playwright.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserDispatch {
    pub command: String,
    /// Target URL for one-shot + session_open commands. Empty string for
    /// commands that don't need a URL (session_step / session_close /
    /// session_list).
    pub url: String,
    pub out_path: Option<String>,
    pub steps: Option<Vec<serde_json::Value>>,
    /// Session identifier for session_* commands. `None` for one-shot
    /// commands and for `session_open` when the caller wants the daemon to
    /// auto-generate an id.
    pub session_id: Option<String>,
    /// Named cookie jar for `session_open`. Sessions sharing the same jar
    /// share the underlying Playwright `BrowserContext` (and therefore
    /// cookies + localStorage). `None` means a fresh isolated context,
    /// which is the default and preserves today's behaviour. Only meaningful
    /// on `session_open` — validation rejects a stray `jar` on any other
    /// command so the wire surface stays tight.
    pub jar: Option<String>,
}

impl BrowserDispatch {
    /// Render the exact JSON payload the Python tool expects on stdin.
    ///
    /// Shape matches `tools/browser.py`'s contract:
    /// `{"command": <name>, "args": {"url": <url>, ...}}`.
    pub fn to_stdin_payload(&self) -> serde_json::Value {
        let mut args = serde_json::Map::new();
        if !self.url.is_empty() {
            args.insert(
                "url".to_string(),
                serde_json::Value::String(self.url.clone()),
            );
        }
        if let Some(out_path) = &self.out_path {
            args.insert(
                "out_path".to_string(),
                serde_json::Value::String(out_path.clone()),
            );
        }
        if let Some(steps) = &self.steps {
            args.insert("steps".to_string(), serde_json::Value::Array(steps.clone()));
        }
        if let Some(session_id) = &self.session_id {
            args.insert(
                "session_id".to_string(),
                serde_json::Value::String(session_id.clone()),
            );
        }
        if let Some(jar) = &self.jar {
            args.insert("jar".to_string(), serde_json::Value::String(jar.clone()));
        }
        serde_json::json!({
            "command": self.command,
            "args": serde_json::Value::Object(args),
        })
    }
}

/// Validate and normalize a caller-provided browser-tool input before spawning
/// Playwright. Returns a structured error on unknown command, missing url, or
/// screenshot missing `out_path`. Pure — no IO — so it is trivially testable.
pub fn prepare_browser_dispatch(args: &serde_json::Value) -> Result<BrowserDispatch> {
    let command = args
        .get("command")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("browser tool requires a `command` field"))?
        .to_string();
    if !BROWSER_COMMANDS.contains(&command.as_str()) {
        bail!(
            "browser: unknown command `{}`; expected one of: {}",
            command,
            BROWSER_COMMANDS.join(", ")
        );
    }

    // Helper: read a field from either the top level or `args.<field>`. The
    // two shapes are both accepted so the chat dispatcher and the raw
    // tools::run caller speak the same json without a transform layer.
    let pick_str = |field: &str| -> Option<String> {
        args.get(field)
            .and_then(|value| value.as_str())
            .or_else(|| {
                args.get("args")
                    .and_then(|a| a.get(field))
                    .and_then(|v| v.as_str())
            })
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };

    let session_id = pick_str("session_id");
    let url_opt = pick_str("url");
    let out_path = pick_str("out_path");
    let jar = pick_str("jar");

    // `jar` is only meaningful on `session_open`. Reject it anywhere else
    // so a typo on the wire fails loudly instead of being silently dropped.
    if jar.is_some() && !BROWSER_COMMANDS_ACCEPTING_JAR.contains(&command.as_str()) {
        bail!(
            "browser: `jar` is only valid on `session_open`, not `{}`",
            command
        );
    }

    // Validate jar shape up front (when present). The Python side re-checks
    // defensively; failing here gives the caller a structured error without
    // spawning the daemon.
    if let Some(jar_value) = jar.as_deref() {
        if !jar_name_regex().is_match(jar_value) {
            bail!("browser: jar must match [a-z0-9_-]{{1,40}}");
        }
    }

    // --- session_* commands ---------------------------------------------
    // These route to the long-running daemon and do NOT require a url.

    if command == "session_open" {
        // At least one of session_id / url must be provided so we don't
        // accept a completely empty open payload.
        if session_id.is_none() && url_opt.is_none() {
            bail!("browser: session_open requires at least one of `session_id` or `url`");
        }
        return Ok(BrowserDispatch {
            command,
            url: url_opt.unwrap_or_default(),
            out_path,
            steps: None,
            session_id,
            jar,
        });
    }

    if command == "session_step" {
        let session_id = session_id
            .ok_or_else(|| anyhow::anyhow!("browser: session_step requires `session_id`"))?;
        let raw = args
            .get("steps")
            .or_else(|| args.get("args").and_then(|a| a.get("steps")))
            .ok_or_else(|| anyhow::anyhow!("browser: session_step requires `steps`"))?;
        let array = raw
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("browser: session_step `steps` must be an array"))?;
        if array.is_empty() {
            bail!("browser: session_step `steps` must be a non-empty array");
        }
        for (idx, step) in array.iter().enumerate() {
            let obj = step.as_object().ok_or_else(|| {
                anyhow::anyhow!("browser: session_step step {} must be an object", idx)
            })?;
            let ty = obj
                .get("type")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty());
            if ty.is_none() {
                bail!(
                    "browser: session_step step {} must have a non-empty string `type` field",
                    idx
                );
            }
        }
        return Ok(BrowserDispatch {
            command,
            url: String::new(),
            out_path: None,
            steps: Some(array.clone()),
            session_id: Some(session_id),
            jar: None,
        });
    }

    if command == "session_close" {
        let session_id = session_id
            .ok_or_else(|| anyhow::anyhow!("browser: session_close requires `session_id`"))?;
        return Ok(BrowserDispatch {
            command,
            url: String::new(),
            out_path: None,
            steps: None,
            session_id: Some(session_id),
            jar: None,
        });
    }

    if command == "session_list" {
        return Ok(BrowserDispatch {
            command,
            url: String::new(),
            out_path: None,
            steps: None,
            session_id: None,
            jar: None,
        });
    }

    if command == "session_list_saved" {
        // Takes no args — returns saved-session metadata persisted by a
        // previous Nyx boot.
        return Ok(BrowserDispatch {
            command,
            url: String::new(),
            out_path: None,
            steps: None,
            session_id: None,
            jar: None,
        });
    }

    if command == "session_restore" {
        // Explicitly rehydrate one saved session by id. `session_id` is
        // required so validation fails loud when a caller forgets it
        // instead of silently becoming a no-op.
        let session_id = session_id
            .ok_or_else(|| anyhow::anyhow!("browser: session_restore requires `session_id`"))?;
        return Ok(BrowserDispatch {
            command,
            url: String::new(),
            out_path: None,
            steps: None,
            session_id: Some(session_id),
            jar: None,
        });
    }

    if command == "jar_list" {
        return Ok(BrowserDispatch {
            command,
            url: String::new(),
            out_path: None,
            steps: None,
            session_id: None,
            jar: None,
        });
    }

    // --- one-shot commands ----------------------------------------------

    let url = url_opt.ok_or_else(|| anyhow::anyhow!("browser: missing `url`"))?;

    if command == "screenshot" && out_path.is_none() {
        bail!("browser: screenshot requires `out_path`");
    }

    // `interact` recipe mode: a non-empty array of typed step objects. We
    // validate the shape here (so a malformed payload fails fast with a
    // structured error) but leave selector/value semantics to Python.
    let steps = if command == "interact" {
        let raw = args
            .get("steps")
            .or_else(|| args.get("args").and_then(|a| a.get("steps")))
            .ok_or_else(|| anyhow::anyhow!("browser: interact requires `steps`"))?;
        let array = raw
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("browser: interact `steps` must be an array"))?;
        if array.is_empty() {
            bail!("browser: interact `steps` must be a non-empty array");
        }
        for (idx, step) in array.iter().enumerate() {
            let obj = step.as_object().ok_or_else(|| {
                anyhow::anyhow!("browser: interact step {} must be an object", idx)
            })?;
            let ty = obj
                .get("type")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty());
            if ty.is_none() {
                bail!(
                    "browser: interact step {} must have a non-empty string `type` field",
                    idx
                );
            }
        }
        Some(array.clone())
    } else {
        None
    };

    Ok(BrowserDispatch {
        command,
        url,
        out_path,
        steps,
        session_id: None,
        jar: None,
    })
}

/// Convert the Python tool's `{"ok": bool, ...}` envelope to the runtime's
/// canonical `{"success": bool, "output": ...}` shape used by every other
/// Nyx tool. Kept public so tests can exercise the adapter directly without
/// touching the sandbox. The `result` field is preserved as `output` so
/// downstream consumers still see the structured payload.
pub fn browser_envelope_from_python(value: serde_json::Value) -> serde_json::Value {
    let ok = value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
    if ok {
        serde_json::json!({
            "success": true,
            "output": value.get("result").cloned().unwrap_or(serde_json::Value::Null),
        })
    } else {
        let error = value
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("browser tool reported failure")
            .to_string();
        serde_json::json!({
            "success": false,
            "error": error,
        })
    }
}

/// Dispatch the browser builtin through the forge sandbox path. The python
/// tool at `tools/browser.py` receives a JSON payload on stdin,
/// writes one back on stdout; this helper adapts that envelope into Nyx's
/// canonical `{"success": ..., "output": ...}` shape.
///
/// Returns `Ok(serde_json::Value)` in all user-input failure modes — bad
/// commands, missing URL, missing `out_path` — so callers that compose
/// tool responses into a chat surface do not have to pattern-match against
/// two different failure encodings. Only genuine Rust-side infrastructure
/// errors (none are surfaced here today) would bubble up as `Err`.
pub async fn run_browser(args: &serde_json::Value) -> Result<serde_json::Value> {
    run_browser_with_state(None, args).await
}

/// Variant that accepts an optional [`AppState`] reference so session-mode
/// commands can reach the long-running daemon. Passing `None` works for
/// every read-only + `interact` command (they route through the one-shot
/// forge sandbox) but session_* commands will return a structured error
/// when no state is provided.
pub async fn run_browser_with_state(
    state: Option<&crate::AppState>,
    args: &serde_json::Value,
) -> Result<serde_json::Value> {
    let dispatch = match prepare_browser_dispatch(args) {
        Ok(dispatch) => dispatch,
        Err(err) => {
            return Ok(serde_json::json!({
                "success": false,
                "error": err.to_string(),
            }));
        }
    };

    if is_browser_session_command(&dispatch.command) {
        let Some(state) = state else {
            return Ok(serde_json::json!({
                "success": false,
                "error": format!(
                    "browser: `{}` requires the session manager — call run_browser_with_state",
                    dispatch.command
                ),
            }));
        };
        return Ok(run_browser_session_command(state, &dispatch).await);
    }

    let payload = dispatch.to_stdin_payload();
    let script_path = PathBuf::from(BROWSER_TOOL_SCRIPT);
    match crate::forge::run_built_tool_at(&script_path, &payload).await {
        Ok(value) => Ok(browser_envelope_from_python(value)),
        Err(err) => Ok(serde_json::json!({
            "success": false,
            "error": format!("browser runtime error: {}", err),
        })),
    }
}

/// Route a validated session_* dispatch to the [`BrowserSessionManager`] and
/// wrap the result in the canonical `{success, output|error}` envelope.
async fn run_browser_session_command(
    state: &crate::AppState,
    dispatch: &BrowserDispatch,
) -> serde_json::Value {
    use crate::runtime::browser_session::{SessionOpenParams, SessionStepParams};

    let manager = state.browser_sessions.clone();
    let result: Result<serde_json::Value> = match dispatch.command.as_str() {
        "session_open" => {
            let url = if dispatch.url.is_empty() {
                None
            } else {
                Some(dispatch.url.clone())
            };
            manager
                .session_open(SessionOpenParams {
                    session_id: dispatch.session_id.clone(),
                    url,
                    jar: dispatch.jar.clone(),
                })
                .await
        }
        "session_step" => {
            let session_id = dispatch
                .session_id
                .clone()
                .expect("session_step validated to carry session_id");
            let steps = dispatch
                .steps
                .clone()
                .expect("session_step validated to carry steps");
            manager
                .session_step(SessionStepParams { session_id, steps })
                .await
        }
        "session_close" => {
            let session_id = dispatch
                .session_id
                .as_deref()
                .expect("session_close validated to carry session_id");
            manager.session_close(session_id).await
        }
        "session_list" => manager.session_list().await,
        "session_list_saved" => manager.session_list_saved().await,
        "session_restore" => {
            let session_id = dispatch
                .session_id
                .as_deref()
                .expect("session_restore validated to carry session_id");
            manager.session_restore(session_id).await
        }
        "jar_list" => manager.jar_list().await,
        other => Err(anyhow::anyhow!("unknown session command `{}`", other)),
    };

    match result {
        Ok(output) => serde_json::json!({
            "success": true,
            "output": output,
        }),
        Err(err) => serde_json::json!({
            "success": false,
            "error": err.to_string(),
        }),
    }
}

#[cfg(test)]
#[path = "../tests/unit/tools.rs"]
mod tests;
