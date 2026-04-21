//! Browser session supervisor — keeps `tools/browser_daemon.py` alive and
//! multiplexes RPCs onto it.
//!
//! Shape at a glance
//! -----------------
//! - One [`BrowserSessionManager`] lives on [`AppState`]. Cheap to construct
//!   (no spawning happens until first use).
//! - On the first `session_*` RPC we lazily spawn a [`DaemonHandle`]: a
//!   tokio-managed child process plus two background tasks — a stdin writer
//!   fed by an mpsc channel, and a stdout reader that demuxes responses back
//!   to per-call oneshot receivers keyed by a monotonically increasing
//!   `req_id`.
//! - Every public method serialises the JSON request, parks a oneshot sender
//!   in a shared pending-map, sends the payload over the writer channel, and
//!   awaits the reply with a 30s timeout.
//! - If the daemon crashes the reader task notices EOF / an IO error, drains
//!   the pending-map with an error so in-flight callers don't hang, and
//!   marks the handle dead. The next RPC call re-spawns. Sessions on the
//!   dead daemon are lost — that's the documented contract.
//!
//! Concurrency model
//! -----------------
//! Callers may fire session_* RPCs concurrently; each request has a unique
//! `req_id` and the reader task routes responses independently. *Per-session*
//! serialisation is enforced by the daemon's single-threaded RPC loop —
//! if two `session_step`s target the same `session_id` they queue on the
//! daemon, not on the Rust side.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::{Mutex, mpsc, oneshot};

/// Per-RPC timeout. The daemon enforces a tighter 10s per step internally;
/// this outer bound covers navigation + step list + Python overhead on one
/// `session_step` call. If the daemon doesn't answer in 30s we return an
/// error but keep the daemon alive — it might just be a slow page.
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Path to the daemon script, relative to the process cwd (Nyx repo root).
const DAEMON_SCRIPT: &str = "tools/browser_daemon.py";

/// Parameters for [`BrowserSessionManager::session_open`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionOpenParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Named cookie jar. Sessions with the same `jar` share cookies +
    /// localStorage by sharing a Playwright `BrowserContext`. `None`
    /// preserves the default (fresh isolated context).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jar: Option<String>,
}

/// Parameters for [`BrowserSessionManager::session_step`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStepParams {
    pub session_id: String,
    pub steps: Vec<serde_json::Value>,
}

/// Outer envelope returned by every daemon call. Mirrors the Python side's
/// `{"id", "ok", "result"|"error"}` shape but we parse it here rather than
/// propagating raw JSON to the tool layer.
#[derive(Debug, Clone)]
struct RpcEnvelope {
    ok: bool,
    result: Option<serde_json::Value>,
    error: Option<String>,
}

type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<RpcEnvelope>>>>;

/// Outgoing message the stdin writer task consumes.
struct OutgoingRequest {
    line: String,
}

/// A live daemon — process handle plus the channel to talk to it. We keep
/// the [`Child`] around so [`Drop`] kills it when the manager is dropped.
struct DaemonHandle {
    child: Child,
    writer: mpsc::Sender<OutgoingRequest>,
    pending: PendingMap,
}

impl DaemonHandle {
    /// Tear down the daemon. Best-effort: we try a `shutdown` RPC first, then
    /// force-kill. Called from `shutdown()` and from [`Drop`] via `kill_on_drop`.
    async fn graceful_close(mut self) {
        // Best-effort shutdown RPC. We don't wait for its reply — we just
        // want the daemon to close its sessions. If the write fails the
        // process is already gone.
        let _ = self
            .writer
            .send(OutgoingRequest {
                line: r#"{"id":"shutdown","method":"shutdown"}"#.to_string(),
            })
            .await;
        // Give it a moment to flush, then force-kill.
        let _ = tokio::time::timeout(Duration::from_secs(2), self.child.wait()).await;
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

/// Supervisor. Lives on [`crate::AppState`]. Cheap to clone — all state is
/// behind `Arc`s and locks.
#[derive(Clone)]
pub struct BrowserSessionManager {
    inner: Arc<Mutex<Option<DaemonHandle>>>,
    req_counter: Arc<AtomicU64>,
}

impl Default for BrowserSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for BrowserSessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserSessionManager").finish()
    }
}

impl BrowserSessionManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            req_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Best-effort shutdown. Closes the daemon if it's running.
    pub async fn shutdown(&self) {
        let mut guard = self.inner.lock().await;
        if let Some(handle) = guard.take() {
            handle.graceful_close().await;
        }
    }

    pub async fn session_open(&self, params: SessionOpenParams) -> Result<serde_json::Value> {
        self.call("session_open", serde_json::to_value(&params)?)
            .await
    }

    pub async fn session_step(&self, params: SessionStepParams) -> Result<serde_json::Value> {
        self.call("session_step", serde_json::to_value(&params)?)
            .await
    }

    pub async fn session_close(&self, session_id: &str) -> Result<serde_json::Value> {
        self.call(
            "session_close",
            serde_json::json!({ "session_id": session_id }),
        )
        .await
    }

    pub async fn session_list(&self) -> Result<serde_json::Value> {
        self.call("session_list", serde_json::json!({})).await
    }

    /// List all cookie jars currently held by the daemon. Returns a payload
    /// of the shape `{"jars": [{"name", "session_count", "created_at_monotonic"}, ...]}`.
    /// An empty list is a valid response when no jar-backed sessions exist.
    pub async fn jar_list(&self) -> Result<serde_json::Value> {
        self.call("jar_list", serde_json::json!({})).await
    }

    /// List saved-session metadata read from the daemon's on-disk state file.
    /// Saved sessions are ones persisted by a previous Nyx boot and not
    /// currently live. Each entry carries `session_id`, `last_url`,
    /// `last_activity_at_epoch`, `jar` (nullable), and `storage_state_path`.
    /// Sessions currently live in-process are excluded so the response never
    /// double-counts.
    pub async fn session_list_saved(&self) -> Result<serde_json::Value> {
        self.call("session_list_saved", serde_json::json!({})).await
    }

    /// Explicitly re-open a previously-saved session. The daemon re-launches
    /// Chromium, re-creates the context with the saved `storage_state`
    /// (cookies + localStorage), and navigates to the saved `last_url`.
    /// Fails with an `expired` error if the saved session is older than
    /// the idle timeout (600s). The restored session is a normal live
    /// session from then on — counts against MAX_SESSIONS, is reapable, etc.
    pub async fn session_restore(&self, session_id: &str) -> Result<serde_json::Value> {
        self.call(
            "session_restore",
            serde_json::json!({ "session_id": session_id }),
        )
        .await
    }

    /// Test hook: backdate a session's `last_activity_at` so the daemon's
    /// inline idle reaper closes it on the next RPC. Not product code —
    /// the browser session integration test uses this to assert the reaper
    /// actually fires without the test having to wait 600 real seconds.
    /// The leading `_` flags it as a test hook; do not call from product code.
    pub async fn _test_expire_session(
        &self,
        session_id: &str,
        offset_seconds: u64,
    ) -> Result<serde_json::Value> {
        self.call(
            "_test_expire_session",
            serde_json::json!({
                "session_id": session_id,
                "offset_seconds": offset_seconds,
            }),
        )
        .await
    }

    /// Test hook: backdate a SAVED session's `last_activity_at_epoch` on disk
    /// so the next `session_restore` rejects it as expired. Mirrors
    /// `_test_expire_session` but targets the persistence file rather than
    /// the in-memory SessionSlot. Not product code — only the persistence
    /// expiry test uses it.
    pub async fn _test_backdate_saved_session(
        &self,
        session_id: &str,
        offset_seconds: u64,
    ) -> Result<serde_json::Value> {
        self.call(
            "_test_backdate_saved_session",
            serde_json::json!({
                "session_id": session_id,
                "offset_seconds": offset_seconds,
            }),
        )
        .await
    }

    /// Guarantee a live daemon. Spawns on first use, respawns if the prior
    /// handle's channel is dead (reader task has exited).
    async fn ensure_daemon(&self) -> Result<mpsc::Sender<OutgoingRequest>> {
        let mut guard = self.inner.lock().await;
        if let Some(handle) = guard.as_ref() {
            if !handle.writer.is_closed() {
                return Ok(handle.writer.clone());
            }
            // Writer channel dropped => reader task exited => daemon dead.
            // Drop the old handle (kill_on_drop cleans up the process) and
            // spawn a fresh one.
            tracing::warn!("browser_daemon: detected dead handle, respawning");
            guard.take();
        }

        let handle = spawn_daemon().await?;
        let sender = handle.writer.clone();
        *guard = Some(handle);
        Ok(sender)
    }

    /// Core RPC: assign a req_id, park a oneshot, send, await with timeout.
    async fn call(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let writer = self.ensure_daemon().await?;
        let pending = {
            let guard = self.inner.lock().await;
            guard
                .as_ref()
                .map(|h| h.pending.clone())
                .ok_or_else(|| anyhow!("daemon handle vanished"))?
        };

        let req_id = self.req_counter.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = pending.lock().await;
            pending.insert(req_id, tx);
        }

        let request = serde_json::json!({
            "id": req_id.to_string(),
            "method": method,
            "params": params,
        });
        let mut line = request.to_string();
        line.push('\n');

        if writer.send(OutgoingRequest { line }).await.is_err() {
            // Writer task died between ensure_daemon and send. Clean the slot.
            let mut pending = pending.lock().await;
            pending.remove(&req_id);
            return Err(anyhow!("browser daemon writer channel closed"));
        }

        let envelope = match tokio::time::timeout(RPC_TIMEOUT, rx).await {
            Ok(Ok(envelope)) => envelope,
            Ok(Err(_)) => {
                // Oneshot sender dropped without sending — the reader
                // closed the pending map on daemon death.
                return Err(anyhow!(
                    "browser daemon closed connection before answering {method}"
                ));
            }
            Err(_) => {
                // Evict our pending slot so a late reply doesn't leak.
                let mut pending = pending.lock().await;
                pending.remove(&req_id);
                tracing::warn!(
                    "browser_daemon: {} timed out after {:?}",
                    method,
                    RPC_TIMEOUT
                );
                return Err(anyhow!(
                    "browser daemon RPC `{method}` timed out after {:?}",
                    RPC_TIMEOUT
                ));
            }
        };

        if envelope.ok {
            Ok(envelope.result.unwrap_or(serde_json::Value::Null))
        } else {
            let err = envelope
                .error
                .unwrap_or_else(|| "browser daemon reported failure".to_string());
            // Attach partial result (e.g. interact-style step records) so
            // the caller can still render something useful.
            if let Some(partial) = envelope.result {
                Err(anyhow!("{}|partial={}", err, partial))
            } else {
                Err(anyhow!(err))
            }
        }
    }
}

/// Spawn the Python daemon and wire up reader/writer tasks.
async fn spawn_daemon() -> Result<DaemonHandle> {
    let cwd = std::env::current_dir()?;
    let script = cwd.join(DAEMON_SCRIPT);
    if !script.is_file() {
        return Err(anyhow!(
            "browser daemon script missing at {}",
            script.display()
        ));
    }

    let mut cmd = tokio::process::Command::new("python3");
    cmd.arg(&script)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .current_dir(&cwd)
        .kill_on_drop(true);

    let mut child = cmd.spawn().map_err(|e| {
        anyhow!(
            "failed to spawn browser daemon ({}): {}",
            script.display(),
            e
        )
    })?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("browser daemon: missing stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("browser daemon: missing stdout"))?;
    // Stderr gets logged on EOF by a side task so crashes leave a trail.
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(log_stderr(stderr));
    }

    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let (writer_tx, writer_rx) = mpsc::channel::<OutgoingRequest>(64);

    tokio::spawn(writer_task(stdin, writer_rx));
    tokio::spawn(reader_task(stdout, pending.clone()));

    Ok(DaemonHandle {
        child,
        writer: writer_tx,
        pending,
    })
}

async fn writer_task(mut stdin: ChildStdin, mut rx: mpsc::Receiver<OutgoingRequest>) {
    while let Some(req) = rx.recv().await {
        if let Err(e) = stdin.write_all(req.line.as_bytes()).await {
            tracing::warn!("browser_daemon: stdin write failed: {}", e);
            break;
        }
        if let Err(e) = stdin.flush().await {
            tracing::warn!("browser_daemon: stdin flush failed: {}", e);
            break;
        }
    }
    // Dropping `stdin` closes the daemon's stdin, prompting a clean exit.
}

async fn reader_task(stdout: ChildStdout, pending: PendingMap) {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                tracing::warn!("browser_daemon: stdout EOF");
                break;
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<serde_json::Value>(trimmed) {
                    Ok(value) => dispatch_response(&pending, value).await,
                    Err(e) => {
                        tracing::warn!(
                            "browser_daemon: non-JSON stdout line: {} (err: {})",
                            crate::trunc(trimmed, 200),
                            e
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!("browser_daemon: stdout read error: {}", e);
                break;
            }
        }
    }

    // Drain all pending waiters with an error so nobody hangs forever.
    let mut pending = pending.lock().await;
    pending.clear();
}

async fn log_stderr(stderr: tokio::process::ChildStderr) {
    let mut reader = BufReader::new(stderr);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => return,
            Ok(_) => {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    tracing::warn!("browser_daemon stderr: {}", trimmed);
                }
            }
            Err(_) => return,
        }
    }
}

async fn dispatch_response(pending: &PendingMap, value: serde_json::Value) {
    let id_str = value.get("id").and_then(|v| {
        v.as_str()
            .map(|s| s.to_string())
            .or_else(|| v.as_u64().map(|n| n.to_string()))
    });
    let Some(id_str) = id_str else {
        tracing::warn!("browser_daemon: response missing id: {}", value);
        return;
    };
    // `shutdown` uses id="shutdown" and we intentionally don't park a oneshot
    // for it. Skip silently.
    let Ok(req_id) = id_str.parse::<u64>() else {
        return;
    };

    let envelope = RpcEnvelope {
        ok: value.get("ok").and_then(|v| v.as_bool()).unwrap_or(false),
        result: value.get("result").cloned(),
        error: value
            .get("error")
            .and_then(|v| v.as_str())
            .map(str::to_string),
    };

    let mut pending = pending.lock().await;
    if let Some(tx) = pending.remove(&req_id) {
        let _ = tx.send(envelope);
    } else {
        tracing::warn!("browser_daemon: response for unknown req_id {}", req_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_open_params_roundtrip() {
        let p = SessionOpenParams {
            session_id: Some("abc".into()),
            url: Some("https://example.com".into()),
            jar: Some("my-jar".into()),
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["session_id"], "abc");
        assert_eq!(v["url"], "https://example.com");
        assert_eq!(v["jar"], "my-jar");
    }

    #[test]
    fn session_open_params_omits_none() {
        let p = SessionOpenParams::default();
        let v = serde_json::to_value(&p).unwrap();
        assert!(v.get("session_id").is_none());
        assert!(v.get("url").is_none());
        assert!(v.get("jar").is_none());
    }

    #[test]
    fn session_step_params_roundtrip() {
        let p = SessionStepParams {
            session_id: "sid".into(),
            steps: vec![serde_json::json!({"type": "click", "selector": "#go"})],
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["session_id"], "sid");
        assert_eq!(v["steps"][0]["type"], "click");
    }
}
