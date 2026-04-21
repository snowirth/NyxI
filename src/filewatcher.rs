//! File watcher — monitors project directories for code changes.
//! Pushes notable activity to the proactive queue so Nyx knows
//! what the user is working on.

use crate::{AppState, ProactiveQueue};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// File extensions worth tracking.
const WATCH_EXTENSIONS: &[&str] = &["rs", "py", "js", "ts", "toml", "json", "md"];

/// Debounce window — batch changes within this period.
const DEBOUNCE_SECS: u64 = 2;

/// Minimum edits in a window to count as "active editing".
const ACTIVE_EDITING_THRESHOLD: usize = 3;

/// How long between saves to still count as one editing session (seconds).
const EDITING_SESSION_GAP: u64 = 120;

/// What happened to a file.
#[derive(Debug, Clone, PartialEq)]
enum ChangeKind {
    Create,
    Modify,
    Delete,
}

impl std::fmt::Display for ChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeKind::Create => write!(f, "created"),
            ChangeKind::Modify => write!(f, "modified"),
            ChangeKind::Delete => write!(f, "deleted"),
        }
    }
}

/// A single observed file change (post-debounce).
#[derive(Debug, Clone)]
struct FileChange {
    path: PathBuf,
    kind: ChangeKind,
}

/// Tracks editing activity per file.
#[derive(Debug)]
struct EditingSession {
    first_save: Instant,
    last_save: Instant,
    save_count: usize,
}

impl EditingSession {
    fn new(now: Instant) -> Self {
        Self {
            first_save: now,
            last_save: now,
            save_count: 1,
        }
    }

    fn duration_min(&self) -> u64 {
        self.last_save.duration_since(self.first_save).as_secs() / 60
    }

    fn is_stale(&self, now: Instant) -> bool {
        now.duration_since(self.last_save).as_secs() > EDITING_SESSION_GAP
    }
}

/// Main entry point — spawned as a background task.
pub async fn run(state: AppState, proactive_queue: ProactiveQueue) {
    // Give the system a moment to settle on startup
    tokio::time::sleep(Duration::from_secs(8)).await;

    let watch_dirs = resolve_watch_dirs();
    if watch_dirs.is_empty() {
        tracing::warn!("filewatcher: no directories to watch, exiting");
        return;
    }

    tracing::info!("filewatcher: online, watching {} dirs", watch_dirs.len());
    for d in &watch_dirs {
        tracing::info!("filewatcher:   {}", d.display());
    }

    // Channel for raw filesystem events
    let (tx, rx) = std::sync::mpsc::channel();
    let rx = Arc::new(Mutex::new(rx));

    // Start the notify watcher on a blocking thread (it uses std sync)
    let watcher_handle = {
        let tx = tx.clone();
        let dirs = watch_dirs.clone();
        tokio::task::spawn_blocking(move || {
            start_watcher(dirs, tx);
        })
    };

    // Process events in the async runtime
    let process_handle = tokio::spawn(process_events(state, rx, proactive_queue));

    // If either task ends, log it
    tokio::select! {
        _ = watcher_handle => tracing::warn!("filewatcher: watcher thread exited"),
        _ = process_handle => tracing::warn!("filewatcher: processor exited"),
    }
}

/// Resolve which directories to watch from NYX_WATCH_DIRS env var.
fn resolve_watch_dirs() -> Vec<PathBuf> {
    let raw = std::env::var("NYX_WATCH_DIRS").unwrap_or_else(|_| ".".to_string());
    raw.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .filter(|p| {
            if p.exists() && p.is_dir() {
                true
            } else {
                tracing::warn!("filewatcher: skipping non-existent dir: {}", p.display());
                false
            }
        })
        .collect()
}

/// Start the blocking notify watcher. Runs forever.
fn start_watcher(dirs: Vec<PathBuf>, tx: std::sync::mpsc::Sender<(PathBuf, ChangeKind)>) {
    use notify::{Config, Event, EventKind, RecursiveMode, Watcher};

    let tx_clone = tx.clone();
    let mut watcher = match notify::RecommendedWatcher::new(
        move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let kind = match event.kind {
                    EventKind::Create(_) => Some(ChangeKind::Create),
                    EventKind::Modify(_) => Some(ChangeKind::Modify),
                    EventKind::Remove(_) => Some(ChangeKind::Delete),
                    _ => None,
                };
                if let Some(k) = kind {
                    for path in event.paths {
                        if is_watched_file(&path) {
                            let _ = tx_clone.send((path, k.clone()));
                        }
                    }
                }
            }
        },
        Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("filewatcher: failed to create watcher: {}", e);
            return;
        }
    };

    for dir in &dirs {
        if let Err(e) = watcher.watch(dir, RecursiveMode::Recursive) {
            tracing::error!("filewatcher: failed to watch {}: {}", dir.display(), e);
        }
    }

    // Block forever — watcher must stay alive
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Check if a file has a watched extension and isn't in a hidden/build directory.
fn is_watched_file(path: &Path) -> bool {
    // Skip hidden dirs and common noise
    let path_str = path.to_string_lossy();
    if path_str.contains("/.")
        || path_str.contains("/target/")
        || path_str.contains("/node_modules/")
        || path_str.contains("/__pycache__/")
        || path_str.contains("/.git/")
    {
        return false;
    }

    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) => WATCH_EXTENSIONS.contains(&ext),
        None => false,
    }
}

/// Async event processor — debounces, tracks sessions, pushes to proactive queue.
async fn process_events(
    state: AppState,
    rx: Arc<Mutex<std::sync::mpsc::Receiver<(PathBuf, ChangeKind)>>>,
    proactive_queue: ProactiveQueue,
) {
    let mut sessions: HashMap<PathBuf, EditingSession> = HashMap::new();
    let mut pending: Vec<FileChange> = Vec::new();
    let mut last_flush = Instant::now();
    // Track what we've already reported to avoid spam
    let mut last_report: Option<Instant> = None;

    loop {
        // Drain available events (non-blocking)
        {
            let rx = rx.lock().await;
            loop {
                match rx.try_recv() {
                    Ok((path, kind)) => {
                        pending.push(FileChange { path, kind });
                    }
                    Err(_) => break,
                }
            }
        }

        let now = Instant::now();

        // Flush if debounce window has passed and we have events
        if !pending.is_empty()
            && now.duration_since(last_flush) >= Duration::from_secs(DEBOUNCE_SECS)
        {
            // Deduplicate: keep latest change per file
            let mut deduped: HashMap<PathBuf, FileChange> = HashMap::new();
            for change in pending.drain(..) {
                deduped.insert(change.path.clone(), change);
            }

            let changes: Vec<FileChange> = deduped.into_values().collect();

            // Update editing sessions
            for change in &changes {
                if change.kind == ChangeKind::Modify || change.kind == ChangeKind::Create {
                    let session = sessions
                        .entry(change.path.clone())
                        .or_insert_with(|| EditingSession::new(now));
                    session.last_save = now;
                    session.save_count += 1;
                }
                if change.kind == ChangeKind::Delete {
                    sessions.remove(&change.path);
                }
            }

            // Decide if this batch is worth reporting
            let should_report = match last_report {
                None => true,
                Some(lr) => now.duration_since(lr).as_secs() >= 30, // max one report per 30s
            };

            if should_report && !changes.is_empty() {
                let msg = format_changes(&changes, &sessions);
                if !msg.is_empty() {
                    tracing::info!("filewatcher: {}", msg);
                    proactive_queue.lock().await.push(msg.clone());
                    if let Err(e) = crate::autonomy::ingest_observation(
                        state.db.as_ref(),
                        crate::autonomy::ObservationInput {
                            kind: "file_activity".to_string(),
                            source: "filewatcher".to_string(),
                            content: msg,
                            context: serde_json::json!({
                                "change_count": changes.len(),
                            }),
                            priority: 0.58,
                        },
                    ) {
                        tracing::warn!("filewatcher: failed to store observation: {}", e);
                    }
                    last_report = Some(now);
                }
            }

            last_flush = now;
        }

        // Prune stale editing sessions
        sessions.retain(|_, s| !s.is_stale(now));

        // Sleep briefly before next poll
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Build a human-readable summary of a batch of changes.
fn format_changes(changes: &[FileChange], sessions: &HashMap<PathBuf, EditingSession>) -> String {
    if changes.is_empty() {
        return String::new();
    }

    let mut parts = Vec::new();

    // Group by kind
    let creates: Vec<_> = changes
        .iter()
        .filter(|c| c.kind == ChangeKind::Create)
        .collect();
    let modifies: Vec<_> = changes
        .iter()
        .filter(|c| c.kind == ChangeKind::Modify)
        .collect();
    let deletes: Vec<_> = changes
        .iter()
        .filter(|c| c.kind == ChangeKind::Delete)
        .collect();

    if !creates.is_empty() {
        let names = file_names(&creates);
        parts.push(format!("created {}", names));
    }
    if !modifies.is_empty() {
        let names = file_names(&modifies);
        parts.push(format!("edited {}", names));
    }
    if !deletes.is_empty() {
        let names = file_names(&deletes);
        parts.push(format!("deleted {}", names));
    }

    // Note any active editing sessions
    let active: Vec<_> = sessions
        .iter()
        .filter(|(_, s)| s.save_count >= ACTIVE_EDITING_THRESHOLD && s.duration_min() >= 1)
        .collect();

    if !active.is_empty() {
        let longest = active.iter().max_by_key(|(_, s)| s.duration_min()).unwrap();
        let name = longest
            .0
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        parts.push(format!(
            "actively editing {} for ~{}min ({} saves)",
            name,
            longest.1.duration_min(),
            longest.1.save_count
        ));
    }

    parts.join(", ")
}

/// Extract short file names from changes, capping at 4.
fn file_names(changes: &[&FileChange]) -> String {
    let names: Vec<String> = changes
        .iter()
        .take(4)
        .map(|c| {
            c.path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "?".to_string())
        })
        .collect();

    let display = names.join(", ");
    if changes.len() > 4 {
        format!("{} (+{} more)", display, changes.len() - 4)
    } else {
        display
    }
}
