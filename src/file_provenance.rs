use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

pub const FILE_PROVENANCE_SCHEMA_VERSION: &str = "nyx_file_provenance.v1";
const DEFAULT_LOG_PATH: &str = "workspace/file_provenance_live.jsonl";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileDiffSummary {
    pub start_line: Option<usize>,
    pub before_preview: String,
    pub after_preview: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileProvenanceEvent {
    pub schema_version: String,
    pub id: String,
    pub actor: String,
    pub source: String,
    pub action_kind: String,
    pub operation_id: String,
    pub target_path: String,
    pub description: Option<String>,
    pub outcome: String,
    pub before_exists: bool,
    pub after_exists: bool,
    pub before_sha256: Option<String>,
    pub after_sha256: Option<String>,
    pub before_bytes: Option<usize>,
    pub after_bytes: Option<usize>,
    pub before_line_count: Option<usize>,
    pub after_line_count: Option<usize>,
    pub diff: FileDiffSummary,
    pub metadata: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct FileMutationProof<'a> {
    pub actor: &'a str,
    pub source: &'a str,
    pub action_kind: &'a str,
    pub operation_id: Option<&'a str>,
    pub description: Option<&'a str>,
    pub outcome: &'a str,
    pub metadata: serde_json::Value,
}

pub fn operation_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn default_log_path() -> PathBuf {
    std::env::var("NYX_FILE_PROVENANCE_LOG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_LOG_PATH))
}

pub fn read_events() -> Result<Vec<FileProvenanceEvent>> {
    read_events_from(&default_log_path())
}

pub fn read_events_from(path: &Path) -> Result<Vec<FileProvenanceEvent>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut events = Vec::new();
    for (line_no, raw_line) in std::fs::read_to_string(path)
        .with_context(|| format!("failed to read provenance log {}", path.display()))?
        .lines()
        .enumerate()
    {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let event = serde_json::from_str::<FileProvenanceEvent>(line).with_context(|| {
            format!(
                "failed to parse provenance event at {}:{}",
                path.display(),
                line_no + 1
            )
        })?;
        events.push(event);
    }
    Ok(events)
}

pub fn record_text_file_mutation(
    path: &Path,
    before: Option<&str>,
    after: Option<&str>,
    proof: FileMutationProof<'_>,
) -> Result<FileProvenanceEvent> {
    let event = build_text_mutation_event(path, before, after, proof);
    append_event(&event)?;
    Ok(event)
}

pub fn write_text_file_with_provenance(
    path: &Path,
    content: &str,
    proof: FileMutationProof<'_>,
) -> Result<FileProvenanceEvent> {
    let before = std::fs::read_to_string(path).ok();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    std::fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))?;

    match record_text_file_mutation(path, before.as_deref(), Some(content), proof) {
        Ok(event) => Ok(event),
        Err(error) => {
            restore_previous_text(path, before.as_deref())?;
            Err(error)
        }
    }
}

fn restore_previous_text(path: &Path, before: Option<&str>) -> Result<()> {
    match before {
        Some(original) => std::fs::write(path, original)
            .with_context(|| format!("failed to restore {}", path.display())),
        None => {
            if path.exists() {
                std::fs::remove_file(path)
                    .with_context(|| format!("failed to remove {}", path.display()))?;
            }
            Ok(())
        }
    }
}

fn append_event(event: &FileProvenanceEvent) -> Result<()> {
    let path = default_log_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create provenance dir {}", parent.display()))?;
    }
    let mut payload = serde_json::to_vec(event)
        .with_context(|| format!("failed to serialize provenance event {}", event.id))?;
    payload.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open provenance log {}", path.display()))?;
    lock_append_file(&file, &path)?;
    file.write_all(&payload)
        .with_context(|| format!("failed to append provenance log {}", path.display()))?;
    file.flush()
        .with_context(|| format!("failed to flush provenance log {}", path.display()))?;
    unlock_append_file(&file, &path)?;
    Ok(())
}

#[cfg(unix)]
fn lock_append_file(file: &std::fs::File, path: &Path) -> Result<()> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to lock provenance log {}", path.display()))
    }
}

#[cfg(not(unix))]
fn lock_append_file(_file: &std::fs::File, _path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn unlock_append_file(file: &std::fs::File, path: &Path) -> Result<()> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to unlock provenance log {}", path.display()))
    }
}

#[cfg(not(unix))]
fn unlock_append_file(_file: &std::fs::File, _path: &Path) -> Result<()> {
    Ok(())
}

fn build_text_mutation_event(
    path: &Path,
    before: Option<&str>,
    after: Option<&str>,
    proof: FileMutationProof<'_>,
) -> FileProvenanceEvent {
    let target_path = normalize_target_path(path);
    let diff = summarize_diff(before.unwrap_or(""), after.unwrap_or(""));
    let operation_id = proof
        .operation_id
        .map(str::to_string)
        .unwrap_or_else(operation_id);
    FileProvenanceEvent {
        schema_version: FILE_PROVENANCE_SCHEMA_VERSION.to_string(),
        id: uuid::Uuid::new_v4().to_string(),
        actor: proof.actor.to_string(),
        source: proof.source.to_string(),
        action_kind: proof.action_kind.to_string(),
        operation_id,
        target_path,
        description: proof
            .description
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        outcome: proof.outcome.to_string(),
        before_exists: before.is_some(),
        after_exists: after.is_some(),
        before_sha256: before.map(sha256_text),
        after_sha256: after.map(sha256_text),
        before_bytes: before.map(str::len),
        after_bytes: after.map(str::len),
        before_line_count: before.map(line_count),
        after_line_count: after.map(line_count),
        diff,
        metadata: proof.metadata,
        created_at: chrono::Utc::now().to_rfc3339(),
    }
}

fn normalize_target_path(path: &Path) -> String {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    absolute
        .strip_prefix(&cwd)
        .unwrap_or(&absolute)
        .to_string_lossy()
        .replace('\\', "/")
}

fn sha256_text(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn line_count(text: &str) -> usize {
    text.lines().count()
}

fn summarize_diff(before: &str, after: &str) -> FileDiffSummary {
    if before == after {
        return FileDiffSummary {
            start_line: None,
            before_preview: preview_change_text(before, 140),
            after_preview: preview_change_text(after, 140),
            summary: "no content change".to_string(),
        };
    }

    let before_lines: Vec<&str> = before.lines().collect();
    let after_lines: Vec<&str> = after.lines().collect();
    let mut prefix = 0usize;
    let max_prefix = before_lines.len().min(after_lines.len());
    while prefix < max_prefix && before_lines[prefix] == after_lines[prefix] {
        prefix += 1;
    }

    let mut before_suffix = before_lines.len();
    let mut after_suffix = after_lines.len();
    while before_suffix > prefix
        && after_suffix > prefix
        && before_lines[before_suffix - 1] == after_lines[after_suffix - 1]
    {
        before_suffix -= 1;
        after_suffix -= 1;
    }

    let before_chunk = before_lines[prefix..before_suffix].join("\n");
    let after_chunk = after_lines[prefix..after_suffix].join("\n");
    FileDiffSummary {
        start_line: Some(prefix + 1),
        before_preview: preview_change_text(&before_chunk, 140),
        after_preview: preview_change_text(&after_chunk, 140),
        summary: format!(
            "replaced `{}` with `{}`",
            preview_change_text(&before_chunk, 70),
            preview_change_text(&after_chunk, 70),
        ),
    }
}

fn preview_change_text(text: &str, max: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    crate::trunc(trimmed, max).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_summary_reports_first_changed_region() {
        let diff = summarize_diff("a\nbefore\nc\n", "a\nafter\nc\n");
        assert_eq!(diff.start_line, Some(2));
        assert!(diff.before_preview.contains("before"));
        assert!(diff.after_preview.contains("after"));
        assert!(diff.summary.contains("replaced"));
    }
}
