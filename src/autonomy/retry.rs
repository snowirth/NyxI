//! Autonomy retry utilities — failure classification, retry keys, timestamp helpers.
//!
//! These are the pure helpers the autonomy loop uses to decide what to do when
//! a task fails: classify the failure, derive a stable retry key, and format
//! or parse timestamps for backoff/quarantine windows. All state mutation (DB
//! writes, reschedule decisions) lives in the parent `autonomy` module.

use super::{FailureClass, RetryState, Task};

pub(super) fn classify_reconcile_failure(error: &str) -> FailureClass {
    let lower = error.to_lowercase();

    if lower.contains("absolute paths not allowed")
        || lower.contains("path traversal not allowed")
        || lower.contains("permission denied")
        || lower.contains("protected core edit not allowed")
    {
        return FailureClass::Unsafe;
    }

    if lower.contains("path required")
        || lower.contains("only supports built tools under tools/")
        || lower.contains("only supports python built tools")
        || lower.contains("invalid built tool filename")
        || lower.contains("built tool missing")
    {
        return FailureClass::Permanent;
    }

    if lower.contains("database is locked")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("temporar")
        || lower.contains("resource busy")
        || lower.contains("interrupted")
        || lower.contains("broken pipe")
    {
        return FailureClass::Transient;
    }

    if lower.contains("still not visible") || lower.contains("not visible in the live self-model") {
        return FailureClass::InconsistentState;
    }

    FailureClass::InconsistentState
}

pub(super) fn classify_run_tool_failure(error: &str) -> FailureClass {
    let lower = error.to_lowercase();

    if lower.contains("blocked:")
        || lower.contains("absolute paths not allowed")
        || lower.contains("path traversal not allowed")
        || lower.contains("permission denied")
        || lower.contains("autonomy tool not allowed")
        || lower.contains("only allows read")
        || lower.contains("action not allowed")
    {
        return FailureClass::Unsafe;
    }

    if lower.contains("file not found")
        || lower.contains("requires path")
        || lower.contains("missing tool")
        || lower.contains("not present in the live self-model")
        || lower.contains("unknown action")
    {
        return FailureClass::Permanent;
    }

    if lower.contains("database is locked")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("temporar")
        || lower.contains("resource busy")
        || lower.contains("interrupted")
        || lower.contains("broken pipe")
    {
        return FailureClass::Transient;
    }

    if lower.contains("parse error")
        || lower.contains("invalid json")
        || lower.contains("malformed")
        || lower.contains("returned failure")
    {
        return FailureClass::InconsistentState;
    }

    FailureClass::InconsistentState
}

pub(super) fn retry_key_for_task(task: &Task) -> Option<String> {
    match task.kind.as_str() {
        "reconcile_self_model" => task
            .args
            .get("target")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|target| format!("reconcile_self_model:{}", target)),
        "run_tool" => {
            let tool = task.tool.as_deref()?.trim();
            let target = run_tool_retry_target(task)?;
            Some(format!(
                "run_tool:{}:{}",
                tool,
                sanitize_retry_key_fragment(&target)
            ))
        }
        _ => None,
    }
}

pub(super) fn retry_task_label(task: &Task) -> String {
    match task.kind.as_str() {
        "run_tool" => format!(
            "run_tool {}",
            task.tool.as_deref().unwrap_or("unknown_tool")
        ),
        _ => task.kind.clone(),
    }
}

pub(super) fn retry_state_target_label(task: &Task, retry_state: &RetryState) -> String {
    retry_state
        .target
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| run_tool_retry_target(task))
        .unwrap_or_else(|| "unknown target".to_string())
}

pub(super) fn run_tool_retry_target(task: &Task) -> Option<String> {
    if task.kind != "run_tool" {
        return None;
    }
    let tool = task.tool.as_deref()?.trim();
    if let Some(explicit) = task
        .args
        .get("retry_key")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(explicit.to_string());
    }

    match tool {
        "file_ops" => task
            .args
            .get("path")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|path| format!("path={}", path)),
        "git_info" => task
            .args
            .get("action")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|action| format!("action={}", action)),
        _ => Some(format!("tool={}", tool)),
    }
}

pub(super) fn sanitize_retry_key_fragment(value: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '=' | ':') {
            Some(ch)
        } else if ch == '-' || ch == '_' || ch.is_whitespace() {
            Some('-')
        } else {
            None
        };
        if let Some(ch) = mapped {
            if ch == '-' {
                if !last_dash && !out.is_empty() {
                    out.push(ch);
                }
                last_dash = true;
            } else {
                out.push(ch);
                last_dash = false;
            }
        }
    }
    out.trim_matches('-').to_string()
}

pub(super) fn retry_timestamp_in_future(value: Option<&str>) -> bool {
    value
        .and_then(parse_retry_timestamp)
        .map(|timestamp| timestamp > chrono::Utc::now())
        .unwrap_or(false)
}

pub(super) fn retry_timestamp_within_window(
    value: &str,
    now: chrono::DateTime<chrono::Utc>,
    window_secs: i64,
) -> bool {
    parse_retry_timestamp(value)
        .map(|timestamp| {
            let age_secs = now.signed_duration_since(timestamp).num_seconds();
            age_secs >= 0 && age_secs <= window_secs
        })
        .unwrap_or(false)
}

pub(super) fn parse_retry_timestamp(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|naive| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc))
        .or_else(|| {
            chrono::DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
        })
}

pub(super) fn format_retry_timestamp(timestamp: chrono::DateTime<chrono::Utc>) -> String {
    timestamp.format("%Y-%m-%d %H:%M:%S").to_string()
}
