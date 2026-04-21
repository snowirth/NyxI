use crate::autonomy::ActionRunRecord;

#[derive(Debug, Clone)]
pub struct FailureClusterSeed {
    pub fingerprint: String,
    pub task_kind: String,
    pub tool: Option<String>,
    pub failure_class: String,
    pub failure_stage: String,
    pub latest_outcome: String,
    pub issue_signature: String,
    pub exemplar_summary: String,
    pub exemplar_error: Option<String>,
    pub target: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub route: Option<String>,
    pub latest_action_run_id: i64,
    pub latest_task_id: i64,
    pub sample_action_run_ids: Vec<i64>,
    pub first_seen_at: String,
    pub last_seen_at: String,
}

pub fn failure_cluster_seed(run: &ActionRunRecord) -> Option<FailureClusterSeed> {
    if !is_failure_run(run) {
        return None;
    }

    let output = run.output.as_ref();
    let exemplar_error = extract_failure_error(output)
        .or_else(|| {
            run.verifier_verdict
                .as_ref()
                .map(|verdict| verdict.summary.trim().to_string())
        })
        .filter(|value| !value.is_empty());
    let issue_basis = exemplar_error
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or(run.summary.as_str());
    let failure_class =
        extract_failure_class(output).unwrap_or_else(|| classify_failure_class(run, issue_basis));
    let failure_stage = failure_stage_for_run(run);
    let issue_signature = canonical_issue_signature(issue_basis);
    let fingerprint = format!(
        "{}|{}|{}|{}",
        normalize_cluster_fragment(&run.task_kind),
        normalize_cluster_fragment(run.tool.as_deref().unwrap_or("")),
        normalize_cluster_fragment(&failure_class),
        issue_signature
    );

    Some(FailureClusterSeed {
        fingerprint,
        task_kind: run.task_kind.clone(),
        tool: run.tool.clone(),
        failure_class,
        failure_stage,
        latest_outcome: run.outcome.clone(),
        issue_signature,
        exemplar_summary: run.summary.trim().to_string(),
        exemplar_error,
        target: extract_first_string(
            output,
            &[
                "/trace/target",
                "/execution/trace/target",
                "/expected_effect/target",
                "/execution/verification/expected_effect/target",
                "/execution/details/evidence/target",
            ],
        ),
        provider: extract_first_string(
            output,
            &[
                "/execution/llm/actual_calls/0/provider",
                "/execution/details/evidence/llm/actual_calls/0/provider",
                "/evidence/llm/actual_calls/0/provider",
            ],
        ),
        model: extract_first_string(
            output,
            &[
                "/execution/llm/actual_calls/0/model",
                "/execution/details/evidence/llm/actual_calls/0/model",
                "/evidence/llm/actual_calls/0/model",
            ],
        ),
        route: extract_first_string(
            output,
            &[
                "/execution/llm/actual_calls/0/route",
                "/execution/details/evidence/llm/actual_calls/0/route",
                "/evidence/llm/actual_calls/0/route",
            ],
        ),
        latest_action_run_id: run.id,
        latest_task_id: run.task_id,
        sample_action_run_ids: vec![run.id],
        first_seen_at: run.created_at.clone(),
        last_seen_at: run.created_at.clone(),
    })
}

fn is_failure_run(run: &ActionRunRecord) -> bool {
    matches!(
        run.outcome.as_str(),
        "failed" | "retry_scheduled" | "quarantined"
    ) || run.verified == Some(false)
}

fn failure_stage_for_run(run: &ActionRunRecord) -> String {
    if run.outcome == "quarantined" {
        "quarantine".to_string()
    } else if run.outcome == "retry_scheduled" {
        "retry".to_string()
    } else if !run.executed {
        "blocked".to_string()
    } else if run.verified == Some(false) {
        "verification".to_string()
    } else {
        "execution".to_string()
    }
}

fn extract_failure_error(output: Option<&serde_json::Value>) -> Option<String> {
    extract_first_string(
        output,
        &[
            "/error",
            "/execution/details/error",
            "/retry_state/last_error",
            "/execution/details/retry_state/last_error",
            "/rollback_reason/summary",
            "/execution/verification/rollback_reason/summary",
            "/verifier_verdict/summary",
            "/execution/verification/verifier_verdict/summary",
        ],
    )
}

fn extract_failure_class(output: Option<&serde_json::Value>) -> Option<String> {
    extract_first_string(
        output,
        &[
            "/retry_state/failure_class",
            "/execution/details/retry_state/failure_class",
        ],
    )
    .map(|value| normalize_cluster_fragment(&value))
    .filter(|value| !value.is_empty())
}

fn extract_first_string(output: Option<&serde_json::Value>, pointers: &[&str]) -> Option<String> {
    let output = output?;
    pointers.iter().find_map(|pointer| {
        output
            .pointer(pointer)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

fn classify_failure_class(run: &ActionRunRecord, message: &str) -> String {
    let lower = message.to_ascii_lowercase();

    if [
        "blocked:",
        "absolute paths not allowed",
        "path traversal not allowed",
        "permission denied",
        "protected core edit not allowed",
        "action not allowed",
        "only allows read",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern))
    {
        return "unsafe".to_string();
    }

    if [
        "file not found",
        "requires path",
        "path required",
        "missing tool",
        "built tool missing",
        "not present in the live self-model",
        "unknown action",
        "invalid built tool filename",
        "only supports built tools",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern))
    {
        return "permanent".to_string();
    }

    if [
        "database is locked",
        "timed out",
        "timeout",
        "temporar",
        "resource busy",
        "interrupted",
        "broken pipe",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern))
    {
        return "transient".to_string();
    }

    if [
        "still not visible",
        "not visible in the live self-model",
        "parse error",
        "invalid json",
        "malformed",
        "returned failure",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern))
    {
        return "inconsistent_state".to_string();
    }

    if run.outcome == "retry_scheduled" {
        "transient".to_string()
    } else if run.outcome == "quarantined"
        && run
            .rollback_reason
            .as_ref()
            .map(|reason| !reason.retryable)
            .unwrap_or(false)
    {
        "permanent".to_string()
    } else if run.verified == Some(false) {
        "inconsistent_state".to_string()
    } else {
        "transient".to_string()
    }
}

fn canonical_issue_signature(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    for pattern in [
        "database is locked",
        "timed out",
        "timeout",
        "permission denied",
        "absolute paths not allowed",
        "path traversal not allowed",
        "still not visible",
        "not visible in the live self-model",
        "parse error",
        "invalid json",
        "missing tool",
        "file not found",
        "unknown action",
        "returned failure",
        "protected core edit not allowed",
    ] {
        if lower.contains(pattern) {
            return pattern.to_string();
        }
    }

    let normalized = lower
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { ' ' })
        .collect::<String>();
    let tokens = normalized
        .split_whitespace()
        .filter(|token| token.len() >= 3)
        .filter(|token| !is_noise_token(token))
        .take(10)
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        "unknown failure".to_string()
    } else {
        tokens.join(" ")
    }
}

fn is_noise_token(token: &str) -> bool {
    if token.chars().all(|ch| ch.is_ascii_digit()) {
        return true;
    }
    if token.len() >= 8 && token.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return true;
    }
    matches!(
        token,
        "task"
            | "after"
            | "attempt"
            | "attempts"
            | "until"
            | "with"
            | "from"
            | "that"
            | "this"
            | "were"
            | "when"
            | "into"
            | "because"
            | "execution"
            | "verified"
            | "retry"
            | "scheduled"
            | "quarantined"
    )
}

fn normalize_cluster_fragment(raw: &str) -> String {
    let normalized = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}
