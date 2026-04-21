use crate::db::ReplayFailureClusterRecord;

#[derive(Debug, Clone)]
pub struct DistilledProcedureCandidate {
    pub title: String,
    pub content: String,
    pub trigger: String,
    pub confidence: f64,
    pub importance: f64,
    pub episode_summary: String,
    pub episode_content: String,
}

pub fn procedure_candidate_from_failure_cluster(
    cluster: &ReplayFailureClusterRecord,
) -> Option<DistilledProcedureCandidate> {
    if cluster.occurrence_count < 2 {
        return None;
    }
    if cluster.issue_signature == "unknown failure" && cluster.occurrence_count < 3 {
        return None;
    }

    let tool_label = cluster
        .tool
        .as_deref()
        .unwrap_or(cluster.task_kind.as_str());
    let title = procedure_title(cluster, tool_label);
    let content = procedure_content(cluster, tool_label);
    let trigger = if let Some(tool) = cluster
        .tool
        .as_deref()
        .filter(|tool| !tool.trim().is_empty())
    {
        format!(
            "replay_failure:{}:{}:{}:{}",
            normalize_fragment(&cluster.task_kind),
            normalize_fragment(tool),
            normalize_fragment(&cluster.failure_class),
            normalize_fragment(&cluster.issue_signature)
        )
    } else {
        format!(
            "replay_failure:{}:{}:{}",
            normalize_fragment(&cluster.task_kind),
            normalize_fragment(&cluster.failure_class),
            normalize_fragment(&cluster.issue_signature)
        )
    };
    let confidence =
        (0.56 + (cluster.occurrence_count.saturating_sub(2) as f64 * 0.08)).clamp(0.56, 0.94);
    let importance =
        (0.62 + (cluster.occurrence_count.saturating_sub(2) as f64 * 0.06)).clamp(0.62, 0.95);
    let exemplar = cluster
        .exemplar_error
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(cluster.exemplar_summary.as_str());
    let episode_summary = format!(
        "replay lesson: {} repeated {} times",
        cluster.issue_signature, cluster.occurrence_count
    );
    let episode_content = format!(
        "Replay lesson from repeated {} failures for task kind {}{}: {}. Observed {} time(s). Latest outcome: {}. Latest exemplar: {}. Sample action runs: {}.",
        cluster.failure_class,
        cluster.task_kind,
        cluster
            .tool
            .as_deref()
            .map(|tool| format!(" via {}", tool))
            .unwrap_or_default(),
        content,
        cluster.occurrence_count,
        cluster.latest_outcome,
        exemplar,
        cluster
            .sample_action_run_ids
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );

    Some(DistilledProcedureCandidate {
        title,
        content,
        trigger,
        confidence,
        importance,
        episode_summary,
        episode_content,
    })
}

fn procedure_title(cluster: &ReplayFailureClusterRecord, tool_label: &str) -> String {
    let title = match cluster.issue_signature.as_str() {
        "file not found" => format!("verify {} paths before retrying", tool_label),
        "permission denied" | "absolute paths not allowed" | "path traversal not allowed" => {
            format!("respect {} path boundaries before retrying", tool_label)
        }
        "database is locked" | "timeout" | "timed out" | "resource busy" => {
            format!("back off after transient {} failures", tool_label)
        }
        "parse error" | "invalid json" | "malformed" | "returned failure" => {
            format!("validate {} output before claiming success", tool_label)
        }
        "still not visible" | "not visible in the live self-model" => {
            "verify live self-model visibility before claiming capability".to_string()
        }
        _ => format!("{} replay lesson: {}", tool_label, cluster.issue_signature),
    };
    crate::trunc(&title, 96).to_string()
}

fn procedure_content(cluster: &ReplayFailureClusterRecord, tool_label: &str) -> String {
    match cluster.issue_signature.as_str() {
        "file not found" => format!(
            "When {} fails because a file path is missing, verify the path exists and correct the target before retrying or claiming success.",
            tool_label
        ),
        "permission denied" | "absolute paths not allowed" | "path traversal not allowed" => {
            format!(
                "When {} is blocked by a path or permission boundary, respect the safety constraint and choose an allowed target before retrying.",
                tool_label
            )
        }
        "database is locked" | "timeout" | "timed out" | "resource busy" | "broken pipe" | "interrupted" => {
            format!(
                "When {} hits a repeated transient failure such as {}, back off and retry later instead of treating it as a permanent breakage.",
                tool_label,
                cluster.issue_signature
            )
        }
        "parse error" | "invalid json" | "malformed" | "returned failure" => format!(
            "When {} returns malformed or inconsistent output, validate the output shape before claiming success and retry with a stricter repair path if needed.",
            tool_label
        ),
        "still not visible" | "not visible in the live self-model" => {
            "When a change reports success but the capability is still not visible in the live self-model, verify registration and live visibility before claiming the improvement.".to_string()
        }
        _ => match cluster.failure_class.as_str() {
            "unsafe" => format!(
                "When {} is repeatedly blocked by a safety boundary around {}, change the plan to respect the boundary instead of forcing the action.",
                tool_label,
                cluster.issue_signature
            ),
            "permanent" => format!(
                "When {} repeats the same permanent failure around {}, inspect the target and repair the root cause before retrying or claiming success.",
                tool_label,
                cluster.issue_signature
            ),
            "inconsistent_state" => format!(
                "When {} repeats an inconsistent-state failure around {}, verify the real system state before reporting success or scheduling follow-up work.",
                tool_label,
                cluster.issue_signature
            ),
            _ => format!(
                "When {} repeats the same failure pattern around {}, slow down, inspect the last error, and apply a repair step before retrying.",
                tool_label,
                cluster.issue_signature
            ),
        },
    }
}

fn normalize_fragment(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}
