use crate::db::ReplayFailureClusterRecord;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct DistilledPolicyCandidate {
    pub kind: String,
    pub scope: String,
    pub title: String,
    pub description: String,
    pub rationale: String,
    pub trigger: String,
    pub proposed_change: serde_json::Value,
    pub evidence: serde_json::Value,
    pub confidence: f64,
    pub importance: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApprovedPolicyDirective {
    pub trigger: String,
    pub title: String,
    pub description: String,
    pub kind: String,
    pub rule: String,
    pub task_kind: Option<String>,
    pub tool: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub route: Option<String>,
    pub preferred_outcome: Option<String>,
}

pub fn candidate_from_failure_cluster(
    cluster: &ReplayFailureClusterRecord,
) -> Option<DistilledPolicyCandidate> {
    if cluster.occurrence_count < 3 {
        return None;
    }
    if cluster.issue_signature == "unknown failure" && cluster.occurrence_count < 4 {
        return None;
    }

    let tool = cluster
        .tool
        .as_deref()
        .unwrap_or(cluster.task_kind.as_str());
    let task_fragment = normalize_fragment(&cluster.task_kind);
    let tool_fragment = cluster
        .tool
        .as_deref()
        .map(normalize_fragment)
        .filter(|value| !value.is_empty());
    let scope = match tool_fragment.as_deref() {
        Some(tool_fragment) => format!("autonomy.{}.{}", task_fragment, tool_fragment),
        None => format!("autonomy.{}", task_fragment),
    };

    let (kind, title, description, trigger_suffix, proposed_change) =
        match cluster.issue_signature.as_str() {
            "file not found" => (
                "tool_guard",
                format!("preflight {} targets before execution", tool),
                format!(
                    "Before {} executes against a target path, verify the path exists and fail fast with a repair hint instead of repeating the same missing-path action.",
                    tool
                ),
                "preflight-path-exists",
                serde_json::json!({
                    "policy_kind": "tool_guard",
                    "rule": "preflight_path_exists",
                    "task_kind": cluster.task_kind,
                    "tool": cluster.tool,
                    "failure_signature": cluster.issue_signature,
                    "preferred_outcome": "block_and_repair",
                }),
            ),
            "permission denied" | "absolute paths not allowed" | "path traversal not allowed" => (
                "tool_guard",
                format!("enforce {} workspace boundaries before execution", tool),
                format!(
                    "When {} hits repeated safety boundary failures, require an allowed in-workspace target before execution instead of retrying the blocked path.",
                    tool
                ),
                "workspace-boundary-preflight",
                serde_json::json!({
                    "policy_kind": "tool_guard",
                    "rule": "enforce_workspace_boundary_preflight",
                    "task_kind": cluster.task_kind,
                    "tool": cluster.tool,
                    "failure_signature": cluster.issue_signature,
                    "preferred_outcome": "repair_target_before_retry",
                }),
            ),
            "still not visible" | "not visible in the live self-model" => (
                "verification_rule",
                "require live self-model confirmation before success".to_string(),
                "After a self-edit or tool-growth change reports success, require a live self-model visibility check before the run can be marked verified.".to_string(),
                "require-live-self-model-confirmation",
                serde_json::json!({
                    "policy_kind": "verification_rule",
                    "rule": "require_live_self_model_confirmation",
                    "task_kind": cluster.task_kind,
                    "tool": cluster.tool,
                    "failure_signature": cluster.issue_signature,
                    "preferred_outcome": "verify_before_claiming_success",
                }),
            ),
            "parse error" | "invalid json" | "malformed" | "returned failure" => (
                "verification_rule",
                format!("verify {} structured output before claiming success", tool),
                format!(
                    "When {} returns malformed or inconsistent output, require schema or shape validation before the run can be treated as successful.",
                    tool
                ),
                "structured-output-validation",
                serde_json::json!({
                    "policy_kind": "verification_rule",
                    "rule": "require_structured_output_validation",
                    "task_kind": cluster.task_kind,
                    "tool": cluster.tool,
                    "failure_signature": cluster.issue_signature,
                    "preferred_outcome": "repair_or_retry_with_stricter_validation",
                }),
            ),
            "database is locked" | "timeout" | "timed out" | "resource busy" | "broken pipe"
            | "interrupted" => (
                "routing_rule",
                format!("back off and retry repeated transient {} failures", tool),
                format!(
                    "When {} repeats the same transient failure, prefer bounded backoff and a guarded retry or fallback route instead of hammering the same path immediately.",
                    tool
                ),
                "bounded-transient-backoff",
                serde_json::json!({
                    "policy_kind": "routing_rule",
                    "rule": "bounded_transient_backoff",
                    "task_kind": cluster.task_kind,
                    "tool": cluster.tool,
                    "provider": cluster.provider,
                    "model": cluster.model,
                    "route": cluster.route,
                    "failure_signature": cluster.issue_signature,
                    "preferred_outcome": "retry_with_backoff_or_fallback",
                }),
            ),
            _ => match cluster.failure_class.as_str() {
                "unsafe" => (
                    "tool_guard",
                    format!("respect {} safety constraints before execution", tool),
                    format!(
                        "Repeated unsafe {} failures show the planner should repair the target or plan before trying the same action again.",
                        tool
                    ),
                    "respect-safety-constraints",
                    serde_json::json!({
                        "policy_kind": "tool_guard",
                        "rule": "respect_safety_constraints_before_execution",
                        "task_kind": cluster.task_kind,
                        "tool": cluster.tool,
                        "failure_signature": cluster.issue_signature,
                        "preferred_outcome": "repair_plan_before_retry",
                    }),
                ),
                "inconsistent_state" => (
                    "verification_rule",
                    format!("verify real {} state before marking success", tool),
                    format!(
                        "Repeated inconsistent-state failures show {} needs an explicit state check before the run can be trusted.",
                        tool
                    ),
                    "verify-real-state-before-success",
                    serde_json::json!({
                        "policy_kind": "verification_rule",
                        "rule": "verify_real_state_before_success",
                        "task_kind": cluster.task_kind,
                        "tool": cluster.tool,
                        "failure_signature": cluster.issue_signature,
                        "preferred_outcome": "verify_before_commit",
                    }),
                ),
                "permanent" => (
                    "tool_guard",
                    format!("preflight {} targets before permanent retries", tool),
                    format!(
                        "Repeated permanent {} failures suggest adding a target validation preflight before retrying the same action path.",
                        tool
                    ),
                    "preflight-target-validation",
                    serde_json::json!({
                        "policy_kind": "tool_guard",
                        "rule": "preflight_target_validation",
                        "task_kind": cluster.task_kind,
                        "tool": cluster.tool,
                        "failure_signature": cluster.issue_signature,
                        "preferred_outcome": "inspect_and_repair_before_retry",
                    }),
                ),
                _ => (
                    "routing_rule",
                    format!("slow down repeated {} retries", tool),
                    format!(
                        "Repeated {} failures should trigger a bounded repair or backoff path instead of immediate re-execution.",
                        tool
                    ),
                    "bounded-retry-repair",
                    serde_json::json!({
                        "policy_kind": "routing_rule",
                        "rule": "bounded_retry_repair",
                        "task_kind": cluster.task_kind,
                        "tool": cluster.tool,
                        "provider": cluster.provider,
                        "model": cluster.model,
                        "route": cluster.route,
                        "failure_signature": cluster.issue_signature,
                        "preferred_outcome": "pause_and_repair_before_retry",
                    }),
                ),
            },
        };

    let confidence =
        (0.6 + (cluster.occurrence_count.saturating_sub(3) as f64 * 0.07)).clamp(0.6, 0.95);
    let importance =
        (0.64 + (cluster.occurrence_count.saturating_sub(3) as f64 * 0.06)).clamp(0.64, 0.96);
    let trigger = match tool_fragment.as_deref() {
        Some(tool_fragment) => format!(
            "policy:{}:{}:{}:{}",
            normalize_fragment(kind),
            task_fragment,
            tool_fragment,
            trigger_suffix
        ),
        None => format!(
            "policy:{}:{}:{}",
            normalize_fragment(kind),
            task_fragment,
            trigger_suffix
        ),
    };
    let rationale = format!(
        "Offline replay saw the {} failure '{}' repeat {} time(s) for task kind {}{}.",
        cluster.failure_class,
        cluster.issue_signature,
        cluster.occurrence_count,
        cluster.task_kind,
        cluster
            .tool
            .as_deref()
            .map(|value| format!(" via {}", value))
            .unwrap_or_default()
    );

    Some(DistilledPolicyCandidate {
        kind: kind.to_string(),
        scope,
        title: crate::trunc(&title, 96).to_string(),
        description,
        rationale,
        trigger,
        proposed_change,
        evidence: serde_json::json!({
            "cluster_id": cluster.id,
            "fingerprint": cluster.fingerprint,
            "occurrence_count": cluster.occurrence_count,
            "failure_class": cluster.failure_class,
            "failure_stage": cluster.failure_stage,
            "issue_signature": cluster.issue_signature,
            "task_kind": cluster.task_kind,
            "tool": cluster.tool,
            "provider": cluster.provider,
            "model": cluster.model,
            "route": cluster.route,
            "sample_action_run_ids": cluster.sample_action_run_ids,
            "latest_action_run_id": cluster.latest_action_run_id,
            "latest_task_id": cluster.latest_task_id,
        }),
        confidence,
        importance,
    })
}

pub fn directive_from_candidate(
    candidate: &crate::db::PolicyCandidateRecord,
) -> Option<ApprovedPolicyDirective> {
    if candidate.status != "approved" {
        return None;
    }

    let kind = candidate
        .proposed_change
        .get("policy_kind")
        .and_then(|value| value.as_str())
        .unwrap_or(candidate.kind.as_str())
        .trim();
    let rule = candidate
        .proposed_change
        .get("rule")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;

    Some(ApprovedPolicyDirective {
        trigger: candidate.trigger.clone(),
        title: candidate.title.clone(),
        description: candidate.description.clone(),
        kind: kind.to_string(),
        rule: rule.to_string(),
        task_kind: candidate
            .proposed_change
            .get("task_kind")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        tool: candidate
            .proposed_change
            .get("tool")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        provider: candidate
            .proposed_change
            .get("provider")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        model: candidate
            .proposed_change
            .get("model")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        route: candidate
            .proposed_change
            .get("route")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        preferred_outcome: candidate
            .proposed_change
            .get("preferred_outcome")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
    })
}

pub fn is_safe_live_policy_kind(kind: &str) -> bool {
    matches!(kind, "tool_guard" | "verification_rule" | "routing_rule")
}

pub fn directive_applies_to_task_tool(
    directive: &ApprovedPolicyDirective,
    task_kind: Option<&str>,
    tool_name: Option<&str>,
) -> bool {
    let task_matches = task_kind.map(|task_kind| {
        directive
            .task_kind
            .as_deref()
            .map(|candidate| normalize_fragment(candidate) == normalize_fragment(task_kind))
            .unwrap_or(true)
    });
    let tool_matches = tool_name.map(|tool_name| {
        directive
            .tool
            .as_deref()
            .map(|candidate| normalize_fragment(candidate) == normalize_fragment(tool_name))
            .unwrap_or(true)
    });

    task_matches.unwrap_or(true) && tool_matches.unwrap_or(true)
}

pub fn directive_applies_to_llm_route(
    directive: &ApprovedPolicyDirective,
    provider: &str,
    route: &str,
    model: Option<&str>,
) -> bool {
    if directive.kind != "routing_rule" {
        return false;
    }

    let provider_matches = directive
        .provider
        .as_deref()
        .map(|candidate| normalize_fragment(candidate) == normalize_fragment(provider))
        .unwrap_or(true);
    let route_matches = directive
        .route
        .as_deref()
        .map(|candidate| normalize_fragment(candidate) == normalize_fragment(route))
        .unwrap_or(true);
    let model_matches = model.map(|model| {
        directive
            .model
            .as_deref()
            .map(|candidate| normalize_fragment(candidate) == normalize_fragment(model))
            .unwrap_or(true)
    });

    provider_matches && route_matches && model_matches.unwrap_or(true)
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
