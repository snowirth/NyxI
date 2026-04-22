use std::path::Path;

use anyhow::{Result, anyhow};

use crate::runtime::{
    SelfModelSnapshot,
    verifier::{ActionVerification, ExpectedEffectContract, RollbackReason},
};

use super::{ACTION_RUN_SCHEMA_VERSION, Db, RetryState, Task, task_trace_refs};

pub(super) fn approved_live_policy_directives_for_task(
    db: &Db,
    task_kind: Option<&str>,
    tool_name: Option<&str>,
    limit: usize,
) -> Vec<crate::improvement::policy::ApprovedPolicyDirective> {
    db.list_approved_policy_candidates(limit)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|candidate| crate::improvement::policy::directive_from_candidate(&candidate))
        .filter(|directive| crate::improvement::policy::is_safe_live_policy_kind(&directive.kind))
        .filter(|directive| {
            crate::improvement::policy::directive_applies_to_task_tool(
                directive, task_kind, tool_name,
            )
        })
        .collect()
}

fn task_expected_target(task: &Task, trace: &super::TaskTraceRefs) -> Option<String> {
    trace.target.clone().or_else(|| match task.kind.as_str() {
        "deliver_message" => Some("proactive_queue".to_string()),
        "store_memory" => task
            .args
            .get("network")
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .or_else(|| Some("knowledge".to_string())),
        "run_tool" => task.tool.clone(),
        "review_operator_brief" => Some("operator_brief".to_string()),
        "review_system_health" => Some("system_health".to_string()),
        "check_mentions" => Some("twitter".to_string()),
        "review_timeline" => Some("twitter".to_string()),
        "review_news" => Some("web_search".to_string()),
        "review_growth" => task
            .args
            .get("growth_event_id")
            .and_then(|value| value.as_i64())
            .map(|value| format!("growth_event:{}", value)),
        _ => task.tool.clone(),
    })
}

pub(super) fn expected_effect_for_task(task: &Task) -> ExpectedEffectContract {
    let trace = task_trace_refs(task);
    let target = task_expected_target(task, &trace);
    match task.kind.as_str() {
        "deliver_message" => ExpectedEffectContract {
            kind: "message_delivery".to_string(),
            target,
            detail: "enqueue a proactive follow-up for the user".to_string(),
            verification_method: "queue_contains_message".to_string(),
        },
        "store_memory" => ExpectedEffectContract {
            kind: "memory_write".to_string(),
            target,
            detail: "store or safely reject the candidate memory".to_string(),
            verification_method: "memory_recall_or_noop_guardrail".to_string(),
        },
        "run_tool" => ExpectedEffectContract {
            kind: "tool_execution".to_string(),
            target,
            detail: "run the tool and capture observable runtime output".to_string(),
            verification_method: "tool_runtime_success_payload".to_string(),
        },
        "review_operator_brief" => ExpectedEffectContract {
            kind: "operator_brief_review".to_string(),
            target,
            detail: "review the current operator brief and surface the most relevant next step"
                .to_string(),
            verification_method: "operator_brief_snapshot".to_string(),
        },
        "review_system_health" => ExpectedEffectContract {
            kind: "system_health_review".to_string(),
            target,
            detail: "review runtime health and summarize concrete concerns when present"
                .to_string(),
            verification_method: "system_health_snapshot".to_string(),
        },
        "check_mentions" => ExpectedEffectContract {
            kind: "mentions_check".to_string(),
            target,
            detail: "check mentions and surface them only when something actionable appears"
                .to_string(),
            verification_method: "twitter_mentions_payload".to_string(),
        },
        "review_timeline" => ExpectedEffectContract {
            kind: "timeline_review".to_string(),
            target,
            detail: "review the twitter timeline and summarize the latest visible activity"
                .to_string(),
            verification_method: "twitter_timeline_payload".to_string(),
        },
        "review_news" => ExpectedEffectContract {
            kind: "news_review".to_string(),
            target,
            detail: "read current web news results for a query and summarize what surfaced"
                .to_string(),
            verification_method: "web_search_results_payload".to_string(),
        },
        "reconcile_self_model" => ExpectedEffectContract {
            kind: "self_model_alignment".to_string(),
            target,
            detail: "make the built capability visible in the live self-model".to_string(),
            verification_method: "live_self_model_snapshot".to_string(),
        },
        "review_growth" => ExpectedEffectContract {
            kind: "growth_review".to_string(),
            target,
            detail: "review a growth signal and distill a reusable lesson when warranted"
                .to_string(),
            verification_method: "review_completion_trace".to_string(),
        },
        _ => ExpectedEffectContract {
            kind: task.kind.clone(),
            target,
            detail: format!("complete the {} task", task.kind),
            verification_method: "action_trace_recorded".to_string(),
        },
    }
}

pub(super) fn stale_recovery_expected_effect(task: &Task) -> ExpectedEffectContract {
    ExpectedEffectContract {
        kind: "stale_task_recovery".to_string(),
        target: Some(format!("task:{}", task.id)),
        detail: format!("return stale {} work to the pending queue", task.kind),
        verification_method: "task_rescheduled_pending".to_string(),
    }
}

pub(super) fn rollback_reason(kind: &str, summary: &str, retryable: bool) -> RollbackReason {
    RollbackReason {
        kind: kind.to_string(),
        summary: summary.to_string(),
        retryable,
    }
}

fn task_status_for_action_outcome(task: &Task, outcome: &str) -> &'static str {
    match outcome {
        "completed" => super::TaskStatus::Completed.as_str(),
        "failed" => super::TaskStatus::Failed.as_str(),
        "retry_scheduled" | "recovered_stale_running_task" | "dependency_blocked" => {
            super::TaskStatus::Pending.as_str()
        }
        "quarantined" => super::TaskStatus::Cancelled.as_str(),
        _ => task.status.as_str(),
    }
}

fn action_run_execution_success(outcome: &str, verification: &ActionVerification) -> bool {
    verification.verified.unwrap_or(
        verification.executed && matches!(outcome, "completed" | "recovered_stale_running_task"),
    )
}

pub(super) fn action_run_output(
    outcome: &str,
    summary: &str,
    task: &Task,
    self_model: Option<&SelfModelSnapshot>,
    planning_notes: &[String],
    world_context: Option<serde_json::Value>,
    error: Option<&str>,
    retry_state: Option<&RetryState>,
    cleanup: Option<serde_json::Value>,
    verification: &ActionVerification,
    evidence: Option<serde_json::Value>,
) -> serde_json::Value {
    let runtime = self_model.map(|self_model| {
        serde_json::json!({
            "self_model_generated_at": self_model.generated_at,
            "capability_count": self_model.capability_count(),
            "constraint_count": self_model.constraints.len(),
            "active_growth_goal_count": self_model.growth.active_goals.len(),
            "hosted_tool_loop_ready": self_model.runtime.hosted_tool_loop_ready,
            "autonomous_llm_ready": self_model.runtime.autonomous_llm_ready,
        })
    });
    let trace = task_trace_refs(task);
    let execution_target = task_expected_target(task, &trace);
    let operator_signal = build_operator_signal(
        task,
        summary,
        &trace,
        execution_target.as_deref(),
        verification,
    );
    let verification_json = serde_json::json!({
        "executed": verification.executed,
        "verified": verification.verified,
        "expected_effect": verification.expected_effect,
        "verifier_verdict": verification.verifier_verdict,
        "rollback_reason": verification.rollback_reason,
    });
    let planning_json = serde_json::json!({
        "notes": planning_notes,
        "world": world_context.clone(),
    });
    let execution_details = serde_json::json!({
        "runtime": runtime.clone(),
        "planning": planning_json.clone(),
        "error": error,
        "retry_state": retry_state,
        "cleanup": cleanup,
        "evidence": evidence.clone(),
        "operator_signal": operator_signal.clone(),
    });
    let execution = serde_json::json!({
        "schema_version": crate::runtime::CHAT_EXECUTION_TRACE_SCHEMA_VERSION,
        "surface": "autonomy",
        "kind": task.kind,
        "summary": summary,
        "outcome": outcome,
        "success": action_run_execution_success(outcome, verification),
        "trace": {
            "source": "autonomy",
            "target": execution_target,
            "task_id": task.id,
            "task_kind": task.kind,
            "tool": task.tool,
            "observation_id": trace.observation_id,
            "growth_event_id": trace.growth_event_id,
            "snapshot_id": trace.snapshot_id,
            "dedupe_key": trace.dedupe_key,
            "retry_key": trace.retry_key,
            "trigger_kind": trace.trigger_kind,
            "capability_name": trace.capability_name,
            "review_kind": trace.review_kind,
        },
        "verification": verification_json.clone(),
        "operator_signal": operator_signal.clone(),
        "details": execution_details,
    });

    serde_json::json!({
        "schema_version": ACTION_RUN_SCHEMA_VERSION,
        "outcome": outcome,
        "summary": summary,
        "task": {
            "id": task.id,
            "kind": task.kind,
            "title": task.title,
            "tool": task.tool,
            "status": task_status_for_action_outcome(task, outcome),
            "priority": task.priority,
        },
        "runtime": runtime,
        "planning": planning_json,
        "kind": task.kind,
        "title": task.title,
        "tool": task.tool,
        "self_model_generated_at": self_model.map(|snapshot| snapshot.generated_at.clone()),
        "capability_count": self_model.map(|snapshot| snapshot.capability_count()),
        "constraint_count": self_model.map(|snapshot| snapshot.constraints.len()),
        "active_growth_goal_count": self_model.map(|snapshot| snapshot.growth.active_goals.len()),
        "hosted_tool_loop_ready": self_model.map(|snapshot| snapshot.runtime.hosted_tool_loop_ready),
        "autonomous_llm_ready": self_model.map(|snapshot| snapshot.runtime.autonomous_llm_ready),
        "planning_notes": planning_notes,
        "world": world_context,
        "trace": trace.clone(),
        "error": error,
        "retry_state": retry_state,
        "cleanup": cleanup,
        "executed": verification.executed,
        "verified": verification.verified,
        "expected_effect": verification.expected_effect,
        "verifier_verdict": verification.verifier_verdict,
        "rollback_reason": verification.rollback_reason,
        "verification": verification_json.clone(),
        "evidence": evidence,
        "operator_signal": operator_signal,
        "execution": execution,
    })
}

fn build_operator_signal(
    task: &Task,
    summary: &str,
    trace: &super::TaskTraceRefs,
    execution_target: Option<&str>,
    verification: &ActionVerification,
) -> serde_json::Value {
    serde_json::json!({
        "task_id": task.id,
        "task_kind": task.kind,
        "task_title": task.title,
        "summary": summary,
        "target": execution_target,
        "attention_required": verification.verified != Some(true) || verification.rollback_reason.is_some(),
        "verification_status": operator_verification_status(verification),
        "verification_summary": operator_verification_summary(verification),
        "verification_checks": verification
            .verifier_verdict
            .as_ref()
            .map(|verdict| verdict.checks.clone())
            .unwrap_or_default(),
        "rollback": verification.rollback_reason.as_ref().map(operator_rollback_signal),
        "inspect": {
            "task_trace_path": format!("/api/autonomy/tasks/{}/trace", task.id),
            "target": execution_target,
            "observation_id": trace.observation_id,
            "growth_event_id": trace.growth_event_id,
            "snapshot_id": trace.snapshot_id,
            "verification_method": verification
                .expected_effect
                .as_ref()
                .map(|effect| effect.verification_method.clone()),
        }
    })
}

fn operator_verification_status(verification: &ActionVerification) -> String {
    verification
        .verifier_verdict
        .as_ref()
        .map(|verdict| verdict.status.clone())
        .unwrap_or_else(|| match (verification.executed, verification.verified) {
            (false, _) => "blocked_before_execution".to_string(),
            (true, Some(true)) => "verified".to_string(),
            (true, Some(false)) => "failed".to_string(),
            (true, None) => "executed_unverified".to_string(),
        })
}

fn operator_verification_summary(verification: &ActionVerification) -> String {
    let status = operator_verification_status(verification).replace('_', " ");
    if let Some(verdict) = verification.verifier_verdict.as_ref() {
        let summary = verdict.summary.trim();
        if !summary.is_empty() {
            return format!("{}: {}", status, crate::trunc(summary, 160));
        }
    }

    let fallback = match (verification.executed, verification.verified) {
        (false, _) => "blocked before execution",
        (true, Some(true)) => "verified cleanly",
        (true, Some(false)) => "failed verification",
        (true, None) => "executed without a recorded verifier verdict",
    };
    format!("{}: {}", status, fallback)
}

fn operator_rollback_signal(reason: &RollbackReason) -> serde_json::Value {
    serde_json::json!({
        "kind": reason.kind,
        "summary": reason.summary,
        "retryable": reason.retryable,
        "display": format!(
            "{}: {} ({})",
            reason.kind.replace('_', " "),
            crate::trunc(reason.summary.trim(), 160),
            if reason.retryable {
                "retryable"
            } else {
                "not retryable"
            },
        ),
    })
}

pub(super) fn sanitize_autonomous_tool_args(
    tool: &str,
    args: &serde_json::Value,
) -> Result<serde_json::Value> {
    match tool {
        "git_info" => {
            let action = args
                .get("action")
                .and_then(|value| value.as_str())
                .unwrap_or("status");
            match action {
                "status" | "diff" | "todos" => Ok(serde_json::json!({ "action": action })),
                "log" => {
                    let count = args
                        .get("count")
                        .and_then(|value| value.as_u64())
                        .unwrap_or(5)
                        .clamp(1, 5);
                    Ok(serde_json::json!({ "action": "log", "count": count }))
                }
                _ => Err(anyhow!(
                    "autonomy tool git_info action not allowed: {}",
                    action
                )),
            }
        }
        "file_ops" => {
            let action = args
                .get("action")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let path = args
                .get("path")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();

            if action != "read" {
                return Err(anyhow!("autonomy tool file_ops only allows read"));
            }
            if path.is_empty() {
                return Err(anyhow!("autonomy tool file_ops requires path"));
            }

            Ok(serde_json::json!({ "action": "read", "path": path }))
        }
        _ => Err(anyhow!("autonomy tool not allowed: {}", tool)),
    }
}

pub(super) fn extract_tool_output(result: &serde_json::Value) -> String {
    if let Some(output) = result.get("output").and_then(|value| value.as_str()) {
        return output.to_string();
    }

    if let Some(error) = result.get("error").and_then(|value| value.as_str()) {
        return error.to_string();
    }

    result.to_string()
}

pub(super) fn compact_tool_output(text: &str) -> String {
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(6)
        .collect();

    if lines.is_empty() {
        text.trim().to_string()
    } else {
        lines.join(" | ")
    }
}

pub(super) fn growth_lesson_text(
    kind: &str,
    target: &str,
    message: &str,
    success: bool,
    repair_rounds: u64,
    self_model: &SelfModelSnapshot,
) -> Option<String> {
    let target_capability = infer_capability_name_from_target(target);
    let capability_visible = target_capability
        .as_deref()
        .map(|name| self_model.has_capability_named(name))
        .unwrap_or(false);
    let target_is_protected = is_protected_core_target(target);

    if kind == "tool_growth_result" {
        if let Some(name) = target_capability.as_deref() {
            if success {
                if capability_visible {
                    return Some(format!(
                        "growth lesson: {} succeeded and capability {} is now visible in the live self-model. {}",
                        kind,
                        name,
                        crate::trunc(message, 160)
                    ));
                }
                return Some(format!(
                    "growth lesson: {} reported success for {} but the capability is not visible in the live self-model yet, so verify promotion and registration. {}",
                    kind,
                    name,
                    crate::trunc(message, 160)
                ));
            }

            return Some(format!(
                "growth lesson: {} failed for {} and the capability is still not visible in the live self-model. {}",
                kind,
                name,
                crate::trunc(message, 160)
            ));
        }
    }

    if kind == "self_model_gap" {
        if let Some(name) = target_capability.as_deref() {
            if capability_visible {
                return Some(format!(
                    "growth lesson: self-model alignment drift for {} was reconciled and capability {} is visible again. {}",
                    target,
                    name,
                    crate::trunc(message, 160)
                ));
            }
        }
        if target.is_empty() {
            return Some(format!(
                "growth lesson: self-model alignment drift was detected and should be audited before claiming new capability state. {}",
                crate::trunc(message, 160)
            ));
        }
        return Some(format!(
            "growth lesson: self-model alignment drift was detected for {} and should be audited before claiming new capability state. {}",
            target,
            crate::trunc(message, 160)
        ));
    }

    if kind == "self_edit_result"
        && target_is_protected
        && self_model.has_constraint("protected_core_writes")
    {
        if !success {
            return Some(format!(
                "growth lesson: protected self-edit failed for {} and still must respect the protected_core_writes boundary. {}",
                target,
                crate::trunc(message, 160)
            ));
        }

        if repair_rounds > 0 {
            return Some(format!(
                "growth lesson: protected self-edit recovered for {} after {} repair rounds under the protected_core_writes boundary. {}",
                target,
                repair_rounds,
                crate::trunc(message, 160)
            ));
        }
    }

    if !success {
        if target.is_empty() {
            return Some(format!(
                "growth lesson: {} failed and needs a better recovery path. {}",
                kind,
                crate::trunc(message, 160)
            ));
        }
        return Some(format!(
            "growth lesson: {} failed for {} and needs a better recovery path. {}",
            kind,
            target,
            crate::trunc(message, 160)
        ));
    }

    if repair_rounds > 0 {
        if target.is_empty() {
            return Some(format!(
                "growth lesson: {} recovered after {} repair rounds. {}",
                kind,
                repair_rounds,
                crate::trunc(message, 160)
            ));
        }
        return Some(format!(
            "growth lesson: {} recovered for {} after {} repair rounds. {}",
            kind,
            target,
            repair_rounds,
            crate::trunc(message, 160)
        ));
    }

    None
}

pub(super) fn infer_capability_name_from_target(target: &str) -> Option<String> {
    let target = target.trim();
    if target.is_empty() {
        return None;
    }

    let file_name = Path::new(target).file_name()?.to_str()?;
    if let Some(stripped) = file_name.strip_suffix(".nyx_tool.json") {
        return Some(stripped.to_string());
    }

    let stem = Path::new(target).file_stem()?.to_str()?;
    Some(stem.to_string())
}

pub(super) fn is_protected_core_target(target: &str) -> bool {
    let trimmed = target.trim();
    trimmed.starts_with("src/")
        || trimmed.starts_with("agents/")
        || matches!(
            trimmed,
            "Cargo.toml" | "Cargo.lock" | "IDENTITY.md" | "SOUL.md"
        )
}

pub(super) fn supports_built_tool_reconcile_target(target: &str) -> bool {
    let trimmed = target.trim();
    trimmed.starts_with("tools/") && trimmed.ends_with(".py")
}
