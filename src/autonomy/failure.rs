use anyhow::{Result, anyhow};

use crate::{
    db::Db,
    runtime::{SelfModelSnapshot, verifier::ActionVerification},
};

use super::dispatch_world::unresolved_dependency_blockers;
use super::execution::{action_run_output, expected_effect_for_task, rollback_reason};
use super::retry::{
    classify_reconcile_failure, classify_run_tool_failure, format_retry_timestamp,
    retry_key_for_task, retry_state_target_label, retry_task_label, retry_timestamp_in_future,
    retry_timestamp_within_window, run_tool_retry_target,
};
use super::{
    DEPENDENCY_BLOCKED_RESCHEDULE_DELAY_SECS, FailureClass, RECONCILE_SELF_MODEL_QUARANTINE_SECS,
    RECONCILE_SELF_MODEL_RETRY_BACKOFF_SECS, RECONCILE_SELF_MODEL_RETRY_WINDOW_SECS,
    RUN_TOOL_QUARANTINE_SECS, RUN_TOOL_RETRY_BACKOFF_SECS, RUN_TOOL_RETRY_WINDOW_SECS, RetryState,
    Task,
};

#[derive(Debug, Clone)]
pub(super) enum RetryDirective {
    Reschedule {
        summary: String,
        retry_state: RetryState,
    },
    Quarantine {
        summary: String,
        retry_state: RetryState,
    },
}

#[derive(Debug, Clone)]
pub(super) struct DependencyBlockedDirective {
    summary: String,
    scheduled_for: String,
}

pub(super) fn record_task_failure(
    db: &Db,
    task: &Task,
    self_model: &SelfModelSnapshot,
    planning_notes: &[String],
    world_context: Option<serde_json::Value>,
    error: &str,
    executed: bool,
) -> Result<()> {
    if let Some(directive) = failure_retry_directive(db, task, error)? {
        return apply_retry_directive(
            db,
            task,
            self_model,
            planning_notes,
            world_context,
            Some(error),
            executed,
            directive,
        );
    }

    let verification = ActionVerification::failed(
        executed,
        Some(expected_effect_for_task(task)),
        error,
        vec![error.to_string()],
        Some(rollback_reason(
            if executed {
                "task_failed"
            } else {
                "execution_blocked"
            },
            error,
            false,
        )),
    );
    let output = action_run_output(
        "failed",
        error,
        task,
        Some(self_model),
        planning_notes,
        world_context,
        Some(error),
        None,
        None,
        &verification,
        None,
    );
    db.fail_autonomy_task(task.id, error)?;
    db.record_autonomy_action_run(task.id, "failed", error, Some(&output))?;
    Ok(())
}

fn failure_retry_directive(db: &Db, task: &Task, error: &str) -> Result<Option<RetryDirective>> {
    match task.kind.as_str() {
        "reconcile_self_model" => reconcile_failure_directive(db, task, error).map(Some),
        "run_tool" => run_tool_failure_directive(db, task, error).map(Some),
        _ => Ok(None),
    }
}

pub(super) fn apply_retry_directive(
    db: &Db,
    task: &Task,
    self_model: &SelfModelSnapshot,
    planning_notes: &[String],
    world_context: Option<serde_json::Value>,
    error: Option<&str>,
    executed: bool,
    directive: RetryDirective,
) -> Result<()> {
    match directive {
        RetryDirective::Reschedule {
            summary,
            retry_state,
        } => {
            let verification = if executed {
                ActionVerification::failed(
                    true,
                    Some(expected_effect_for_task(task)),
                    format!("execution needs retry: {}", summary),
                    vec![
                        format!("failure_class={}", retry_state.failure_class.as_str()),
                        format!(
                            "next_retry_at={}",
                            retry_state.next_retry_at.as_deref().unwrap_or("unset")
                        ),
                    ],
                    Some(rollback_reason("retry_scheduled", &summary, true)),
                )
            } else {
                ActionVerification::deferred(
                    Some(expected_effect_for_task(task)),
                    format!("execution deferred by retry gate: {}", summary),
                    vec![
                        format!("failure_class={}", retry_state.failure_class.as_str()),
                        format!(
                            "next_retry_at={}",
                            retry_state.next_retry_at.as_deref().unwrap_or("unset")
                        ),
                    ],
                    Some(rollback_reason("retry_scheduled", &summary, true)),
                )
            };
            let output = action_run_output(
                "retry_scheduled",
                &summary,
                task,
                Some(self_model),
                planning_notes,
                world_context.clone(),
                error,
                Some(&retry_state),
                None,
                &verification,
                None,
            );
            db.reschedule_autonomy_task(task.id, retry_state.next_retry_at.as_deref(), &summary)?;
            db.record_autonomy_action_run(task.id, "retry_scheduled", &summary, Some(&output))?;
        }
        RetryDirective::Quarantine {
            summary,
            retry_state,
        } => {
            let verification = ActionVerification::failed(
                executed,
                Some(expected_effect_for_task(task)),
                format!("execution quarantined: {}", summary),
                vec![
                    format!("failure_class={}", retry_state.failure_class.as_str()),
                    format!(
                        "quarantined_until={}",
                        retry_state.quarantined_until.as_deref().unwrap_or("unset")
                    ),
                ],
                Some(rollback_reason(
                    "quarantined",
                    &summary,
                    retry_state.failure_class != FailureClass::Permanent,
                )),
            );
            let output = action_run_output(
                "quarantined",
                &summary,
                task,
                Some(self_model),
                planning_notes,
                world_context.clone(),
                error,
                Some(&retry_state),
                None,
                &verification,
                None,
            );
            db.cancel_autonomy_task(task.id, &summary)?;
            db.record_autonomy_action_run(task.id, "quarantined", &summary, Some(&output))?;
        }
    }

    Ok(())
}

pub(super) fn apply_dependency_blocked_directive(
    db: &Db,
    task: &Task,
    self_model: &SelfModelSnapshot,
    planning_notes: &[String],
    world_context: Option<serde_json::Value>,
    directive: DependencyBlockedDirective,
) -> Result<()> {
    let verification = ActionVerification::deferred(
        Some(expected_effect_for_task(task)),
        format!(
            "execution deferred by unresolved dependency: {}",
            directive.summary
        ),
        vec![
            "dependency blockers are still unresolved".to_string(),
            format!("scheduled_for={}", directive.scheduled_for),
        ],
        Some(rollback_reason(
            "dependency_blocked",
            &directive.summary,
            true,
        )),
    );
    let output = action_run_output(
        "dependency_blocked",
        &directive.summary,
        task,
        Some(self_model),
        planning_notes,
        world_context,
        None,
        None,
        None,
        &verification,
        None,
    );
    db.reschedule_autonomy_task(task.id, Some(&directive.scheduled_for), &directive.summary)?;
    db.record_autonomy_action_run(
        task.id,
        "dependency_blocked",
        &directive.summary,
        Some(&output),
    )?;
    Ok(())
}

pub(super) fn retry_gate_for_task(db: &Db, task: &Task) -> Result<Option<RetryDirective>> {
    let Some(retry_key) = retry_key_for_task(task) else {
        return Ok(None);
    };
    let Some(retry_state) = db.get_autonomy_retry_state(&retry_key)? else {
        return Ok(None);
    };
    let target = retry_state_target_label(task, &retry_state);
    let task_label = retry_task_label(task);

    if retry_timestamp_in_future(retry_state.quarantined_until.as_deref()) {
        let until = retry_state.quarantined_until.clone().unwrap_or_default();
        return Ok(Some(RetryDirective::Quarantine {
            summary: format!(
                "{} for {} is quarantined until {} after a {} failure",
                task_label,
                target,
                until,
                retry_state.failure_class.as_str()
            ),
            retry_state,
        }));
    }

    if retry_timestamp_in_future(retry_state.next_retry_at.as_deref()) {
        let next_retry_at = retry_state.next_retry_at.clone().unwrap_or_default();
        return Ok(Some(RetryDirective::Reschedule {
            summary: format!(
                "{} for {} is waiting until {} after a {} failure",
                task_label,
                target,
                next_retry_at,
                retry_state.failure_class.as_str()
            ),
            retry_state,
        }));
    }

    Ok(None)
}

pub(super) fn dependency_blocker_directive(
    world_snapshot: Option<&crate::world::projects::ProjectGraphSnapshot>,
    task: &Task,
) -> Option<DependencyBlockedDirective> {
    let blockers = unresolved_dependency_blockers(world_snapshot, task);
    if blockers.is_empty() {
        return None;
    }

    let scheduled_for = format_retry_timestamp(
        chrono::Utc::now() + chrono::Duration::seconds(DEPENDENCY_BLOCKED_RESCHEDULE_DELAY_SECS),
    );
    let blocker_summary = blockers
        .iter()
        .map(|blocker| format!("{} ({})", blocker.id, blocker.summary))
        .collect::<Vec<_>>()
        .join("; ");

    Some(DependencyBlockedDirective {
        summary: format!(
            "{} deferred until {} because unresolved blockers remain: {}",
            retry_task_label(task),
            scheduled_for,
            crate::trunc(&blocker_summary, 220)
        ),
        scheduled_for,
    })
}

fn reconcile_failure_directive(db: &Db, task: &Task, error: &str) -> Result<RetryDirective> {
    let retry_key = retry_key_for_task(task).ok_or_else(|| {
        anyhow!(
            "reconcile_self_model task {} is missing a retry key",
            task.id
        )
    })?;
    let now = chrono::Utc::now();
    let now_text = format_retry_timestamp(now);
    let existing = db.get_autonomy_retry_state(&retry_key)?;
    let existing_first_failed_at = existing
        .as_ref()
        .and_then(|state| state.first_failed_at.as_deref());
    let in_window = existing_first_failed_at
        .map(|value| {
            retry_timestamp_within_window(value, now, RECONCILE_SELF_MODEL_RETRY_WINDOW_SECS)
        })
        .unwrap_or(false);
    let attempt_count = existing
        .as_ref()
        .map(|state| {
            if in_window {
                state.attempt_count + 1
            } else {
                1
            }
        })
        .unwrap_or(1);
    let failure_class = classify_reconcile_failure(error);
    let next_retry_at = match failure_class {
        FailureClass::Transient | FailureClass::InconsistentState => {
            RECONCILE_SELF_MODEL_RETRY_BACKOFF_SECS
                .get((attempt_count - 1) as usize)
                .map(|seconds| format_retry_timestamp(now + chrono::Duration::seconds(*seconds)))
        }
        FailureClass::Permanent | FailureClass::Unsafe => None,
    };
    let quarantined_until = if next_retry_at.is_none() {
        Some(format_retry_timestamp(
            now + chrono::Duration::seconds(RECONCILE_SELF_MODEL_QUARANTINE_SECS),
        ))
    } else {
        None
    };
    let retry_state = RetryState {
        key: retry_key,
        task_kind: task.kind.clone(),
        target: task
            .args
            .get("target")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        failure_class,
        attempt_count,
        first_failed_at: if in_window {
            existing
                .as_ref()
                .and_then(|state| state.first_failed_at.clone())
                .or(Some(now_text.clone()))
        } else {
            Some(now_text.clone())
        },
        last_failed_at: Some(now_text.clone()),
        next_retry_at,
        quarantined_until,
        last_error: Some(error.to_string()),
        last_task_id: Some(task.id),
        last_growth_event_id: task
            .args
            .get("growth_event_id")
            .and_then(|value| value.as_i64()),
        last_snapshot_id: task
            .args
            .get("snapshot_id")
            .and_then(|value| value.as_i64()),
        updated_at: now_text,
    };
    db.upsert_autonomy_retry_state(&retry_state)?;

    let total_failure_budget = RECONCILE_SELF_MODEL_RETRY_BACKOFF_SECS.len() + 1;
    let target = retry_state
        .target
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("unknown target");
    let failure_summary = crate::trunc(error, 160);

    if let Some(next_retry_at) = retry_state.next_retry_at.clone() {
        return Ok(RetryDirective::Reschedule {
            summary: format!(
                "reconcile_self_model attempt {}/{} for {} scheduled at {} after {} failure: {}",
                retry_state.attempt_count,
                total_failure_budget,
                target,
                next_retry_at,
                retry_state.failure_class.as_str(),
                failure_summary
            ),
            retry_state,
        });
    }

    Ok(RetryDirective::Quarantine {
        summary: format!(
            "reconcile_self_model quarantined {} until {} after {} failure on attempt {}/{}: {}",
            target,
            retry_state
                .quarantined_until
                .as_deref()
                .unwrap_or("unknown time"),
            retry_state.failure_class.as_str(),
            retry_state.attempt_count,
            total_failure_budget,
            failure_summary
        ),
        retry_state,
    })
}

fn run_tool_failure_directive(db: &Db, task: &Task, error: &str) -> Result<RetryDirective> {
    let retry_key = retry_key_for_task(task)
        .ok_or_else(|| anyhow!("run_tool task {} is missing a retry key", task.id))?;
    let now = chrono::Utc::now();
    let now_text = format_retry_timestamp(now);
    let existing = db.get_autonomy_retry_state(&retry_key)?;
    let existing_first_failed_at = existing
        .as_ref()
        .and_then(|state| state.first_failed_at.as_deref());
    let in_window = existing_first_failed_at
        .map(|value| retry_timestamp_within_window(value, now, RUN_TOOL_RETRY_WINDOW_SECS))
        .unwrap_or(false);
    let attempt_count = existing
        .as_ref()
        .map(|state| {
            if in_window {
                state.attempt_count + 1
            } else {
                1
            }
        })
        .unwrap_or(1);
    let failure_class = classify_run_tool_failure(error);
    let next_retry_at = match failure_class {
        FailureClass::Transient | FailureClass::InconsistentState => RUN_TOOL_RETRY_BACKOFF_SECS
            .get((attempt_count - 1) as usize)
            .map(|seconds| format_retry_timestamp(now + chrono::Duration::seconds(*seconds))),
        FailureClass::Permanent | FailureClass::Unsafe => None,
    };
    let quarantined_until = if next_retry_at.is_none() {
        Some(format_retry_timestamp(
            now + chrono::Duration::seconds(RUN_TOOL_QUARANTINE_SECS),
        ))
    } else {
        None
    };
    let retry_target = run_tool_retry_target(task)
        .or_else(|| task.tool.as_deref().map(|tool| format!("tool={}", tool)));
    let retry_state = RetryState {
        key: retry_key,
        task_kind: task.kind.clone(),
        target: retry_target.clone(),
        failure_class,
        attempt_count,
        first_failed_at: if in_window {
            existing
                .as_ref()
                .and_then(|state| state.first_failed_at.clone())
                .or(Some(now_text.clone()))
        } else {
            Some(now_text.clone())
        },
        last_failed_at: Some(now_text.clone()),
        next_retry_at,
        quarantined_until,
        last_error: Some(error.to_string()),
        last_task_id: Some(task.id),
        last_growth_event_id: task
            .args
            .get("growth_event_id")
            .and_then(|value| value.as_i64()),
        last_snapshot_id: task
            .args
            .get("snapshot_id")
            .and_then(|value| value.as_i64()),
        updated_at: now_text,
    };
    db.upsert_autonomy_retry_state(&retry_state)?;

    let total_failure_budget = RUN_TOOL_RETRY_BACKOFF_SECS.len() + 1;
    let target = retry_state_target_label(task, &retry_state);
    let task_label = retry_task_label(task);
    let failure_summary = crate::trunc(error, 160);

    if let Some(next_retry_at) = retry_state.next_retry_at.clone() {
        return Ok(RetryDirective::Reschedule {
            summary: format!(
                "{} attempt {}/{} for {} scheduled at {} after {} failure: {}",
                task_label,
                retry_state.attempt_count,
                total_failure_budget,
                target,
                next_retry_at,
                retry_state.failure_class.as_str(),
                failure_summary
            ),
            retry_state,
        });
    }

    Ok(RetryDirective::Quarantine {
        summary: format!(
            "{} quarantined {} until {} after {} failure on attempt {}/{}: {}",
            task_label,
            target,
            retry_state
                .quarantined_until
                .as_deref()
                .unwrap_or("unknown time"),
            retry_state.failure_class.as_str(),
            retry_state.attempt_count,
            total_failure_budget,
            failure_summary
        ),
        retry_state,
    })
}
