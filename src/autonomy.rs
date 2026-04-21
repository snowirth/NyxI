use std::collections::HashSet;
use std::path::Path;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

use crate::{
    AppState, ProactiveQueue,
    db::Db,
    runtime::{
        SelfModelSnapshot,
        verifier::{ActionVerification, ExpectedEffectContract, RollbackReason},
    },
};

mod dispatch_world;
mod execution;
mod failure;
mod promote;
mod retry;

use dispatch_world::{
    action_run_world_context, append_world_planning_notes, order_ready_tasks_for_dispatch,
    refresh_project_graph_snapshot,
};
use execution::{
    action_run_output, approved_live_policy_directives_for_task, compact_tool_output,
    expected_effect_for_task, extract_tool_output, growth_lesson_text,
    infer_capability_name_from_target, rollback_reason, sanitize_autonomous_tool_args,
    stale_recovery_expected_effect,
};
use failure::{
    apply_dependency_blocked_directive, apply_retry_directive, dependency_blocker_directive,
    record_task_failure, retry_gate_for_task,
};
use promote::promote_observation;
use retry::{
    format_retry_timestamp, parse_retry_timestamp, retry_key_for_task, run_tool_retry_target,
};

const READY_TASK_FETCH_LIMIT: usize = 16;
const DISPATCH_TASK_BATCH_LIMIT: usize = 4;
const DEPENDENCY_BLOCKED_RESCHEDULE_DELAY_SECS: i64 = 120;
pub(super) const GOAL_AWARENESS: &str = "Maintain situational awareness";
pub(super) const GOAL_PROJECT_TRACKING: &str = "Track active project work";
pub(super) const GOAL_KNOWLEDGE_CAPTURE: &str = "Capture durable knowledge";
pub(super) const GOAL_TIMELY_FOLLOW_UP: &str = "Surface timely follow-ups";
pub(super) const GOAL_GROWTH_COORDINATION: &str = "Coordinate growth and adaptation";
pub(super) const GOAL_SELF_MODEL_ALIGNMENT: &str = "Keep self-model aligned";
const PROJECT_SNAPSHOT_COOLDOWN_SECS: i64 = 90;
pub(super) const REVIEW_GROWTH_TASK_COOLDOWN_SECS: i64 = 300;
pub(super) const SELF_MODEL_RECONCILE_TASK_COOLDOWN_SECS: i64 = 300;
const RECONCILE_SELF_MODEL_RETRY_WINDOW_SECS: i64 = 6 * 60 * 60;
const RECONCILE_SELF_MODEL_RETRY_BACKOFF_SECS: [i64; 2] = [5 * 60, 30 * 60];
const RECONCILE_SELF_MODEL_QUARANTINE_SECS: i64 = 24 * 60 * 60;
const RUN_TOOL_RETRY_WINDOW_SECS: i64 = 60 * 60;
const RUN_TOOL_RETRY_BACKOFF_SECS: [i64; 3] = [60, 5 * 60, 20 * 60];
const RUN_TOOL_QUARANTINE_SECS: i64 = 6 * 60 * 60;
pub(crate) const STALE_RUNNING_TASK_TIMEOUT_SECS: i64 = 10 * 60;
const STALE_RUNNING_TASK_RECOVERY_DELAY_SECS: i64 = 60;
const ACTION_RUN_SCHEMA_VERSION: &str = "autonomy_action_run.v1";

#[derive(Debug, Clone)]
struct PlannedTask {
    planning_notes: Vec<String>,
}

#[derive(Debug, Clone)]
struct TaskExecutionResult {
    summary: String,
    verification: ActionVerification,
    evidence: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StaleRunningTaskRecovery {
    task_id: i64,
    kind: String,
    title: String,
    last_run_at: Option<String>,
    recovered_at: String,
    stale_after_secs: i64,
    stale_for_secs: Option<i64>,
    scheduled_for: String,
    strategy: String,
}

#[derive(Debug, Clone)]
struct WorldTaskContext {
    project_title: String,
    project_status: String,
    workstream_id: String,
    workstream_title: String,
    workstream_status: String,
    workstream_index: usize,
    workstream_ready_count: usize,
    workstream_running_count: usize,
    resume_index: usize,
    preview_index: Option<usize>,
    blocked_by: Vec<String>,
    active_blocker_count: usize,
    resume_reason: String,
    operator_state: String,
    retry_hint: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum GoalStatus {
    Active,
    OnHold,
    Completed,
    Abandoned,
}

impl GoalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::OnHold => "on_hold",
            Self::Completed => "completed",
            Self::Abandoned => "abandoned",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "on_hold" => Self::OnHold,
            "completed" => Self::Completed,
            "abandoned" => Self::Abandoned,
            _ => Self::Active,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub id: i64,
    pub kind: String,
    pub source: String,
    pub content: String,
    pub context: serde_json::Value,
    pub priority: f64,
    pub created_at: String,
    pub consumed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Goal {
    pub id: i64,
    pub title: String,
    pub status: GoalStatus,
    pub priority: f64,
    pub source: String,
    pub details: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub last_reviewed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: i64,
    pub goal_id: Option<i64>,
    pub kind: String,
    pub title: String,
    pub status: TaskStatus,
    pub tool: Option<String>,
    pub args: serde_json::Value,
    pub notes: Option<String>,
    pub priority: f64,
    pub scheduled_for: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub last_run_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionRun {
    pub id: i64,
    pub task_id: i64,
    pub outcome: String,
    pub summary: String,
    pub executed: bool,
    pub verified: Option<bool>,
    pub expected_effect: Option<ExpectedEffectContract>,
    pub verifier_verdict: Option<crate::runtime::verifier::VerifierVerdict>,
    pub rollback_reason: Option<RollbackReason>,
    pub output: Option<serde_json::Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionRunRecord {
    pub id: i64,
    pub task_id: i64,
    pub task_kind: String,
    pub task_title: String,
    pub task_status: TaskStatus,
    pub tool: Option<String>,
    pub goal_id: Option<i64>,
    pub outcome: String,
    pub summary: String,
    pub executed: bool,
    pub verified: Option<bool>,
    pub expected_effect: Option<ExpectedEffectContract>,
    pub verifier_verdict: Option<crate::runtime::verifier::VerifierVerdict>,
    pub rollback_reason: Option<RollbackReason>,
    pub output: Option<serde_json::Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskTraceRefs {
    pub observation_id: Option<i64>,
    pub growth_event_id: Option<i64>,
    pub snapshot_id: Option<i64>,
    pub dedupe_key: Option<String>,
    pub retry_key: Option<String>,
    pub target: Option<String>,
    pub trigger_kind: Option<String>,
    pub capability_name: Option<String>,
    pub review_kind: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Transient,
    Permanent,
    Unsafe,
    InconsistentState,
}

impl FailureClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Permanent => "permanent",
            Self::Unsafe => "unsafe",
            Self::InconsistentState => "inconsistent_state",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "permanent" => Self::Permanent,
            "unsafe" => Self::Unsafe,
            "inconsistent_state" => Self::InconsistentState,
            _ => Self::Transient,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryState {
    pub key: String,
    pub task_kind: String,
    pub target: Option<String>,
    pub failure_class: FailureClass,
    pub attempt_count: i64,
    pub first_failed_at: Option<String>,
    pub last_failed_at: Option<String>,
    pub next_retry_at: Option<String>,
    pub quarantined_until: Option<String>,
    pub last_error: Option<String>,
    pub last_task_id: Option<i64>,
    pub last_growth_event_id: Option<i64>,
    pub last_snapshot_id: Option<i64>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrowthEvent {
    pub id: i64,
    pub kind: String,
    pub source: String,
    pub target: Option<String>,
    pub summary: String,
    pub success: bool,
    pub details: serde_json::Value,
    pub created_at: String,
}

pub(crate) fn project_graph_retry_key(task: &Task) -> Option<String> {
    retry_key_for_task(task)
}

pub(crate) fn project_graph_target(task: &Task) -> Option<String> {
    task.args
        .get("target")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| run_tool_retry_target(task))
        .or_else(|| {
            task.tool
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|tool| format!("tool={}", tool))
        })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationInput {
    pub kind: String,
    pub source: String,
    pub content: String,
    pub context: serde_json::Value,
    pub priority: f64,
}

impl ObservationInput {
    pub fn new(
        kind: impl Into<String>,
        source: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            kind: kind.into(),
            source: source.into(),
            content: content.into(),
            context: serde_json::json!({}),
            priority: 0.5,
        }
    }
}

pub fn ingest_observation(db: &Db, input: ObservationInput) -> Result<Observation> {
    let observation_id = db.add_autonomy_observation(
        &input.kind,
        &input.source,
        &input.content,
        &input.context,
        clamp_priority(input.priority),
    )?;
    let observation = db
        .get_autonomy_observation(observation_id)?
        .ok_or_else(|| anyhow!("missing autonomy observation {}", observation_id))?;

    promote_observation(db, &observation)?;
    Ok(observation)
}

pub async fn run(state: AppState, proactive_queue: ProactiveQueue) {
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    tracing::info!("autonomy: online");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        if let Err(e) = dispatch_ready_tasks(&state, &proactive_queue).await {
            tracing::warn!("autonomy: dispatch failed: {}", e);
        }
    }
}

pub async fn dispatch_ready_tasks(
    state: &AppState,
    proactive_queue: &ProactiveQueue,
) -> Result<usize> {
    prune_inactive_retry_state(state.db.as_ref())?;
    let recovered = recover_stale_running_tasks(state).await?;
    let world_snapshot =
        refresh_project_graph_snapshot(state.db.as_ref(), "autonomy_dispatch_start");
    let world_changes = crate::world::state::load_project_graph_changes(state.db.as_ref());
    let mut tasks = state.db.list_ready_autonomy_tasks(READY_TASK_FETCH_LIMIT);
    order_ready_tasks_for_dispatch(&mut tasks, world_snapshot.as_ref());
    tasks.truncate(DISPATCH_TASK_BATCH_LIMIT);
    let mut executed = 0usize;
    let mut graph_dirty = recovered > 0;

    for task in tasks {
        if !state.db.claim_autonomy_task(task.id)? {
            continue;
        }

        let self_model = state.self_model_snapshot().await;
        let world_context =
            action_run_world_context(world_snapshot.as_ref(), world_changes.as_ref(), &task);

        if let Some(directive) = retry_gate_for_task(state.db.as_ref(), &task)? {
            let mut planning_notes = planning_notes_for_task(
                &self_model,
                &task,
                world_snapshot.as_ref(),
                world_changes.as_ref(),
            );
            planning_notes.push("retry gate deferred execution".to_string());
            apply_retry_directive(
                state.db.as_ref(),
                &task,
                &self_model,
                &planning_notes,
                world_context.clone(),
                None,
                false,
                directive,
            )?;
            graph_dirty = true;
            continue;
        }

        if let Some(directive) = dependency_blocker_directive(world_snapshot.as_ref(), &task) {
            let mut planning_notes = planning_notes_for_task(
                &self_model,
                &task,
                world_snapshot.as_ref(),
                world_changes.as_ref(),
            );
            planning_notes.push("dependency blocker deferred execution".to_string());
            apply_dependency_blocked_directive(
                state.db.as_ref(),
                &task,
                &self_model,
                &planning_notes,
                world_context.clone(),
                directive,
            )?;
            graph_dirty = true;
            continue;
        }

        let planned = match plan_task(
            state.db.as_ref(),
            &task,
            &self_model,
            world_snapshot.as_ref(),
            world_changes.as_ref(),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                let summary = error.to_string();
                let mut planning_notes = planning_notes_for_task(
                    &self_model,
                    &task,
                    world_snapshot.as_ref(),
                    world_changes.as_ref(),
                );
                planning_notes.push(format!("planning rejected: {}", summary));
                record_task_failure(
                    state.db.as_ref(),
                    &task,
                    &self_model,
                    &planning_notes,
                    world_context.clone(),
                    &summary,
                    false,
                )?;
                graph_dirty = true;
                continue;
            }
        };

        match execute_task(state, proactive_queue, &task, &self_model).await {
            Ok(execution) => {
                if let Some(retry_key) = retry_key_for_task(&task) {
                    state.db.clear_autonomy_retry_state(&retry_key)?;
                }
                let output = action_run_output(
                    "completed",
                    &execution.summary,
                    &task,
                    Some(&self_model),
                    &planned.planning_notes,
                    world_context.clone(),
                    None,
                    None,
                    None,
                    &execution.verification,
                    Some(execution.evidence),
                );
                state
                    .db
                    .complete_autonomy_task(task.id, &execution.summary)?;
                state.db.record_autonomy_action_run(
                    task.id,
                    "completed",
                    &execution.summary,
                    Some(&output),
                )?;
                executed += 1;
                graph_dirty = true;
            }
            Err(e) => {
                let summary = e.to_string();
                record_task_failure(
                    state.db.as_ref(),
                    &task,
                    &self_model,
                    &planned.planning_notes,
                    world_context.clone(),
                    &summary,
                    true,
                )?;
                graph_dirty = true;
            }
        }
    }

    if graph_dirty {
        refresh_project_graph_snapshot(state.db.as_ref(), "autonomy_dispatch_end");
    }

    Ok(executed)
}

async fn recover_stale_running_tasks(state: &AppState) -> Result<usize> {
    let now = chrono::Utc::now();
    let stale_before =
        format_retry_timestamp(now - chrono::Duration::seconds(STALE_RUNNING_TASK_TIMEOUT_SECS));
    let stale_tasks = state
        .db
        .list_stale_running_autonomy_tasks(&stale_before, 16)?;
    if stale_tasks.is_empty() {
        return Ok(0);
    }

    let self_model = state.self_model_snapshot().await;
    let recovered_at = format_retry_timestamp(now);
    let scheduled_for = format_retry_timestamp(
        now + chrono::Duration::seconds(STALE_RUNNING_TASK_RECOVERY_DELAY_SECS),
    );
    let mut recovered = 0usize;

    for task in stale_tasks {
        let stale_for_secs = task
            .last_run_at
            .as_deref()
            .and_then(parse_retry_timestamp)
            .map(|started_at| now.signed_duration_since(started_at).num_seconds())
            .filter(|age_secs| *age_secs >= 0);
        let recovery = StaleRunningTaskRecovery {
            task_id: task.id,
            kind: task.kind.clone(),
            title: task.title.clone(),
            last_run_at: task.last_run_at.clone(),
            recovered_at: recovered_at.clone(),
            stale_after_secs: STALE_RUNNING_TASK_TIMEOUT_SECS,
            stale_for_secs,
            scheduled_for: scheduled_for.clone(),
            strategy: "reschedule_pending".to_string(),
        };
        let recovery_note = format!(
            "recovered stale running task at {} after exceeding {}s execution window; rescheduled for {}",
            recovery.recovered_at, recovery.stale_after_secs, recovery.scheduled_for
        );
        if !state
            .db
            .recover_stale_autonomy_task(task.id, Some(&scheduled_for), &recovery_note)?
        {
            continue;
        }

        let summary = if let Some(age_secs) = recovery.stale_for_secs {
            format!(
                "recovered stale running {} task {} after {}s; rescheduled at {}",
                task.kind, task.id, age_secs, recovery.scheduled_for
            )
        } else {
            format!(
                "recovered stale running {} task {}; rescheduled at {}",
                task.kind, task.id, recovery.scheduled_for
            )
        };
        let planning_notes = vec![
            format!(
                "stale running recovery triggered after {}s timeout",
                STALE_RUNNING_TASK_TIMEOUT_SECS
            ),
            format!(
                "recovery_delay_secs={}",
                STALE_RUNNING_TASK_RECOVERY_DELAY_SECS
            ),
        ];
        let output = action_run_output(
            "recovered_stale_running_task",
            &summary,
            &task,
            Some(&self_model),
            &planning_notes,
            None,
            Some("stale running task recovered before retry"),
            None,
            Some(serde_json::json!({
                "stale_running_recovery": recovery,
            })),
            &ActionVerification::verified(
                stale_recovery_expected_effect(&task),
                format!(
                    "verified stale task {} returned to the pending queue",
                    task.id
                ),
                vec![format!(
                    "task {} was rescheduled for {} after stale-running recovery",
                    task.id, scheduled_for
                )],
            ),
            None,
        );
        state.db.record_autonomy_action_run(
            task.id,
            "recovered_stale_running_task",
            &summary,
            Some(&output),
        )?;
        recovered += 1;
    }

    if recovered > 0 {
        tracing::warn!(
            "autonomy: recovered {} stale running task(s) back to pending",
            recovered
        );
    }

    Ok(recovered)
}

fn prune_inactive_retry_state(db: &Db) -> Result<usize> {
    let now = chrono::Utc::now();
    let deleted = db.prune_inactive_autonomy_retry_state(
        &format_retry_timestamp(now),
        &format_retry_timestamp(
            now - chrono::Duration::seconds(RECONCILE_SELF_MODEL_RETRY_WINDOW_SECS),
        ),
    )?;
    if deleted > 0 {
        tracing::info!("autonomy: pruned {} inactive retry state rows", deleted);
    }
    Ok(deleted)
}

fn plan_task(
    db: &Db,
    task: &Task,
    self_model: &SelfModelSnapshot,
    world_snapshot: Option<&crate::world::projects::ProjectGraphSnapshot>,
    world_changes: Option<&crate::world::projects::ProjectGraphChangeSet>,
) -> Result<PlannedTask> {
    let mut planning_notes =
        planning_notes_for_task(self_model, task, world_snapshot, world_changes);

    match task.kind.as_str() {
        "deliver_message" => {
            planning_notes.push(format!(
                "timely_follow_up_goal_active={}",
                self_model.has_active_goal(GOAL_TIMELY_FOLLOW_UP)
            ));
        }
        "store_memory" => {
            planning_notes.push(format!(
                "memory_count_before={}",
                self_model.runtime.memory_count
            ));
        }
        "run_tool" => {
            let tool = task
                .tool
                .as_deref()
                .ok_or_else(|| anyhow!("run_tool task {} missing tool", task.id))?;
            if !self_model.has_capability_named(tool) {
                return Err(anyhow!(
                    "autonomy plan rejected tool {} because it is not present in the live self-model",
                    tool
                ));
            }
            planning_notes.push(format!("tool_visible_in_self_model={}", tool));
            for directive in
                approved_live_policy_directives_for_task(db, Some("run_tool"), Some(tool), 12)
            {
                planning_notes.push(format!(
                    "approved_policy {} => {}",
                    directive.trigger, directive.rule
                ));
                if directive.rule == "preflight_path_exists" {
                    let path = task
                        .args
                        .get("path")
                        .and_then(|value| value.as_str())
                        .map(str::trim)
                        .filter(|value| !value.is_empty());
                    if let Some(path) = path {
                        if !Path::new(path).exists() {
                            let summary = format!(
                                "approved policy blocked {} because file not found: {}",
                                tool, path
                            );
                            db.record_policy_runtime_event_by_trigger(
                                &directive.trigger,
                                "live_guard_blocked",
                                &summary,
                                &serde_json::json!({
                                    "surface": "autonomy_plan",
                                    "task_id": task.id,
                                    "task_kind": task.kind,
                                    "tool": tool,
                                    "path": path,
                                }),
                            )
                            .ok();
                            return Err(anyhow!(summary));
                        }
                    }
                }
            }
        }
        "reconcile_self_model" => {
            planning_notes.push(format!(
                "self_model_alignment_goal_active={}",
                self_model.has_active_goal(GOAL_SELF_MODEL_ALIGNMENT)
            ));
            if let Some(capability_name) = task
                .args
                .get("capability_name")
                .and_then(|value| value.as_str())
            {
                planning_notes.push(format!(
                    "capability_visible_before_reconcile={}",
                    self_model.has_capability_named(capability_name)
                ));
            }
            if let Some(target) = task.args.get("target").and_then(|value| value.as_str()) {
                if !target.trim().is_empty() {
                    planning_notes.push(format!("reconcile_target={}", target));
                }
            }
            for directive in
                approved_live_policy_directives_for_task(db, Some("reconcile_self_model"), None, 8)
            {
                planning_notes.push(format!(
                    "approved_policy {} => {}",
                    directive.trigger, directive.rule
                ));
            }
        }
        "review_growth" => {
            planning_notes.push(format!(
                "growth_goal_active={}",
                self_model.has_active_goal(GOAL_GROWTH_COORDINATION)
            ));
            planning_notes.push(format!(
                "self_model_alignment_goal_active={}",
                self_model.has_active_goal(GOAL_SELF_MODEL_ALIGNMENT)
            ));
            planning_notes.push(format!(
                "recent_growth_events={}",
                self_model.growth.recent_events.len()
            ));
        }
        _ => {}
    }

    Ok(PlannedTask { planning_notes })
}

fn base_planning_notes(self_model: &SelfModelSnapshot) -> Vec<String> {
    vec![
        format!("self-model snapshot {}", self_model.generated_at),
        format!("capabilities={}", self_model.capability_count()),
    ]
}

fn planning_notes_for_task(
    self_model: &SelfModelSnapshot,
    task: &Task,
    world_snapshot: Option<&crate::world::projects::ProjectGraphSnapshot>,
    world_changes: Option<&crate::world::projects::ProjectGraphChangeSet>,
) -> Vec<String> {
    let mut notes = base_planning_notes(self_model);
    append_world_planning_notes(&mut notes, task, world_snapshot, world_changes);
    notes
}

async fn execute_task(
    state: &AppState,
    proactive_queue: &ProactiveQueue,
    task: &Task,
    self_model: &SelfModelSnapshot,
) -> Result<TaskExecutionResult> {
    match task.kind.as_str() {
        "deliver_message" => {
            let message = task
                .args
                .get("message")
                .and_then(|value| value.as_str())
                .unwrap_or(task.title.as_str())
                .trim();
            if message.is_empty() {
                return Err(anyhow!("deliver_message task {} has no message", task.id));
            }

            let queued = {
                let mut queue = proactive_queue.lock().await;
                queue.push(message.to_string());
                queue.iter().any(|queued| queued == message)
            };
            let expected_effect = expected_effect_for_task(task);
            let verification = if queued {
                ActionVerification::verified(
                    expected_effect,
                    format!("verified proactive message delivery for task {}", task.id),
                    vec!["queued message matched the expected payload".to_string()],
                )
            } else {
                ActionVerification::failed(
                    true,
                    Some(expected_effect),
                    "message enqueue was attempted but could not be re-observed",
                    vec![
                        "proactive queue did not contain the expected message after enqueue"
                            .to_string(),
                    ],
                    None,
                )
            };
            Ok(TaskExecutionResult {
                summary: format!("delivered message: {}", crate::trunc(message, 80)),
                verification,
                evidence: serde_json::json!({
                    "queued_message": message,
                }),
            })
        }
        "store_memory" => {
            let content = task
                .args
                .get("content")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            if content.is_empty() {
                return Err(anyhow!("store_memory task {} has no content", task.id));
            }

            let network = task
                .args
                .get("network")
                .and_then(|value| value.as_str())
                .unwrap_or("knowledge");
            let importance = task
                .args
                .get("importance")
                .and_then(|value| value.as_f64())
                .unwrap_or(task.priority);

            let expected_effect = expected_effect_for_task(task);
            match state.db.remember(content, network, importance)? {
                Some(id) => {
                    let stored_content = state.db.get_memory_content(&id)?;
                    let recalled = state.db.recall(content, 5);
                    let recall_verified = recalled.iter().any(|memory| memory == content);
                    let verification = match stored_content.as_deref() {
                        Some(stored) if stored == content => {
                            let mut checks =
                                vec![format!("memory {} was written to the database", id)];
                            if recall_verified {
                                checks.push("recall returned the stored content".to_string());
                            } else {
                                checks.push(
                                    "memory row was reloaded directly even though recall did not surface an exact match yet"
                                        .to_string(),
                                );
                            }
                            ActionVerification::verified(
                                expected_effect,
                                format!("verified stored memory {}", id),
                                checks,
                            )
                        }
                        Some(stored) => ActionVerification::failed(
                            true,
                            Some(expected_effect),
                            format!("memory {} was written but direct readback did not match", id),
                            vec![
                                format!("memory {} was inserted into the database", id),
                                format!(
                                    "direct readback returned different content: {}",
                                    crate::trunc(stored, 120)
                                ),
                            ],
                            None,
                        ),
                        None => ActionVerification::failed(
                            true,
                            Some(expected_effect),
                            format!("memory {} was written but direct readback could not find it", id),
                            vec![format!(
                                "memory {} was inserted but a direct lookup by id returned nothing",
                                id
                            )],
                            None,
                        ),
                    };
                    Ok(TaskExecutionResult {
                        summary: format!("stored memory: {}", crate::trunc(content, 80)),
                        verification,
                        evidence: serde_json::json!({
                            "memory_id": id,
                            "stored": true,
                            "network": network,
                            "content": content,
                        }),
                    })
                }
                None => Ok(TaskExecutionResult {
                    summary: format!("memory already known or rejected: {}", crate::trunc(content, 80)),
                    verification: ActionVerification::verified(
                        expected_effect,
                        "memory guardrails produced a safe no-op",
                        vec!["remember() returned no insert, so the candidate was deduped or rejected".to_string()],
                    ),
                    evidence: serde_json::json!({
                        "stored": false,
                        "network": network,
                        "content": content,
                    }),
                }),
            }
        }
        "run_tool" => {
            let tool = task
                .tool
                .as_deref()
                .ok_or_else(|| anyhow!("run_tool task {} missing tool", task.id))?;
            let tool_args = sanitize_autonomous_tool_args(tool, &task.args)?;
            let result = crate::tools::run(tool, &tool_args).await?;

            if result.get("success").and_then(|value| value.as_bool()) == Some(false) {
                let err = result["error"].as_str().unwrap_or("tool returned failure");
                return Err(anyhow!("autonomy tool {} failed: {}", tool, err));
            }

            let output = extract_tool_output(&result);
            let expected_effect = expected_effect_for_task(task);
            if task
                .args
                .get("deliver_output")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
            {
                let prefix = task
                    .args
                    .get("message_prefix")
                    .and_then(|value| value.as_str())
                    .unwrap_or("autonomy tool");
                let max_chars = task
                    .args
                    .get("max_output_chars")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(240) as usize;
                let compact = compact_tool_output(&output);
                let message = format!("{}: {}", prefix, crate::trunc(&compact, max_chars));
                let queued = {
                    let mut queue = proactive_queue.lock().await;
                    queue.push(message.clone());
                    queue.iter().any(|queued| queued == &message)
                };
                let mut checks = vec!["tool returned a non-failing runtime payload".to_string()];
                if queued {
                    checks.push("tool output summary landed in the proactive queue".to_string());
                    return Ok(TaskExecutionResult {
                        summary: format!(
                            "ran tool {}: {}",
                            tool,
                            crate::trunc(&compact_tool_output(&output), 80)
                        ),
                        verification: ActionVerification::verified(
                            expected_effect,
                            format!("verified tool {} execution and output delivery", tool),
                            checks,
                        ),
                        evidence: serde_json::json!({
                            "tool_args": tool_args,
                            "tool_result": result,
                            "delivered_message": message,
                        }),
                    });
                }
                checks.push(
                    "tool output summary was generated but not observed in the proactive queue"
                        .to_string(),
                );
                return Ok(TaskExecutionResult {
                    summary: format!(
                        "ran tool {}: {}",
                        tool,
                        crate::trunc(&compact_tool_output(&output), 80)
                    ),
                    verification: ActionVerification::failed(
                        true,
                        Some(expected_effect),
                        format!(
                            "tool {} executed but output delivery could not be verified",
                            tool
                        ),
                        checks,
                        None,
                    ),
                    evidence: serde_json::json!({
                        "tool_args": tool_args,
                        "tool_result": result,
                        "delivered_message": message,
                    }),
                });
            }

            Ok(TaskExecutionResult {
                summary: format!(
                    "ran tool {}: {}",
                    tool,
                    crate::trunc(&compact_tool_output(&output), 80)
                ),
                verification: ActionVerification::verified(
                    expected_effect,
                    format!("verified tool {} runtime success payload", tool),
                    vec!["tool returned a non-failing runtime payload".to_string()],
                ),
                evidence: serde_json::json!({
                    "tool_args": tool_args,
                    "tool_result": result,
                }),
            })
        }
        "reconcile_self_model" => {
            let trigger_kind = task
                .args
                .get("trigger_kind")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let success = task
                .args
                .get("success")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let target = task
                .args
                .get("target")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            let capability_name = task
                .args
                .get("capability_name")
                .and_then(|value| value.as_str())
                .map(str::to_string)
                .or_else(|| infer_capability_name_from_target(target))
                .unwrap_or_default();
            let message = task
                .args
                .get("message")
                .and_then(|value| value.as_str())
                .unwrap_or(task.title.as_str())
                .trim();

            if trigger_kind != "tool_growth_result" || !success || target.is_empty() {
                return Err(anyhow!(
                    "unsupported self-model reconciliation task {} for trigger {}",
                    task.id,
                    trigger_kind
                ));
            }

            let cancelled = cancel_stale_self_model_gap_tasks(state.db.as_ref(), task.id, target)?;
            if !capability_name.is_empty() && self_model.has_capability_named(&capability_name) {
                let summary = if cancelled > 0 {
                    format!(
                        "self-model already aligned for {}; cancelled {} stale task(s)",
                        capability_name, cancelled
                    )
                } else {
                    format!("self-model already aligned for {}", capability_name)
                };
                return Ok(TaskExecutionResult {
                    summary,
                    verification: ActionVerification::verified(
                        expected_effect_for_task(task),
                        format!(
                            "verified {} was already visible in the live self-model",
                            capability_name
                        ),
                        vec![
                            "live self-model already contained the expected capability".to_string(),
                        ],
                    ),
                    evidence: serde_json::json!({
                        "already_aligned": true,
                        "capability_name": capability_name,
                        "cancelled_stale_tasks": cancelled,
                    }),
                });
            }

            let repair = crate::forge::reconcile_built_tool_registration(target, message)?;
            let mut summary = if repair.manifest_created {
                format!(
                    "reconciled self-model gap for {} by recreating {}",
                    repair.tool_name, repair.manifest_path
                )
            } else {
                format!(
                    "reconciled self-model gap for {} by refreshing {}",
                    repair.tool_name, repair.manifest_path
                )
            };
            if cancelled > 0 {
                summary.push_str(&format!("; cancelled {} stale task(s)", cancelled));
            }

            state
                .persist_self_model_snapshot_and_detect_gaps(
                    "autonomy",
                    "self_model_reconcile_result",
                    Some(target),
                    &summary,
                    true,
                    task.args
                        .get("growth_event_id")
                        .and_then(|value| value.as_i64()),
                )
                .await;

            let refreshed = state.self_model_snapshot().await;
            let manifest_exists = Path::new(&repair.manifest_path).exists();
            let capability_label = if capability_name.is_empty() {
                repair.tool_name.clone()
            } else {
                capability_name.clone()
            };
            let capability_visible =
                !capability_label.is_empty() && refreshed.has_capability_named(&capability_label);
            let mut checks = vec![format!("manifest present at {}", repair.manifest_path)];
            if capability_visible {
                checks.push(format!(
                    "capability {} is visible in the live self-model",
                    capability_label
                ));
            } else {
                checks.push(format!(
                    "capability {} is still missing from the live self-model",
                    capability_label
                ));
            }
            let verification = if manifest_exists && capability_visible {
                ActionVerification::verified(
                    expected_effect_for_task(task),
                    format!(
                        "verified self-model reconciliation for {}",
                        capability_label
                    ),
                    checks,
                )
            } else {
                ActionVerification::failed(
                    true,
                    Some(expected_effect_for_task(task)),
                    format!(
                        "reconciliation ran but the live self-model still does not show {}",
                        capability_label
                    ),
                    checks,
                    Some(rollback_reason(
                        "self_model_reconcile_incomplete",
                        &format!(
                            "reconciliation for {} needs follow-up because verification did not pass",
                            capability_label
                        ),
                        true,
                    )),
                )
            };

            Ok(TaskExecutionResult {
                summary,
                verification,
                evidence: serde_json::json!({
                    "repair": repair,
                    "cancelled_stale_tasks": cancelled,
                    "manifest_exists": manifest_exists,
                    "capability_visible": capability_visible,
                }),
            })
        }
        "review_growth" => {
            let growth_event = task
                .args
                .get("growth_event_id")
                .and_then(|value| value.as_i64())
                .and_then(|id| state.db.get_growth_event(id).ok().flatten());

            let kind = growth_event
                .as_ref()
                .map(|event| event.kind.as_str())
                .or_else(|| task.args.get("kind").and_then(|value| value.as_str()))
                .unwrap_or("growth");
            let success = growth_event
                .as_ref()
                .map(|event| event.success)
                .or_else(|| task.args.get("success").and_then(|value| value.as_bool()))
                .unwrap_or(false);
            let target = growth_event
                .as_ref()
                .and_then(|event| event.target.as_deref())
                .or_else(|| task.args.get("target").and_then(|value| value.as_str()))
                .unwrap_or("");
            let message = growth_event
                .as_ref()
                .map(|event| event.summary.as_str())
                .or_else(|| task.args.get("message").and_then(|value| value.as_str()))
                .unwrap_or(task.title.as_str())
                .trim();
            let repair_rounds = growth_event
                .as_ref()
                .and_then(|event| event.details.get("repair_rounds"))
                .and_then(|value| value.as_u64())
                .or_else(|| {
                    task.args
                        .get("repair_rounds")
                        .and_then(|value| value.as_u64())
                })
                .unwrap_or(0);
            let mut lesson_text = None;
            let mut lesson_id = None;

            if let Some(lesson) =
                growth_lesson_text(kind, target, message, success, repair_rounds, self_model)
            {
                lesson_text = Some(lesson.clone());
                if let Some(id) = state.db.remember(&lesson, "lesson", 0.67)? {
                    state.embed_memory_background(id.clone(), lesson.clone());
                    lesson_id = Some(id);
                }
            }

            let summary = if target.is_empty() {
                format!(
                    "reviewed {} growth event: {}",
                    kind,
                    crate::trunc(message, 80)
                )
            } else {
                format!(
                    "reviewed {} growth for {}: {}",
                    kind,
                    target,
                    crate::trunc(message, 80)
                )
            };

            if task
                .args
                .get("deliver_output")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
            {
                proactive_queue.lock().await.push(summary.clone());
            }

            let mut checks = vec![format!("reviewed {} growth signal", kind)];
            if let Some(lesson) = lesson_text.as_ref() {
                checks.push(format!(
                    "distilled lesson candidate: {}",
                    crate::trunc(lesson, 80)
                ));
            } else {
                checks
                    .push("no durable lesson candidate was promoted from this review".to_string());
            }
            if let Some(id) = lesson_id.as_ref() {
                checks.push(format!("stored lesson memory {}", id));
            }

            Ok(TaskExecutionResult {
                summary,
                verification: ActionVerification::verified(
                    expected_effect_for_task(task),
                    format!("verified growth review completion for {}", kind),
                    checks,
                ),
                evidence: serde_json::json!({
                    "growth_kind": kind,
                    "growth_target": target,
                    "growth_success": success,
                    "repair_rounds": repair_rounds,
                    "lesson": lesson_text,
                    "lesson_id": lesson_id,
                }),
            })
        }
        _ => Err(anyhow!("unsupported autonomy task kind: {}", task.kind)),
    }
}

fn clamp_priority(priority: f64) -> f64 {
    priority.clamp(0.0, 1.0)
}

fn task_title(prefix: &str, content: &str) -> String {
    let collapsed = content.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("{}: {}", prefix, crate::trunc(&collapsed, 80))
}

pub(crate) fn task_trace_refs(task: &Task) -> TaskTraceRefs {
    TaskTraceRefs {
        observation_id: task
            .args
            .get("observation_id")
            .and_then(|value| value.as_i64()),
        growth_event_id: task
            .args
            .get("growth_event_id")
            .and_then(|value| value.as_i64()),
        snapshot_id: task
            .args
            .get("snapshot_id")
            .and_then(|value| value.as_i64()),
        dedupe_key: task
            .args
            .get("dedupe_key")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        retry_key: retry_key_for_task(task),
        target: task
            .args
            .get("target")
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty()),
        trigger_kind: task
            .args
            .get("trigger_kind")
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty()),
        capability_name: task
            .args
            .get("capability_name")
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty()),
        review_kind: task
            .args
            .get("kind")
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty()),
    }
}

fn cancel_stale_self_model_gap_tasks(db: &Db, current_task_id: i64, target: &str) -> Result<usize> {
    if target.trim().is_empty() {
        return Ok(0);
    }

    let mut tasks = db.list_autonomy_tasks_with_status(TaskStatus::Pending, 128)?;
    tasks.extend(db.list_autonomy_tasks_with_status(TaskStatus::Running, 128)?);
    let mut seen = HashSet::new();
    let mut cancelled = 0usize;
    for task in tasks {
        if !seen.insert(task.id) {
            continue;
        }
        if task.id == current_task_id || !is_stale_self_model_gap_task(&task, target) {
            continue;
        }
        db.cancel_autonomy_task(
            task.id,
            &format!(
                "cancelled after self-model alignment recovered for {} via reconcile task {}",
                target, current_task_id
            ),
        )?;
        cancelled += 1;
    }

    Ok(cancelled)
}

fn is_stale_self_model_gap_task(task: &Task, target: &str) -> bool {
    let task_target = task
        .args
        .get("target")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim();
    if task_target != target.trim() {
        return false;
    }

    match task.kind.as_str() {
        "reconcile_self_model" => true,
        "review_growth" => {
            task.args.get("kind").and_then(|value| value.as_str()) == Some("self_model_gap")
        }
        _ => false,
    }
}

fn should_schedule_project_snapshot(db: &Db) -> bool {
    let now = chrono::Utc::now().timestamp();
    let last = db
        .get_state("autonomy:last_project_snapshot_at")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);

    if now - last < PROJECT_SNAPSHOT_COOLDOWN_SECS {
        return false;
    }

    db.set_state("autonomy:last_project_snapshot_at", &now.to_string());
    true
}
