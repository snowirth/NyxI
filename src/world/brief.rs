use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    autonomy::{self, ActionRunRecord},
    db::Db,
};

use super::{
    follow_up,
    projects::{
        ProjectGraphCounts, ProjectGraphSnapshot, ResumeQueueItem, WorldFocusSummary,
        derive_world_focus,
    },
    state,
};

pub const OPERATOR_BRIEF_SCHEMA_VERSION: &str = "nyx_operator_brief.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorBrief {
    pub schema_version: String,
    pub generated_at: String,
    pub source: String,
    pub used_persisted_snapshot: bool,
    pub compile_error: Option<String>,
    pub focus: WorldFocusSummary,
    pub counts: ProjectGraphCounts,
    pub active_project_summary: Option<String>,
    pub what_matters_summary: String,
    pub blocker_summary: Option<String>,
    pub trust_summary: Option<String>,
    pub operator_next_step: Option<String>,
    pub queue_summary: Option<String>,
    pub resume_brief: OperatorResumeBrief,
    pub summary_lines: Vec<String>,
    pub status_reply: String,
    pub recent_changes: Vec<String>,
    pub resume_queue_preview: Vec<OperatorResumeItem>,
    pub recent_action: Option<OperatorRecentAction>,
    pub recent_action_reply: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorResumeBrief {
    pub what_matters_now: String,
    pub blocked_right_now: Option<String>,
    pub what_changed_last: Option<String>,
    pub what_nyx_already_did: Option<String>,
    pub what_should_happen_next: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorResumeItem {
    pub task_id: i64,
    pub kind: String,
    pub title: String,
    pub goal_title: Option<String>,
    pub priority: f64,
    pub resume_reason: String,
    pub blocked_by_count: usize,
    pub operator_state: String,
    pub retry_hint: Option<String>,
    pub top_blocker_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorRecentAction {
    pub action_run_id: i64,
    pub task_id: i64,
    pub task_kind: String,
    pub task_title: String,
    pub goal_title: Option<String>,
    pub target: Option<String>,
    pub outcome: String,
    pub summary: String,
    pub why: String,
    pub trigger_summary: Option<String>,
    pub intent_summary: String,
    pub change_summary: String,
    pub outcome_summary: String,
    pub verification: String,
    pub verification_status: String,
    pub verification_checks: Vec<String>,
    pub rollback: Option<String>,
    pub next_step: Option<String>,
    pub executed: bool,
    pub verified: Option<bool>,
    pub created_at: String,
}

pub fn compile_operator_brief(db: &Db, source: &str) -> Result<OperatorBrief> {
    let (snapshot, used_persisted_snapshot, compile_error) =
        match state::compile_and_persist_project_graph(db, source) {
            Ok(snapshot) => (snapshot, false, None),
            Err(error) => {
                let Some(snapshot) = state::load_project_graph(db) else {
                    return Err(error);
                };
                (snapshot, true, Some(error.to_string()))
            }
        };
    let focus = derive_world_focus(&snapshot);
    let recent_changes = state::load_project_graph_changes(db)
        .map(|changes| changes.summary.into_iter().take(4).collect::<Vec<_>>())
        .unwrap_or_default();
    let recent_action = build_recent_action_brief(db, &snapshot)?;
    let what_matters_summary =
        build_what_matters_summary(&snapshot, &focus, recent_action.as_ref());
    let blocker_summary = build_blocker_summary(&snapshot, &focus);
    let trust_summary = build_trust_summary(recent_action.as_ref());
    let operator_next_step = build_operator_next_step(&snapshot, recent_action.as_ref());
    let queue_summary = build_queue_summary(&snapshot);
    let what_changed_last =
        build_what_changed_last_summary(&recent_changes, recent_action.as_ref());
    let what_nyx_already_did = build_what_nyx_already_did_summary(recent_action.as_ref());
    let resume_brief = OperatorResumeBrief {
        what_matters_now: what_matters_summary.clone(),
        blocked_right_now: blocker_summary.clone(),
        what_changed_last: what_changed_last.clone(),
        what_nyx_already_did: what_nyx_already_did.clone(),
        what_should_happen_next: operator_next_step.clone(),
    };
    let summary_lines = build_summary_lines(
        &what_matters_summary,
        blocker_summary.as_deref(),
        trust_summary.as_deref(),
        operator_next_step.as_deref(),
        queue_summary.as_deref(),
        what_changed_last.as_deref(),
        what_nyx_already_did.as_deref(),
        &recent_changes,
    );
    let status_reply = render_status_reply_from_lines(&summary_lines);
    let recent_action_reply = render_recent_action_reply_from_action(recent_action.as_ref());

    Ok(OperatorBrief {
        schema_version: OPERATOR_BRIEF_SCHEMA_VERSION.to_string(),
        generated_at: snapshot.compiled_at.clone(),
        source: source.to_string(),
        used_persisted_snapshot,
        compile_error,
        focus,
        counts: snapshot.counts.clone(),
        active_project_summary: snapshot
            .projects
            .first()
            .map(|project| project.summary.clone()),
        what_matters_summary,
        blocker_summary,
        trust_summary,
        operator_next_step,
        queue_summary,
        resume_brief,
        summary_lines,
        status_reply,
        recent_changes,
        resume_queue_preview: snapshot
            .resume_queue
            .iter()
            .take(3)
            .map(|item| OperatorResumeItem {
                task_id: item.task_id,
                kind: item.kind.clone(),
                title: item.title.clone(),
                goal_title: item.goal_title.clone(),
                priority: item.priority,
                resume_reason: item.resume_reason.clone(),
                blocked_by_count: item.active_blocker_count,
                operator_state: item.operator_state.clone(),
                retry_hint: item.retry_hint.clone(),
                top_blocker_summary: item.top_blocker_summary.clone(),
            })
            .collect(),
        recent_action,
        recent_action_reply,
    })
}

pub fn render_status_reply(brief: &OperatorBrief) -> String {
    brief.status_reply.clone()
}

pub fn render_recent_action_reply(brief: &OperatorBrief) -> String {
    brief.recent_action_reply.clone()
}

fn select_operator_action_run(db: &Db) -> Result<Option<ActionRunRecord>> {
    let recent_runs = db.list_recent_autonomy_action_runs(12)?;
    Ok(recent_runs
        .iter()
        .find(|run| action_run_requires_follow_up(run))
        .cloned()
        .or_else(|| recent_runs.into_iter().next()))
}

fn action_run_requires_follow_up(action_run: &ActionRunRecord) -> bool {
    if matches!(
        action_run.task_status,
        crate::autonomy::TaskStatus::Completed | crate::autonomy::TaskStatus::Cancelled
    ) {
        return false;
    }
    if action_run.verified == Some(true) {
        return false;
    }
    action_run.rollback_reason.is_some()
        || action_run.verified == Some(false)
        || (action_run.executed && action_run.verified.is_none())
        || !action_run.executed
        || matches!(
            action_run.outcome.as_str(),
            "failed" | "retry_scheduled" | "quarantined" | "dependency_blocked"
        )
}

fn build_recent_action_brief(
    db: &Db,
    snapshot: &ProjectGraphSnapshot,
) -> Result<Option<OperatorRecentAction>> {
    let Some(action_run) = select_operator_action_run(db)? else {
        return Ok(None);
    };
    let task = db.get_autonomy_task(action_run.task_id)?;
    let goal_titles = db
        .list_active_autonomy_goals()?
        .into_iter()
        .map(|goal| (goal.id, goal.title))
        .collect::<HashMap<_, _>>();
    let resume_item = snapshot
        .resume_queue
        .iter()
        .find(|item| item.task_id == action_run.task_id);
    let trace = task.as_ref().map(autonomy::task_trace_refs);
    let observation = trace
        .as_ref()
        .and_then(|trace| trace.observation_id)
        .map(|id| db.get_autonomy_observation(id))
        .transpose()?
        .flatten();
    let growth_event = trace
        .as_ref()
        .and_then(|trace| trace.growth_event_id)
        .map(|id| db.get_growth_event(id))
        .transpose()?
        .flatten();
    let goal_title = task
        .as_ref()
        .and_then(|task| task.goal_id)
        .and_then(|goal_id| goal_titles.get(&goal_id).cloned())
        .or_else(|| resume_item.and_then(|item| item.goal_title.clone()));
    let target = task
        .as_ref()
        .and_then(autonomy::project_graph_target)
        .or_else(|| action_target_from_output(action_run.output.as_ref()));
    let trigger_summary =
        build_trigger_summary(observation.as_ref(), growth_event.as_ref(), trace.as_ref());
    let intent_summary = build_intent_summary(&action_run);
    let change_summary = build_change_summary(&action_run, target.as_deref());
    let why = build_action_reason(
        &action_run,
        goal_title.as_deref(),
        resume_item,
        trigger_summary.as_deref(),
    );
    let verification = describe_verification(&action_run);
    let verification_status = build_verification_status(&action_run);
    let verification_checks = action_run
        .verifier_verdict
        .as_ref()
        .map(|verdict| verdict.checks.clone())
        .unwrap_or_default();
    let next_step = build_next_step(snapshot, &action_run);

    Ok(Some(OperatorRecentAction {
        action_run_id: action_run.id,
        task_id: action_run.task_id,
        task_kind: action_run.task_kind.clone(),
        task_title: action_run.task_title.clone(),
        goal_title,
        target,
        outcome: action_run.outcome.clone(),
        summary: action_run.summary.clone(),
        why,
        trigger_summary,
        intent_summary,
        change_summary,
        outcome_summary: build_outcome_summary(&action_run),
        verification,
        verification_status,
        verification_checks,
        rollback: action_run
            .rollback_reason
            .as_ref()
            .map(|reason| reason.summary.clone()),
        next_step,
        executed: action_run.executed,
        verified: action_run.verified,
        created_at: action_run.created_at.clone(),
    }))
}

fn build_what_matters_summary(
    snapshot: &ProjectGraphSnapshot,
    focus: &WorldFocusSummary,
    recent_action: Option<&OperatorRecentAction>,
) -> String {
    if let Some(item) = snapshot.resume_queue.first() {
        let priority = recent_action
            .filter(|action| action.task_id == item.task_id && action.verified != Some(true))
            .map(describe_recent_action_priority)
            .unwrap_or_else(|| describe_resume_item_priority(item));
        return format!(
            "What matters right now: {}{} because {}.",
            item.title,
            resume_item_context(item, focus),
            priority
        );
    }

    if let Some(workstream_title) = focus.active_workstream_title.as_deref() {
        let project_title = focus
            .active_project_title
            .as_deref()
            .unwrap_or("your active project");
        return format!(
            "What matters right now: {} in {} is {}.",
            workstream_title,
            project_title,
            humanize_status(focus.active_workstream_status.as_deref())
        );
    }

    if let Some(project_title) = focus.active_project_title.as_deref() {
        return format!(
            "What matters right now: {} is {}.",
            project_title,
            humanize_status(focus.active_project_status.as_deref())
        );
    }

    "What matters right now: nothing urgent is queued, but Nyx is still tracking your world."
        .to_string()
}

fn build_blocker_summary(
    snapshot: &ProjectGraphSnapshot,
    focus: &WorldFocusSummary,
) -> Option<String> {
    if focus.blocker_count == 0 {
        return None;
    }

    let blocker_word = pluralize(focus.blocker_count, "blocker", "blockers");
    let top_blocker = focus
        .top_blocker_summary
        .as_deref()
        .map(|summary| crate::trunc(summary, 180));

    if let Some(item) = snapshot
        .resume_queue
        .first()
        .filter(|item| item.active_blocker_count > 0)
    {
        let current_blocker_count = focus.resume_focus_blocker_count.max(1);
        let current_blocker_word = pluralize(current_blocker_count, "blocker", "blockers");
        let current_blocker_verb = if current_blocker_count == 1 {
            "is"
        } else {
            "are"
        };
        let urgency = item
            .top_blocker_urgency
            .as_deref()
            .map(describe_blocker_urgency)
            .unwrap_or("");
        return Some(match top_blocker {
            Some(top_blocker) => format!(
                "Blockers: {} {} {} holding {}{}. Top blocker: {}.",
                current_blocker_count,
                current_blocker_word,
                current_blocker_verb,
                item.title,
                urgency,
                top_blocker
            ),
            None => format!(
                "Blockers: {} {} {} holding {}{}.",
                current_blocker_count,
                current_blocker_word,
                current_blocker_verb,
                item.title,
                urgency
            ),
        });
    }

    let blocker_verb = if focus.blocker_count == 1 {
        "is"
    } else {
        "are"
    };
    Some(match top_blocker {
        Some(top_blocker) => format!(
            "Blockers: {} active {} {} visible in the wider queue. Top blocker: {}.",
            focus.blocker_count, blocker_word, blocker_verb, top_blocker
        ),
        None => format!(
            "Blockers: {} active {} {} visible in the wider queue.",
            focus.blocker_count, blocker_word, blocker_verb
        ),
    })
}

fn build_trust_summary(recent_action: Option<&OperatorRecentAction>) -> Option<String> {
    let action = recent_action?;
    let label = if action.verified == Some(true) {
        "Trust signal"
    } else {
        "Trust warning"
    };
    Some(format!(
        "{}: latest autonomy action {} {}. {}.",
        label,
        action.task_title,
        action.outcome_summary,
        describe_action_trust_state(action)
    ))
}

fn build_operator_next_step(
    snapshot: &ProjectGraphSnapshot,
    recent_action: Option<&OperatorRecentAction>,
) -> Option<String> {
    if let Some(next_step) = recent_action
        .filter(|action| action.verified != Some(true))
        .and_then(|action| action.next_step.as_deref())
    {
        return Some(format!(
            "Next operator move: {}.",
            crate::trunc(next_step, 180)
        ));
    }

    if let Some(focus_item) = snapshot.resume_queue.first() {
        return match focus_item.operator_state.as_str() {
            "blocked" | "waiting_retry_window" => {
                if let Some(alternative) = snapshot.resume_queue.iter().find(|item| {
                    item.task_id != focus_item.task_id && is_actionable_resume_item(item)
                }) {
                    Some(format!(
                        "Next operator move: {} while {} is blocked.",
                        describe_resume_item_move(alternative),
                        focus_item.title
                    ))
                } else {
                    Some(format!(
                        "Next operator move: {}.",
                        describe_resume_item_move(focus_item)
                    ))
                }
            }
            _ => Some(format!(
                "Next operator move: {}.",
                describe_resume_item_move(focus_item)
            )),
        };
    }

    follow_up::next_step_hint(snapshot)
        .map(|hint| format!("Next operator move: {}.", crate::trunc(&hint, 180)))
}

fn build_queue_summary(snapshot: &ProjectGraphSnapshot) -> Option<String> {
    if snapshot.counts.ready_tasks == 0
        && snapshot.counts.running_tasks == 0
        && snapshot.counts.resume_queue == 0
    {
        return None;
    }

    let mut preview = snapshot
        .resume_queue
        .iter()
        .take(3)
        .map(describe_resume_queue_preview)
        .collect::<Vec<_>>();
    let remaining = snapshot.resume_queue.len().saturating_sub(preview.len());
    if remaining > 0 {
        preview.push(format!("+{} more", remaining));
    }

    let item_word = pluralize(
        snapshot.counts.resume_queue,
        "follow-through item",
        "follow-through items",
    );

    if preview.is_empty() {
        Some(format!(
            "Autonomy queue: {} ready, {} running, {} total {}.",
            snapshot.counts.ready_tasks,
            snapshot.counts.running_tasks,
            snapshot.counts.resume_queue,
            item_word
        ))
    } else {
        Some(format!(
            "Autonomy queue: {}. {} ready, {} running, {} total {}.",
            preview.join("; "),
            snapshot.counts.ready_tasks,
            snapshot.counts.running_tasks,
            snapshot.counts.resume_queue,
            item_word
        ))
    }
}

fn build_summary_lines(
    what_matters_summary: &str,
    blocker_summary: Option<&str>,
    trust_summary: Option<&str>,
    operator_next_step: Option<&str>,
    queue_summary: Option<&str>,
    what_changed_last: Option<&str>,
    what_nyx_already_did: Option<&str>,
    recent_changes: &[String],
) -> Vec<String> {
    let mut lines = vec![what_matters_summary.to_string()];

    if let Some(blocker_summary) = blocker_summary {
        lines.push(blocker_summary.to_string());
    }

    if let Some(trust_summary) = trust_summary {
        lines.push(trust_summary.to_string());
    }

    if let Some(operator_next_step) = operator_next_step {
        lines.push(operator_next_step.to_string());
    }

    if let Some(queue_summary) = queue_summary {
        lines.push(queue_summary.to_string());
    }

    if let Some(what_changed_last) = what_changed_last {
        lines.push(what_changed_last.to_string());
    } else if let Some(change) = recent_changes.first() {
        lines.push(format!("What changed last: {}.", crate::trunc(change, 180)));
    }

    if let Some(what_nyx_already_did) = what_nyx_already_did {
        lines.push(what_nyx_already_did.to_string());
    }

    if lines.is_empty() {
        lines.push(
            "Nothing urgent is currently queued. Nyx is idle but still tracking your world."
                .to_string(),
        );
    }

    lines
}

fn build_what_changed_last_summary(
    recent_changes: &[String],
    recent_action: Option<&OperatorRecentAction>,
) -> Option<String> {
    recent_changes
        .first()
        .map(|change| format!("What changed last: {}.", crate::trunc(change, 180)))
        .or_else(|| {
            recent_action.map(|action| {
                format!(
                    "What changed last: {}.",
                    crate::trunc(&action.change_summary, 180)
                )
            })
        })
}

fn build_what_nyx_already_did_summary(
    recent_action: Option<&OperatorRecentAction>,
) -> Option<String> {
    recent_action.map(|action| {
        format!(
            "What Nyx already did: {} ({}).",
            action.task_title, action.outcome_summary
        )
    })
}

fn build_action_reason(
    action_run: &ActionRunRecord,
    goal_title: Option<&str>,
    resume_item: Option<&crate::world::projects::ResumeQueueItem>,
    trigger_summary: Option<&str>,
) -> String {
    let mut reasons = Vec::new();
    if let Some(goal_title) = goal_title {
        reasons.push(format!("it supports the goal \"{}\"", goal_title));
    }
    if let Some(resume_item) = resume_item {
        reasons.push(resume_item.resume_reason.clone());
    }
    if let Some(trigger_summary) = trigger_summary {
        reasons.push(trigger_summary.to_string());
    }
    if reasons.is_empty() {
        reasons.push(format!(
            "it was the most recent {} action on record",
            action_run.task_kind
        ));
    }
    reasons.join("; ")
}

fn build_trigger_summary(
    observation: Option<&autonomy::Observation>,
    growth_event: Option<&autonomy::GrowthEvent>,
    trace: Option<&autonomy::TaskTraceRefs>,
) -> Option<String> {
    if let Some(observation) = observation {
        return Some(format!(
            "it was triggered by a {} observation: {}",
            observation.kind,
            crate::trunc(&observation.content, 140)
        ));
    }
    if let Some(growth_event) = growth_event {
        return Some(format!(
            "it followed the growth event {}: {}",
            growth_event.kind,
            crate::trunc(&growth_event.summary, 140)
        ));
    }
    if let Some(trace) = trace {
        if let Some(trigger_kind) = trace.trigger_kind.as_deref() {
            if let Some(capability_name) = trace.capability_name.as_deref() {
                return Some(format!(
                    "it was triggered by {} for capability {}",
                    trigger_kind, capability_name
                ));
            }
            return Some(format!("it was triggered by {}", trigger_kind));
        }
    }
    None
}

fn describe_verification(action_run: &ActionRunRecord) -> String {
    match action_run.verified {
        Some(true) => action_run
            .verifier_verdict
            .as_ref()
            .map(|verdict| format!("verified: {}", verdict.summary))
            .unwrap_or_else(|| "verified".to_string()),
        Some(false) => action_run
            .rollback_reason
            .as_ref()
            .map(|reason| format!("not verified: {}", reason.summary))
            .or_else(|| {
                action_run
                    .verifier_verdict
                    .as_ref()
                    .map(|verdict| format!("not verified: {}", verdict.summary))
            })
            .unwrap_or_else(|| "not verified".to_string()),
        None if !action_run.executed => action_run
            .verifier_verdict
            .as_ref()
            .map(|verdict| format!("blocked before execution: {}", verdict.summary))
            .unwrap_or_else(|| "blocked before execution".to_string()),
        None => action_run
            .verifier_verdict
            .as_ref()
            .map(|verdict| {
                format!(
                    "executed without explicit verification: {}",
                    verdict.summary
                )
            })
            .unwrap_or_else(|| "executed without explicit verification".to_string()),
    }
}

fn build_intent_summary(action_run: &ActionRunRecord) -> String {
    action_run
        .expected_effect
        .as_ref()
        .map(|effect| effect.detail.trim().to_string())
        .filter(|detail| !detail.is_empty())
        .unwrap_or_else(|| format!("complete the {} task", action_run.task_kind))
}

fn build_change_summary(action_run: &ActionRunRecord, target: Option<&str>) -> String {
    if !action_run.executed {
        return "No world change was applied because the action stopped before execution."
            .to_string();
    }

    let summary = action_run.summary.trim();
    match target.map(str::trim).filter(|value| !value.is_empty()) {
        Some(target) => format!("Target {} now reflects this action: {}", target, summary),
        None => summary.to_string(),
    }
}

fn build_outcome_summary(action_run: &ActionRunRecord) -> String {
    let outcome = humanize_value(&action_run.outcome);
    match (action_run.executed, action_run.verified) {
        (false, _) => format!("{outcome} before execution"),
        (true, Some(true)) => format!("{outcome} and verified"),
        (true, Some(false)) => format!("{outcome} but not verified"),
        (true, None) => format!("{outcome} without explicit verification"),
    }
}

fn build_verification_status(action_run: &ActionRunRecord) -> String {
    match (action_run.executed, action_run.verified) {
        (false, _) => "blocked_before_execution".to_string(),
        (true, Some(true)) => "verified".to_string(),
        (true, Some(false)) => "not_verified".to_string(),
        (true, None) => "executed_unverified".to_string(),
    }
}

fn build_next_step(
    snapshot: &ProjectGraphSnapshot,
    action_run: &ActionRunRecord,
) -> Option<String> {
    if let Some(reason) = action_run.rollback_reason.as_ref() {
        let lead = if reason.retryable {
            "Retry after addressing"
        } else {
            "Handle rollback note"
        };
        return Some(format!("{lead}: {}", reason.summary));
    }

    snapshot
        .resume_queue
        .iter()
        .find(|item| item.task_id != action_run.task_id && is_actionable_resume_item(item))
        .or_else(|| {
            snapshot
                .resume_queue
                .iter()
                .find(|item| item.task_id == action_run.task_id)
        })
        .map(|item| {
            format!(
                "{} because {}",
                item.title,
                item.resume_reason.trim_end_matches('.')
            )
        })
}

fn action_target_from_output(output: Option<&serde_json::Value>) -> Option<String> {
    output
        .and_then(|value| value.pointer("/execution/trace/target"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn humanize_status(status: Option<&str>) -> String {
    status.unwrap_or("active").replace('_', " ")
}

fn humanize_value(value: &str) -> String {
    value.replace('_', " ")
}

fn pluralize<'a>(count: usize, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

fn resume_item_context(item: &ResumeQueueItem, focus: &WorldFocusSummary) -> String {
    let goal_title = item
        .goal_title
        .as_deref()
        .filter(|goal_title| *goal_title != item.title);
    let workstream_title = focus
        .active_workstream_title
        .as_deref()
        .filter(|workstream_title| {
            *workstream_title != item.title && Some(*workstream_title) != goal_title
        });

    let mut context = String::new();
    if let Some(goal_title) = goal_title.or(workstream_title) {
        context.push_str(" for ");
        context.push_str(goal_title);
    }
    if let Some(project_title) = focus.active_project_title.as_deref() {
        context.push_str(" in ");
        context.push_str(project_title);
    }
    context
}

fn describe_resume_item_priority(item: &ResumeQueueItem) -> String {
    match item.operator_state.as_str() {
        "needs_intervention" => item
            .top_blocker_summary
            .as_deref()
            .map(|summary| {
                format!(
                    "it needs intervention now because {}",
                    crate::trunc(summary, 120)
                )
            })
            .unwrap_or_else(|| "it needs intervention now".to_string()),
        "retry_needed" => item
            .retry_hint
            .as_deref()
            .map(|hint| {
                format!(
                    "the last follow-through still needs attention: {}",
                    crate::trunc(hint, 120)
                )
            })
            .unwrap_or_else(|| "the last follow-through still needs attention".to_string()),
        "waiting_retry_window" => "it is waiting for the retry window to reopen".to_string(),
        "blocked" => {
            let blocker_word = pluralize(item.active_blocker_count, "blocker", "blockers");
            format!(
                "it is blocked by {} active {}",
                item.active_blocker_count, blocker_word
            )
        }
        "in_flight" => "it is already in flight".to_string(),
        "scheduled" => "it is the next scheduled follow-through".to_string(),
        _ => "it is ready to move now".to_string(),
    }
}

fn describe_recent_action_priority(action: &OperatorRecentAction) -> String {
    match action.verification_status.as_str() {
        "not_verified" => "the last follow-through was not verified".to_string(),
        "executed_unverified" => {
            "the last follow-through still needs explicit verification".to_string()
        }
        _ => "the last follow-through stopped before execution".to_string(),
    }
}

fn describe_resume_item_move(item: &ResumeQueueItem) -> String {
    match item.operator_state.as_str() {
        "needs_intervention" => item
            .top_blocker_summary
            .as_deref()
            .map(|summary| {
                format!(
                    "intervene on {} because {}",
                    item.title,
                    crate::trunc(summary, 140)
                )
            })
            .unwrap_or_else(|| format!("intervene on {} now", item.title)),
        "retry_needed" => item
            .retry_hint
            .as_deref()
            .map(|hint| format!("retry {} because {}", item.title, crate::trunc(hint, 140)))
            .unwrap_or_else(|| format!("retry {}", item.title)),
        "waiting_retry_window" => format!("let {} wait for its retry window to reopen", item.title),
        "blocked" => item
            .top_blocker_summary
            .as_deref()
            .map(|summary| {
                format!(
                    "clear the blocker on {} because {}",
                    item.title,
                    crate::trunc(summary, 140)
                )
            })
            .unwrap_or_else(|| format!("clear the blocker on {}", item.title)),
        "in_flight" => format!(
            "check progress on {} because it is already in flight",
            item.title
        ),
        "scheduled" => format!(
            "prepare {} because it is the next scheduled follow-through",
            item.title
        ),
        _ => format!("{} because it is ready to move now", item.title),
    }
}

fn describe_resume_queue_preview(item: &ResumeQueueItem) -> String {
    match item.operator_state.as_str() {
        "needs_intervention" => format!("{} (needs intervention)", item.title),
        "retry_needed" => format!("{} (retry needed)", item.title),
        "waiting_retry_window" => format!("{} (waiting retry window)", item.title),
        "blocked" => {
            let blocker_word = pluralize(item.active_blocker_count, "blocker", "blockers");
            format!(
                "{} (blocked by {} {})",
                item.title, item.active_blocker_count, blocker_word
            )
        }
        "in_flight" => format!("{} (in flight)", item.title),
        "scheduled" => format!("{} (scheduled)", item.title),
        _ => format!("{} (ready now)", item.title),
    }
}

fn is_actionable_resume_item(item: &ResumeQueueItem) -> bool {
    matches!(
        item.operator_state.as_str(),
        "needs_intervention" | "retry_needed" | "ready_now"
    )
}

fn describe_blocker_urgency(urgency: &str) -> &'static str {
    match urgency {
        "critical" => " and it is critical",
        "needs_intervention" => " and it needs intervention now",
        "aging" => " and it has been aging",
        _ => "",
    }
}

fn describe_action_trust_state(action: &OperatorRecentAction) -> String {
    match action.verification_status.as_str() {
        "verified" => "Explicit verification landed for that follow-through".to_string(),
        "not_verified" => {
            "Verification failed, so operator follow-through is still open".to_string()
        }
        "executed_unverified" => {
            "Execution landed, but explicit verification is still missing".to_string()
        }
        _ => "The action stopped before execution, so no follow-through landed yet".to_string(),
    }
}

fn render_status_reply_from_lines(summary_lines: &[String]) -> String {
    if summary_lines.is_empty() {
        "Nothing urgent is currently queued. Nyx is idle but still tracking your world.".to_string()
    } else {
        summary_lines.join("\n")
    }
}

fn render_recent_action_reply_from_action(action: Option<&OperatorRecentAction>) -> String {
    let Some(action) = action else {
        return "I do not have a recent autonomy action on record to explain yet.".to_string();
    };

    let mut lines = vec![format!(
        "Most recent autonomy action: {}.",
        action.task_title
    )];
    lines.push(format!("Intent: {}.", action.intent_summary));
    lines.push(format!("Why: {}.", action.why));
    if let Some(trigger_summary) = action.trigger_summary.as_deref() {
        lines.push(format!("Trigger: {}.", trigger_summary));
    }
    lines.push(format!("What happened: {}.", action.summary));
    lines.push(format!("What changed: {}.", action.change_summary));
    if let Some(target) = action.target.as_deref() {
        lines.push(format!("Target: {}.", target));
    }
    lines.push(format!("Outcome: {}.", action.outcome_summary));
    lines.push(format!("Trust: {}.", describe_action_trust_state(action)));
    lines.push(format!("Verification: {}.", action.verification));
    if !action.verification_checks.is_empty() {
        lines.push(format!(
            "Verification checks: {}.",
            action.verification_checks.join("; ")
        ));
    }
    if let Some(next_step) = action.next_step.as_deref() {
        lines.push(format!("Next step: {}.", next_step));
    }
    if let Some(rollback) = action
        .rollback
        .as_deref()
        .filter(|_| action.verified != Some(true))
    {
        lines.push(format!("Rollback or retry note: {}.", rollback));
    }
    lines.join("\n")
}
