use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::runtime::verifier::RollbackReason;

use super::{
    follow_up,
    projects::{WorldFocusSummary, derive_world_focus},
    state,
};

pub const CROSS_SURFACE_CONTINUITY_SCHEMA_VERSION: &str = "nyx_cross_surface_continuity.v1";

const CONTINUITY_HISTORY_LIMIT_PER_SURFACE: usize = 8;
const CONTINUITY_SURFACE_LIMIT: usize = 4;
const LOW_SIGNAL_ACTIVITY_TEXT: &[&str] = &[
    "ok",
    "okay",
    "yeah",
    "yep",
    "yup",
    "sure",
    "thanks",
    "thank you",
    "cool",
    "nice",
    "got it",
    "catch me up",
    "what were we doing?",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossSurfaceContinuityBrief {
    pub schema_version: String,
    pub generated_at: String,
    pub source: String,
    pub used_persisted_snapshot: bool,
    pub compile_error: Option<String>,
    pub focus: WorldFocusSummary,
    pub active_surface: Option<String>,
    pub active_channel: Option<String>,
    pub recent_surface_activity: Vec<CrossSurfaceActivity>,
    pub resume_focus_state: Option<String>,
    pub blocker_summary: Option<String>,
    pub next_step_hint: Option<String>,
    pub resume_cues: Vec<String>,
    pub recent_action_signal: Option<CrossSurfaceActionSignal>,
    pub continuity_reply: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossSurfaceActivity {
    pub surface: String,
    pub channel: String,
    pub role: String,
    pub preview: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossSurfaceActionSignal {
    pub action_run_id: i64,
    pub task_id: i64,
    pub task_kind: String,
    pub task_title: String,
    pub target: Option<String>,
    pub outcome: String,
    pub verification_status: String,
    pub verification_summary: String,
    pub rollback_summary: Option<String>,
    pub rollback_retryable: Option<bool>,
    pub inspect_path: String,
    pub created_at: String,
}

pub fn compile_cross_surface_continuity(
    db: &Db,
    source: &str,
) -> Result<CrossSurfaceContinuityBrief> {
    compile_cross_surface_continuity_inner(db, source, true)
}

/// Compile the cross-surface continuity brief without persisting the freshly compiled
/// project graph snapshot. Use this from read-only contexts (e.g. chat prompt assembly)
/// so that reading the brief does not invalidate other caches keyed on world focus.
pub fn read_cross_surface_continuity(db: &Db, source: &str) -> Result<CrossSurfaceContinuityBrief> {
    compile_cross_surface_continuity_inner(db, source, false)
}

fn compile_cross_surface_continuity_inner(
    db: &Db,
    source: &str,
    persist: bool,
) -> Result<CrossSurfaceContinuityBrief> {
    let compile_result = if persist {
        state::compile_and_persist_project_graph(db, source)
    } else {
        super::projects::compile_project_graph(db, source)
    };
    let (snapshot, used_persisted_snapshot, compile_error) = match compile_result {
        Ok(snapshot) => (snapshot, false, None),
        Err(error) => {
            let Some(snapshot) = state::load_project_graph(db) else {
                return Err(error);
            };
            (snapshot, true, Some(error.to_string()))
        }
    };
    let focus = derive_world_focus(&snapshot);
    let resume_focus = snapshot.resume_queue.first();
    let recent_surface_activity = recent_surface_activity(db);
    let active_surface = recent_surface_activity
        .first()
        .map(|activity| activity.surface.clone());
    let active_channel = recent_surface_activity
        .first()
        .map(|activity| activity.channel.clone());
    let resume_focus_state = resume_focus.map(|item| item.operator_state.clone());
    let blocker_summary = resume_focus
        .filter(|item| item.active_blocker_count > 0)
        .and_then(|item| item.top_blocker_summary.clone());
    let next_step_hint = resume_focus
        .map(describe_continuity_next_step)
        .or_else(|| follow_up::next_step_hint(&snapshot));
    let recent_action_signal = build_recent_action_signal(db)?;
    let resume_cues = build_resume_cues(
        &focus,
        resume_focus,
        &recent_surface_activity,
        blocker_summary.as_deref(),
        next_step_hint.as_deref(),
        recent_action_signal.as_ref(),
    );
    let continuity_reply = render_continuity_reply_from_lines(&resume_cues);

    Ok(CrossSurfaceContinuityBrief {
        schema_version: CROSS_SURFACE_CONTINUITY_SCHEMA_VERSION.to_string(),
        generated_at: snapshot.compiled_at,
        source: source.to_string(),
        used_persisted_snapshot,
        compile_error,
        focus,
        active_surface,
        active_channel,
        recent_surface_activity,
        resume_focus_state,
        blocker_summary,
        next_step_hint,
        resume_cues,
        recent_action_signal,
        continuity_reply,
    })
}

pub fn render_continuity_reply(brief: &CrossSurfaceContinuityBrief) -> String {
    brief.continuity_reply.clone()
}

pub fn render_prompt_context(
    brief: &CrossSurfaceContinuityBrief,
    current_channel: &str,
) -> Option<String> {
    let current_surface = surface_kind_from_channel(current_channel);
    let mut lines = brief
        .recent_surface_activity
        .iter()
        .filter(|activity| activity.surface != current_surface)
        .take(2)
        .map(|activity| {
            format!(
                "- recent {} context: {}",
                surface_title(&activity.surface),
                activity.preview
            )
        })
        .collect::<Vec<_>>();

    if let Some(resume_title) = brief.focus.resume_focus_title.as_deref() {
        let reason = brief
            .focus
            .resume_focus_reason
            .as_deref()
            .unwrap_or("it is the strongest next follow-through item");
        lines.push(format!(
            "- current follow-through: {} because {}",
            resume_title, reason
        ));
    }

    if let Some(resume_focus_state) = brief.resume_focus_state.as_deref() {
        lines.push(format!(
            "- resume state: {}",
            humanize_label(resume_focus_state)
        ));
    }

    if let Some(next_step_hint) = brief.next_step_hint.as_deref() {
        lines.push(format!("- next operator move: {}", next_step_hint));
    }

    if let Some(action_signal) = brief.recent_action_signal.as_ref().filter(|signal| {
        signal.verification_status != "verified" || signal.rollback_summary.is_some()
    }) {
        let mut action_line = format!(
            "- latest autonomy follow-through: {} ended {} with {}",
            action_signal.task_title,
            humanize_label(&action_signal.outcome),
            action_signal.verification_summary
        );
        if let Some(rollback) = action_signal.rollback_summary.as_deref() {
            action_line.push_str(&format!("; rollback/retry signal {}", rollback));
        }
        lines.push(action_line);
    }

    if let Some(blocker_summary) = brief.blocker_summary.as_deref() {
        lines.push(format!("- blocker context: {}", blocker_summary));
    }

    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

fn recent_surface_activity(db: &Db) -> Vec<CrossSurfaceActivity> {
    let mut candidates = Vec::new();

    push_surface_activity(
        &mut candidates,
        db.get_history_with_meta("web", CONTINUITY_HISTORY_LIMIT_PER_SURFACE),
    );
    push_surface_activity(
        &mut candidates,
        db.get_history_with_meta("voice", CONTINUITY_HISTORY_LIMIT_PER_SURFACE),
    );
    push_surface_activity(
        &mut candidates,
        db.get_history_with_meta("mcp", CONTINUITY_HISTORY_LIMIT_PER_SURFACE),
    );
    push_surface_activity(
        &mut candidates,
        db.get_history_with_meta_by_prefix("telegram:", CONTINUITY_HISTORY_LIMIT_PER_SURFACE),
    );
    push_surface_activity(
        &mut candidates,
        db.get_history_with_meta_by_prefix("discord:", CONTINUITY_HISTORY_LIMIT_PER_SURFACE),
    );

    candidates.sort_by(|(left_id, _), (right_id, _)| right_id.cmp(left_id));
    candidates
        .into_iter()
        .map(|(_, activity)| activity)
        .take(CONTINUITY_SURFACE_LIMIT)
        .collect()
}

fn push_surface_activity(
    out: &mut Vec<(i64, CrossSurfaceActivity)>,
    entries: Vec<(i64, String, String, String, String)>,
) {
    let Some((message_id, channel, role, content, timestamp)) =
        select_meaningful_surface_entry(&entries)
    else {
        return;
    };
    out.push((
        *message_id,
        CrossSurfaceActivity {
            surface: surface_kind_from_channel(channel),
            channel: channel.clone(),
            role: role.clone(),
            preview: crate::trunc(content.trim(), 160).to_string(),
            timestamp: timestamp.clone(),
        },
    ));
}

fn select_meaningful_surface_entry(
    entries: &[(i64, String, String, String, String)],
) -> Option<&(i64, String, String, String, String)> {
    entries
        .iter()
        .rev()
        .find(|(_, _, role, content, _)| role == "user" && is_meaningful_activity(content))
        .or_else(|| {
            entries
                .iter()
                .rev()
                .find(|(_, _, _, content, _)| is_meaningful_activity(content))
        })
}

fn is_meaningful_activity(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.len() < 8 {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    !LOW_SIGNAL_ACTIVITY_TEXT.iter().any(|item| lower == *item)
}

fn render_continuity_reply_from_parts(
    focus: &WorldFocusSummary,
    recent_surface_activity: &[CrossSurfaceActivity],
) -> Vec<String> {
    let mut lines = Vec::new();

    if let Some(latest) = recent_surface_activity.first() {
        let action = if latest.role == "user" {
            "you last said"
        } else {
            "Nyx last replied"
        };
        lines.push(format!(
            "Most recent cross-surface thread was on {} via {} at {}: {} '{}'.",
            surface_title(&latest.surface),
            latest.channel,
            latest.timestamp,
            action,
            latest.preview
        ));
    }

    if let Some(other_surface) = recent_surface_activity.iter().skip(1).find(|activity| {
        recent_surface_activity
            .first()
            .map(|latest| latest.surface != activity.surface)
            .unwrap_or(true)
    }) {
        lines.push(format!(
            "Carry-over context also came through {} via {}: '{}'.",
            surface_title(&other_surface.surface),
            other_surface.channel,
            other_surface.preview
        ));
    }

    if let Some(resume_title) = focus.resume_focus_title.as_deref() {
        let reason = focus
            .resume_focus_reason
            .as_deref()
            .unwrap_or("it is the strongest next follow-through item");
        lines.push(format!(
            "Current follow-through is {} because {}.",
            resume_title, reason
        ));
    } else if let Some(workstream_title) = focus.active_workstream_title.as_deref() {
        lines.push(format!("Current workstream focus is {}.", workstream_title));
    }

    if focus.resume_focus_blocker_count > 0 {
        lines.push(format!(
            "That follow-through still has {} blocker(s) attached.",
            focus.resume_focus_blocker_count
        ));
    } else if focus.blocker_count > 0 {
        lines.push(format!(
            "There are {} blocker(s) visible in the wider queue.",
            focus.blocker_count
        ));
    }

    if let Some(top_blocker) = focus.top_blocker_summary.as_deref() {
        lines.push(format!("Top blocker still visible is {}.", top_blocker));
    }

    lines
}

fn build_resume_cues(
    focus: &WorldFocusSummary,
    resume_focus: Option<&crate::world::projects::ResumeQueueItem>,
    recent_surface_activity: &[CrossSurfaceActivity],
    blocker_summary: Option<&str>,
    next_step_hint: Option<&str>,
    recent_action_signal: Option<&CrossSurfaceActionSignal>,
) -> Vec<String> {
    let mut lines = render_continuity_reply_from_parts(focus, recent_surface_activity);

    if let Some(resume_focus) = resume_focus {
        lines.push(format!(
            "Resume state for {} is {}.",
            resume_focus.title,
            humanize_label(&resume_focus.operator_state)
        ));
    }

    if let Some(blocker_summary) = blocker_summary {
        lines.push(format!("Current blocker signal: {}.", blocker_summary));
    }

    if let Some(next_step_hint) = next_step_hint {
        lines.push(format!("Next operator move should be {}.", next_step_hint));
    }

    if let Some(action_signal) = recent_action_signal {
        let mut action_line = format!(
            "Latest autonomous follow-through was {} with outcome {}. Verification signal: {}.",
            action_signal.task_title,
            humanize_label(&action_signal.outcome),
            action_signal.verification_summary
        );
        if let Some(target) = action_signal.target.as_deref() {
            action_line.push_str(&format!(" Target: {}.", target));
        }
        if let Some(rollback) = action_signal.rollback_summary.as_deref() {
            action_line.push_str(&format!(" Rollback or retry signal: {}.", rollback));
        }
        lines.push(action_line);
    }

    lines
}

fn render_continuity_reply_from_lines(lines: &[String]) -> String {
    if lines.is_empty() {
        "I do not have a strong cross-surface continuity thread to resume yet.".to_string()
    } else {
        lines.join(" ")
    }
}

fn build_recent_action_signal(db: &Db) -> Result<Option<CrossSurfaceActionSignal>> {
    let Some(action_run) = select_continuity_action_run(db)? else {
        return Ok(None);
    };

    let target = action_run
        .expected_effect
        .as_ref()
        .and_then(|effect| effect.target.clone())
        .or_else(|| action_target_from_output(action_run.output.as_ref()));

    Ok(Some(CrossSurfaceActionSignal {
        action_run_id: action_run.id,
        task_id: action_run.task_id,
        task_kind: action_run.task_kind.clone(),
        task_title: action_run.task_title.clone(),
        target,
        outcome: action_run.outcome.clone(),
        verification_status: verification_status(&action_run),
        verification_summary: verification_summary(&action_run),
        rollback_summary: action_run.rollback_reason.as_ref().map(rollback_summary),
        rollback_retryable: action_run
            .rollback_reason
            .as_ref()
            .map(|reason| reason.retryable),
        inspect_path: format!("/api/autonomy/action-runs/{}/trace", action_run.id),
        created_at: action_run.created_at.clone(),
    }))
}

fn describe_continuity_next_step(item: &crate::world::projects::ResumeQueueItem) -> String {
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
            .unwrap_or_else(|| format!("intervene on {}", item.title)),
        "retry_needed" => item
            .retry_hint
            .as_deref()
            .map(|hint| format!("retry {} because {}", item.title, crate::trunc(hint, 140)))
            .unwrap_or_else(|| format!("retry {}", item.title)),
        "waiting_retry_window" => format!("wait until {} can retry safely", item.title),
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
        "in_flight" => format!("check progress on {}", item.title),
        "scheduled" => format!("prepare {}", item.title),
        _ => format!("do {}", item.title),
    }
}

fn select_continuity_action_run(db: &Db) -> Result<Option<crate::autonomy::ActionRunRecord>> {
    let recent_runs = db.list_recent_autonomy_action_runs(12)?;
    Ok(recent_runs
        .iter()
        .find(|run| action_run_requires_follow_up(run))
        .cloned()
        .or_else(|| recent_runs.into_iter().next()))
}

fn action_run_requires_follow_up(action_run: &crate::autonomy::ActionRunRecord) -> bool {
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

fn action_target_from_output(output: Option<&serde_json::Value>) -> Option<String> {
    output
        .and_then(|value| value.pointer("/execution/trace/target"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn verification_status(action_run: &crate::autonomy::ActionRunRecord) -> String {
    action_run
        .verifier_verdict
        .as_ref()
        .map(|verdict| verdict.status.clone())
        .unwrap_or_else(|| match (action_run.executed, action_run.verified) {
            (false, _) => "blocked_before_execution".to_string(),
            (true, Some(true)) => "verified".to_string(),
            (true, Some(false)) => "failed".to_string(),
            (true, None) => "executed_unverified".to_string(),
        })
}

fn verification_summary(action_run: &crate::autonomy::ActionRunRecord) -> String {
    let status = humanize_label(&verification_status(action_run));
    if let Some(verdict) = action_run.verifier_verdict.as_ref() {
        let summary = verdict.summary.trim();
        if !summary.is_empty() {
            return format!("{}: {}", status, crate::trunc(summary, 160));
        }
    }

    let fallback = match (action_run.executed, action_run.verified) {
        (false, _) => "blocked before execution",
        (true, Some(true)) => "verified cleanly",
        (true, Some(false)) => "failed verification",
        (true, None) => "executed without a recorded verifier verdict",
    };
    format!("{}: {}", status, fallback)
}

fn rollback_summary(reason: &RollbackReason) -> String {
    format!(
        "{}: {} ({})",
        humanize_label(&reason.kind),
        crate::trunc(reason.summary.trim(), 160),
        if reason.retryable {
            "retryable"
        } else {
            "not retryable"
        },
    )
}

fn humanize_label(value: &str) -> String {
    value.replace(['_', '-'], " ")
}

fn surface_kind_from_channel(channel: &str) -> String {
    if channel.starts_with("telegram:") {
        "telegram".to_string()
    } else if channel.starts_with("discord:") {
        "discord".to_string()
    } else if channel == "mcp" {
        "mcp".to_string()
    } else if channel == "voice" {
        "voice".to_string()
    } else {
        "web".to_string()
    }
}

fn surface_title(surface: &str) -> &str {
    match surface {
        "telegram" => "Telegram",
        "discord" => "Discord",
        "voice" => "voice",
        "mcp" => "MCP",
        _ => "web",
    }
}
