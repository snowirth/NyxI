//! Autonomy promotion — turning raw observations into goals and tasks.
//!
//! `promote_observation` is the translation layer between passive signals
//! (consciousness insights, file activity, growth telemetry, self-model gaps)
//! and the autonomy work queue. Dedupe keys, review gates, and reconcile-arg
//! shaping live here; the actual task execution lives back in the parent
//! `autonomy` module.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use anyhow::Result;

use crate::db::Db;

use super::execution::{infer_capability_name_from_target, supports_built_tool_reconcile_target};
use super::{
    GOAL_AWARENESS, GOAL_GROWTH_COORDINATION, GOAL_KNOWLEDGE_CAPTURE, GOAL_PROJECT_TRACKING,
    GOAL_SELF_MODEL_ALIGNMENT, GOAL_TIMELY_FOLLOW_UP, Observation,
    REVIEW_GROWTH_TASK_COOLDOWN_SECS, SELF_MODEL_RECONCILE_TASK_COOLDOWN_SECS, clamp_priority,
    should_schedule_project_snapshot, task_title,
};

pub(super) fn promote_observation(db: &Db, observation: &Observation) -> Result<()> {
    let content = observation.content.trim();

    match observation.kind.as_str() {
        "attention_prompt" | "return_event" => {
            db.upsert_autonomy_goal(
                GOAL_AWARENESS,
                &observation.source,
                Some("Maintain awareness of the user's current context, pacing, and breaks."),
                clamp_priority(observation.priority.max(0.55)),
            )?;
        }
        "file_activity" => {
            let goal_id = db.upsert_autonomy_goal(
                GOAL_PROJECT_TRACKING,
                &observation.source,
                Some("Track active files and workstreams so follow-ups stay grounded in current work."),
                clamp_priority(observation.priority.max(0.55)),
            )?;

            if should_schedule_project_snapshot(db) {
                db.create_autonomy_task(
                    Some(goal_id),
                    "run_tool",
                    &task_title("Review repo state", content),
                    Some("git_info"),
                    &serde_json::json!({
                        "action": "status",
                        "deliver_output": true,
                        "message_prefix": "autonomy repo snapshot",
                        "max_output_chars": 280,
                        "observation_id": observation.id,
                    }),
                    Some("Created from recent file activity."),
                    clamp_priority(observation.priority.max(0.62)),
                    None,
                )?;
            }
        }
        "memory_candidate" => {
            let goal_id = db.upsert_autonomy_goal(
                GOAL_KNOWLEDGE_CAPTURE,
                &observation.source,
                Some("Capture durable knowledge discovered by background processing."),
                clamp_priority(observation.priority.max(0.6)),
            )?;

            if content.len() >= 10 {
                db.create_autonomy_task(
                    Some(goal_id),
                    "store_memory",
                    &task_title("Store memory", content),
                    Some("db.remember"),
                    &serde_json::json!({
                        "content": content,
                        "network": "knowledge",
                        "importance": clamp_priority(observation.priority.max(0.6)),
                        "observation_id": observation.id,
                    }),
                    Some("Created from a background memory candidate."),
                    clamp_priority(observation.priority.max(0.6)),
                    None,
                )?;
            }
        }
        "memory_stored" => {
            db.upsert_autonomy_goal(
                GOAL_KNOWLEDGE_CAPTURE,
                &observation.source,
                Some("Capture durable knowledge discovered by background processing."),
                clamp_priority(observation.priority.max(0.6)),
            )?;
        }
        "consciousness_insight" | "overnight_briefing" => {
            let goal_id = db.upsert_autonomy_goal(
                GOAL_TIMELY_FOLLOW_UP,
                &observation.source,
                Some("Surface concise, useful follow-ups derived from background reasoning."),
                clamp_priority(observation.priority.max(0.7)),
            )?;

            if content.len() >= 10 {
                db.create_autonomy_task(
                    Some(goal_id),
                    "deliver_message",
                    &task_title("Deliver message", content),
                    Some("proactive_queue"),
                    &serde_json::json!({
                        "message": content,
                        "observation_id": observation.id,
                        "source": observation.source,
                    }),
                    Some("Created from a background insight."),
                    clamp_priority(observation.priority.max(0.7)),
                    None,
                )?;
            }
        }
        "overnight_staged" => {
            db.upsert_autonomy_goal(
                GOAL_TIMELY_FOLLOW_UP,
                &observation.source,
                Some("Prepare candidate follow-ups before they are reviewed and delivered."),
                clamp_priority(observation.priority.max(0.6)),
            )?;
        }
        "tool_growth_result" | "self_edit_result" | "memory_consolidation" | "user_adaptation" => {
            let goal_id = db.upsert_autonomy_goal(
                GOAL_GROWTH_COORDINATION,
                &observation.source,
                Some(
                    "Review growth telemetry from self-edits, tool building, memory consolidation, and user adaptation.",
                ),
                clamp_priority(observation.priority.max(0.66)),
            )?;

            if should_review_growth_observation(observation) {
                maybe_create_autonomy_task(
                    db,
                    Some(goal_id),
                    "review_growth",
                    &task_title("Review growth", content),
                    Some("growth.review"),
                    serde_json::json!({
                        "observation_id": observation.id,
                        "growth_event_id": observation.context.get("growth_event_id").and_then(|value| value.as_i64()),
                        "kind": observation.kind,
                        "message": content,
                        "success": observation.context.get("success").and_then(|value| value.as_bool()).unwrap_or(false),
                        "target": observation.context.get("target").and_then(|value| value.as_str()).unwrap_or(""),
                        "repair_rounds": observation.context.get("repair_rounds").and_then(|value| value.as_u64()).unwrap_or(0),
                        "deliver_output": observation.context.get("deliver_output").and_then(|value| value.as_bool()).unwrap_or(false),
                    }),
                    Some("Created from persisted growth telemetry."),
                    clamp_priority(observation.priority.max(0.66)),
                    None,
                    growth_review_dedupe_key(observation),
                    REVIEW_GROWTH_TASK_COOLDOWN_SECS,
                )?;
            }
        }
        "self_model_gap" => {
            let goal_id = db.upsert_autonomy_goal(
                GOAL_SELF_MODEL_ALIGNMENT,
                &observation.source,
                Some(
                    "Review mismatches between live runtime self-model snapshots and recent growth telemetry.",
                ),
                clamp_priority(observation.priority.max(0.74)),
            )?;

            let gap_key = self_model_gap_task_key(observation);

            if let Some(args) = reconcile_self_model_args(observation) {
                maybe_create_autonomy_task(
                    db,
                    Some(goal_id),
                    "reconcile_self_model",
                    &task_title("Reconcile self-model", content),
                    None,
                    args,
                    Some("Created from a bounded self-model alignment audit."),
                    clamp_priority(observation.priority.max(0.79)),
                    None,
                    Some(format!("reconcile:{}", gap_key)),
                    SELF_MODEL_RECONCILE_TASK_COOLDOWN_SECS,
                )?;
            }

            maybe_create_autonomy_task(
                db,
                Some(goal_id),
                "review_growth",
                &task_title("Review self-model gap", content),
                Some("growth.review"),
                serde_json::json!({
                    "observation_id": observation.id,
                    "kind": observation.kind,
                    "message": content,
                    "success": false,
                    "growth_event_id": observation.context.get("growth_event_id").and_then(|value| value.as_i64()),
                    "target": observation.context.get("target").and_then(|value| value.as_str()).unwrap_or(""),
                    "snapshot_id": observation.context.get("snapshot_id").and_then(|value| value.as_i64()),
                    "deliver_output": observation.context.get("deliver_output").and_then(|value| value.as_bool()).unwrap_or(false),
                }),
                Some("Created from a persisted self-model alignment audit."),
                clamp_priority(observation.priority.max(0.74)),
                None,
                Some(format!("review:{}", gap_key)),
                REVIEW_GROWTH_TASK_COOLDOWN_SECS,
            )?;
        }
        _ => {}
    }

    db.mark_autonomy_observation_consumed(observation.id)?;
    Ok(())
}

fn maybe_create_autonomy_task(
    db: &Db,
    goal_id: Option<i64>,
    kind: &str,
    title: &str,
    tool: Option<&str>,
    mut args: serde_json::Value,
    notes: Option<&str>,
    priority: f64,
    scheduled_for: Option<&str>,
    dedupe_key: Option<String>,
    cooldown_secs: i64,
) -> Result<Option<i64>> {
    if let Some(dedupe_key) = dedupe_key {
        if let Some(object) = args.as_object_mut() {
            object.insert("dedupe_key".into(), serde_json::json!(dedupe_key.clone()));
        }
        if db.has_recent_autonomy_task_with_dedupe_key(kind, &dedupe_key, cooldown_secs)? {
            return Ok(None);
        }
    }

    Ok(Some(db.create_autonomy_task(
        goal_id,
        kind,
        title,
        tool,
        &args,
        notes,
        priority,
        scheduled_for,
    )?))
}

fn growth_review_dedupe_key(observation: &Observation) -> Option<String> {
    if observation.kind == "self_model_gap" {
        return Some(format!(
            "self_model_gap:{}",
            self_model_gap_task_key(observation)
        ));
    }

    if let Some(growth_event_id) = observation
        .context
        .get("growth_event_id")
        .and_then(|value| value.as_i64())
    {
        return Some(format!("growth_event:{}", growth_event_id));
    }

    let target = observation
        .context
        .get("target")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    Some(format!(
        "{}:{}:{}",
        observation.kind,
        target,
        short_hash(observation.content.as_str())
    ))
}

fn reconcile_self_model_args(observation: &Observation) -> Option<serde_json::Value> {
    let trigger_kind = observation
        .context
        .get("trigger_kind")
        .and_then(|value| value.as_str())?;
    let success = observation
        .context
        .get("success")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let target = observation
        .context
        .get("target")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim();

    if trigger_kind != "tool_growth_result"
        || !success
        || !supports_built_tool_reconcile_target(target)
    {
        return None;
    }

    Some(serde_json::json!({
        "trigger_kind": trigger_kind,
        "message": observation.content.as_str(),
        "success": success,
        "observation_id": observation.id,
        "growth_event_id": observation.context.get("growth_event_id").and_then(|value| value.as_i64()),
        "target": target,
        "capability_name": observation.context.get("capability_name").and_then(|value| value.as_str()).unwrap_or(""),
        "snapshot_id": observation.context.get("snapshot_id").and_then(|value| value.as_i64()),
        "deliver_output": observation.context.get("deliver_output").and_then(|value| value.as_bool()).unwrap_or(false),
    }))
}

fn self_model_gap_task_key(observation: &Observation) -> String {
    let trigger_kind = observation
        .context
        .get("trigger_kind")
        .and_then(|value| value.as_str())
        .unwrap_or("self_model_gap");
    let target = observation
        .context
        .get("target")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim();
    let capability_name = observation
        .context
        .get("capability_name")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .or_else(|| infer_capability_name_from_target(target))
        .unwrap_or_default();

    format!("{}:{}:{}", trigger_kind, target, capability_name)
}

fn should_review_growth_observation(observation: &Observation) -> bool {
    let success = observation
        .context
        .get("success")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let repair_rounds = observation
        .context
        .get("repair_rounds")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let deliver_output = observation
        .context
        .get("deliver_output")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let lower = observation.content.to_lowercase();
    let looks_risky = [
        "failed",
        "failure",
        "repair",
        "retry",
        "quarantine",
        "mismatch",
        "not visible",
        "error",
        "gap",
    ]
    .iter()
    .any(|needle| lower.contains(needle));

    !success || repair_rounds > 0 || deliver_output || looks_risky || observation.priority >= 0.72
}

fn short_hash(text: &str) -> String {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
