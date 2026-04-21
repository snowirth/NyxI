use std::hash::{Hash, Hasher};

use super::{SELF_MODEL_GAP_COOLDOWN_SECS, SelfModelSnapshot};

#[derive(Debug)]
pub(super) struct DetectedSelfModelGap {
    pub content: String,
    pub target: Option<String>,
    pub priority: f64,
    pub context: serde_json::Value,
}

pub(super) fn detect_self_model_gaps(
    trigger_kind: &str,
    trigger_target: Option<&str>,
    summary: &str,
    success: bool,
    growth_event_id: Option<i64>,
    snapshot: &SelfModelSnapshot,
    snapshot_id: i64,
) -> Vec<DetectedSelfModelGap> {
    let mut gaps = Vec::new();

    if trigger_kind == "tool_growth_result" {
        if let Some(target) = trigger_target {
            if let Some(capability_name) = infer_capability_name_from_target(target) {
                let visible = snapshot.has_capability_named(&capability_name);
                if success && !visible {
                    gaps.push(DetectedSelfModelGap {
                        content: format!(
                            "self-model gap: tool growth reported success for {} but capability {} is not visible in the live self-model yet. {}",
                            target,
                            capability_name,
                            crate::trunc(summary, 160)
                        ),
                        target: Some(target.to_string()),
                        priority: 0.82,
                        context: serde_json::json!({
                            "trigger_kind": trigger_kind,
                            "target": target,
                            "success": success,
                            "growth_event_id": growth_event_id,
                            "capability_name": capability_name,
                            "snapshot_id": snapshot_id,
                        }),
                    });
                } else if !success && visible {
                    gaps.push(DetectedSelfModelGap {
                        content: format!(
                            "self-model gap: tool growth reported failure for {} but capability {} is already visible in the live self-model. {}",
                            target,
                            capability_name,
                            crate::trunc(summary, 160)
                        ),
                        target: Some(target.to_string()),
                        priority: 0.78,
                        context: serde_json::json!({
                            "trigger_kind": trigger_kind,
                            "target": target,
                            "success": success,
                            "growth_event_id": growth_event_id,
                            "capability_name": capability_name,
                            "snapshot_id": snapshot_id,
                        }),
                    });
                }
            }
        }
    }

    if trigger_kind == "self_edit_result" {
        if let Some(target) = trigger_target {
            if is_protected_core_target(target) && !snapshot.has_constraint("protected_core_writes")
            {
                gaps.push(DetectedSelfModelGap {
                    content: format!(
                        "self-model gap: protected self-edit touched {} but the live self-model is missing the protected_core_writes boundary. {}",
                        target,
                        crate::trunc(summary, 160)
                    ),
                    target: Some(target.to_string()),
                    priority: 0.84,
                    context: serde_json::json!({
                        "trigger_kind": trigger_kind,
                        "target": target,
                        "success": success,
                        "growth_event_id": growth_event_id,
                        "snapshot_id": snapshot_id,
                    }),
                });
            }
        }
    }

    gaps
}

fn infer_capability_name_from_target(target: &str) -> Option<String> {
    let target = target.trim();
    if target.is_empty() {
        return None;
    }

    let file_name = std::path::Path::new(target).file_name()?.to_str()?;
    if let Some(stripped) = file_name.strip_suffix(".nyx_tool.json") {
        return Some(stripped.to_string());
    }

    let stem = std::path::Path::new(target).file_stem()?.to_str()?;
    Some(stem.to_string())
}

fn is_protected_core_target(target: &str) -> bool {
    let trimmed = target.trim();
    trimmed.starts_with("src/")
        || trimmed.starts_with("agents/")
        || matches!(
            trimmed,
            "Cargo.toml" | "Cargo.lock" | "IDENTITY.md" | "SOUL.md"
        )
}

pub(super) fn self_model_gap_signature(
    trigger_kind: &str,
    target: Option<&str>,
    content: &str,
) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    trigger_kind.hash(&mut hasher);
    target.unwrap_or("").hash(&mut hasher);
    content.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub(super) fn should_emit_self_model_gap(db: &crate::db::Db, signature: &str) -> bool {
    let key = format!("self_model_gap:{}", signature);
    let now = chrono::Utc::now().timestamp();
    let last = db
        .get_state(&key)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);

    if now - last < SELF_MODEL_GAP_COOLDOWN_SECS {
        return false;
    }

    db.set_state(&key, &now.to_string());
    true
}
