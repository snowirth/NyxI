use axum::http::{HeaderMap, StatusCode};
use axum::{
    Json,
    extract::{Query, State},
};
use serde::Deserialize;

use crate::AppState;

use super::{check_auth, internal_error};

pub(super) async fn self_model(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;
    let snapshot = state.self_model_snapshot().await;
    Ok(Json(serde_json::to_value(snapshot).unwrap_or_else(
        |_| serde_json::json!({"error": "failed to serialize self-model"}),
    )))
}

pub(super) async fn self_model_history(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;
    let snapshots = state
        .db
        .list_recent_self_model_snapshots(12)
        .unwrap_or_default();
    Ok(Json(serde_json::json!({ "snapshots": snapshots })))
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct SelfModelDiffQuery {
    from_id: Option<i64>,
    to_id: Option<i64>,
}

pub(super) async fn self_model_diff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SelfModelDiffQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let (mode, from_ref, from_snapshot, to_ref, to_snapshot) = match (query.from_id, query.to_id) {
        (Some(from_id), Some(to_id)) => {
            let from_record = state
                .db
                .get_self_model_snapshot(from_id)
                .map_err(internal_error)?
                .ok_or_else(|| {
                    (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({"error": format!("self-model snapshot {} not found", from_id)})),
                    )
                })?;
            let to_record = state
                .db
                .get_self_model_snapshot(to_id)
                .map_err(internal_error)?
                .ok_or_else(|| {
                    (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({"error": format!("self-model snapshot {} not found", to_id)})),
                    )
                })?;
            let from_snapshot =
                crate::runtime::self_model::parse_self_model_snapshot_record(&from_record)
                    .ok_or_else(|| {
                        internal_error(format!("failed to parse self-model snapshot {}", from_id))
                    })?;
            let to_snapshot =
                crate::runtime::self_model::parse_self_model_snapshot_record(&to_record)
                    .ok_or_else(|| {
                        internal_error(format!("failed to parse self-model snapshot {}", to_id))
                    })?;
            (
                "history_pair",
                snapshot_reference_from_record(&from_record),
                from_snapshot,
                snapshot_reference_from_record(&to_record),
                to_snapshot,
            )
        }
        _ => {
            let snapshots = state
                .db
                .list_recent_self_model_snapshots(2)
                .map_err(internal_error)?;
            if snapshots.len() >= 2 {
                let newer = &snapshots[0];
                let older = &snapshots[1];
                let older_snapshot =
                    crate::runtime::self_model::parse_self_model_snapshot_record(older)
                        .ok_or_else(|| {
                            internal_error("failed to parse older self-model snapshot")
                        })?;
                let newer_snapshot =
                    crate::runtime::self_model::parse_self_model_snapshot_record(newer)
                        .ok_or_else(|| {
                            internal_error("failed to parse newer self-model snapshot")
                        })?;
                (
                    "history_pair",
                    snapshot_reference_from_record(older),
                    older_snapshot,
                    snapshot_reference_from_record(newer),
                    newer_snapshot,
                )
            } else if snapshots.len() == 1 {
                let stored = &snapshots[0];
                let stored_snapshot =
                    crate::runtime::self_model::parse_self_model_snapshot_record(stored)
                        .ok_or_else(|| {
                            internal_error("failed to parse stored self-model snapshot")
                        })?;
                let live_snapshot = state.self_model_snapshot().await;
                (
                    "history_to_live",
                    snapshot_reference_from_record(stored),
                    stored_snapshot,
                    snapshot_reference_from_live(&live_snapshot),
                    live_snapshot,
                )
            } else {
                return Ok(Json(serde_json::json!({
                    "available": false,
                    "reason": "No persisted self-model snapshots are available yet.",
                })));
            }
        }
    };

    let diff = crate::runtime::self_model::diff_snapshots(&from_snapshot, &to_snapshot);
    Ok(Json(serde_json::json!({
        "available": true,
        "mode": mode,
        "from": from_ref,
        "to": to_ref,
        "has_changes": diff.has_changes(),
        "summary": diff.summary,
        "changes": diff,
    })))
}

fn snapshot_reference_from_record(record: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "kind": "history",
        "id": record.get("id").and_then(|value| value.as_i64()),
        "generated_at": record.pointer("/snapshot/generated_at").and_then(|value| value.as_str()),
        "recorded_at": record.get("created_at").and_then(|value| value.as_str()),
        "source": record.get("source").and_then(|value| value.as_str()),
        "trigger_kind": record.get("trigger_kind").and_then(|value| value.as_str()),
        "trigger_target": record.get("trigger_target").and_then(|value| value.as_str()),
        "summary": record.get("summary").and_then(|value| value.as_str()),
    })
}

fn snapshot_reference_from_live(
    snapshot: &crate::runtime::self_model::SelfModelSnapshot,
) -> serde_json::Value {
    serde_json::json!({
        "kind": "live",
        "id": serde_json::Value::Null,
        "generated_at": snapshot.generated_at.clone(),
        "recorded_at": serde_json::Value::Null,
        "source": "runtime",
        "trigger_kind": "live_snapshot",
        "trigger_target": serde_json::Value::Null,
        "summary": "Current live self-model snapshot",
    })
}
