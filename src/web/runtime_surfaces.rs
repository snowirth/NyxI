use axum::http::{HeaderMap, StatusCode};
use axum::{
    Json,
    extract::{Query, State},
};
use serde::Deserialize;

use crate::AppState;

use super::{check_auth, internal_error, project_growth_event};

const SYSTEM_HEALTH_SCHEMA_VERSION: &str = "nyx_system_health.v1";
const SYSTEM_HEALTH_RECENT_LIMIT: usize = 5;

#[derive(Debug, Default, Deserialize)]
pub(super) struct GrowthEventQuery {
    limit: Option<usize>,
    kind: Option<String>,
    source: Option<String>,
    target: Option<String>,
    success: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct SystemIncidentQuery {
    limit: Option<usize>,
    kind: Option<String>,
    source: Option<String>,
    severity: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct ReplayFailureClusterQuery {
    limit: Option<usize>,
    task_kind: Option<String>,
    failure_class: Option<String>,
    tool: Option<String>,
}

pub(super) async fn growth_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<GrowthEventQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let kind = query
        .kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let source = query
        .source
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let target = query
        .target
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let success = query.success;

    let total_count = state
        .db
        .count_growth_events_filtered(kind, source, target, success)
        .map_err(internal_error)?;
    let events = state
        .db
        .list_recent_growth_events_filtered(kind, source, target, success, limit)
        .map_err(internal_error)?
        .into_iter()
        .map(project_growth_event)
        .collect::<Vec<_>>();

    Ok(Json(serde_json::json!({
        "filters": {
            "kind": kind,
            "source": source,
            "target": target,
            "success": success,
        },
        "total_count": total_count,
        "truncated": total_count > events.len(),
        "events": events,
    })))
}

pub(super) async fn system_health(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let snapshot = state.self_model_snapshot().await;
    let builtin_tools = crate::tools::builtin_tool_runtime_statuses();
    let self_built_tools = crate::forge::list_registered_tool_runtime_statuses(state.db.as_ref())
        .map_err(internal_error)?;
    let plugin_tools = state.plugins.tool_statuses();
    let mcp_tools = state.mcp_hub.tool_statuses().await;

    let builtin_summary = summarize_tool_status_group(&builtin_tools);
    let self_built_summary = summarize_tool_status_group(&self_built_tools);
    let plugin_summary = summarize_tool_status_group(&plugin_tools);
    let mcp_summary = summarize_tool_status_group(&mcp_tools);

    let total_tools =
        builtin_tools.len() + self_built_tools.len() + plugin_tools.len() + mcp_tools.len();
    let ready_tools = builtin_tools.iter().filter(|tool| tool.ready).count()
        + self_built_tools.iter().filter(|tool| tool.ready).count()
        + plugin_tools.iter().filter(|tool| tool.ready).count()
        + mcp_tools.iter().filter(|tool| tool.ready).count();
    let blocked_tools = total_tools.saturating_sub(ready_tools);

    let stale_running_before = (chrono::Utc::now()
        - chrono::Duration::seconds(crate::autonomy::STALE_RUNNING_TASK_TIMEOUT_SECS))
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();

    let ready_task_count = state
        .db
        .count_ready_autonomy_tasks()
        .map_err(internal_error)?;
    let running_task_count = state
        .db
        .count_autonomy_tasks_with_status(crate::autonomy::TaskStatus::Running)
        .map_err(internal_error)?;
    let stale_running_task_count = state
        .db
        .count_stale_running_autonomy_tasks(&stale_running_before)
        .map_err(internal_error)?;
    let quarantine_count = state
        .db
        .count_autonomy_action_runs_filtered(None, Some("quarantined"))
        .map_err(internal_error)?;
    let retry_scheduled_count = state
        .db
        .count_autonomy_action_runs_filtered(None, Some("retry_scheduled"))
        .map_err(internal_error)?;
    let unhealthy_built_tool_count =
        crate::forge::count_unhealthy_built_tools(state.db.as_ref()).map_err(internal_error)?;

    let recent_quarantined = state
        .db
        .list_recent_autonomy_action_runs_filtered(
            None,
            Some("quarantined"),
            SYSTEM_HEALTH_RECENT_LIMIT,
        )
        .map_err(internal_error)?;
    let recent_retry_scheduled = state
        .db
        .list_recent_autonomy_action_runs_filtered(
            None,
            Some("retry_scheduled"),
            SYSTEM_HEALTH_RECENT_LIMIT,
        )
        .map_err(internal_error)?;
    let unhealthy_built_tools =
        crate::forge::list_unhealthy_built_tools(state.db.as_ref(), SYSTEM_HEALTH_RECENT_LIMIT)
            .map_err(internal_error)?;
    let recent_growth_events = state
        .db
        .list_recent_growth_events_filtered(None, None, None, None, SYSTEM_HEALTH_RECENT_LIMIT)
        .map_err(internal_error)?
        .into_iter()
        .map(project_growth_event)
        .collect::<Vec<_>>();

    let mut concerns = Vec::new();
    if !snapshot.runtime.hosted_tool_loop_ready {
        push_system_health_concern(
            &mut concerns,
            "warning",
            "hosted_tool_loop_unready",
            "Primary chat model does not expose the hosted tool loop yet.",
            Some(format!(
                "Current primary route is {}. Direct chat still works, but automatic tool-loop behavior is limited.",
                snapshot.runtime.user_chat_primary
            )),
            1,
        );
    }
    if !snapshot.runtime.autonomous_llm_ready {
        push_system_health_concern(
            &mut concerns,
            "warning",
            "autonomous_llm_unready",
            "No autonomous LLM lane is configured for fallback or background work.",
            Some(
                "Configure Ollama or NIM so autonomy and growth paths can recover cleanly."
                    .to_string(),
            ),
            1,
        );
    }

    append_blocked_tool_concern(
        &mut concerns,
        "builtin_tools_blocked",
        "Built-in tools",
        &builtin_tools,
    );
    append_blocked_tool_concern(
        &mut concerns,
        "self_built_tools_blocked",
        "Self-built tools",
        &self_built_tools,
    );
    append_blocked_tool_concern(
        &mut concerns,
        "plugin_tools_blocked",
        "Plugin tools",
        &plugin_tools,
    );
    append_blocked_tool_concern(&mut concerns, "mcp_tools_blocked", "MCP tools", &mcp_tools);

    if stale_running_task_count > 0 {
        push_system_health_concern(
            &mut concerns,
            "critical",
            "stale_running_tasks",
            format!(
                "{} stale running autonomy task(s) need recovery.",
                stale_running_task_count
            ),
            Some(format!(
                "Tasks older than {}s are considered stale and should be rescheduled or investigated.",
                crate::autonomy::STALE_RUNNING_TASK_TIMEOUT_SECS
            )),
            stale_running_task_count,
        );
    }
    if quarantine_count > 0 {
        push_system_health_concern(
            &mut concerns,
            "warning",
            "quarantined_action_runs",
            format!(
                "{} autonomy action run(s) are quarantined.",
                quarantine_count
            ),
            recent_quarantined
                .first()
                .map(|run| format!("Most recent quarantine: {}", run.summary)),
            quarantine_count,
        );
    }
    if retry_scheduled_count > 0 {
        push_system_health_concern(
            &mut concerns,
            "warning",
            "retry_scheduled_backlog",
            format!(
                "{} autonomy action run(s) are waiting for another retry window.",
                retry_scheduled_count
            ),
            recent_retry_scheduled
                .first()
                .map(|run| format!("Most recent retry scheduling: {}", run.summary)),
            retry_scheduled_count,
        );
    }
    if unhealthy_built_tool_count > 0 {
        let detail = unhealthy_built_tools
            .iter()
            .map(|tool| tool.tool_name.as_str())
            .take(3)
            .collect::<Vec<_>>()
            .join(", ");
        push_system_health_concern(
            &mut concerns,
            "warning",
            "unhealthy_built_tools",
            format!(
                "{} self-built tool(s) currently have recorded failures or quarantine state.",
                unhealthy_built_tool_count
            ),
            if detail.is_empty() {
                None
            } else {
                Some(format!("Examples: {}", detail))
            },
            unhealthy_built_tool_count,
        );
    }

    let status = if concerns.iter().any(|item| item["severity"] == "critical") {
        "attention"
    } else if concerns.is_empty() {
        "ok"
    } else {
        "degraded"
    };

    Ok(Json(serde_json::json!({
        "schema_version": SYSTEM_HEALTH_SCHEMA_VERSION,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "status": status,
        "version": "2.1.0",
        "uptime_s": state.start_time.elapsed().as_secs(),
        "readiness": {
            "llm": {
                "user_chat_primary": snapshot.runtime.user_chat_primary,
                "hosted_tool_loop_ready": snapshot.runtime.hosted_tool_loop_ready,
                "autonomous_llm_ready": snapshot.runtime.autonomous_llm_ready,
                "providers": snapshot.runtime.providers,
            },
            "runtime": {
                "memory_count": snapshot.runtime.memory_count,
                "message_count": snapshot.runtime.message_count,
                "active_goal_count": snapshot.runtime.active_goal_count,
                "recent_growth_event_count": snapshot.runtime.recent_growth_event_count,
                "background_reflection_active": snapshot.runtime.background_reflection_active,
                "background_thought_count": snapshot.runtime.background_thought_count,
            },
            "tools": {
                "builtin": builtin_summary,
                "self_built": self_built_summary,
                "plugin": plugin_summary,
                "mcp": mcp_summary,
                "totals": {
                    "total": total_tools,
                    "ready": ready_tools,
                    "blocked": blocked_tools,
                }
            },
            "autonomy": {
                "ready_tasks": ready_task_count,
                "running_tasks": running_task_count,
                "stale_running_tasks": stale_running_task_count,
                "quarantined_action_runs": quarantine_count,
                "retry_scheduled_action_runs": retry_scheduled_count,
                "unhealthy_built_tools": unhealthy_built_tool_count,
            }
        },
        "concerns": concerns,
        "recent": {
            "quarantined_runs": recent_quarantined,
            "retry_scheduled_runs": recent_retry_scheduled,
            "unhealthy_built_tools": unhealthy_built_tools,
            "growth_events": recent_growth_events,
        }
    })))
}

pub(super) async fn operator_brief(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let brief =
        crate::world::brief::compile_operator_brief(state.db.as_ref(), "web_operator_brief")
            .map_err(internal_error)?;
    Ok(Json(serde_json::to_value(brief).unwrap_or_else(
        |_| serde_json::json!({"error": "failed to serialize operator brief"}),
    )))
}

pub(super) async fn operator_continuity(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let brief = crate::world::continuity::compile_cross_surface_continuity(
        state.db.as_ref(),
        "web_operator_continuity",
    )
    .map_err(internal_error)?;
    Ok(Json(serde_json::to_value(brief).unwrap_or_else(
        |_| serde_json::json!({"error": "failed to serialize operator continuity brief"}),
    )))
}

pub(super) async fn system_incidents(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SystemIncidentQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let kind = query
        .kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let source = query
        .source
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let severity = query
        .severity
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let total_count = state
        .db
        .count_system_incidents_filtered(kind, source, severity)
        .map_err(internal_error)?;
    let incidents = state
        .db
        .list_recent_system_incidents_filtered(kind, source, severity, limit)
        .map_err(internal_error)?;

    Ok(Json(serde_json::json!({
        "filters": {
            "kind": kind,
            "source": source,
            "severity": severity,
        },
        "total_count": total_count,
        "truncated": total_count > incidents.len(),
        "incidents": incidents,
    })))
}

pub(super) async fn replay_failure_clusters(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ReplayFailureClusterQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let task_kind = query
        .task_kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let failure_class = query
        .failure_class
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let tool = query
        .tool
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    state.db.ingest_replay_failure_clusters(500).ok();
    let total_count = state
        .db
        .count_replay_failure_clusters_filtered(task_kind, failure_class, tool)
        .map_err(internal_error)?;
    let failure_clusters = state
        .db
        .list_recent_replay_failure_clusters_filtered(task_kind, failure_class, tool, limit)
        .map_err(internal_error)?;

    Ok(Json(serde_json::json!({
        "filters": {
            "task_kind": task_kind,
            "failure_class": failure_class,
            "tool": tool,
        },
        "total_count": total_count,
        "truncated": total_count > failure_clusters.len(),
        "failure_clusters": failure_clusters,
    })))
}

fn summarize_tool_status_group(statuses: &[crate::tools::ToolRuntimeStatus]) -> serde_json::Value {
    let ready_count = statuses.iter().filter(|tool| tool.ready).count();
    let blocked_count = statuses.len().saturating_sub(ready_count);
    let blocked_items = statuses
        .iter()
        .filter(|tool| !tool.ready)
        .take(SYSTEM_HEALTH_RECENT_LIMIT)
        .map(project_tool_status)
        .collect::<Vec<_>>();

    serde_json::json!({
        "total": statuses.len(),
        "ready": ready_count,
        "blocked": blocked_count,
        "blocked_items": blocked_items,
        "truncated_blocked_items": blocked_count > blocked_items.len(),
    })
}

fn project_tool_status(status: &crate::tools::ToolRuntimeStatus) -> serde_json::Value {
    serde_json::json!({
        "kind": status.kind,
        "name": status.name,
        "status": status.status,
        "issue": status.issue,
        "requires_network": status.requires_network,
        "sandboxed": status.sandboxed,
        "filename": status.filename,
        "command": status.command,
        "server_name": status.server_name,
        "source": status.source,
        "quarantined_until": status.quarantined_until,
    })
}

fn append_blocked_tool_concern(
    concerns: &mut Vec<serde_json::Value>,
    code: &str,
    label: &str,
    statuses: &[crate::tools::ToolRuntimeStatus],
) {
    let blocked = statuses
        .iter()
        .filter(|tool| !tool.ready)
        .collect::<Vec<_>>();
    if blocked.is_empty() {
        return;
    }

    let detail = blocked
        .iter()
        .take(3)
        .map(|tool| {
            let issue = tool.issue.as_deref().unwrap_or("blocked");
            format!("{} ({})", tool.name, issue)
        })
        .collect::<Vec<_>>()
        .join("; ");

    push_system_health_concern(
        concerns,
        "warning",
        code,
        format!("{} have {} blocked item(s).", label, blocked.len()),
        if detail.is_empty() {
            None
        } else {
            Some(detail)
        },
        blocked.len(),
    );
}

fn push_system_health_concern(
    concerns: &mut Vec<serde_json::Value>,
    severity: &str,
    code: &str,
    summary: impl Into<String>,
    detail: Option<String>,
    count: usize,
) {
    concerns.push(serde_json::json!({
        "severity": severity,
        "code": code,
        "summary": summary.into(),
        "detail": detail,
        "count": count,
    }));
}
