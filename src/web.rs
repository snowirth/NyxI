//! Web server — dashboard + API.
//! Fix #4: API token auth on mutating endpoints.

use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
};
use serde::Deserialize;
use tower_http::cors::{Any, CorsLayer};

use crate::{AppState, db};

mod runtime_surfaces;
mod self_model_api;

use runtime_surfaces::{
    growth_events, operator_brief, operator_continuity, replay_failure_clusters, system_health,
    system_incidents,
};
use self_model_api::{self_model, self_model_diff, self_model_history};
const MEMORY_OVERVIEW_SCHEMA_VERSION: &str = "nyx_memory_overview.v3";
const POLICY_OVERVIEW_SCHEMA_VERSION: &str = "nyx_policy_overview.v1";

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/api/chat", post(chat))
        .route("/api/chat/traces", get(chat_traces))
        .route("/api/execution/ledger", get(execution_ledger))
        .route("/api/memory/working-set", get(memory_working_set))
        .route("/api/memory/provenance", get(memory_provenance))
        .route("/api/memory/overview", get(memory_overview))
        .route("/api/policy/overview", get(policy_overview))
        .route("/api/operator/brief", get(operator_brief))
        .route("/api/operator/continuity", get(operator_continuity))
        .route("/api/world/projects", get(world_projects))
        .route("/api/growth/events", get(growth_events))
        .route("/api/forge/build", post(forge_build))
        .route("/api/forge/evolve", post(forge_evolve))
        .route("/api/self-model", get(self_model))
        .route("/api/self-model/history", get(self_model_history))
        .route("/api/self-model/diff", get(self_model_diff))
        .route("/api/tools/overview", get(tools_overview))
        .route("/api/tools/dispatch", post(tools_dispatch))
        .route("/api/autonomy/overview", get(autonomy_overview))
        .route("/api/autonomy/action-runs", get(autonomy_action_runs))
        .route("/api/autonomy/tasks/{id}/trace", get(autonomy_task_trace))
        .route(
            "/api/autonomy/action-runs/{id}/trace",
            get(autonomy_action_run_trace),
        )
        .route("/api/replay/failure-clusters", get(replay_failure_clusters))
        .route("/api/system/health", get(system_health))
        .route("/api/system/incidents", get(system_incidents))
        .route("/api/stats", get(stats))
        .route("/api/proactive", get(proactive))
        .route("/api/history", get(history))
        .route("/api/voice/say", post(voice_say))
        .route("/api/voice/listen", post(voice_listen))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}

async fn index() -> axum::response::Html<String> {
    let html = std::fs::read_to_string("static/index.html")
        .unwrap_or_else(|_| "<h1>Nyx V2</h1><p>static/index.html not found</p>".into());
    axum::response::Html(html)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": "2.1.0",
        "uptime_s": state.start_time.elapsed().as_secs(),
        "memories": state.db.memory_count(),
    }))
}

/// Auth check — if api_token is set, require it in Authorization header
fn check_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    if state.config.api_token.is_empty() {
        return Ok(()); // No token configured = open (local-only use)
    }
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = auth.strip_prefix("Bearer ").unwrap_or(auth);
    if token == state.config.api_token {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        ))
    }
}

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, Json<serde_json::Value>) {
    tracing::error!("web api error: {}", error);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": error.to_string() })),
    )
}

async fn chat(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let text = body["message"].as_str().unwrap_or("");
    let sender = body["user"].as_str().unwrap_or("web");

    if text.is_empty() {
        return Ok(Json(serde_json::json!({"error": "empty message"})));
    }

    let response = state.handle("web", sender, text).await;
    Ok(Json(serde_json::json!({"response": response})))
}

async fn stats(State(state): State<AppState>) -> Json<serde_json::Value> {
    let (input_tok, output_tok) = state.llm.usage();
    let cost = (input_tok as f64 * 1.0 / 1_000_000.0) + (output_tok as f64 * 5.0 / 1_000_000.0);

    Json(serde_json::json!({
        "uptime_s": state.start_time.elapsed().as_secs(),
        "memories": state.db.memory_count(),
        "messages": state.db.message_count(),
        "tokens": {"input": input_tok, "output": output_tok},
        "cost_usd": format!("{:.4}", cost),
        "version": "2.1.0",
    }))
}

#[derive(Debug, Default, Deserialize)]
struct ChatTraceQuery {
    limit: Option<usize>,
    channel: Option<String>,
    route: Option<String>,
    outcome: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ExecutionLedgerQuery {
    limit: Option<usize>,
    surface: Option<String>,
    kind: Option<String>,
    outcome: Option<String>,
    source: Option<String>,
    reference_kind: Option<String>,
    reference_id: Option<i64>,
    correlation_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct MemoryWorkingSetQuery {
    query: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct MemoryProvenanceQuery {
    query: Option<String>,
    channel: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct MemoryOverviewQuery {
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct PolicyOverviewQuery {
    limit: Option<usize>,
}

async fn chat_traces(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ChatTraceQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let channel = query
        .channel
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let route = query
        .route
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let outcome = query
        .outcome
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let total_count = state
        .db
        .count_chat_traces_filtered(channel, route, outcome)
        .map_err(internal_error)?;
    let traces = state
        .db
        .list_recent_chat_traces_filtered(channel, route, outcome, limit)
        .map_err(internal_error)?;

    Ok(Json(serde_json::json!({
        "total_count": total_count,
        "truncated": total_count > traces.len(),
        "filters": {
            "channel": channel,
            "route": route,
            "outcome": outcome,
        },
        "traces": traces,
    })))
}

async fn execution_ledger(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ExecutionLedgerQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let surface = query
        .surface
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let kind = query
        .kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let outcome = query
        .outcome
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let source = query
        .source
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let reference_kind = query
        .reference_kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let correlation_id = query
        .correlation_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let total_count = state
        .db
        .count_execution_ledger_filtered(
            surface,
            kind,
            outcome,
            source,
            reference_kind,
            query.reference_id,
            correlation_id,
        )
        .map_err(internal_error)?;
    let entries = state
        .db
        .list_recent_execution_ledger_filtered(
            surface,
            kind,
            outcome,
            source,
            reference_kind,
            query.reference_id,
            correlation_id,
            limit,
        )
        .map_err(internal_error)?;

    Ok(Json(serde_json::json!({
        "filters": {
            "surface": surface,
            "kind": kind,
            "outcome": outcome,
            "source": source,
            "reference_kind": reference_kind,
            "reference_id": query.reference_id,
            "correlation_id": correlation_id,
        },
        "total_count": total_count,
        "truncated": total_count > entries.len(),
        "entries": entries,
    })))
}

async fn memory_working_set(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MemoryWorkingSetQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let Some(memory_query) = query
        .query
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "query is required"})),
        ));
    };
    let limit = query.limit.unwrap_or(8).clamp(1, 20);
    let working_set = state
        .memory_working_set_for_query(memory_query, limit)
        .await;
    Ok(Json(serde_json::to_value(working_set).unwrap_or_else(
        |_| serde_json::json!({"error": "failed to serialize memory working set"}),
    )))
}

async fn memory_provenance(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MemoryProvenanceQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let channel = query
        .channel
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("web");
    let requested_query = query.query.as_deref().unwrap_or("");
    let limit = query.limit.unwrap_or(4).clamp(1, 6);
    let brief = state
        .memory_provenance_brief(channel, requested_query, limit, "web_memory_provenance")
        .await;
    Ok(Json(serde_json::to_value(brief).unwrap_or_else(
        |_| serde_json::json!({"error": "failed to serialize memory provenance brief"}),
    )))
}

async fn memory_overview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MemoryOverviewQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let limit = query.limit.unwrap_or(5).clamp(1, 25);
    let recent_claims = state
        .db
        .list_recent_memory_claims(limit)
        .map_err(internal_error)?;
    let recent_procedures = state
        .db
        .list_recent_memory_procedures(limit)
        .map_err(internal_error)?;
    let recent_sources = state
        .db
        .list_recent_memory_sources(limit)
        .map_err(internal_error)?;
    let recent_capsules = state
        .db
        .list_recent_memory_capsules(limit)
        .map_err(internal_error)?;
    let recent_refresh_jobs = state
        .db
        .list_recent_memory_refresh_jobs(limit)
        .map_err(internal_error)?;
    let due_refresh_jobs = state
        .db
        .count_due_memory_refresh_jobs()
        .map_err(internal_error)?;

    Ok(Json(serde_json::json!({
        "schema_version": MEMORY_OVERVIEW_SCHEMA_VERSION,
        "counts": {
            "legacy_memories": state.db.memory_count(),
            "typed_episodes": state.db.memory_episode_count(),
            "typed_claims": state.db.memory_claim_count(),
            "typed_procedures": state.db.memory_procedure_count(),
            "typed_sources": state.db.memory_source_count(),
            "typed_entities": state.db.memory_entity_count(),
            "session_capsules": state.db.memory_session_capsule_count(),
            "refresh_jobs": state.db.memory_refresh_job_count(),
            "due_refresh_jobs": due_refresh_jobs,
            "stale_claims": state.db.stale_memory_claim_count(),
        },
        "explanation": build_memory_overview_explanation(
            &recent_claims,
            &recent_procedures,
            &recent_sources,
            &recent_capsules,
            &recent_refresh_jobs,
            due_refresh_jobs,
        ),
        "recent_claims": recent_claims,
        "recent_procedures": recent_procedures,
        "recent_sources": recent_sources,
        "recent_capsules": recent_capsules,
        "recent_refresh_jobs": recent_refresh_jobs,
    })))
}

async fn policy_overview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PolicyOverviewQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let limit = query.limit.unwrap_or(5).clamp(1, 25);
    let recent_candidates = state
        .db
        .list_recent_policy_candidates(limit)
        .map_err(internal_error)?;
    let approved_candidates = state
        .db
        .list_approved_policy_candidates(limit)
        .map_err(internal_error)?;
    let recent_evaluations = state
        .db
        .list_recent_policy_evaluations(None, limit)
        .map_err(internal_error)?;
    let recent_events = state
        .db
        .list_recent_policy_change_events(None, limit * 2)
        .map_err(internal_error)?;

    Ok(Json(serde_json::json!({
        "schema_version": POLICY_OVERVIEW_SCHEMA_VERSION,
        "counts": {
            "candidates": state.db.policy_candidate_count(),
            "approved_candidates": approved_candidates.len(),
            "evaluations": state.db.policy_evaluation_count(),
            "change_events": state.db.policy_change_event_count(),
        },
        "approved_candidates": approved_candidates,
        "recent_candidates": recent_candidates,
        "recent_evaluations": recent_evaluations,
        "recent_change_events": recent_events,
    })))
}

async fn world_projects(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let (snapshot, used_persisted_snapshot, compile_error) =
        match crate::world::state::compile_and_persist_project_graph(
            state.db.as_ref(),
            "web_world_projects",
        ) {
            Ok(snapshot) => (snapshot, false, None),
            Err(error) => {
                let Some(snapshot) = crate::world::state::load_project_graph(state.db.as_ref())
                else {
                    return Err(internal_error(error));
                };
                (snapshot, true, Some(error.to_string()))
            }
        };
    let changes = crate::world::state::load_project_graph_changes(state.db.as_ref());

    let mut payload = serde_json::to_value(&snapshot).map_err(internal_error)?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "state_key".to_string(),
            serde_json::json!(crate::world::state::PROJECT_GRAPH_STATE_KEY),
        );
        object.insert(
            "changes_state_key".to_string(),
            serde_json::json!(crate::world::state::PROJECT_GRAPH_CHANGES_STATE_KEY),
        );
        object.insert(
            "used_persisted_snapshot".to_string(),
            serde_json::json!(used_persisted_snapshot),
        );
        object.insert(
            "changes".to_string(),
            serde_json::to_value(changes).map_err(internal_error)?,
        );
        if let Some(error) = compile_error {
            object.insert("compile_error".to_string(), serde_json::json!(error));
        }
    }

    Ok(Json(payload))
}

fn project_growth_event(event: crate::autonomy::GrowthEvent) -> serde_json::Value {
    let execution = event.details.get("execution").cloned();
    let llm = event.details.pointer("/execution/llm").cloned();
    let request = event.details.get("request").cloned();
    let auto_run_preview = event.details.get("auto_run_preview").cloned();
    let registered_tool_inspection = event.details.get("registered_tool_inspection").cloned();
    let execution_delta = execution_delta_from_root(&event.details);

    serde_json::json!({
        "id": event.id,
        "kind": event.kind,
        "source": event.source,
        "target": event.target,
        "summary": event.summary,
        "success": event.success,
        "created_at": event.created_at,
        "request": request,
        "execution": execution,
        "execution_delta": execution_delta,
        "llm": llm,
        "auto_run_preview": auto_run_preview,
        "registered_tool_inspection": registered_tool_inspection,
        "details": event.details,
    })
}

fn execution_delta_from_root(root: &serde_json::Value) -> Option<serde_json::Value> {
    let execution = root.get("execution")?;
    let llm = execution.get("llm");
    let predicted_intent = llm
        .and_then(|value| value.get("predicted_intent_model"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string);
    let predicted_implementation = llm
        .and_then(|value| value.get("predicted_implementation_model"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string);
    let actual_calls = llm
        .and_then(|value| value.get("actual_calls"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let actual_models = actual_calls
        .iter()
        .filter_map(call_model_label)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let used_fallback = actual_calls.iter().any(|call| {
        call.get("route")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .contains("fallback")
    });
    let repair_rounds = root
        .get("repair_rounds")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);

    let mut items = Vec::new();
    if let Some(predicted) = predicted_intent.as_deref() {
        match actual_call_for_stage(&actual_calls, &["intent", "guardrail"]) {
            Some(call) => {
                if let Some(actual) = call_model_label(call) {
                    if actual != predicted {
                        items.push(format!(
                            "intent lane predicted {}, actual call used {}",
                            predicted, actual
                        ));
                    }
                }
            }
            None => items.push(format!(
                "intent lane predicted {}, but no explicit intent-stage call was recorded",
                predicted
            )),
        }
    }
    if let Some(predicted) = predicted_implementation.as_deref() {
        match actual_call_for_stage(&actual_calls, &["implementation", "generation", "code"]) {
            Some(call) => {
                if let Some(actual) = call_model_label(call) {
                    if actual != predicted {
                        items.push(format!(
                            "implementation lane predicted {}, actual call used {}",
                            predicted, actual
                        ));
                    }
                }
            }
            None => items.push(format!(
                "implementation lane predicted {}, but no implementation-stage call was recorded",
                predicted
            )),
        }
    }
    if used_fallback {
        items.push("at least one execution stage used a fallback route".to_string());
    }
    if repair_rounds > 0 {
        items.push(format!(
            "execution needed {} repair round(s) before completion",
            repair_rounds
        ));
    }
    if items.is_empty()
        && actual_models.is_empty()
        && predicted_intent.is_none()
        && predicted_implementation.is_none()
    {
        return None;
    }

    let summary = if items.is_empty() {
        if actual_models.is_empty() {
            "execution trace recorded without model drift details".to_string()
        } else {
            format!("Execution used {}.", actual_models.join(", "))
        }
    } else {
        items.iter().take(2).cloned().collect::<Vec<_>>().join("; ")
    };

    Some(serde_json::json!({
        "summary": summary,
        "used_fallback": used_fallback,
        "repair_rounds": repair_rounds,
        "predicted": {
            "intent_model": predicted_intent,
            "implementation_model": predicted_implementation,
        },
        "actual_models": actual_models,
        "items": items,
    }))
}

fn actual_call_for_stage<'a>(
    calls: &'a [serde_json::Value],
    stage_keywords: &[&str],
) -> Option<&'a serde_json::Value> {
    calls
        .iter()
        .find(|call| {
            let stage = call
                .get("stage")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            stage_keywords.iter().any(|keyword| stage.contains(keyword))
        })
        .or_else(|| calls.last())
}

fn call_model_label(call: &serde_json::Value) -> Option<String> {
    let provider = call
        .get("provider")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim();
    let model = call
        .get("model")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim();
    match (provider.is_empty(), model.is_empty()) {
        (true, true) => None,
        (false, true) => Some(provider.to_string()),
        (true, false) => Some(model.to_string()),
        (false, false) => Some(format!("{}:{}", provider, model)),
    }
}

fn build_memory_overview_explanation(
    claims: &[db::MemoryClaimRecord],
    procedures: &[db::MemoryProcedureRecord],
    sources: &[db::MemorySourceRecord],
    capsules: &[db::MemorySessionCapsuleRecord],
    refresh_jobs: &[db::MemoryRefreshJobRecord],
    due_refresh_jobs: usize,
) -> serde_json::Value {
    let stale_claims = claims.iter().filter(|claim| claim.is_stale).count();
    let sourced_claims = claims
        .iter()
        .filter(|claim| claim.source_id.is_some())
        .count();
    let mut recent_items = Vec::new();

    for claim in claims.iter().take(2) {
        let mut reasons = vec!["recently updated typed claim".to_string()];
        if claim.is_stale {
            reasons.push("still visible because it needs refresh review".to_string());
        }
        if claim.source_id.is_some() {
            reasons.push("backed by a supporting source".to_string());
        }
        if claim.scope == "personal" || claim.kind == "preference" {
            reasons.push("also influences personal or preference state".to_string());
        }
        recent_items.push(serde_json::json!({
            "kind": "claim",
            "label": crate::trunc(&claim.statement, 120),
            "reason": reasons.join("; "),
        }));
    }

    for procedure in procedures.iter().take(2) {
        recent_items.push(serde_json::json!({
            "kind": "procedure",
            "label": crate::trunc(&procedure.content, 120),
            "reason": "recently updated reusable action guidance".to_string(),
        }));
    }

    for source in sources.iter().take(1) {
        let mut reasons = vec!["recently observed supporting source".to_string()];
        reasons.push(format!("trust tier {:.2}", source.trust_tier));
        if let Some(publisher) = source
            .publisher
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            reasons.push(format!("publisher {}", publisher));
        }
        recent_items.push(serde_json::json!({
            "kind": "source",
            "label": crate::trunc(
                source
                    .title
                    .as_deref()
                    .filter(|title| !title.trim().is_empty())
                    .unwrap_or(&source.url_or_ref),
                120,
            ),
            "reason": reasons.join("; "),
        }));
    }

    for capsule in capsules.iter().take(1) {
        recent_items.push(serde_json::json!({
            "kind": "capsule",
            "label": crate::trunc(&capsule.summary, 120),
            "reason": "recent session capsule kept as a fast recall window".to_string(),
        }));
    }

    for job in refresh_jobs.iter().take(1) {
        let reason = if job.status == "pending" {
            "queued to refresh stale or aging sourced memory"
        } else if job.status == "running" {
            "actively refreshing sourced memory"
        } else {
            "records the latest refresh attempt for sourced memory"
        };
        recent_items.push(serde_json::json!({
            "kind": "refresh_job",
            "label": crate::trunc(&job.refresh_query, 120),
            "reason": reason,
        }));
    }

    serde_json::json!({
        "summary": format!(
            "Overview includes {} claims ({} stale, {} sourced), {} procedures, {} capsules, and {} due refresh jobs.",
            claims.len(),
            stale_claims,
            sourced_claims,
            procedures.len(),
            capsules.len(),
            due_refresh_jobs,
        ),
        "recent_items": recent_items,
    })
}

async fn tools_overview(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let builtin_tools = crate::tools::builtin_tool_runtime_statuses();
    let self_built_tools = crate::forge::list_registered_tool_runtime_statuses(state.db.as_ref())
        .map_err(internal_error)?;
    let plugin_tools = state.plugins.tool_statuses();
    let mcp_tools = state.mcp_hub.tool_statuses().await;

    let builtin_ready = builtin_tools.iter().filter(|tool| tool.ready).count();
    let self_built_ready = self_built_tools.iter().filter(|tool| tool.ready).count();
    let plugin_ready = plugin_tools.iter().filter(|tool| tool.ready).count();
    let mcp_ready = mcp_tools.iter().filter(|tool| tool.ready).count();

    let total_tools =
        builtin_tools.len() + self_built_tools.len() + plugin_tools.len() + mcp_tools.len();
    let total_ready = builtin_ready + self_built_ready + plugin_ready + mcp_ready;

    Ok(Json(serde_json::json!({
        "counts": {
            "builtin_tools": builtin_tools.len(),
            "builtin_ready": builtin_ready,
            "self_built_tools": self_built_tools.len(),
            "self_built_ready": self_built_ready,
            "plugin_tools": plugin_tools.len(),
            "plugin_ready": plugin_ready,
            "mcp_tools": mcp_tools.len(),
            "mcp_ready": mcp_ready,
            "total_tools": total_tools,
            "total_ready": total_ready,
            "total_blocked": total_tools.saturating_sub(total_ready),
        },
        "builtin_tools": builtin_tools,
        "self_built_tools": self_built_tools,
        "plugin_tools": plugin_tools,
        "mcp_tools": mcp_tools,
    })))
}

#[derive(Debug, Default, Deserialize)]
struct ToolDispatchRequest {
    name: String,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    dry_run: bool,
}

async fn tools_dispatch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ToolDispatchRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let name = body.name.trim();
    if name.is_empty() {
        return Ok(Json(serde_json::json!({ "error": "tool name required" })));
    }

    let input = if body.input.is_object() {
        body.input
    } else {
        serde_json::json!({})
    };
    let result = state
        .dispatch_chat_tool_action(name, input, body.dry_run)
        .await;
    Ok(Json(serde_json::to_value(result).unwrap_or_else(
        |_| serde_json::json!({"error": "failed to serialize tool dispatch result"}),
    )))
}

#[derive(Debug, Default, Deserialize)]
struct ForgeEvolveRequest {
    message: String,
    #[serde(default)]
    dry_run: bool,
}

async fn forge_evolve(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ForgeEvolveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let message = body.message.trim();
    if message.is_empty() {
        return Ok(Json(serde_json::json!({ "error": "message required" })));
    }

    let result = crate::forge::dispatch_evolve_action(&state.llm, message, body.dry_run).await;
    Ok(Json(serde_json::to_value(result).unwrap_or_else(
        |_| serde_json::json!({"error": "failed to serialize evolve dispatch result"}),
    )))
}

#[derive(Debug, Default, Deserialize)]
struct ForgeBuildRequest {
    message: String,
    #[serde(default)]
    nyx_response: String,
    #[serde(default)]
    dry_run: bool,
}

async fn forge_build(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ForgeBuildRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let message = body.message.trim();
    if message.is_empty() {
        return Ok(Json(serde_json::json!({ "error": "message required" })));
    }

    let result = crate::forge::dispatch_build_tool_action(
        &state.llm,
        message,
        body.nyx_response.trim(),
        body.dry_run,
    )
    .await;
    Ok(Json(serde_json::to_value(result).unwrap_or_else(
        |_| serde_json::json!({"error": "failed to serialize build dispatch result"}),
    )))
}

#[derive(Debug, Default, Deserialize)]
struct ActionRunQuery {
    limit: Option<usize>,
    kind: Option<String>,
    outcome: Option<String>,
}

async fn autonomy_overview(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let ready_limit = 12usize;
    let running_limit = 12usize;
    let stale_running_limit = 8usize;
    let recent_action_limit = 12usize;
    let reconcile_limit = 8usize;
    let stale_recovery_limit = 8usize;
    let unhealthy_tool_limit = 8usize;
    let growth_event_limit = 8usize;
    let stale_running_before = (chrono::Utc::now()
        - chrono::Duration::seconds(crate::autonomy::STALE_RUNNING_TASK_TIMEOUT_SECS))
    .format("%Y-%m-%d %H:%M:%S")
    .to_string();

    let active_goals = state
        .db
        .list_active_autonomy_goals()
        .map_err(internal_error)?;
    let ready_tasks = state
        .db
        .try_list_ready_autonomy_tasks(ready_limit)
        .map_err(internal_error)?;
    let running_tasks = state
        .db
        .list_autonomy_tasks_with_status(crate::autonomy::TaskStatus::Running, running_limit)
        .map_err(internal_error)?;
    let stale_running_tasks = state
        .db
        .list_stale_running_autonomy_tasks(&stale_running_before, stale_running_limit)
        .map_err(internal_error)?;
    let recent_action_runs = state
        .db
        .list_recent_autonomy_action_runs(recent_action_limit)
        .map_err(internal_error)?;
    let recent_reconciliations = state
        .db
        .list_recent_autonomy_action_runs_for_task_kind("reconcile_self_model", reconcile_limit)
        .map_err(internal_error)?;
    let recent_stale_recoveries = state
        .db
        .list_recent_autonomy_action_runs_with_outcome(
            "recovered_stale_running_task",
            stale_recovery_limit,
        )
        .map_err(internal_error)?;
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
    let action_run_count = state
        .db
        .count_autonomy_action_runs()
        .map_err(internal_error)?;
    let reconcile_count = state
        .db
        .count_autonomy_action_runs_for_task_kind("reconcile_self_model")
        .map_err(internal_error)?;
    let stale_recovery_count = state
        .db
        .count_autonomy_action_runs_with_outcome("recovered_stale_running_task")
        .map_err(internal_error)?;
    let unhealthy_built_tools =
        crate::forge::list_unhealthy_built_tools(state.db.as_ref(), unhealthy_tool_limit)
            .map_err(internal_error)?;
    let unhealthy_built_tool_count =
        crate::forge::count_unhealthy_built_tools(state.db.as_ref()).map_err(internal_error)?;
    let recent_growth_event_count = state
        .db
        .count_growth_events_filtered(None, None, None, None)
        .map_err(internal_error)?;
    let recent_growth_events = state
        .db
        .list_recent_growth_events_filtered(None, None, None, None, growth_event_limit)
        .map_err(internal_error)?
        .into_iter()
        .map(project_growth_event)
        .collect::<Vec<_>>();

    Ok(Json(serde_json::json!({
        "counts": {
            "active_goals": active_goals.len(),
            "ready_tasks": ready_task_count,
            "running_tasks": running_task_count,
            "stale_running_tasks": stale_running_task_count,
            "recent_action_runs": action_run_count,
            "recent_reconciliations": reconcile_count,
            "recent_stale_recoveries": stale_recovery_count,
            "unhealthy_built_tools": unhealthy_built_tool_count,
            "recent_growth_events": recent_growth_event_count,
        },
        "limits": {
            "ready_tasks": ready_limit,
            "running_tasks": running_limit,
            "stale_running_tasks": stale_running_limit,
            "recent_action_runs": recent_action_limit,
            "recent_reconciliations": reconcile_limit,
            "recent_stale_recoveries": stale_recovery_limit,
            "unhealthy_built_tools": unhealthy_tool_limit,
            "recent_growth_events": growth_event_limit,
        },
        "timeouts": {
            "stale_running_task_timeout_secs": crate::autonomy::STALE_RUNNING_TASK_TIMEOUT_SECS,
        },
        "truncated": {
            "ready_tasks": ready_task_count > ready_tasks.len(),
            "running_tasks": running_task_count > running_tasks.len(),
            "stale_running_tasks": stale_running_task_count > stale_running_tasks.len(),
            "recent_action_runs": action_run_count > recent_action_runs.len(),
            "recent_reconciliations": reconcile_count > recent_reconciliations.len(),
            "recent_stale_recoveries": stale_recovery_count > recent_stale_recoveries.len(),
            "unhealthy_built_tools": unhealthy_built_tool_count > unhealthy_built_tools.len(),
            "recent_growth_events": recent_growth_event_count > recent_growth_events.len(),
        },
        "active_goals": active_goals,
        "ready_tasks": ready_tasks,
        "running_tasks": running_tasks,
        "stale_running_tasks": stale_running_tasks,
        "recent_action_runs": recent_action_runs,
        "recent_reconciliations": recent_reconciliations,
        "recent_stale_recoveries": recent_stale_recoveries,
        "unhealthy_built_tools": unhealthy_built_tools,
        "recent_growth_events": recent_growth_events,
    })))
}

async fn autonomy_action_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ActionRunQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let kind = query
        .kind
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let outcome = query
        .outcome
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let total_count = state
        .db
        .count_autonomy_action_runs_filtered(kind, outcome)
        .map_err(internal_error)?;
    let action_runs = state
        .db
        .list_recent_autonomy_action_runs_filtered(kind, outcome, limit)
        .map_err(internal_error)?;

    Ok(Json(serde_json::json!({
        "filters": {
            "kind": kind,
            "outcome": outcome,
        },
        "total_count": total_count,
        "truncated": total_count > action_runs.len(),
        "action_runs": action_runs,
    })))
}

async fn autonomy_task_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let Some(task) = state.db.get_autonomy_task(id).map_err(internal_error)? else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("autonomy task {} not found", id)})),
        ));
    };

    Ok(Json(build_task_trace_payload(&state, &task)?))
}

async fn autonomy_action_run_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;

    let Some(action_run) = state
        .db
        .get_autonomy_action_run_record(id)
        .map_err(internal_error)?
    else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("autonomy action run {} not found", id)})),
        ));
    };

    let task = state
        .db
        .get_autonomy_task(action_run.task_id)
        .map_err(internal_error)?;
    let task_trace = task
        .as_ref()
        .map(|task| build_task_trace_payload(&state, task))
        .transpose()?
        .unwrap_or_else(|| serde_json::json!({}));
    let output_trace = action_run.output.as_ref().and_then(|output| {
        output
            .get("trace")
            .cloned()
            .or_else(|| output.pointer("/execution/trace").cloned())
    });
    let output_execution_delta = action_run
        .output
        .as_ref()
        .and_then(execution_delta_from_root);
    let growth_event_execution_delta = task_trace
        .get("growth_event")
        .and_then(|value| value.get("details"))
        .and_then(execution_delta_from_root);
    let operator_summary =
        build_action_run_trace_operator_summary(&action_run, &task_trace, output_trace.as_ref());

    Ok(Json(serde_json::json!({
        "action_run": action_run,
        "output_trace": output_trace,
        "output_execution_delta": output_execution_delta,
        "growth_event_execution_delta": growth_event_execution_delta,
        "task_trace": task_trace,
        "operator_summary": operator_summary,
    })))
}

fn build_task_trace_payload(
    state: &AppState,
    task: &crate::autonomy::Task,
) -> Result<serde_json::Value, (StatusCode, Json<serde_json::Value>)> {
    let trace = crate::autonomy::task_trace_refs(task);
    let observation = trace
        .observation_id
        .map(|id| state.db.get_autonomy_observation(id))
        .transpose()
        .map_err(internal_error)?
        .flatten();
    let growth_event = trace
        .growth_event_id
        .map(|id| state.db.get_growth_event(id))
        .transpose()
        .map_err(internal_error)?
        .flatten();
    let snapshot = trace
        .snapshot_id
        .map(|id| state.db.get_self_model_snapshot(id))
        .transpose()
        .map_err(internal_error)?
        .flatten();
    let action_runs = state
        .db
        .list_autonomy_action_runs(task.id)
        .map_err(internal_error)?;
    let retry_state = action_runs
        .iter()
        .rev()
        .find_map(|run| run.output.as_ref()?.get("retry_state").cloned())
        .or_else(|| {
            trace
                .retry_key
                .as_deref()
                .map(|key| state.db.get_autonomy_retry_state(key))
                .transpose()
                .ok()
                .flatten()
                .flatten()
                .and_then(|retry_state| serde_json::to_value(retry_state).ok())
        });
    let current_retry_state = trace
        .retry_key
        .as_deref()
        .map(|key| state.db.get_autonomy_retry_state(key))
        .transpose()
        .map_err(internal_error)?
        .flatten();
    let built_tool_health_target = task
        .args
        .get("target")
        .and_then(|value| value.as_str())
        .or_else(|| {
            growth_event
                .as_ref()
                .and_then(|event| event.target.as_deref())
        });
    let built_tool_health = built_tool_health_target
        .map(|target| crate::forge::built_tool_health_for_target(state.db.as_ref(), target))
        .transpose()
        .map_err(internal_error)?
        .flatten();
    let growth_event_execution_delta = growth_event
        .as_ref()
        .and_then(|event| execution_delta_from_root(&event.details));
    let operator_summary = build_task_trace_operator_summary(
        task,
        &trace,
        observation.as_ref(),
        growth_event.as_ref(),
        snapshot.as_ref(),
        &action_runs,
        retry_state.as_ref(),
        current_retry_state.as_ref(),
        built_tool_health.as_ref(),
    );

    Ok(serde_json::json!({
        "task": task,
        "trace": trace,
        "observation": observation,
        "growth_event": growth_event,
        "growth_event_execution_delta": growth_event_execution_delta,
        "snapshot": snapshot,
        "retry_state": retry_state,
        "current_retry_state": current_retry_state,
        "built_tool_health": built_tool_health,
        "action_runs": action_runs,
        "operator_summary": operator_summary,
    }))
}

fn build_task_trace_operator_summary(
    task: &crate::autonomy::Task,
    trace: &crate::autonomy::TaskTraceRefs,
    observation: Option<&crate::autonomy::Observation>,
    growth_event: Option<&crate::autonomy::GrowthEvent>,
    snapshot: Option<&serde_json::Value>,
    action_runs: &[crate::autonomy::ActionRun],
    retry_state: Option<&serde_json::Value>,
    current_retry_state: Option<&crate::autonomy::RetryState>,
    built_tool_health: Option<&crate::forge::BuiltToolHealthState>,
) -> serde_json::Value {
    let headline = format!(
        "Task #{} ({}) is {}: {}.",
        task.id,
        humanize_trace_label(&task.kind),
        humanize_trace_label(task.status.as_str()),
        clean_trace_text(&task.title),
    );

    let mut cause_parts = Vec::new();
    if let Some(observation) = observation {
        cause_parts.push(format!(
            "Triggered by observation #{} ({}) from {}: {}.",
            observation.id,
            humanize_trace_label(&observation.kind),
            clean_trace_text(&observation.source),
            clean_trace_text(&observation.content),
        ));
    }
    if let Some(growth_event) = growth_event {
        let target = growth_event
            .target
            .as_deref()
            .map(clean_trace_text)
            .unwrap_or_else(|| "an unspecified target".to_string());
        cause_parts.push(format!(
            "Linked growth event #{} ({}) {} for {}.",
            growth_event.id,
            humanize_trace_label(&growth_event.kind),
            if growth_event.success {
                "succeeded"
            } else {
                "failed"
            },
            target,
        ));
    } else if let Some(trigger_kind) = trace.trigger_kind.as_deref() {
        cause_parts.push(format!(
            "Trigger kind is {}.",
            humanize_trace_label(trigger_kind),
        ));
    }
    if let Some(target) = trace.target.as_deref() {
        cause_parts.push(format!("Primary target is {}.", clean_trace_text(target)));
    }
    if let Some(capability_name) = trace.capability_name.as_deref() {
        cause_parts.push(format!(
            "Capability focus is {}.",
            clean_trace_text(capability_name),
        ));
    }
    if let Some(review_kind) = trace.review_kind.as_deref() {
        cause_parts.push(format!(
            "Review lane is {}.",
            humanize_trace_label(review_kind),
        ));
    }
    let cause = if cause_parts.is_empty() {
        format!(
            "No upstream observation or growth event is attached beyond the task arguments for {}.",
            clean_trace_text(&task.title),
        )
    } else {
        cause_parts.join(" ")
    };

    let status = if let Some(latest_run) = action_runs.last() {
        format!(
            "Task is {} with {} recorded action run{}; latest outcome was {} at {}. {}",
            humanize_trace_label(task.status.as_str()),
            action_runs.len(),
            plural_suffix(action_runs.len()),
            humanize_trace_label(&latest_run.outcome),
            latest_run.created_at,
            describe_action_run_status_from_values(
                latest_run.executed,
                latest_run.verified,
                latest_run.summary.as_str(),
            ),
        )
    } else {
        format!(
            "Task is {} with no recorded action runs yet.",
            humanize_trace_label(task.status.as_str()),
        )
    };

    let retry = current_retry_state
        .map(describe_retry_state)
        .or_else(|| retry_state.and_then(describe_retry_state_value));
    let quarantine =
        describe_task_quarantine_state(current_retry_state, built_tool_health, action_runs.last());
    let readable_cause =
        summarize_task_cause(task, trace, observation, growth_event, built_tool_health);
    let readable_status = summarize_task_status(task, action_runs);
    let readable_retry = retry
        .as_ref()
        .map(|value| shorten_trace_detail(value))
        .unwrap_or_else(|| "No retry state is currently attached.".to_string());
    let readable_events = summarize_task_events(
        observation,
        growth_event,
        snapshot,
        action_runs.last(),
        built_tool_health,
    );
    let latest_action_signal = action_runs.last().map(|latest_run| {
        build_action_signal_json(
            latest_run.id,
            &latest_run.outcome,
            &latest_run.summary,
            latest_run.executed,
            latest_run.verified,
            latest_run.expected_effect.as_ref(),
            latest_run.verifier_verdict.as_ref(),
            latest_run.rollback_reason.as_ref(),
            action_output_target(
                latest_run.output.as_ref(),
                latest_run.expected_effect.as_ref(),
            ),
            format!("/api/autonomy/action-runs/{}/trace", latest_run.id),
        )
    });
    let growth_signal = growth_event.map(|event| {
        build_growth_signal_json(
            event.id,
            &event.kind,
            event.success,
            event.target.as_deref(),
            &event.summary,
            &format!("/api/autonomy/tasks/{}/trace", task.id),
        )
    });
    let inspection =
        build_task_trace_inspection(task.id, trace, action_runs.last().map(|run| run.id));

    let mut important_events = Vec::new();
    if let Some(observation) = observation {
        push_unique_trace_line(
            &mut important_events,
            format!(
                "Observation #{}: {}.",
                observation.id,
                clean_trace_text(&observation.content),
            ),
        );
    }
    if let Some(growth_event) = growth_event {
        let target = growth_event
            .target
            .as_deref()
            .map(clean_trace_text)
            .unwrap_or_else(|| "an unspecified target".to_string());
        push_unique_trace_line(
            &mut important_events,
            format!(
                "Growth event #{} {} for {}: {}.",
                growth_event.id,
                if growth_event.success {
                    "succeeded"
                } else {
                    "failed"
                },
                target,
                clean_trace_text(&growth_event.summary),
            ),
        );
    }
    if let Some(snapshot) = snapshot {
        let summary = snapshot
            .get("summary")
            .and_then(|value| value.as_str())
            .map(clean_trace_text)
            .unwrap_or_else(|| "self-model snapshot captured".to_string());
        let id = snapshot
            .get("id")
            .and_then(|value| value.as_i64())
            .map(|value| format!("#{}", value))
            .unwrap_or_else(|| "record".to_string());
        push_unique_trace_line(
            &mut important_events,
            format!("Snapshot {}: {}.", id, summary),
        );
    }
    if let Some(built_tool_health) = built_tool_health {
        push_unique_trace_line(
            &mut important_events,
            describe_built_tool_health(built_tool_health),
        );
    }
    if let Some(latest_run) = action_runs.last() {
        push_unique_trace_line(
            &mut important_events,
            format!(
                "Latest action run #{} ended {}: {}.",
                latest_run.id,
                humanize_trace_label(&latest_run.outcome),
                clean_trace_text(&latest_run.summary),
            ),
        );
    }

    serde_json::json!({
        "headline": headline,
        "cause": cause,
        "status": status,
        "retry": retry,
        "quarantine": quarantine,
        "readable_summary": format!(
            "{} {} {}",
            readable_cause,
            readable_status,
            shorten_trace_detail(&readable_retry),
        ),
        "cause_summary": readable_cause,
        "status_summary": readable_status,
        "retry_summary": readable_retry,
        "important_event_summaries": readable_events,
        "important_events": important_events,
        "latest_action_signal": latest_action_signal,
        "growth_signal": growth_signal,
        "inspection": inspection,
    })
}

fn build_action_run_trace_operator_summary(
    action_run: &crate::autonomy::ActionRunRecord,
    task_trace: &serde_json::Value,
    output_trace: Option<&serde_json::Value>,
) -> serde_json::Value {
    let target = action_run_trace_target(action_run, task_trace, output_trace);
    let inspect_path = format!("/api/autonomy/action-runs/{}/trace", action_run.id);
    let verification_signal = build_verification_signal_json(
        action_run.executed,
        action_run.verified,
        action_run.expected_effect.as_ref(),
        action_run.verifier_verdict.as_ref(),
        target.as_deref(),
        &inspect_path,
    );
    let rollback_signal =
        build_rollback_signal_json(action_run.rollback_reason.as_ref(), &inspect_path);
    let growth_signal = build_growth_signal_from_task_trace(task_trace, action_run.task_id);
    let inspection = build_action_run_inspection(action_run, task_trace, target.as_deref());
    let headline = format!(
        "Action run #{} for task #{} ({}) ended {}: {}.",
        action_run.id,
        action_run.task_id,
        humanize_trace_label(&action_run.task_kind),
        humanize_trace_label(&action_run.outcome),
        clean_trace_text(&action_run.summary),
    );
    let cause = task_trace
        .get("operator_summary")
        .and_then(|value| value.get("cause"))
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "This run belongs to task #{}: {}.",
                action_run.task_id,
                clean_trace_text(&action_run.task_title),
            )
        });
    let status = format!(
        "Task status is {}. {}",
        humanize_trace_label(action_run.task_status.as_str()),
        describe_action_run_status_from_values(
            action_run.executed,
            action_run.verified,
            action_run.summary.as_str(),
        ),
    );
    let verification = describe_action_run_verification(action_run);
    let rollback = action_run
        .rollback_reason
        .as_ref()
        .map(describe_rollback_reason);
    let quarantine = describe_action_run_quarantine_state(action_run, task_trace, output_trace);
    let readable_cause = task_trace
        .get("operator_summary")
        .and_then(|value| value.get("cause_summary"))
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("This run belongs to task #{}.", action_run.task_id));
    let readable_status = summarize_action_run_status(action_run);
    let readable_verification = shorten_trace_detail(&verification);
    let readable_rollback = rollback
        .as_ref()
        .map(|value| shorten_trace_detail(value))
        .unwrap_or_else(|| "No rollback was recorded.".to_string());
    let readable_events = summarize_action_run_events(action_run, task_trace, output_trace);

    let mut important_events = Vec::new();
    push_unique_trace_line(
        &mut important_events,
        format!(
            "Recorded at {} with outcome {}.",
            action_run.created_at,
            humanize_trace_label(&action_run.outcome),
        ),
    );
    if let Some(expected_effect) = action_run.expected_effect.as_ref() {
        push_unique_trace_line(
            &mut important_events,
            describe_expected_effect(expected_effect),
        );
    }
    if let Some(verifier_verdict) = action_run.verifier_verdict.as_ref() {
        push_unique_trace_line(
            &mut important_events,
            format!(
                "Verifier marked the run {}: {}.",
                humanize_trace_label(&verifier_verdict.status),
                clean_trace_text(&verifier_verdict.summary),
            ),
        );
    }
    if let Some(rollback_reason) = action_run.rollback_reason.as_ref() {
        push_unique_trace_line(
            &mut important_events,
            describe_rollback_reason(rollback_reason),
        );
    }
    if let Some(target) = target.as_deref() {
        push_unique_trace_line(
            &mut important_events,
            format!("Output trace targeted {}.", target),
        );
    }
    if let Some(growth_event_summary) = task_trace
        .get("growth_event")
        .and_then(|value| value.get("summary"))
        .and_then(|value| value.as_str())
    {
        push_unique_trace_line(
            &mut important_events,
            format!(
                "Related growth event: {}.",
                clean_trace_text(growth_event_summary),
            ),
        );
    }

    serde_json::json!({
        "headline": headline,
        "cause": cause,
        "status": status,
        "verification": verification,
        "rollback": rollback,
        "quarantine": quarantine,
        "verification_signal": verification_signal,
        "rollback_signal": rollback_signal,
        "growth_signal": growth_signal,
        "inspection": inspection,
        "readable_summary": format!(
            "{} {} {}",
            readable_cause,
            readable_status,
            readable_verification,
        ),
        "cause_summary": readable_cause,
        "status_summary": readable_status,
        "verification_summary": readable_verification,
        "rollback_summary": readable_rollback,
        "important_event_summaries": readable_events,
        "important_events": important_events,
    })
}

fn describe_action_run_status_from_values(
    executed: bool,
    verified: Option<bool>,
    summary: &str,
) -> String {
    let execution = if executed {
        match verified {
            Some(true) => "It executed and verified cleanly".to_string(),
            Some(false) => "It executed but verification failed".to_string(),
            None => "It executed without a recorded verification verdict".to_string(),
        }
    } else {
        "It was blocked before execution".to_string()
    };
    format!("{}: {}.", execution, clean_trace_text(summary))
}

fn describe_action_run_verification(action_run: &crate::autonomy::ActionRunRecord) -> String {
    describe_action_run_verification_parts(
        action_run.executed,
        action_run.verifier_verdict.as_ref(),
    )
}

fn describe_action_run_verification_parts(
    executed: bool,
    verifier_verdict: Option<&crate::runtime::verifier::VerifierVerdict>,
) -> String {
    if !executed {
        let mut message = "No verification ran because the action never executed.".to_string();
        if let Some(verifier_verdict) = verifier_verdict {
            message.push_str(&format!(
                " Verifier status is {} at {}: {}.",
                humanize_trace_label(&verifier_verdict.status),
                verifier_verdict.checked_at,
                clean_trace_text(&verifier_verdict.summary),
            ));
            if !verifier_verdict.checks.is_empty() {
                message.push_str(&format!(
                    " Checks: {}.",
                    verifier_verdict
                        .checks
                        .iter()
                        .map(|check| clean_trace_text(check))
                        .collect::<Vec<_>>()
                        .join("; "),
                ));
            }
        }
        return message;
    }

    if let Some(verifier_verdict) = verifier_verdict {
        let mut message = format!(
            "Verifier status is {} at {}: {}.",
            humanize_trace_label(&verifier_verdict.status),
            verifier_verdict.checked_at,
            clean_trace_text(&verifier_verdict.summary),
        );
        if !verifier_verdict.checks.is_empty() {
            message.push_str(&format!(
                " Checks: {}.",
                verifier_verdict
                    .checks
                    .iter()
                    .map(|check| clean_trace_text(check))
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }
        return message;
    }

    "No verifier verdict was recorded for this run.".to_string()
}

fn build_action_signal_json(
    action_run_id: i64,
    outcome: &str,
    summary: &str,
    executed: bool,
    verified: Option<bool>,
    expected_effect: Option<&crate::runtime::verifier::ExpectedEffectContract>,
    verifier_verdict: Option<&crate::runtime::verifier::VerifierVerdict>,
    rollback_reason: Option<&crate::runtime::verifier::RollbackReason>,
    target: Option<String>,
    inspect_path: String,
) -> serde_json::Value {
    serde_json::json!({
        "action_run_id": action_run_id,
        "outcome": outcome,
        "summary": clean_trace_text(summary),
        "executed": executed,
        "verified": verified,
        "target": target,
        "inspect_path": inspect_path,
        "attention_required": verified != Some(true) || rollback_reason.is_some(),
        "verification_signal": build_verification_signal_json(
            executed,
            verified,
            expected_effect,
            verifier_verdict,
            target.as_deref(),
            &inspect_path,
        ),
        "rollback_signal": build_rollback_signal_json(rollback_reason, &inspect_path),
    })
}

fn build_verification_signal_json(
    executed: bool,
    verified: Option<bool>,
    expected_effect: Option<&crate::runtime::verifier::ExpectedEffectContract>,
    verifier_verdict: Option<&crate::runtime::verifier::VerifierVerdict>,
    target: Option<&str>,
    inspect_path: &str,
) -> serde_json::Value {
    let status = verification_status_key(executed, verified, verifier_verdict);
    let summary = verifier_verdict
        .map(|value| clean_trace_text(&value.summary))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default_verification_summary(executed, verified));
    let effective_target = target.map(clean_trace_text).or_else(|| {
        expected_effect
            .and_then(|effect| effect.target.as_deref())
            .map(clean_trace_text)
    });

    serde_json::json!({
        "status": status,
        "summary": summary,
        "display": describe_action_run_verification_parts(executed, verifier_verdict),
        "executed": executed,
        "verified": verified,
        "checked_at": verifier_verdict.map(|value| value.checked_at.clone()),
        "checks": verifier_verdict
            .map(|value| value.checks.clone())
            .unwrap_or_default(),
        "target": effective_target,
        "verification_method": expected_effect
            .map(|effect| clean_trace_text(&effect.verification_method)),
        "intent": expected_effect
            .map(|effect| clean_trace_text(&effect.detail)),
        "expected_effect": expected_effect,
        "inspect_path": inspect_path,
        "attention_required": verified != Some(true),
    })
}

fn build_rollback_signal_json(
    rollback_reason: Option<&crate::runtime::verifier::RollbackReason>,
    inspect_path: &str,
) -> serde_json::Value {
    rollback_reason
        .map(|reason| {
            serde_json::json!({
                "kind": reason.kind,
                "summary": clean_trace_text(&reason.summary),
                "display": describe_rollback_reason(reason),
                "retryable": reason.retryable,
                "inspect_path": inspect_path,
                "attention_required": true,
            })
        })
        .unwrap_or(serde_json::Value::Null)
}

fn build_growth_signal_json(
    growth_event_id: i64,
    kind: &str,
    success: bool,
    target: Option<&str>,
    summary: &str,
    inspect_path: &str,
) -> serde_json::Value {
    serde_json::json!({
        "growth_event_id": growth_event_id,
        "kind": kind,
        "success": success,
        "target": target.map(clean_trace_text),
        "summary": clean_trace_text(summary),
        "inspect_path": inspect_path,
    })
}

fn build_growth_signal_from_task_trace(
    task_trace: &serde_json::Value,
    task_id: i64,
) -> serde_json::Value {
    let Some(growth_event) = task_trace.get("growth_event") else {
        return serde_json::Value::Null;
    };

    match (
        growth_event.get("id").and_then(|value| value.as_i64()),
        growth_event.get("kind").and_then(|value| value.as_str()),
        growth_event
            .get("success")
            .and_then(|value| value.as_bool()),
        growth_event.get("summary").and_then(|value| value.as_str()),
    ) {
        (Some(id), Some(kind), Some(success), Some(summary)) => build_growth_signal_json(
            id,
            kind,
            success,
            growth_event.get("target").and_then(|value| value.as_str()),
            summary,
            &format!("/api/autonomy/tasks/{}/trace", task_id),
        ),
        _ => serde_json::Value::Null,
    }
}

fn build_task_trace_inspection(
    task_id: i64,
    trace: &crate::autonomy::TaskTraceRefs,
    latest_action_run_id: Option<i64>,
) -> serde_json::Value {
    serde_json::json!({
        "task_trace_path": format!("/api/autonomy/tasks/{}/trace", task_id),
        "latest_action_run_trace_path": latest_action_run_id
            .map(|id| format!("/api/autonomy/action-runs/{}/trace", id)),
        "target": trace.target.as_deref().map(clean_trace_text),
        "observation_id": trace.observation_id,
        "growth_event_id": trace.growth_event_id,
        "snapshot_id": trace.snapshot_id,
    })
}

fn build_action_run_inspection(
    action_run: &crate::autonomy::ActionRunRecord,
    task_trace: &serde_json::Value,
    target: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({
        "action_run_trace_path": format!("/api/autonomy/action-runs/{}/trace", action_run.id),
        "task_trace_path": format!("/api/autonomy/tasks/{}/trace", action_run.task_id),
        "target": target.map(clean_trace_text),
        "observation_id": task_trace
            .pointer("/trace/observation_id")
            .and_then(|value| value.as_i64()),
        "growth_event_id": task_trace
            .pointer("/trace/growth_event_id")
            .and_then(|value| value.as_i64()),
        "snapshot_id": task_trace
            .pointer("/trace/snapshot_id")
            .and_then(|value| value.as_i64()),
    })
}

fn action_output_target(
    output: Option<&serde_json::Value>,
    expected_effect: Option<&crate::runtime::verifier::ExpectedEffectContract>,
) -> Option<String> {
    output
        .and_then(|value| value.pointer("/execution/trace/target"))
        .and_then(|value| value.as_str())
        .or_else(|| expected_effect.and_then(|effect| effect.target.as_deref()))
        .map(clean_trace_text)
}

fn verification_status_key(
    executed: bool,
    verified: Option<bool>,
    verifier_verdict: Option<&crate::runtime::verifier::VerifierVerdict>,
) -> String {
    verifier_verdict
        .map(|value| value.status.clone())
        .unwrap_or_else(|| match (executed, verified) {
            (false, _) => "blocked_before_execution".to_string(),
            (true, Some(true)) => "verified".to_string(),
            (true, Some(false)) => "failed".to_string(),
            (true, None) => "executed_unverified".to_string(),
        })
}

fn default_verification_summary(executed: bool, verified: Option<bool>) -> String {
    match (executed, verified) {
        (false, _) => "action never executed".to_string(),
        (true, Some(true)) => "verified cleanly".to_string(),
        (true, Some(false)) => "verification failed".to_string(),
        (true, None) => "no verifier verdict was recorded".to_string(),
    }
}

fn describe_expected_effect(
    expected_effect: &crate::runtime::verifier::ExpectedEffectContract,
) -> String {
    let target = expected_effect
        .target
        .as_deref()
        .map(clean_trace_text)
        .unwrap_or_else(|| "an unspecified target".to_string());
    format!(
        "Expected effect {} on {}: {} (verify via {}).",
        humanize_trace_label(&expected_effect.kind),
        target,
        clean_trace_text(&expected_effect.detail),
        clean_trace_text(&expected_effect.verification_method),
    )
}

fn describe_rollback_reason(rollback_reason: &crate::runtime::verifier::RollbackReason) -> String {
    format!(
        "Rollback reason {}: {}{}.",
        humanize_trace_label(&rollback_reason.kind),
        clean_trace_text(&rollback_reason.summary),
        if rollback_reason.retryable {
            " Retry remains possible"
        } else {
            " Retry is not planned"
        },
    )
}

fn describe_retry_state(retry_state: &crate::autonomy::RetryState) -> String {
    let target = retry_state
        .target
        .as_deref()
        .map(clean_trace_text)
        .unwrap_or_else(|| humanize_trace_label(&retry_state.task_kind));
    let mut message = format!(
        "Active retry state is {} after {} attempt{} for {}.",
        describe_failure_class(retry_state.failure_class),
        retry_state.attempt_count,
        plural_suffix(retry_state.attempt_count as usize),
        target,
    );
    if let Some(next_retry_at) = retry_state.next_retry_at.as_deref() {
        message.push_str(&format!(" Next retry is scheduled for {}.", next_retry_at));
    }
    if let Some(quarantined_until) = retry_state.quarantined_until.as_deref() {
        message.push_str(&format!(
            " Quarantine remains in place until {}.",
            quarantined_until
        ));
    }
    if let Some(last_error) = retry_state.last_error.as_deref() {
        message.push_str(&format!(" Last error: {}.", clean_trace_text(last_error)));
    }
    message
}

fn describe_retry_state_value(retry_state: &serde_json::Value) -> Option<String> {
    let failure_class = retry_state
        .get("failure_class")
        .and_then(|value| value.as_str())
        .map(humanize_trace_label)?;
    let attempt_count = retry_state
        .get("attempt_count")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let target = retry_state
        .get("target")
        .and_then(|value| value.as_str())
        .map(clean_trace_text)
        .unwrap_or_else(|| "the current target".to_string());
    let mut message = format!(
        "Stored retry state is {} after {} attempt{} for {}.",
        failure_class,
        attempt_count,
        plural_suffix(attempt_count.max(0) as usize),
        target,
    );
    if let Some(next_retry_at) = retry_state
        .get("next_retry_at")
        .and_then(|value| value.as_str())
    {
        message.push_str(&format!(" Next retry is scheduled for {}.", next_retry_at));
    }
    if let Some(quarantined_until) = retry_state
        .get("quarantined_until")
        .and_then(|value| value.as_str())
    {
        message.push_str(&format!(
            " Quarantine remains in place until {}.",
            quarantined_until
        ));
    }
    if let Some(last_error) = retry_state
        .get("last_error")
        .and_then(|value| value.as_str())
    {
        message.push_str(&format!(" Last error: {}.", clean_trace_text(last_error)));
    }
    Some(message)
}

fn describe_built_tool_health(health: &crate::forge::BuiltToolHealthState) -> String {
    let mut message = format!(
        "Tool health for {} shows {} failure{}.",
        clean_trace_text(&health.tool_name),
        health.failure_count,
        plural_suffix(health.failure_count),
    );
    if let Some(quarantined_until) = health.quarantined_until.as_deref() {
        message.push_str(&format!(" It is quarantined until {}.", quarantined_until));
    }
    if let Some(last_error) = health.last_error.as_deref() {
        message.push_str(&format!(" Last error: {}.", clean_trace_text(last_error)));
    }
    message
}

fn summarize_task_cause(
    task: &crate::autonomy::Task,
    trace: &crate::autonomy::TaskTraceRefs,
    observation: Option<&crate::autonomy::Observation>,
    growth_event: Option<&crate::autonomy::GrowthEvent>,
    built_tool_health: Option<&crate::forge::BuiltToolHealthState>,
) -> String {
    if let Some(observation) = observation {
        return format!(
            "Started from observation #{} about {}.",
            observation.id,
            humanize_trace_label(&observation.kind),
        );
    }
    if let Some(growth_event) = growth_event {
        let target = growth_event
            .target
            .as_deref()
            .map(clean_trace_text)
            .unwrap_or_else(|| "an unspecified target".to_string());
        return format!(
            "Started from growth event #{} for {}.",
            growth_event.id, target,
        );
    }
    if let Some(target) = trace.target.as_deref() {
        return format!("Started to act on {}.", clean_trace_text(target));
    }
    if let Some(tool) = task.tool.as_deref() {
        return format!("Started to act on {}.", clean_trace_text(tool));
    }
    if let Some(tool_health) = built_tool_health {
        return format!(
            "Started because {} needs follow-up.",
            clean_trace_text(&tool_health.tool_name),
        );
    }
    format!("Started for task {}.", clean_trace_text(&task.title))
}

fn summarize_task_status(
    task: &crate::autonomy::Task,
    action_runs: &[crate::autonomy::ActionRun],
) -> String {
    if let Some(latest_run) = action_runs.last() {
        return format!(
            "Task is {} and the latest run {}.",
            humanize_trace_label(task.status.as_str()),
            summarize_action_run_outcome(
                &latest_run.outcome,
                latest_run.executed,
                latest_run.verified,
            ),
        );
    }
    format!(
        "Task is {} and has not produced an action run yet.",
        humanize_trace_label(task.status.as_str()),
    )
}

fn summarize_task_events(
    observation: Option<&crate::autonomy::Observation>,
    growth_event: Option<&crate::autonomy::GrowthEvent>,
    snapshot: Option<&serde_json::Value>,
    latest_run: Option<&crate::autonomy::ActionRun>,
    built_tool_health: Option<&crate::forge::BuiltToolHealthState>,
) -> Vec<String> {
    let mut events = Vec::new();
    if let Some(observation) = observation {
        events.push(format!(
            "Observation #{} came from {}.",
            observation.id,
            clean_trace_text(&observation.source),
        ));
    }
    if let Some(growth_event) = growth_event {
        events.push(format!(
            "Growth event #{} {}.",
            growth_event.id,
            if growth_event.success {
                "reported success"
            } else {
                "reported failure"
            },
        ));
    }
    if let Some(snapshot) = snapshot {
        if let Some(id) = snapshot.get("id").and_then(|value| value.as_i64()) {
            events.push(format!("Snapshot #{} is attached.", id));
        }
    }
    if let Some(latest_run) = latest_run {
        events.push(format!(
            "Latest run #{} {}.",
            latest_run.id,
            summarize_action_run_outcome(
                &latest_run.outcome,
                latest_run.executed,
                latest_run.verified,
            ),
        ));
    }
    if let Some(built_tool_health) = built_tool_health {
        let mut event = format!(
            "{} has {} recorded failure{}",
            clean_trace_text(&built_tool_health.tool_name),
            built_tool_health.failure_count,
            plural_suffix(built_tool_health.failure_count),
        );
        if let Some(quarantined_until) = built_tool_health.quarantined_until.as_deref() {
            event.push_str(&format!(" and is quarantined until {}", quarantined_until));
        }
        event.push('.');
        events.push(event);
    }
    events
}

fn summarize_action_run_status(action_run: &crate::autonomy::ActionRunRecord) -> String {
    format!(
        "Run {}.",
        summarize_action_run_outcome(
            &action_run.outcome,
            action_run.executed,
            action_run.verified,
        ),
    )
}

fn summarize_action_run_outcome(outcome: &str, executed: bool, verified: Option<bool>) -> String {
    match (executed, verified) {
        (false, _) => format!("ended {} before execution", humanize_trace_label(outcome),),
        (true, Some(true)) => format!(
            "ended {} and verified cleanly",
            humanize_trace_label(outcome),
        ),
        (true, Some(false)) => format!(
            "ended {} but failed verification",
            humanize_trace_label(outcome),
        ),
        (true, None) => format!(
            "ended {} without a verifier verdict",
            humanize_trace_label(outcome),
        ),
    }
}

fn summarize_action_run_events(
    action_run: &crate::autonomy::ActionRunRecord,
    task_trace: &serde_json::Value,
    output_trace: Option<&serde_json::Value>,
) -> Vec<String> {
    let mut events = vec![format!(
        "Recorded at {} with outcome {}.",
        action_run.created_at,
        humanize_trace_label(&action_run.outcome),
    )];
    if let Some(target) = action_run_trace_target(action_run, task_trace, output_trace) {
        events.push(format!("Output trace points to {}.", target));
    }
    if let Some(status) = action_run
        .verifier_verdict
        .as_ref()
        .map(|value| humanize_trace_label(&value.status))
    {
        events.push(format!("Verifier status is {}.", status));
    }
    if let Some(summary) = task_trace
        .get("operator_summary")
        .and_then(|value| value.get("status_summary"))
        .and_then(|value| value.as_str())
    {
        events.push(summary.to_string());
    }
    events
}

fn action_run_trace_target(
    action_run: &crate::autonomy::ActionRunRecord,
    task_trace: &serde_json::Value,
    output_trace: Option<&serde_json::Value>,
) -> Option<String> {
    output_trace
        .and_then(|value| value.get("target"))
        .and_then(|value| value.as_str())
        .or_else(|| {
            action_run
                .output
                .as_ref()
                .and_then(|value| value.pointer("/execution/trace/target"))
                .and_then(|value| value.as_str())
        })
        .or_else(|| {
            task_trace
                .get("trace")
                .and_then(|value| value.get("target"))
                .and_then(|value| value.as_str())
        })
        .map(clean_trace_text)
}

fn describe_task_quarantine_state(
    current_retry_state: Option<&crate::autonomy::RetryState>,
    built_tool_health: Option<&crate::forge::BuiltToolHealthState>,
    latest_run: Option<&crate::autonomy::ActionRun>,
) -> Option<String> {
    if let Some(quarantined_until) =
        current_retry_state.and_then(|value| value.quarantined_until.as_deref())
    {
        return Some(format!(
            "Task retry lane is quarantined until {}.",
            quarantined_until
        ));
    }
    if let Some(health) = built_tool_health {
        if let Some(quarantined_until) = health.quarantined_until.as_deref() {
            return Some(format!(
                "Related tool {} is quarantined until {}.",
                clean_trace_text(&health.tool_name),
                quarantined_until,
            ));
        }
    }
    if let Some(latest_run) = latest_run {
        if latest_run.outcome == "quarantined" || !latest_run.executed {
            return Some(format!(
                "Latest run #{} was quarantined before execution.",
                latest_run.id,
            ));
        }
    }
    None
}

fn describe_action_run_quarantine_state(
    action_run: &crate::autonomy::ActionRunRecord,
    task_trace: &serde_json::Value,
    output_trace: Option<&serde_json::Value>,
) -> Option<String> {
    if let Some(rollback_reason) = action_run.rollback_reason.as_ref() {
        if rollback_reason.kind == "quarantined" {
            return Some(shorten_trace_detail(&describe_rollback_reason(
                rollback_reason,
            )));
        }
    }
    if let Some(quarantine) = task_trace
        .get("operator_summary")
        .and_then(|value| value.get("quarantine"))
        .and_then(|value| value.as_str())
    {
        return Some(quarantine.to_string());
    }
    if let Some(target) = output_trace
        .and_then(|value| value.get("target"))
        .and_then(|value| value.as_str())
    {
        if !action_run.executed {
            return Some(format!(
                "Execution was blocked before touching {}.",
                clean_trace_text(target),
            ));
        }
    }
    None
}

fn shorten_trace_detail(value: &str) -> String {
    clean_trace_text(value).trim_end_matches('.').to_string()
}

fn describe_failure_class(failure_class: crate::autonomy::FailureClass) -> &'static str {
    match failure_class {
        crate::autonomy::FailureClass::Transient => "transient",
        crate::autonomy::FailureClass::Permanent => "permanent",
        crate::autonomy::FailureClass::Unsafe => "unsafe",
        crate::autonomy::FailureClass::InconsistentState => "inconsistent state",
    }
}

fn clean_trace_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn humanize_trace_label(value: &str) -> String {
    clean_trace_text(value)
        .replace(['_', '-'], " ")
        .trim()
        .to_string()
}

fn plural_suffix(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn push_unique_trace_line(lines: &mut Vec<String>, line: String) {
    let line = clean_trace_text(&line);
    if !line.is_empty() && !lines.iter().any(|existing| existing == &line) {
        lines.push(line);
    }
}

/// Get chat history for the web channel (with timestamps)
async fn history(State(state): State<AppState>) -> Json<serde_json::Value> {
    let messages = state.db.get_history_with_time("web", 50);
    let history: Vec<serde_json::Value> = messages
        .iter()
        .map(
            |(role, content, ts)| serde_json::json!({"role": role, "content": content, "time": ts}),
        )
        .collect();
    Json(serde_json::json!({"messages": history}))
}

/// Speak text aloud using macOS TTS
async fn voice_say(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;
    let text = body["text"].as_str().unwrap_or("");
    if !text.is_empty() {
        crate::voice::speak(text).await;
    }
    Ok(Json(serde_json::json!({"status": "speaking"})))
}

/// Record audio, transcribe, handle through Nyx, speak response
async fn voice_listen(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    check_auth(&state, &headers)?;
    let duration = body["duration"].as_u64().unwrap_or(5) as u32;
    let response = crate::voice::voice_interaction(&state, duration).await;
    Ok(Json(serde_json::json!({"response": response})))
}

/// Poll for proactive messages from the awareness engine
async fn proactive(State(state): State<AppState>) -> Json<serde_json::Value> {
    let mut q = state.proactive_queue.lock().await;
    if q.is_empty() {
        Json(serde_json::json!({"messages": []}))
    } else {
        let msgs: Vec<String> = q.drain(..).collect();
        Json(serde_json::json!({"messages": msgs}))
    }
}
