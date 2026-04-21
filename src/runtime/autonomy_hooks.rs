use crate::{AppState, autonomy, forge};

const EXECUTION_TRACE_SCHEMA_VERSION: &str = "nyx_execution_trace.v1";

fn with_execution_trace(
    mut details: serde_json::Value,
    surface: &str,
    kind: &str,
    source: &str,
    target: Option<&str>,
    summary: &str,
    success: bool,
    execution_extra: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut execution = serde_json::json!({
        "schema_version": EXECUTION_TRACE_SCHEMA_VERSION,
        "surface": surface,
        "kind": kind,
        "summary": summary,
        "outcome": if success { "completed" } else { "failed" },
        "success": success,
        "trace": {
            "source": source,
            "target": target,
        }
    });
    if let (Some(extra), Some(execution_object)) = (execution_extra, execution.as_object_mut()) {
        if let Some(extra_object) = extra.as_object() {
            for (key, value) in extra_object {
                execution_object.insert(key.clone(), value.clone());
            }
        }
    }

    if let Some(object) = details.as_object_mut() {
        object.insert("execution".into(), execution);
        details
    } else {
        serde_json::json!({
            "payload": details,
            "execution": execution,
        })
    }
}

impl AppState {
    pub(crate) fn record_self_edit_growth(
        &self,
        source: &str,
        request: &str,
        target: &str,
        summary: &str,
        success: bool,
        telemetry: &forge::EvolveTelemetry,
    ) {
        let target = if target.is_empty() {
            None
        } else {
            Some(target)
        };
        let details = with_execution_trace(
            serde_json::json!({
                "request": request,
                "repair_rounds": telemetry.repair_rounds,
                "protected_core": telemetry.protected_core,
                "strategy": telemetry.strategy,
                "verification_mode": telemetry.verification_mode,
                "file_provenance_operation_id": telemetry.file_provenance_operation_id,
            }),
            "forge",
            "self_edit_result",
            source,
            target,
            summary,
            success,
            Some(serde_json::json!({
                "executed": telemetry.executed,
                "verified": telemetry.verified,
                "llm": &telemetry.llm_usage,
            })),
        );
        self.record_growth_event(
            "self_edit_result",
            source,
            target,
            summary,
            success,
            0.82,
            false,
            details,
        );
    }

    pub(crate) fn record_tool_build_growth(
        &self,
        source: &str,
        request: &str,
        target: &str,
        summary: &str,
        success: bool,
        telemetry: &forge::ForgeTelemetry,
        extra_details: Option<serde_json::Value>,
    ) {
        let mut details = serde_json::json!({
            "request": request,
            "repair_rounds": telemetry.repair_rounds,
            "verification_failures": telemetry.verification_failures,
            "runtime_failures": telemetry.runtime_failures,
            "file_provenance_operation_id": telemetry.file_provenance_operation_id,
        });
        if let Some(extra) = extra_details {
            if let (Some(details_obj), Some(extra_obj)) =
                (details.as_object_mut(), extra.as_object())
            {
                for (key, value) in extra_obj {
                    details_obj.insert(key.clone(), value.clone());
                }
            }
        }
        let target = if target.is_empty() {
            None
        } else {
            Some(target)
        };
        let details = with_execution_trace(
            details,
            "forge",
            "tool_growth_result",
            source,
            target,
            summary,
            success,
            Some(serde_json::json!({
                "executed": telemetry.executed,
                "verified": telemetry.verified,
                "llm": &telemetry.llm_usage,
            })),
        );
        self.record_growth_event(
            "tool_growth_result",
            source,
            target,
            summary,
            success,
            0.78,
            false,
            details,
        );
    }

    pub(crate) fn record_memory_consolidation_growth(
        &self,
        source: &str,
        summary: &str,
        details: serde_json::Value,
    ) {
        let details = with_execution_trace(
            details,
            "growth_event",
            "memory_consolidation",
            source,
            None,
            summary,
            true,
            None,
        );
        self.record_growth_event(
            "memory_consolidation",
            source,
            None,
            summary,
            true,
            0.68,
            false,
            details,
        );
    }

    pub(crate) fn record_user_adaptation_growth(
        &self,
        source: &str,
        summary: &str,
        details: serde_json::Value,
    ) {
        let details = with_execution_trace(
            details,
            "growth_event",
            "user_adaptation",
            source,
            None,
            summary,
            true,
            None,
        );
        self.record_growth_event(
            "user_adaptation",
            source,
            None,
            summary,
            true,
            0.66,
            false,
            details,
        );
    }

    pub(crate) fn record_growth_event(
        &self,
        kind: &str,
        source: &str,
        target: Option<&str>,
        summary: &str,
        success: bool,
        priority: f64,
        deliver_output: bool,
        details: serde_json::Value,
    ) {
        let mut context = details;
        if let Some(object) = context.as_object_mut() {
            object.insert("success".into(), serde_json::json!(success));
            object.insert("deliver_output".into(), serde_json::json!(deliver_output));
            if let Some(target) = target {
                object.insert("target".into(), serde_json::json!(target));
            }
        }

        match self
            .db
            .record_growth_event(kind, source, target, summary, success, &context)
        {
            Ok(growth_event_id) => {
                if let Err(error) = autonomy::ingest_observation(
                    self.db.as_ref(),
                    autonomy::ObservationInput {
                        kind: kind.to_string(),
                        source: source.to_string(),
                        content: summary.to_string(),
                        context: {
                            let mut observation_context = context;
                            if let Some(object) = observation_context.as_object_mut() {
                                object.insert(
                                    "growth_event_id".into(),
                                    serde_json::json!(growth_event_id),
                                );
                            }
                            observation_context
                        },
                        priority,
                    },
                ) {
                    tracing::warn!("autonomy: failed to ingest growth observation: {}", error);
                }

                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    let state = self.clone();
                    let source = source.to_string();
                    let kind = kind.to_string();
                    let target = target.map(str::to_string);
                    let summary = summary.to_string();
                    handle.spawn(async move {
                        state
                            .persist_self_model_snapshot_and_detect_gaps(
                                &source,
                                &kind,
                                target.as_deref(),
                                &summary,
                                success,
                                Some(growth_event_id),
                            )
                            .await;
                    });
                }
            }
            Err(error) => tracing::warn!("autonomy: failed to persist growth event: {}", error),
        }
    }
}
