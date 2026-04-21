//! Forge — dual-lane code creation pipeline.
//!
//! Two LLM lanes collaborate to produce code:
//! - Intent lane: understands user intent, defines UX, sets test criteria
//! - Implementation lane: plans implementation, writes code, spots bugs
//!
//! Nyx routes those lanes to whichever models are actually available at runtime.
//! They work in parallel where possible, Nyx merges their perspectives.
//! Max 2 verification rounds. Most builds pass on first verify.
//!
//! Flow:
//!   understand (parallel) → merge spec → code → verify (parallel) → deploy
//!                                                  ↓ fail
//!                                           fix → re-verify → deploy or abort

use crate::llm::{LlmGate, LlmResponseTrace};
use std::sync::Arc;

mod built_tools;
mod evolve_planning;
mod helpers;
mod protected_evolve;
pub mod sandbox;
mod tool_build;

const MAX_REPAIR_ATTEMPTS: usize = 2;

const BUILT_TOOL_MANIFEST_SUFFIX: &str = ".nyx_tool.json";
const BUILT_TOOL_HEALTH_FAILURE_WINDOW_SECS: i64 = 12 * 60 * 60;
const BUILT_TOOL_HEALTH_QUARANTINE_AFTER_FAILURES: usize = 2;
const BUILT_TOOL_HEALTH_QUARANTINE_SECS: i64 = 24 * 60 * 60;

pub use built_tools::{
    built_tool_health_for_target, count_unhealthy_built_tools, inspect_requested_registered_tool,
    list_registered_tool_runtime_statuses, list_unhealthy_built_tools, load_registered_tools,
    reconcile_built_tool_registration, request_mentions_registered_tool, run_registered_tool,
    run_registered_tool_checked, tools_for_prompt, visible_registered_tools,
};

/// Execute a caller-known Python built-tool file via the forge sandbox path.
///
/// Thin public wrapper around the internal `helpers::run_built_tool_file` so
/// non-forge callers (e.g. `src/tools.rs` dispatching the `browser` builtin)
/// can share the same sandbox-aware execution path used by forge-built tools,
/// without adding a second entrypoint.
pub async fn run_built_tool_at(
    path: &std::path::Path,
    args: &serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    helpers::run_built_tool_file(path, args).await
}
use built_tools::{
    built_tool_manifest_path_for, built_tool_timestamp_in_future, remove_built_tool_artifacts,
    tool_name_from_spec, write_built_tool_manifest,
};
use evolve_planning::plan_evolve_change;
#[cfg(test)]
use evolve_planning::{
    best_markdown_anchor_candidate, ensure_requested_doc_note, replacement_delta_preview,
    requested_doc_note_sentence, reroute_markdown_note_plan,
};
use helpers::{
    accept_repair_candidate, chat_forge_intent_lane_traced, normalize_compare_text,
    parse_json_response, parse_tool_input_json, run_built_tool_file, run_tool_with_expectation,
    sanitize_relative_repo_path, strip_code_fences,
};
#[cfg(test)]
use protected_evolve::{
    PlannedChange, apply_protected_change_in_project, apply_protected_change_with_self_repair,
    backup_path_for, summarize_planned_change,
};
use protected_evolve::{
    blocked_evolve_preflight, build_evolve_preflight, execute_evolve_plan, telemetry_for_preflight,
};
#[cfg(test)]
use tool_build::derive_auto_run_input;
use tool_build::{
    blocked_build_preflight, build_tool_preflight, execute_build_spec, plan_tool_build,
};

/// The merged task spec — what both models agreed needs building.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskSpec {
    pub tool_name: String,
    pub filename: String,
    pub purpose: String,
    pub inputs: String,
    pub outputs: String,
    pub approach: String,
    pub test_input: String,
    pub test_expected: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuiltToolManifest {
    pub name: String,
    pub filename: String,
    pub description: String,
    pub inputs: String,
    pub outputs: String,
    pub test_input: String,
    pub test_expected: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuiltToolRegistrationRepair {
    pub tool_name: String,
    pub filename: String,
    pub manifest_path: String,
    pub manifest_created: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ForgeTelemetry {
    pub repair_rounds: usize,
    pub verification_failures: usize,
    pub runtime_failures: usize,
    pub executed: bool,
    pub verified: Option<bool>,
    pub llm_usage: ForgeLlmUsage,
}

impl ForgeTelemetry {
    pub(super) fn set_execution_status(&mut self, executed: bool, verified: Option<bool>) {
        self.executed = executed;
        self.verified = verified;
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ForgeLlmUsage {
    pub predicted_intent_model: String,
    pub predicted_implementation_model: String,
    pub actual_calls: Vec<ForgeLlmCall>,
}

impl ForgeLlmUsage {
    fn for_llm(llm: &LlmGate) -> Self {
        Self {
            predicted_intent_model: llm.preferred_chat_model_label(),
            predicted_implementation_model: llm.preferred_autonomous_model_label(),
            actual_calls: Vec::new(),
        }
    }

    fn record(&mut self, stage: impl Into<String>, trace: &LlmResponseTrace) {
        self.actual_calls.push(ForgeLlmCall {
            stage: stage.into(),
            provider: trace.provider.clone(),
            model: trace.model.clone(),
            route: trace.route.clone(),
            latency_ms: trace.latency_ms,
        });
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForgeLlmCall {
    pub stage: String,
    pub provider: String,
    pub model: String,
    pub route: String,
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ToolRunPreview {
    pub input: serde_json::Value,
    pub output: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RegisteredToolInspection {
    pub name: String,
    pub filename: String,
    pub healthy: bool,
    pub issue: Option<String>,
    pub health: Option<BuiltToolHealthState>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuiltToolHealthState {
    pub tool_name: String,
    pub filename: String,
    pub failure_count: usize,
    pub first_failed_at: Option<String>,
    pub last_failed_at: Option<String>,
    pub quarantined_until: Option<String>,
    pub last_error: Option<String>,
    pub updated_at: String,
}

impl BuiltToolHealthState {
    pub fn is_currently_quarantined(&self) -> bool {
        built_tool_timestamp_in_future(self.quarantined_until.as_deref())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EvolveTelemetry {
    pub protected_core: bool,
    pub repair_rounds: usize,
    pub strategy: String,
    pub verification_mode: String,
    pub executed: bool,
    pub verified: Option<bool>,
    pub llm_usage: ForgeLlmUsage,
}

impl Default for EvolveTelemetry {
    fn default() -> Self {
        Self {
            protected_core: false,
            repair_rounds: 0,
            strategy: "file_ops".into(),
            verification_mode: "tool:file_ops".into(),
            executed: false,
            verified: None,
            llm_usage: ForgeLlmUsage::default(),
        }
    }
}

impl EvolveTelemetry {
    pub(super) fn set_execution_status(&mut self, executed: bool, verified: Option<bool>) {
        self.executed = executed;
        self.verified = verified;
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerificationStepPreview {
    pub label: String,
    pub program: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub env_keys: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EvolvePlanPreview {
    pub path: String,
    pub protected_core: bool,
    pub description: String,
    pub search_preview: String,
    pub replace_preview: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EvolvePreflight {
    pub user_request: String,
    pub intent_model: String,
    pub implementation_model: String,
    pub ready: bool,
    pub issue: Option<String>,
    pub plan: Option<EvolvePlanPreview>,
    pub path_exists: bool,
    pub search_found: bool,
    pub verification_mode: String,
    pub verification_steps: Vec<VerificationStepPreview>,
    pub validations: Vec<String>,
    pub change_summary: Option<String>,
    pub change_report: Option<ChangeReport>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EvolveDispatchResult {
    pub dry_run: bool,
    pub success: bool,
    pub executed: bool,
    pub verified: Option<bool>,
    pub preflight: EvolvePreflight,
    pub path: Option<String>,
    pub description: Option<String>,
    pub change_summary: Option<String>,
    pub change_report: Option<ChangeReport>,
    pub telemetry: EvolveTelemetry,
    pub error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuildSpecPreview {
    pub tool_name: String,
    pub filename: String,
    pub purpose: String,
    pub inputs: String,
    pub outputs: String,
    pub approach: String,
    pub test_input: String,
    pub test_expected: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuildPreflight {
    pub user_request: String,
    pub nyx_response: String,
    pub intent_model: String,
    pub implementation_model: String,
    pub ready: bool,
    pub issue: Option<String>,
    pub spec: Option<BuildSpecPreview>,
    pub target_exists: bool,
    pub manifest_path: Option<String>,
    pub auto_run_input: Option<serde_json::Value>,
    pub validations: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuildDispatchResult {
    pub dry_run: bool,
    pub success: bool,
    pub executed: bool,
    pub verified: Option<bool>,
    pub preflight: BuildPreflight,
    pub filename: Option<String>,
    pub description: Option<String>,
    pub auto_run: Option<ToolRunPreview>,
    pub telemetry: ForgeTelemetry,
    pub error: Option<String>,
}

/// Result of a forge operation.
pub enum ForgeResult {
    /// Tool was built and written to disk.
    Success {
        filename: String,
        description: String,
        auto_run: Option<ToolRunPreview>,
        telemetry: ForgeTelemetry,
    },
    /// Both models tried, couldn't produce valid code.
    Failed {
        reason: String,
        telemetry: ForgeTelemetry,
    },
}

/// Result of a code modification.
pub enum EvolveResult {
    /// Code was modified successfully.
    Success {
        path: String,
        description: String,
        change_summary: String,
        change_report: ChangeReport,
        telemetry: EvolveTelemetry,
    },
    /// Modification failed verification.
    Failed {
        reason: String,
        telemetry: EvolveTelemetry,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeReport {
    pub path: String,
    pub line: Option<usize>,
    pub before: String,
    pub after: String,
    pub explanation: String,
}

/// Build a new tool using dual-model collaboration.
pub async fn build_tool(llm: &Arc<LlmGate>, user_request: &str, nyx_response: &str) -> ForgeResult {
    let result = dispatch_build_tool_action(llm, user_request, nyx_response, false).await;
    if result.success {
        ForgeResult::Success {
            filename: result.filename.unwrap_or_default(),
            description: result.description.unwrap_or_default(),
            auto_run: result.auto_run,
            telemetry: result.telemetry,
        }
    } else {
        ForgeResult::Failed {
            reason: result
                .error
                .or_else(|| result.preflight.issue.clone())
                .unwrap_or_else(|| "tool build failed".to_string()),
            telemetry: result.telemetry,
        }
    }
}

pub async fn dispatch_build_tool_action(
    llm: &Arc<LlmGate>,
    user_request: &str,
    nyx_response: &str,
    dry_run: bool,
) -> BuildDispatchResult {
    let mut telemetry = ForgeTelemetry {
        llm_usage: ForgeLlmUsage::for_llm(llm.as_ref()),
        ..ForgeTelemetry::default()
    };
    let spec = match plan_tool_build(llm, user_request, nyx_response, &mut telemetry.llm_usage)
        .await
    {
        Ok(spec) => spec,
        Err(error) => {
            let preflight = blocked_build_preflight(user_request, nyx_response, &error.to_string());
            return BuildDispatchResult {
                dry_run,
                success: false,
                executed: telemetry.executed,
                verified: telemetry.verified,
                preflight,
                filename: None,
                description: None,
                auto_run: None,
                telemetry,
                error: Some(error.to_string()),
            };
        }
    };

    let preflight = build_tool_preflight(llm.as_ref(), user_request, nyx_response, &spec);
    if !preflight.ready {
        return BuildDispatchResult {
            dry_run,
            success: false,
            executed: telemetry.executed,
            verified: telemetry.verified,
            filename: preflight.spec.as_ref().map(|spec| spec.filename.clone()),
            description: preflight.spec.as_ref().map(|spec| spec.purpose.clone()),
            auto_run: None,
            telemetry,
            error: preflight.issue.clone(),
            preflight,
        };
    }

    if dry_run {
        return BuildDispatchResult {
            dry_run: true,
            success: true,
            executed: telemetry.executed,
            verified: telemetry.verified,
            filename: preflight.spec.as_ref().map(|spec| spec.filename.clone()),
            description: preflight.spec.as_ref().map(|spec| spec.purpose.clone()),
            auto_run: None,
            telemetry,
            error: None,
            preflight,
        };
    }

    tracing::info!(
        "forge: building tool for \"{}\" with intent={} impl={}",
        crate::trunc(&user_request, 50),
        preflight.intent_model,
        preflight.implementation_model
    );
    match execute_build_spec(llm, user_request, &spec, telemetry).await {
        ForgeResult::Success {
            filename,
            description,
            auto_run,
            telemetry,
        } => BuildDispatchResult {
            dry_run: false,
            success: true,
            executed: telemetry.executed,
            verified: telemetry.verified,
            preflight,
            filename: Some(filename),
            description: Some(description),
            auto_run,
            telemetry,
            error: None,
        },
        ForgeResult::Failed { reason, telemetry } => BuildDispatchResult {
            dry_run: false,
            success: false,
            executed: telemetry.executed,
            verified: telemetry.verified,
            filename: preflight.spec.as_ref().map(|spec| spec.filename.clone()),
            description: preflight.spec.as_ref().map(|spec| spec.purpose.clone()),
            auto_run: None,
            telemetry,
            error: Some(reason),
            preflight,
        },
    }
}

/// Modify existing code using dual-model collaboration.
pub async fn evolve_code(llm: &Arc<LlmGate>, user_request: &str) -> EvolveResult {
    let result = dispatch_evolve_action(llm, user_request, false).await;
    if result.success {
        EvolveResult::Success {
            path: result.path.unwrap_or_default(),
            description: result.description.unwrap_or_default(),
            change_summary: result.change_summary.unwrap_or_default(),
            change_report: result.change_report.unwrap_or_else(|| ChangeReport {
                path: String::new(),
                line: None,
                before: String::new(),
                after: String::new(),
                explanation: String::new(),
            }),
            telemetry: result.telemetry,
        }
    } else {
        EvolveResult::Failed {
            reason: result
                .error
                .or_else(|| result.preflight.issue.clone())
                .unwrap_or_else(|| "evolve failed".to_string()),
            telemetry: result.telemetry,
        }
    }
}

pub async fn dispatch_evolve_action(
    llm: &Arc<LlmGate>,
    user_request: &str,
    dry_run: bool,
) -> EvolveDispatchResult {
    let mut telemetry = EvolveTelemetry {
        llm_usage: ForgeLlmUsage::for_llm(llm.as_ref()),
        ..EvolveTelemetry::default()
    };
    let plan = match plan_evolve_change(llm, user_request, &mut telemetry.llm_usage).await {
        Ok(plan) => plan,
        Err(error) => {
            let preflight = blocked_evolve_preflight(user_request, &error.to_string());
            return EvolveDispatchResult {
                dry_run,
                success: false,
                executed: telemetry.executed,
                verified: telemetry.verified,
                preflight,
                path: None,
                description: None,
                change_summary: None,
                change_report: None,
                telemetry,
                error: Some(error.to_string()),
            };
        }
    };

    let preflight = build_evolve_preflight(llm.as_ref(), user_request, &plan);
    let telemetry = telemetry_for_preflight(&preflight, telemetry.llm_usage.clone(), 0);
    if !preflight.ready {
        return EvolveDispatchResult {
            dry_run,
            success: false,
            executed: telemetry.executed,
            verified: telemetry.verified,
            path: preflight.plan.as_ref().map(|plan| plan.path.clone()),
            description: preflight.plan.as_ref().map(|plan| plan.description.clone()),
            change_summary: preflight.change_summary.clone(),
            change_report: preflight.change_report.clone(),
            telemetry,
            error: preflight.issue.clone(),
            preflight,
        };
    }

    if dry_run {
        return EvolveDispatchResult {
            dry_run: true,
            success: true,
            executed: telemetry.executed,
            verified: telemetry.verified,
            path: preflight.plan.as_ref().map(|plan| plan.path.clone()),
            description: preflight.plan.as_ref().map(|plan| plan.description.clone()),
            change_summary: preflight.change_summary.clone(),
            change_report: preflight.change_report.clone(),
            telemetry,
            error: None,
            preflight,
        };
    }

    match execute_evolve_plan(llm, &plan, telemetry).await {
        EvolveResult::Success {
            path,
            description,
            change_summary,
            change_report,
            telemetry,
        } => EvolveDispatchResult {
            dry_run: false,
            success: true,
            executed: telemetry.executed,
            verified: telemetry.verified,
            preflight,
            path: Some(path),
            description: Some(description),
            change_summary: Some(change_summary),
            change_report: Some(change_report),
            telemetry,
            error: None,
        },
        EvolveResult::Failed { reason, telemetry } => EvolveDispatchResult {
            dry_run: false,
            success: false,
            executed: telemetry.executed,
            verified: telemetry.verified,
            path: preflight.plan.as_ref().map(|plan| plan.path.clone()),
            description: preflight.plan.as_ref().map(|plan| plan.description.clone()),
            change_summary: preflight.change_summary.clone(),
            change_report: preflight.change_report.clone(),
            telemetry,
            error: Some(reason),
            preflight,
        },
    }
}

#[cfg(test)]
#[path = "../tests/unit/forge.rs"]
mod tests;
