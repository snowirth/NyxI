use anyhow::{Result, anyhow, bail};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::db::Db;
use crate::file_provenance::{FileMutationProof, write_text_file_with_provenance};
use crate::tools::ToolRuntimeStatus;

use super::{
    BUILT_TOOL_HEALTH_FAILURE_WINDOW_SECS, BUILT_TOOL_HEALTH_QUARANTINE_AFTER_FAILURES,
    BUILT_TOOL_HEALTH_QUARANTINE_SECS, BUILT_TOOL_MANIFEST_SUFFIX, BuiltToolHealthState,
    BuiltToolManifest, BuiltToolRegistrationRepair, RegisteredToolInspection, TaskSpec,
    parse_tool_input_json, run_built_tool_file, run_tool_with_expectation,
    sanitize_relative_repo_path,
};

pub(super) fn tool_name_from_spec(spec: &TaskSpec) -> Result<String> {
    let path = sanitize_relative_repo_path(&spec.filename)?;
    if !path.starts_with("tools") {
        bail!("built tool must live under tools/: {}", path.display());
    }
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("invalid tool filename: {}", path.display()))?;
    Ok(stem.to_string())
}

pub(super) fn built_tool_manifest_path_for(spec: &TaskSpec) -> Option<String> {
    let tool_name = tool_name_from_spec(spec).ok()?;
    Some(format!("tools/{}{}", tool_name, BUILT_TOOL_MANIFEST_SUFFIX))
}

pub(super) fn built_tool_timestamp_in_future(value: Option<&str>) -> bool {
    value
        .and_then(parse_built_tool_timestamp)
        .map(|timestamp| timestamp > chrono::Utc::now())
        .unwrap_or(false)
}

fn built_tool_timestamp_within_window(
    value: &str,
    now: chrono::DateTime<chrono::Utc>,
    window_secs: i64,
) -> bool {
    parse_built_tool_timestamp(value)
        .map(|timestamp| {
            let age_secs = now.signed_duration_since(timestamp).num_seconds();
            age_secs >= 0 && age_secs <= window_secs
        })
        .unwrap_or(false)
}

fn parse_built_tool_timestamp(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|naive| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc))
        .or_else(|| {
            chrono::DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
        })
}

pub(super) fn format_built_tool_timestamp(timestamp: chrono::DateTime<chrono::Utc>) -> String {
    timestamp.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn repair_manifest_description(tool_name: &str, summary: &str) -> String {
    let collapsed = summary.split_whitespace().collect::<Vec<_>>().join(" ");
    let collapsed = collapsed.trim();
    if collapsed.is_empty() {
        return format!("Recovered registration for self-built tool {}", tool_name);
    }
    crate::trunc(collapsed, 160).to_string()
}

fn manifest_path_for_tool_name(tool_name: &str) -> PathBuf {
    Path::new("tools").join(format!("{}{}", tool_name, BUILT_TOOL_MANIFEST_SUFFIX))
}

pub(super) fn write_built_tool_manifest(spec: &TaskSpec, operation_id: Option<&str>) -> Result<()> {
    let tool_name = tool_name_from_spec(spec)?;
    let manifest = BuiltToolManifest {
        name: tool_name.clone(),
        filename: spec.filename.clone(),
        description: spec.purpose.clone(),
        inputs: spec.inputs.clone(),
        outputs: spec.outputs.clone(),
        test_input: spec.test_input.clone(),
        test_expected: spec.test_expected.clone(),
    };
    let path = manifest_path_for_tool_name(&tool_name);
    let content = serde_json::to_string_pretty(&manifest)?;
    write_text_file_with_provenance(
        &path,
        &content,
        FileMutationProof {
            actor: "nyx",
            source: "forge.built_tools",
            action_kind: "tool_manifest_write",
            operation_id,
            description: Some(spec.purpose.as_str()),
            outcome: "committed",
            metadata: serde_json::json!({
                "tool_name": tool_name,
                "filename": spec.filename,
                "path_kind": "tool_manifest",
                "trigger": "tool_build",
            }),
        },
    )?;
    sync_core_tool_registry_rs(
        operation_id,
        "forge.built_tools",
        "core_registry_sync",
        Some("sync protected registry after tool manifest update"),
        serde_json::json!({
            "trigger": "tool_build",
            "tool_name": spec.tool_name,
            "filename": spec.filename,
        }),
    )?;
    Ok(())
}

pub fn reconcile_built_tool_registration(
    target: &str,
    summary: &str,
) -> Result<BuiltToolRegistrationRepair> {
    let tool_path = sanitize_relative_repo_path(target)?;
    if !tool_path.starts_with("tools") {
        bail!(
            "bounded self-model reconciliation only supports built tools under tools/: {}",
            tool_path.display()
        );
    }
    if tool_path.extension().and_then(|value| value.to_str()) != Some("py") {
        bail!(
            "bounded self-model reconciliation only supports Python built tools: {}",
            tool_path.display()
        );
    }
    if !tool_path.exists() {
        bail!("built tool missing: {}", tool_path.display());
    }

    let tool_name = tool_path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("invalid built tool filename: {}", tool_path.display()))?
        .to_string();
    let filename = tool_path.to_string_lossy().to_string();
    let manifest_path = manifest_path_for_tool_name(&tool_name);
    let existing_manifest = std::fs::read_to_string(&manifest_path)
        .ok()
        .and_then(|content| serde_json::from_str::<BuiltToolManifest>(&content).ok());
    let manifest_created = existing_manifest.is_none();
    let manifest = BuiltToolManifest {
        name: tool_name.clone(),
        filename: filename.clone(),
        description: existing_manifest
            .as_ref()
            .map(|manifest| manifest.description.trim())
            .filter(|description| !description.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| repair_manifest_description(&tool_name, summary)),
        inputs: existing_manifest
            .as_ref()
            .map(|manifest| manifest.inputs.clone())
            .unwrap_or_default(),
        outputs: existing_manifest
            .as_ref()
            .map(|manifest| manifest.outputs.clone())
            .unwrap_or_default(),
        test_input: existing_manifest
            .as_ref()
            .map(|manifest| manifest.test_input.clone())
            .unwrap_or_default(),
        test_expected: existing_manifest
            .as_ref()
            .map(|manifest| manifest.test_expected.clone())
            .unwrap_or_default(),
    };

    let operation_id = crate::file_provenance::operation_id();
    write_text_file_with_provenance(
        &manifest_path,
        &serde_json::to_string_pretty(&manifest)?,
        FileMutationProof {
            actor: "nyx",
            source: "forge.reconcile_self_model",
            action_kind: "tool_manifest_reconcile",
            operation_id: Some(operation_id.as_str()),
            description: Some(summary),
            outcome: "committed",
            metadata: serde_json::json!({
                "tool_name": tool_name,
                "filename": filename,
                "path_kind": "tool_manifest",
                "manifest_created": manifest_created,
                "trigger": "reconcile_self_model",
            }),
        },
    )?;
    sync_core_tool_registry_rs(
        Some(operation_id.as_str()),
        "forge.reconcile_self_model",
        "core_registry_sync",
        Some("sync protected registry after self-model reconciliation"),
        serde_json::json!({
            "trigger": "reconcile_self_model",
            "tool_name": tool_name,
            "filename": filename,
            "manifest_created": manifest_created,
        }),
    )?;

    let visible = load_registered_tools()
        .into_iter()
        .any(|tool| tool.name.eq_ignore_ascii_case(&tool_name) && tool.filename == filename);
    if !visible {
        bail!(
            "reconciled registration for {} but the capability is still not visible",
            tool_name
        );
    }

    Ok(BuiltToolRegistrationRepair {
        tool_name,
        filename,
        manifest_path: manifest_path.to_string_lossy().to_string(),
        manifest_created,
    })
}

pub(super) fn remove_built_tool_artifacts(spec: &TaskSpec, operation_id: Option<&str>) {
    std::fs::remove_file(&spec.filename).ok();
    if let Ok(tool_name) = tool_name_from_spec(spec) {
        std::fs::remove_file(manifest_path_for_tool_name(&tool_name)).ok();
    }
    sync_core_tool_registry_rs(
        operation_id,
        "forge.cleanup",
        "core_registry_sync",
        Some("sync protected registry after cleanup"),
        serde_json::json!({
            "trigger": "cleanup",
            "tool_name": spec.tool_name,
            "filename": spec.filename,
        }),
    )
    .ok();
}

fn load_registered_tools_from_manifests() -> Vec<BuiltToolManifest> {
    let tools_dir = Path::new("tools");
    let Ok(entries) = std::fs::read_dir(tools_dir) else {
        return Vec::new();
    };

    entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let file_name = path.file_name()?.to_str()?;
            if !file_name.ends_with(BUILT_TOOL_MANIFEST_SUFFIX) {
                return None;
            }
            let content = std::fs::read_to_string(&path).ok()?;
            let manifest = serde_json::from_str::<BuiltToolManifest>(&content).ok()?;
            normalize_registered_tool_manifest(manifest)
        })
        .collect()
}

pub fn load_registered_tools() -> Vec<BuiltToolManifest> {
    let mut seen = HashSet::new();
    let mut tools = Vec::new();

    for tool in load_registered_tools_from_manifests() {
        if seen.insert(tool.name.clone()) {
            tools.push(tool);
        }
    }

    for tool in crate::built_tools_registry::CORE_BUILT_TOOLS {
        let manifest = BuiltToolManifest {
            name: tool.name.to_string(),
            filename: tool.filename.to_string(),
            description: tool.description.to_string(),
            inputs: String::new(),
            outputs: String::new(),
            test_input: String::new(),
            test_expected: String::new(),
        };
        let Some(manifest) = normalize_registered_tool_manifest(manifest) else {
            continue;
        };
        if seen.insert(manifest.name.clone()) {
            tools.push(manifest);
        }
    }

    tools
}

pub fn visible_registered_tools(db: &Db) -> Vec<BuiltToolManifest> {
    load_registered_tools()
        .into_iter()
        .filter(|tool| !built_tool_is_actively_quarantined(db, &tool.name))
        .collect()
}

fn normalize_registered_tool_manifest(manifest: BuiltToolManifest) -> Option<BuiltToolManifest> {
    let path = sanitize_relative_repo_path(&manifest.filename).ok()?;
    if !path.starts_with("tools") {
        return None;
    }
    if path.extension().and_then(|value| value.to_str()) != Some("py") {
        return None;
    }
    if !path.exists() {
        return None;
    }

    Some(BuiltToolManifest {
        filename: path.to_string_lossy().to_string(),
        ..manifest
    })
}

pub async fn inspect_requested_registered_tool(
    db: &Db,
    text: &str,
) -> Option<RegisteredToolInspection> {
    let lower = text.to_lowercase();
    let text_tokens = tokenize_tool_reference(text);
    let tool = load_registered_tools()
        .into_iter()
        .find(|tool| registered_tool_name_matches(&lower, &text_tokens, &tool.name))?;

    if let Ok(Some(health)) = db.get_built_tool_health(&tool.name) {
        if health.is_currently_quarantined() {
            return Some(RegisteredToolInspection {
                name: tool.name,
                filename: tool.filename,
                healthy: false,
                issue: Some(format_built_tool_health_issue(&health)),
                health: Some(health),
            });
        }
    }

    match validate_registered_tool_manifest(&tool).await {
        Ok(()) => {
            db.clear_built_tool_health(&tool.name).ok();
            Some(RegisteredToolInspection {
                name: tool.name,
                filename: tool.filename,
                healthy: true,
                issue: None,
                health: None,
            })
        }
        Err(error) => {
            let health = record_built_tool_validation_failure(db, &tool, &error.to_string()).ok();
            let issue = health
                .as_ref()
                .map(format_built_tool_health_issue)
                .unwrap_or_else(|| error.to_string());
            Some(RegisteredToolInspection {
                name: tool.name,
                filename: tool.filename,
                healthy: false,
                issue: Some(issue),
                health,
            })
        }
    }
}

async fn validate_registered_tool_manifest(tool: &BuiltToolManifest) -> Result<()> {
    let tool_path = sanitize_relative_repo_path(&tool.filename)?;
    let input = parse_tool_input_json(&tool.test_input)?;
    run_tool_with_expectation(&tool_path, &input, &tool.test_expected).await?;
    Ok(())
}

fn built_tool_is_actively_quarantined(db: &Db, tool_name: &str) -> bool {
    db.get_built_tool_health(tool_name)
        .ok()
        .flatten()
        .map(|health| health.is_currently_quarantined())
        .unwrap_or(false)
}

fn record_built_tool_validation_failure(
    db: &Db,
    tool: &BuiltToolManifest,
    error: &str,
) -> Result<BuiltToolHealthState> {
    let now = chrono::Utc::now();
    let now_text = format_built_tool_timestamp(now);
    let existing = db.get_built_tool_health(&tool.name)?;
    let in_window = existing
        .as_ref()
        .and_then(|state| state.first_failed_at.as_deref())
        .map(|value| {
            built_tool_timestamp_within_window(value, now, BUILT_TOOL_HEALTH_FAILURE_WINDOW_SECS)
        })
        .unwrap_or(false);
    let failure_count = existing
        .as_ref()
        .map(|state| {
            if in_window {
                state.failure_count + 1
            } else {
                1
            }
        })
        .unwrap_or(1);
    let quarantined_until = if failure_count >= BUILT_TOOL_HEALTH_QUARANTINE_AFTER_FAILURES {
        Some(format_built_tool_timestamp(
            now + chrono::Duration::seconds(BUILT_TOOL_HEALTH_QUARANTINE_SECS),
        ))
    } else {
        None
    };
    let state = BuiltToolHealthState {
        tool_name: tool.name.clone(),
        filename: tool.filename.clone(),
        failure_count,
        first_failed_at: if in_window {
            existing
                .as_ref()
                .and_then(|state| state.first_failed_at.clone())
                .or(Some(now_text.clone()))
        } else {
            Some(now_text.clone())
        },
        last_failed_at: Some(now_text.clone()),
        quarantined_until,
        last_error: Some(error.to_string()),
        updated_at: now_text,
    };
    db.upsert_built_tool_health(&state)?;
    Ok(state)
}

fn format_built_tool_health_issue(state: &BuiltToolHealthState) -> String {
    let detail = state
        .last_error
        .as_deref()
        .map(|value| crate::trunc(value, 160).to_string())
        .unwrap_or_else(|| "unknown issue".into());
    if let Some(until) = state.quarantined_until.as_deref() {
        format!(
            "quarantined until {} after {} validation failure(s): {}",
            until, state.failure_count, detail
        )
    } else {
        format!(
            "validation failed {} time(s) recently: {}",
            state.failure_count, detail
        )
    }
}

fn built_tool_name_from_target(target: &str) -> Option<String> {
    let path = sanitize_relative_repo_path(target).ok()?;
    let file_name = path.file_name()?.to_str()?;
    if !file_name.ends_with(".py") {
        return None;
    }
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::to_string)
}

pub fn request_mentions_registered_tool(text: &str) -> bool {
    let lower = text.to_lowercase();
    let text_tokens = tokenize_tool_reference(text);
    load_registered_tools()
        .into_iter()
        .any(|tool| registered_tool_name_matches(&lower, &text_tokens, &tool.name))
}

pub fn built_tool_health_for_target(db: &Db, target: &str) -> Result<Option<BuiltToolHealthState>> {
    let Some(tool_name) = built_tool_name_from_target(target) else {
        return Ok(None);
    };
    db.get_built_tool_health(&tool_name)
}

pub fn list_unhealthy_built_tools(db: &Db, limit: usize) -> Result<Vec<BuiltToolHealthState>> {
    db.list_unhealthy_built_tools(limit)
}

pub fn count_unhealthy_built_tools(db: &Db) -> Result<usize> {
    db.count_unhealthy_built_tools()
}

pub fn list_registered_tool_runtime_statuses(db: &Db) -> Result<Vec<ToolRuntimeStatus>> {
    let mut statuses = Vec::new();
    for tool in load_registered_tools() {
        let path = sanitize_relative_repo_path(&tool.filename).ok();
        let health = db.get_built_tool_health(&tool.name)?;
        let issue = if let Some(health) = &health {
            if health.is_currently_quarantined() {
                Some(format_built_tool_health_issue(health))
            } else if let Some(detail) = health.last_error.as_ref() {
                Some(crate::trunc(detail, 200).to_string())
            } else {
                None
            }
        } else if path.is_none() {
            Some(format!("invalid built tool filename {}", tool.filename))
        } else if !path.as_ref().is_some_and(|candidate| candidate.is_file()) {
            Some(format!("built tool file missing at {}", tool.filename))
        } else {
            None
        };
        let status = if issue.is_some() { "blocked" } else { "ready" };
        statuses.push(ToolRuntimeStatus {
            kind: "self_built".to_string(),
            name: tool.name.clone(),
            description: tool.description.clone(),
            ready: issue.is_none(),
            status: status.to_string(),
            issue,
            requires_network: false,
            sandboxed: false,
            filename: Some(tool.filename.clone()),
            command: Some(format!("python3 {}", tool.filename)),
            server_name: None,
            source: Some("forge".to_string()),
            quarantined_until: health.and_then(|state| state.quarantined_until),
        });
    }
    statuses.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(statuses)
}

fn registered_tool_name_matches(text_lower: &str, text_tokens: &[String], tool_name: &str) -> bool {
    let tool_lower = tool_name.to_lowercase();
    if contains_bounded_tool_name(text_lower, &tool_lower) {
        return true;
    }

    let tool_tokens = tokenize_tool_reference(tool_name);
    !tool_tokens.is_empty() && token_sequence_present(text_tokens, &tool_tokens)
}

fn contains_bounded_tool_name(text: &str, needle: &str) -> bool {
    if needle.trim().is_empty() {
        return false;
    }

    text.match_indices(needle).any(|(index, _)| {
        let starts_cleanly = text[..index]
            .chars()
            .next_back()
            .map(|ch| !is_tool_reference_char(ch))
            .unwrap_or(true);
        let end = index + needle.len();
        let ends_cleanly = text[end..]
            .chars()
            .next()
            .map(|ch| !is_tool_reference_char(ch))
            .unwrap_or(true);
        starts_cleanly && ends_cleanly
    })
}

fn tokenize_tool_reference(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for ch in text.chars() {
        if is_tool_reference_char(ch) {
            current.extend(ch.to_lowercase());
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn token_sequence_present(haystack: &[String], needle: &[String]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }

    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn is_tool_reference_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '-'
}

pub fn tools_for_prompt(db: &Db) -> String {
    let tools = visible_registered_tools(db);
    if tools.is_empty() {
        return String::new();
    }

    let lines: Vec<String> = tools
        .into_iter()
        .map(|tool| format!("- {} (built): {}", tool.name, tool.description))
        .collect();

    format!(
        "<built_tools>\nAvailable self-built tools:\n{}\n</built_tools>",
        lines.join("\n")
    )
}

pub async fn run_registered_tool_checked(
    db: &Db,
    name: &str,
    args: &serde_json::Value,
) -> Option<Result<serde_json::Value>> {
    let tool = load_registered_tools()
        .into_iter()
        .find(|tool| tool.name == name)?;
    if let Ok(Some(health)) = db.get_built_tool_health(name) {
        if health.is_currently_quarantined() {
            return Some(Err(anyhow!(
                "built tool {} is quarantined until {}",
                name,
                health
                    .quarantined_until
                    .unwrap_or_else(|| "unknown time".into())
            )));
        }
    }
    let tool_path = sanitize_relative_repo_path(&tool.filename).ok()?;
    Some(run_built_tool_file(&tool_path, args).await)
}

pub async fn run_registered_tool(
    name: &str,
    args: &serde_json::Value,
) -> Option<Result<serde_json::Value>> {
    let tool = load_registered_tools()
        .into_iter()
        .find(|tool| tool.name == name)?;
    let tool_path = sanitize_relative_repo_path(&tool.filename).ok()?;
    Some(run_built_tool_file(&tool_path, args).await)
}

fn core_registry_path() -> PathBuf {
    std::env::var("NYX_BUILT_TOOL_CORE_REGISTRY")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("src/built_tools_registry.rs"))
}

pub(super) fn sync_core_tool_registry_rs(
    operation_id: Option<&str>,
    source: &str,
    action_kind: &str,
    description: Option<&str>,
    metadata: serde_json::Value,
) -> Result<()> {
    let path = core_registry_path();

    let tools = load_registered_tools_from_manifests();
    let entries = tools
        .iter()
        .map(|tool| {
            format!(
                "    CoreBuiltTool {{ name: {:?}, filename: {:?}, description: {:?} }},",
                tool.name, tool.filename, tool.description
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let content = format!(
        "//! Auto-generated registry of self-built tools promoted into core.\n\
//!\n\
//! Forge rewrites this file after a successful build-and-smoke-test cycle.\n\
//! The runtime still uses manifest files for immediate access; this registry\n\
//! is the protected-core copy that becomes part of the compiled system.\n\n\
#[derive(Debug, Clone, Copy)]\n\
pub struct CoreBuiltTool {{\n\
    pub name: &'static str,\n\
    pub filename: &'static str,\n\
    pub description: &'static str,\n\
}}\n\n\
pub static CORE_BUILT_TOOLS: &[CoreBuiltTool] = &[\n\
{}\n\
];\n",
        entries
    );

    write_text_file_with_provenance(
        &path,
        &content,
        FileMutationProof {
            actor: "nyx",
            source,
            action_kind,
            operation_id,
            description,
            outcome: "committed",
            metadata: {
                let mut object = metadata.as_object().cloned().unwrap_or_default();
                object.insert(
                    "path_kind".to_string(),
                    serde_json::json!("protected_core_registry"),
                );
                object.insert(
                    "registered_tool_count".to_string(),
                    serde_json::json!(tools.len()),
                );
                serde_json::Value::Object(object)
            },
        },
    )?;
    Ok(())
}
