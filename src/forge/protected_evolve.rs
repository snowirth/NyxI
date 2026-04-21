use anyhow::{Context, Result, anyhow, bail};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::sandbox::{SandboxSpec, shared_host_sandbox};
use crate::llm::LlmGate;

use super::{
    ChangeReport, EvolvePlanPreview, EvolvePreflight, EvolveResult, EvolveTelemetry, ForgeLlmUsage,
    MAX_REPAIR_ATTEMPTS, VerificationStepPreview, accept_repair_candidate, normalize_compare_text,
    sanitize_relative_repo_path,
};

#[derive(Debug, Clone)]
pub(super) struct PlannedChange {
    pub(super) path: String,
    pub(super) search: String,
    pub(super) replace: String,
    pub(super) description: String,
}

#[derive(Debug)]
pub(super) struct ProtectedEditOutcome {
    pub(super) path: String,
    pub(super) repair_rounds: usize,
    pub(super) change_summary: String,
    pub(super) change_report: ChangeReport,
}

#[derive(Debug)]
pub(super) struct ProtectedEditFailure {
    pub(super) reason: String,
    pub(super) executed: bool,
}

impl std::fmt::Display for ProtectedEditFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.executed {
            write!(f, "{} (after partial execution)", self.reason)
        } else {
            f.write_str(&self.reason)
        }
    }
}

pub(super) fn build_evolve_preflight(
    llm: &LlmGate,
    user_request: &str,
    plan: &PlannedChange,
) -> EvolvePreflight {
    let mut validations = Vec::new();
    let change_summary = Some(summarize_planned_change(plan));
    let change_report = Some(build_change_report(plan));
    let intent_model = llm.preferred_chat_model_label();
    let implementation_model = llm.preferred_autonomous_model_label();

    let rel_path = match sanitize_relative_repo_path(&plan.path) {
        Ok(path) => path,
        Err(error) => {
            return EvolvePreflight {
                user_request: user_request.to_string(),
                intent_model,
                implementation_model,
                ready: false,
                issue: Some(error.to_string()),
                plan: None,
                path_exists: false,
                search_found: false,
                verification_mode: EvolveTelemetry::default().verification_mode,
                verification_steps: Vec::new(),
                validations,
                change_summary,
                change_report,
            };
        }
    };

    let protected_core = is_protected_core_path(&rel_path);
    let project_root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let full_path = project_root.join(&rel_path);
    let path_exists = full_path.exists();
    if path_exists {
        validations.push("target path exists".to_string());
    }

    let mut search_found = false;
    let mut issue = None;
    let mut verification_steps = Vec::new();
    let verification_mode = if protected_core {
        current_self_edit_verification_mode()
    } else {
        EvolveTelemetry::default().verification_mode
    };

    if !path_exists {
        issue = Some(format!("file not found: {}", rel_path.display()));
    } else {
        match std::fs::read_to_string(&full_path) {
            Ok(content) => {
                if content.contains(&plan.search) {
                    search_found = true;
                    validations.push("search text found in target file".to_string());
                    if protected_core {
                        let updated = content.replacen(&plan.search, &plan.replace, 1);
                        match validate_protected_content_preview(&rel_path, &updated) {
                            Ok(()) => validations
                                .push("protected candidate passes static validation".to_string()),
                            Err(error) => issue = Some(error.to_string()),
                        }
                        verification_steps =
                            verification_specs_for_protected_path(&project_root, &rel_path)
                                .into_iter()
                                .map(|spec| VerificationStepPreview {
                                    label: spec.label,
                                    program: spec.program,
                                    args: spec.args,
                                    cwd: spec.cwd.to_string_lossy().to_string(),
                                    env_keys: spec.envs.into_iter().map(|(key, _)| key).collect(),
                                })
                                .collect();
                    } else {
                        validations.push(
                            "non-protected edit will execute through file_ops evolve with rollback"
                                .to_string(),
                        );
                    }
                } else {
                    issue = Some("search text not found".to_string());
                }
            }
            Err(error) => issue = Some(format!("failed to read {}: {}", rel_path.display(), error)),
        }
    }

    let plan_preview = Some(EvolvePlanPreview {
        path: rel_path.to_string_lossy().to_string(),
        protected_core,
        description: plan.description.clone(),
        search_preview: preview_change_text(&plan.search, 140),
        replace_preview: preview_change_text(&plan.replace, 140),
    });

    EvolvePreflight {
        user_request: user_request.to_string(),
        intent_model,
        implementation_model,
        ready: issue.is_none(),
        issue,
        plan: plan_preview,
        path_exists,
        search_found,
        verification_mode,
        verification_steps,
        validations,
        change_summary,
        change_report,
    }
}

pub(super) async fn execute_evolve_plan(
    llm: &Arc<LlmGate>,
    plan: &PlannedChange,
    mut telemetry: EvolveTelemetry,
) -> EvolveResult {
    if let Ok(rel_path) = sanitize_relative_repo_path(&plan.path) {
        if is_protected_core_path(&rel_path) {
            let project_root = match std::env::current_dir() {
                Ok(path) => path,
                Err(error) => {
                    return EvolveResult::Failed {
                        reason: error.to_string(),
                        telemetry,
                    };
                }
            };
            return match apply_protected_change_with_self_repair(
                &project_root,
                llm,
                plan,
                &mut telemetry.llm_usage,
            )
            .await
            {
                Ok(outcome) => {
                    let mut telemetry =
                        protected_evolve_telemetry(telemetry.llm_usage, outcome.repair_rounds);
                    telemetry.set_execution_status(true, Some(true));
                    EvolveResult::Success {
                        path: outcome.path,
                        description: plan.description.clone(),
                        change_summary: outcome.change_summary,
                        change_report: outcome.change_report,
                        telemetry,
                    }
                }
                Err(error) => {
                    telemetry.set_execution_status(error.executed, error.executed.then_some(false));
                    EvolveResult::Failed {
                        reason: error.reason,
                        telemetry,
                    }
                }
            };
        }
    }

    match apply_planned_change(plan).await {
        EvolveResult::Success {
            path,
            description,
            change_summary,
            change_report,
            telemetry: result_telemetry,
        } => {
            telemetry.set_execution_status(result_telemetry.executed, result_telemetry.verified);
            EvolveResult::Success {
                path,
                description,
                change_summary,
                change_report,
                telemetry,
            }
        }
        EvolveResult::Failed {
            reason,
            telemetry: result_telemetry,
        } => {
            telemetry.set_execution_status(result_telemetry.executed, result_telemetry.verified);
            EvolveResult::Failed { reason, telemetry }
        }
    }
}

pub(super) fn blocked_evolve_preflight(user_request: &str, issue: &str) -> EvolvePreflight {
    EvolvePreflight {
        user_request: user_request.to_string(),
        intent_model: "unavailable".to_string(),
        implementation_model: "unavailable".to_string(),
        ready: false,
        issue: Some(issue.to_string()),
        plan: None,
        path_exists: false,
        search_found: false,
        verification_mode: EvolveTelemetry::default().verification_mode,
        verification_steps: Vec::new(),
        validations: Vec::new(),
        change_summary: None,
        change_report: None,
    }
}

pub(super) fn telemetry_for_preflight(
    preflight: &EvolvePreflight,
    llm_usage: ForgeLlmUsage,
    repair_rounds: usize,
) -> EvolveTelemetry {
    if preflight
        .plan
        .as_ref()
        .map(|plan| plan.protected_core)
        .unwrap_or(false)
    {
        protected_evolve_telemetry(llm_usage, repair_rounds)
    } else {
        EvolveTelemetry {
            llm_usage,
            ..EvolveTelemetry::default()
        }
    }
}

async fn apply_planned_change(plan: &PlannedChange) -> EvolveResult {
    let change_report = build_change_report(plan);
    match crate::tools::run(
        "file_ops",
        &serde_json::json!({
            "action": "evolve",
            "path": plan.path,
            "search": plan.search,
            "replace": plan.replace,
            "description": plan.description
        }),
    )
    .await
    {
        Ok(result) => {
            let output = result["output"]
                .as_str()
                .unwrap_or(result["error"].as_str().unwrap_or("failed"));
            if result["error"].is_string() {
                let mut telemetry = EvolveTelemetry::default();
                telemetry.set_execution_status(false, None);
                EvolveResult::Failed {
                    reason: output.to_string(),
                    telemetry,
                }
            } else {
                let mut telemetry = EvolveTelemetry::default();
                telemetry.set_execution_status(true, Some(true));
                EvolveResult::Success {
                    path: plan.path.clone(),
                    description: plan.description.clone(),
                    change_summary: summarize_planned_change(plan),
                    change_report,
                    telemetry,
                }
            }
        }
        Err(error) => {
            let mut telemetry = EvolveTelemetry::default();
            telemetry.set_execution_status(false, None);
            EvolveResult::Failed {
                reason: error.to_string(),
                telemetry,
            }
        }
    }
}

#[cfg(test)]
pub(super) fn apply_protected_change_in_project(
    project_root: &Path,
    plan: &PlannedChange,
) -> Result<String> {
    let rel_path = sanitize_relative_repo_path(&plan.path)?;
    if !is_protected_core_path(&rel_path) {
        bail!("protected core edit not allowed: {}", rel_path.display());
    }

    let full_path = project_root.join(&rel_path);
    if !full_path.exists() {
        bail!("file not found: {}", rel_path.display());
    }

    let content = std::fs::read_to_string(&full_path)
        .with_context(|| format!("failed to read {}", rel_path.display()))?;
    if !content.contains(&plan.search) {
        bail!("search text not found");
    }

    let updated = content.replacen(&plan.search, &plan.replace, 1);
    let backup_path = backup_path_for(&full_path);
    std::fs::copy(&full_path, &backup_path)
        .with_context(|| format!("failed to create backup for {}", rel_path.display()))?;

    let write_result = (|| -> Result<()> {
        std::fs::write(&full_path, &updated)
            .with_context(|| format!("failed to write {}", rel_path.display()))?;
        validate_protected_file(&rel_path, &full_path, &updated)?;
        Ok(())
    })();

    match write_result {
        Ok(()) => {
            std::fs::remove_file(&backup_path).ok();
            Ok(rel_path.to_string_lossy().to_string())
        }
        Err(error) => {
            std::fs::copy(&backup_path, &full_path).ok();
            std::fs::remove_file(&backup_path).ok();
            Err(error)
        }
    }
}

pub(super) async fn apply_protected_change_with_self_repair(
    project_root: &Path,
    llm: &Arc<LlmGate>,
    plan: &PlannedChange,
    llm_usage: &mut ForgeLlmUsage,
) -> std::result::Result<ProtectedEditOutcome, ProtectedEditFailure> {
    let rel_path =
        sanitize_relative_repo_path(&plan.path).map_err(|error| ProtectedEditFailure {
            reason: error.to_string(),
            executed: false,
        })?;
    if !is_protected_core_path(&rel_path) {
        return Err(ProtectedEditFailure {
            reason: format!("protected core edit not allowed: {}", rel_path.display()),
            executed: false,
        });
    }

    let full_path = project_root.join(&rel_path);
    if !full_path.exists() {
        return Err(ProtectedEditFailure {
            reason: format!("file not found: {}", rel_path.display()),
            executed: false,
        });
    }

    let original_content = std::fs::read_to_string(&full_path)
        .with_context(|| format!("failed to read {}", rel_path.display()))
        .map_err(|error| ProtectedEditFailure {
            reason: error.to_string(),
            executed: false,
        })?;
    if !original_content.contains(&plan.search) {
        return Err(ProtectedEditFailure {
            reason: "search text not found".to_string(),
            executed: false,
        });
    }

    let backup_path = backup_path_for(&full_path);
    std::fs::copy(&full_path, &backup_path)
        .with_context(|| format!("failed to create backup for {}", rel_path.display()))
        .map_err(|error| ProtectedEditFailure {
            reason: error.to_string(),
            executed: false,
        })?;

    let mut current_content = original_content.replacen(&plan.search, &plan.replace, 1);
    let mut feedback: Option<String> = None;
    let mut seen_states = HashSet::new();
    let mut repair_attempts = 0usize;
    let mut executed = false;

    loop {
        let state_fingerprint = format!(
            "{}\n---feedback---\n{}",
            normalize_compare_text(&current_content),
            feedback.as_deref().unwrap_or("")
        );
        if !seen_states.insert(state_fingerprint) {
            restore_backup(&backup_path, &full_path);
            return Err(ProtectedEditFailure {
                reason: "self-repair loop stalled: repeated state with no progress".to_string(),
                executed,
            });
        }

        executed = true;
        match write_and_verify_protected_candidate(
            project_root,
            &rel_path,
            &full_path,
            &current_content,
        )
        .await
        {
            Ok(()) => {
                std::fs::remove_file(&backup_path).ok();
                return Ok(ProtectedEditOutcome {
                    path: rel_path.to_string_lossy().to_string(),
                    repair_rounds: repair_attempts,
                    change_summary: summarize_content_change(&original_content, &current_content),
                    change_report: build_change_report_from_contents(
                        &rel_path.to_string_lossy(),
                        &original_content,
                        &current_content,
                        &plan.description,
                    ),
                });
            }
            Err(issues) => {
                if repair_attempts >= MAX_REPAIR_ATTEMPTS {
                    restore_backup(&backup_path, &full_path);
                    return Err(ProtectedEditFailure {
                        reason: format!(
                            "self-repair budget exhausted after {} rounds: {}",
                            MAX_REPAIR_ATTEMPTS,
                            crate::trunc(&issues, 160)
                        ),
                        executed,
                    });
                }
                tracing::info!(
                    "forge: protected verification failed for {} (round {})",
                    rel_path.display(),
                    repair_attempts + 1
                );
                feedback = Some(issues.clone());
                current_content = match repair_protected_file_content(
                    llm,
                    &rel_path,
                    plan,
                    &current_content,
                    &issues,
                    repair_attempts,
                    llm_usage,
                )
                .await
                {
                    Ok(code) => code,
                    Err(error) => {
                        restore_backup(&backup_path, &full_path);
                        return Err(ProtectedEditFailure {
                            reason: format!("fix failed: {}", error),
                            executed,
                        });
                    }
                };
                repair_attempts += 1;
            }
        }
    }
}

fn restore_backup(backup_path: &Path, full_path: &Path) {
    std::fs::copy(backup_path, full_path).ok();
    std::fs::remove_file(backup_path).ok();
}

async fn write_and_verify_protected_candidate(
    project_root: &Path,
    rel_path: &Path,
    full_path: &Path,
    content: &str,
) -> std::result::Result<(), String> {
    std::fs::write(full_path, content)
        .map_err(|error| format!("failed to write {}: {}", rel_path.display(), error))?;
    validate_protected_file(rel_path, full_path, content).map_err(|error| error.to_string())?;
    verify_protected_change(project_root, rel_path).await
}

async fn verify_protected_change(
    project_root: &Path,
    rel_path: &Path,
) -> std::result::Result<(), String> {
    for spec in verification_specs_for_protected_path(project_root, rel_path) {
        run_verification_command(&spec).await?;
    }
    Ok(())
}

struct VerificationCommandSpec {
    program: String,
    args: Vec<String>,
    envs: Vec<(String, String)>,
    cwd: PathBuf,
    label: String,
}

fn verification_specs_for_protected_path(
    project_root: &Path,
    rel_path: &Path,
) -> Vec<VerificationCommandSpec> {
    if syntax_only_self_edit_verification() {
        return Vec::new();
    }

    let file_name = rel_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");

    match rel_path.extension().and_then(|ext| ext.to_str()) {
        Some("rs") => vec![cargo_test_spec(project_root)],
        Some("py") => vec![VerificationCommandSpec {
            program: "python3".into(),
            args: vec![
                "-m".into(),
                "py_compile".into(),
                rel_path.to_string_lossy().to_string(),
            ],
            envs: Vec::new(),
            cwd: project_root.to_path_buf(),
            label: format!("python3 -m py_compile {}", rel_path.display()),
        }],
        Some("toml") if file_name == "Cargo.toml" || file_name == "Cargo.lock" => {
            vec![cargo_test_spec(project_root)]
        }
        _ => Vec::new(),
    }
}

fn syntax_only_self_edit_verification() -> bool {
    std::env::var("NYX_SELF_EDIT_VERIFY_MODE")
        .map(|value| {
            let normalized = value.trim().to_lowercase();
            normalized == "syntax" || normalized == "light"
        })
        .unwrap_or(false)
}

fn current_self_edit_verification_mode() -> String {
    if syntax_only_self_edit_verification() {
        "syntax".into()
    } else {
        "full".into()
    }
}

fn protected_evolve_telemetry(llm_usage: ForgeLlmUsage, repair_rounds: usize) -> EvolveTelemetry {
    EvolveTelemetry {
        protected_core: true,
        repair_rounds,
        strategy: "self_repair".into(),
        verification_mode: current_self_edit_verification_mode(),
        executed: false,
        verified: None,
        llm_usage,
    }
}

fn cargo_test_spec(project_root: &Path) -> VerificationCommandSpec {
    VerificationCommandSpec {
        program: "cargo".into(),
        args: vec!["test".into(), "-q".into()],
        envs: vec![(
            "CARGO_TARGET_DIR".into(),
            project_root
                .join("target")
                .join("nyx_self_edit")
                .to_string_lossy()
                .to_string(),
        )],
        cwd: project_root.to_path_buf(),
        label: "cargo test -q".into(),
    }
}

async fn run_verification_command(
    spec: &VerificationCommandSpec,
) -> std::result::Result<(), String> {
    let mut command = tokio::process::Command::new(&spec.program);
    command
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (key, value) in &spec.envs {
        command.env(key, value);
    }

    let child = command
        .spawn()
        .map_err(|error| format!("failed to start {}: {}", spec.label, error))?;

    let output = tokio::time::timeout(
        std::time::Duration::from_secs(180),
        child.wait_with_output(),
    )
    .await
    .map_err(|_| format!("verification timed out: {}", spec.label))?
    .map_err(|error| format!("verification failed to run {}: {}", spec.label, error))?;

    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let detail = if !stderr.is_empty() && !stdout.is_empty() {
        format!("stdout:\n{}\n\nstderr:\n{}", stdout, stderr)
    } else if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        "no output".into()
    };
    Err(format!(
        "verification command failed: {}\n{}",
        spec.label, detail
    ))
}

async fn repair_protected_file_content(
    llm: &Arc<LlmGate>,
    rel_path: &Path,
    plan: &PlannedChange,
    current_content: &str,
    feedback: &str,
    repair_round: usize,
    llm_usage: &mut ForgeLlmUsage,
) -> Result<String> {
    let fix_prompt = format!(
        "Fix this protected project file after Nyx changed it and verification failed.\n\n\
         File: {}\n\
         Requested change: {}\n\
         Original replacement intent: replace `{}` with `{}`\n\
         Repair round: {}\n\n\
         Verification failure:\n{}\n\n\
         Current file contents:\n{}\n\n\
         Reply with ONLY the full corrected contents of this file, no markdown fences.",
        rel_path.display(),
        plan.description,
        preview_change_text(&plan.search, 120),
        preview_change_text(&plan.replace, 120),
        repair_round + 1,
        crate::trunc(feedback, 4000),
        crate::trunc(current_content, 6000),
    );
    let current_norm = normalize_compare_text(current_content);
    let stage = format!("repair_protected_round_{}", repair_round + 1);

    if llm.has_ollama() {
        if let Ok(candidate) = llm.chat_ollama_direct_traced(&fix_prompt, 1200).await {
            llm_usage.record(stage.clone(), &candidate.trace);
            if let Some(code) = accept_repair_candidate(&candidate.text, &current_norm) {
                return Ok(code);
            }
        }
    }

    if llm.has_nim() {
        if let Ok(candidate) = llm.chat_nim_direct_traced(&fix_prompt, 1200).await {
            llm_usage.record(stage.clone(), &candidate.trace);
            if let Some(code) = accept_repair_candidate(&candidate.text, &current_norm) {
                return Ok(code);
            }
        }
    }

    if llm.has_anthropic() {
        if let Ok(candidate) = llm.chat_anthropic_direct_traced(&fix_prompt, 1200).await {
            llm_usage.record(stage.clone(), &candidate.trace);
            if let Some(code) = accept_repair_candidate(&candidate.text, &current_norm) {
                return Ok(code);
            }
        }
    }

    if let Ok(candidate) = llm.chat_auto_with_fallback_traced(&fix_prompt, 1200).await {
        llm_usage.record(stage.clone(), &candidate.trace);
        if let Some(code) = accept_repair_candidate(&candidate.text, &current_norm) {
            return Ok(code);
        }
    }

    let fallback = llm.chat_traced(&fix_prompt, 1200).await?;
    llm_usage.record(stage, &fallback.trace);
    accept_repair_candidate(&fallback.text, &current_norm)
        .ok_or_else(|| anyhow!("all repair providers returned unchanged or invalid file content"))
}

fn is_protected_core_path(path: &Path) -> bool {
    path.starts_with("src")
        || path.starts_with("agents")
        || path == Path::new("IDENTITY.md")
        || path == Path::new("SOUL.md")
        || path == Path::new("Cargo.toml")
        || path == Path::new("Cargo.lock")
}

pub(super) fn backup_path_for(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.bak", path.display()))
}

fn validate_protected_file(rel_path: &Path, full_path: &Path, content: &str) -> Result<()> {
    let file_name = rel_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");

    if file_name == "Cargo.toml" || file_name == "Cargo.lock" {
        toml::from_str::<toml::Value>(content)
            .map_err(|error| anyhow!("invalid TOML in {}: {}", rel_path.display(), error))?;
        return Ok(());
    }

    match rel_path.extension().and_then(|ext| ext.to_str()) {
        Some("rs") => {
            syn::parse_file(content).map_err(|error| {
                anyhow!("invalid Rust syntax in {}: {}", rel_path.display(), error)
            })?;
        }
        Some("py") => validate_python_file(full_path)?,
        Some("md") if file_name == "SOUL.md" || file_name == "IDENTITY.md" => {
            if content.trim().is_empty() {
                bail!("{} cannot be empty", file_name);
            }
        }
        Some("toml") => {
            toml::from_str::<toml::Value>(content)
                .map_err(|error| anyhow!("invalid TOML in {}: {}", rel_path.display(), error))?;
        }
        _ => {}
    }

    Ok(())
}

fn validate_protected_content_preview(rel_path: &Path, content: &str) -> Result<()> {
    let file_name = rel_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");

    if file_name == "Cargo.toml" || file_name == "Cargo.lock" {
        toml::from_str::<toml::Value>(content)
            .map_err(|error| anyhow!("invalid TOML in {}: {}", rel_path.display(), error))?;
        return Ok(());
    }

    match rel_path.extension().and_then(|ext| ext.to_str()) {
        Some("rs") => {
            syn::parse_file(content).map_err(|error| {
                anyhow!("invalid Rust syntax in {}: {}", rel_path.display(), error)
            })?;
        }
        Some("py") => validate_python_content(rel_path, content)?,
        Some("md") if file_name == "SOUL.md" || file_name == "IDENTITY.md" => {
            if content.trim().is_empty() {
                bail!("{} cannot be empty", file_name);
            }
        }
        Some("toml") => {
            toml::from_str::<toml::Value>(content)
                .map_err(|error| anyhow!("invalid TOML in {}: {}", rel_path.display(), error))?;
        }
        _ => {}
    }

    Ok(())
}

fn validate_python_file(path: &Path) -> Result<()> {
    let spec = SandboxSpec::new([
        "-c".to_string(),
        "import pathlib, sys; compile(pathlib.Path(sys.argv[1]).read_text(), sys.argv[1], 'exec')"
            .to_string(),
        path.to_string_lossy().to_string(),
    ]);
    let output = shared_host_sandbox()
        .run_python_blocking(&spec)
        .with_context(|| format!("failed to run python3 for {}", path.display()))?;

    if output.success {
        return Ok(());
    }

    let stderr = output.stderr_str();
    let stdout = output.stdout_str();
    let detail = stderr.trim().if_empty_then(stdout.trim());
    if detail.is_empty() {
        bail!("invalid Python syntax in {}", path.display());
    }
    bail!("invalid Python syntax in {}: {}", path.display(), detail);
}

fn validate_python_content(path: &Path, content: &str) -> Result<()> {
    let spec = SandboxSpec::new([
        "-c".to_string(),
        "import sys; compile(sys.stdin.read(), sys.argv[1], 'exec')".to_string(),
        path.to_string_lossy().to_string(),
    ])
    .stdin_bytes(content.as_bytes().to_vec());

    let output = shared_host_sandbox()
        .run_python_blocking(&spec)
        .with_context(|| format!("failed to run python3 for {}", path.display()))?;

    if output.success {
        return Ok(());
    }

    let stderr = output.stderr_str();
    let stdout = output.stdout_str();
    let detail = stderr.trim().if_empty_then(stdout.trim());
    if detail.is_empty() {
        bail!("invalid Python syntax in {}", path.display());
    }
    bail!("invalid Python syntax in {}: {}", path.display(), detail);
}

pub(super) fn summarize_planned_change(plan: &PlannedChange) -> String {
    format!(
        "replaced `{}` with `{}`",
        preview_change_text(&plan.search, 70),
        preview_change_text(&plan.replace, 70),
    )
}

fn summarize_content_change(before: &str, after: &str) -> String {
    let (_, before_chunk, after_chunk) = change_region(before, after);
    format!(
        "replaced `{}` with `{}`",
        preview_change_text(&before_chunk, 70),
        preview_change_text(&after_chunk, 70),
    )
}

fn build_change_report(plan: &PlannedChange) -> ChangeReport {
    ChangeReport {
        path: plan.path.clone(),
        line: locate_change_start_line(&plan.path, &plan.search),
        before: preview_change_text(&plan.search, 140),
        after: preview_change_text(&plan.replace, 140),
        explanation: if plan.description.trim().is_empty() {
            summarize_planned_change(plan)
        } else {
            plan.description.trim().to_string()
        },
    }
}

fn build_change_report_from_contents(
    path: &str,
    before: &str,
    after: &str,
    explanation: &str,
) -> ChangeReport {
    let (line, before_chunk, after_chunk) = change_region(before, after);
    ChangeReport {
        path: path.to_string(),
        line,
        before: preview_change_text(&before_chunk, 140),
        after: preview_change_text(&after_chunk, 140),
        explanation: if explanation.trim().is_empty() {
            summarize_content_change(before, after)
        } else {
            explanation.trim().to_string()
        },
    }
}

fn change_region(before: &str, after: &str) -> (Option<usize>, String, String) {
    if before == after {
        return (None, before.to_string(), after.to_string());
    }

    let before_lines: Vec<&str> = before.lines().collect();
    let after_lines: Vec<&str> = after.lines().collect();
    let mut prefix = 0usize;
    let max_prefix = before_lines.len().min(after_lines.len());
    while prefix < max_prefix && before_lines[prefix] == after_lines[prefix] {
        prefix += 1;
    }

    let mut before_suffix = before_lines.len();
    let mut after_suffix = after_lines.len();
    while before_suffix > prefix
        && after_suffix > prefix
        && before_lines[before_suffix - 1] == after_lines[after_suffix - 1]
    {
        before_suffix -= 1;
        after_suffix -= 1;
    }

    (
        Some(prefix + 1),
        before_lines[prefix..before_suffix].join("\n"),
        after_lines[prefix..after_suffix].join("\n"),
    )
}

fn locate_change_start_line(path: &str, search: &str) -> Option<usize> {
    if path.trim().is_empty() || search.is_empty() {
        return None;
    }

    let content = std::fs::read_to_string(path).ok()?;
    let index = content.find(search)?;
    Some(
        content[..index]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1,
    )
}

fn preview_change_text(text: &str, max: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return "(empty)".into();
    }
    if collapsed.len() <= max {
        return collapsed;
    }

    let keep = max.saturating_sub(3);
    format!("{}...", crate::trunc(&collapsed, keep))
}

trait EmptyFallback {
    fn if_empty_then<'a>(&'a self, fallback: &'a str) -> &'a str;
}

impl EmptyFallback for str {
    fn if_empty_then<'a>(&'a self, fallback: &'a str) -> &'a str {
        if self.is_empty() { fallback } else { self }
    }
}
