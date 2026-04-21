use anyhow::{Result, anyhow, bail};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use crate::llm::LlmGate;

use super::{
    BuildPreflight, BuildSpecPreview, ForgeLlmUsage, ForgeResult, ForgeTelemetry,
    MAX_REPAIR_ATTEMPTS, TaskSpec, ToolRunPreview, accept_repair_candidate,
    built_tool_manifest_path_for, chat_forge_intent_lane_traced, normalize_compare_text,
    parse_json_response, parse_tool_input_json, remove_built_tool_artifacts, run_built_tool_file,
    run_tool_with_expectation, sanitize_relative_repo_path, strip_code_fences, tool_name_from_spec,
    write_built_tool_manifest,
};

enum VerifyResult {
    Pass,
    Fail(String),
}

pub(super) async fn plan_tool_build(
    llm: &Arc<LlmGate>,
    user_request: &str,
    nyx_response: &str,
    llm_usage: &mut ForgeLlmUsage,
) -> Result<TaskSpec> {
    let intent_prompt = format!(
        "A user asked an AI assistant something it can't do yet. We need to build a Python tool.\n\n\
         User asked: \"{}\"\nAssistant said: \"{}\"\n\n\
         Define what this tool should do from the USER's perspective:\n\
         1. TOOL_NAME: short snake_case name\n\
         2. PURPOSE: what it does in one sentence\n\
         3. INPUTS: what JSON fields it expects on stdin\n\
         4. OUTPUTS: what JSON fields it returns on stdout\n\
         5. TEST_INPUT: example JSON input to verify it works\n\
         6. TEST_EXPECTED: what a correct output should contain\n\n\
         Reply as JSON only, no markdown.",
        user_request,
        crate::trunc(nyx_response, 100)
    );

    let implementation_prompt = format!(
        "We need a Python tool (reads JSON from stdin, prints JSON to stdout).\n\n\
         Task: \"{}\"\n\n\
         Plan the implementation:\n\
         1. APPROACH: how to implement this (which Python stdlib modules, what algorithm)\n\
         2. EDGE_CASES: what could go wrong\n\
         3. FILENAME: tools/name.py\n\n\
         Only use these imports: json, sys, os.path, urllib.request, urllib.parse, \
         datetime, math, re, hashlib, base64, time, pathlib, collections, itertools, functools.\n\n\
         Reply as JSON only, no markdown.",
        user_request
    );

    let (intent_result, implementation_result) = tokio::join!(
        chat_forge_intent_lane_traced(llm, &intent_prompt, 200),
        llm.chat_auto_with_fallback_traced(&implementation_prompt, 200)
    );

    let intent_response = intent_result.map_err(|error| {
        anyhow!(
            "forge intent lane ({}) failed: {}",
            llm_usage.predicted_intent_model,
            error
        )
    })?;
    llm_usage.record("plan_tool_intent", &intent_response.trace);
    let intent_spec = parse_json_response(&intent_response.text);

    let implementation_response = implementation_result.map_err(|error| {
        anyhow!(
            "forge implementation lane ({}) failed: {}",
            llm_usage.predicted_implementation_model,
            error
        )
    })?;
    llm_usage.record("plan_tool_implementation", &implementation_response.trace);
    let implementation_spec = parse_json_response(&implementation_response.text);

    let raw_tool_name = intent_spec["TOOL_NAME"]
        .as_str()
        .or(intent_spec["tool_name"].as_str())
        .unwrap_or("");
    let tool_name = normalize_tool_name(raw_tool_name, user_request);
    let default_filename = format!("tools/{}.py", tool_name);
    let filename = implementation_spec["FILENAME"]
        .as_str()
        .or(implementation_spec["filename"].as_str())
        .unwrap_or(&default_filename);

    let purpose = intent_spec["PURPOSE"]
        .as_str()
        .or(intent_spec["purpose"].as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let inputs = intent_spec["INPUTS"]
        .as_str()
        .or(intent_spec["inputs"].as_str())
        .unwrap_or("{}")
        .trim()
        .to_string();
    let outputs = intent_spec["OUTPUTS"]
        .as_str()
        .or(intent_spec["outputs"].as_str())
        .unwrap_or("{}")
        .trim()
        .to_string();
    let approach = implementation_spec["APPROACH"]
        .as_str()
        .or(implementation_spec["approach"].as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| synthesize_tool_approach(user_request, &purpose, &inputs, &outputs));

    Ok(TaskSpec {
        tool_name,
        filename: filename.to_string(),
        purpose,
        inputs,
        outputs,
        approach,
        test_input: intent_spec["TEST_INPUT"]
            .as_str()
            .or(intent_spec["test_input"].as_str())
            .unwrap_or("{}")
            .to_string(),
        test_expected: intent_spec["TEST_EXPECTED"]
            .as_str()
            .or(intent_spec["test_expected"].as_str())
            .unwrap_or("")
            .to_string(),
    })
}

fn normalize_tool_name(raw_tool_name: &str, user_request: &str) -> String {
    let cleaned = raw_tool_name
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let collapsed = cleaned
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    if !collapsed.is_empty() && collapsed != "unnamed_tool" {
        return collapsed;
    }

    let fallback = user_request
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(|part| {
            let token = part.trim().to_ascii_lowercase();
            if token.len() < 3 { None } else { Some(token) }
        })
        .take(4)
        .collect::<Vec<_>>()
        .join("_");
    if fallback.is_empty() {
        "unnamed_tool".to_string()
    } else {
        format!("{}_tool", fallback)
    }
}

fn synthesize_tool_approach(
    user_request: &str,
    purpose: &str,
    inputs: &str,
    outputs: &str,
) -> String {
    let request_hint = user_request
        .split_whitespace()
        .take(18)
        .collect::<Vec<_>>()
        .join(" ");
    let purpose_hint = if purpose.trim().is_empty() {
        "perform the requested transformation"
    } else {
        purpose.trim()
    };

    format!(
        "Read JSON from stdin with json.load(sys.stdin), inspect the expected fields described by INPUTS ({}), implement the requested behavior ({}) using only the allowed Python standard library modules, and print a structured JSON result matching OUTPUTS ({}). Request hint: {}.",
        crate::trunc(inputs, 120),
        crate::trunc(purpose_hint, 160),
        crate::trunc(outputs, 120),
        crate::trunc(&request_hint, 140),
    )
}

pub(super) fn build_tool_preflight(
    llm: &LlmGate,
    user_request: &str,
    nyx_response: &str,
    spec: &TaskSpec,
) -> BuildPreflight {
    let mut validations = Vec::new();
    let mut issue = None;
    let intent_model = llm.preferred_chat_model_label();
    let implementation_model = llm.preferred_autonomous_model_label();
    let spec_preview = BuildSpecPreview {
        tool_name: spec.tool_name.clone(),
        filename: spec.filename.clone(),
        purpose: spec.purpose.clone(),
        inputs: spec.inputs.clone(),
        outputs: spec.outputs.clone(),
        approach: spec.approach.clone(),
        test_input: spec.test_input.clone(),
        test_expected: spec.test_expected.clone(),
    };

    let auto_run_input = derive_auto_run_input(user_request, spec);
    if auto_run_input.is_some() {
        validations.push("auto-run input can be derived from the request".to_string());
    } else {
        validations.push("no deterministic auto-run input was derived".to_string());
    }

    let manifest_path = built_tool_manifest_path_for(spec);
    let mut target_exists = false;

    match tool_name_from_spec(spec) {
        Ok(tool_name) => {
            validations.push(format!("tool name resolves to {}", tool_name));
        }
        Err(error) => {
            issue.get_or_insert_with(|| error.to_string());
        }
    }

    match sanitize_relative_repo_path(&spec.filename) {
        Ok(path) => {
            if path.starts_with("tools") {
                validations.push("target path is under tools/".to_string());
            }
            if path.extension().and_then(|ext| ext.to_str()) == Some("py") {
                validations.push("target file uses .py extension".to_string());
            } else {
                issue.get_or_insert_with(|| {
                    format!("built tool must be a Python file: {}", path.display())
                });
            }
            target_exists = path.exists();
            if target_exists {
                issue.get_or_insert_with(|| format!("{} already exists", path.display()));
            }
        }
        Err(error) => {
            issue.get_or_insert_with(|| error.to_string());
        }
    }

    if let Some(manifest_path) = manifest_path.as_deref() {
        if Path::new(manifest_path).exists() {
            issue.get_or_insert_with(|| format!("{} already exists", manifest_path));
        }
    }

    match parse_tool_input_json(&spec.test_input) {
        Ok(_) => validations.push("test_input parses as JSON".to_string()),
        Err(error) => {
            issue.get_or_insert_with(|| error.to_string());
        }
    }

    if spec.purpose.trim().is_empty() {
        issue.get_or_insert_with(|| "tool purpose is empty".to_string());
    }
    if spec.approach.trim().is_empty() {
        issue.get_or_insert_with(|| "implementation approach is empty".to_string());
    }

    BuildPreflight {
        user_request: user_request.to_string(),
        nyx_response: nyx_response.to_string(),
        intent_model,
        implementation_model,
        ready: issue.is_none(),
        issue,
        spec: Some(spec_preview),
        target_exists,
        manifest_path,
        auto_run_input,
        validations,
    }
}

pub(super) fn blocked_build_preflight(
    user_request: &str,
    nyx_response: &str,
    issue: &str,
) -> BuildPreflight {
    BuildPreflight {
        user_request: user_request.to_string(),
        nyx_response: nyx_response.to_string(),
        intent_model: "unavailable".to_string(),
        implementation_model: "unavailable".to_string(),
        ready: false,
        issue: Some(issue.to_string()),
        spec: None,
        target_exists: false,
        manifest_path: None,
        auto_run_input: None,
        validations: Vec::new(),
    }
}

pub(super) async fn execute_build_spec(
    llm: &Arc<LlmGate>,
    user_request: &str,
    spec: &TaskSpec,
    mut telemetry: ForgeTelemetry,
) -> ForgeResult {
    tracing::info!("forge: spec merged — {} ({})", spec.tool_name, spec.purpose);

    let code_prompt = format!(
        "Write a complete Python tool based on this spec.\n\n\
         Tool: {}\nPurpose: {}\nInputs (JSON stdin): {}\nOutputs (JSON stdout): {}\n\
         Approach: {}\n\n\
         Requirements:\n\
         - Read JSON from stdin: data = json.load(sys.stdin)\n\
         - Print JSON to stdout: print(json.dumps(result))\n\
         - Handle errors: print(json.dumps({{\"error\": str(e)}}))\n\
         - Only imports: json, sys, os.path, urllib.request, urllib.parse, datetime, math, re, hashlib, base64, time, pathlib, collections, itertools, functools\n\n\
         Reply with ONLY the Python code, no markdown fences, no explanation.",
        spec.tool_name, spec.purpose, spec.inputs, spec.outputs, spec.approach
    );

    let code = match llm.chat_auto_with_fallback_traced(&code_prompt, 600).await {
        Ok(response) => {
            telemetry
                .llm_usage
                .record("code_generation", &response.trace);
            strip_code_fences(&response.text)
        }
        Err(error) => {
            telemetry.set_execution_status(false, None);
            return ForgeResult::Failed {
                reason: format!(
                    "code generation failed via {}: {}",
                    llm.preferred_autonomous_model_label(),
                    error
                ),
                telemetry,
            };
        }
    };

    if code.len() < 30 {
        telemetry.set_execution_status(false, None);
        return ForgeResult::Failed {
            reason: "generated code too short".into(),
            telemetry,
        };
    }

    match verify_code(llm, spec, &code, &mut telemetry.llm_usage).await {
        VerifyResult::Pass => {
            deploy_fix_and_register_tool(llm, user_request, spec, &code, None, telemetry).await
        }
        VerifyResult::Fail(feedback) => {
            tracing::info!("forge: first verify failed, fixing");
            telemetry.verification_failures += 1;
            deploy_fix_and_register_tool(llm, user_request, spec, &code, Some(feedback), telemetry)
                .await
        }
    }
}

async fn verify_code(
    llm: &Arc<LlmGate>,
    spec: &TaskSpec,
    code: &str,
    llm_usage: &mut ForgeLlmUsage,
) -> VerifyResult {
    let intent_verify = format!(
        "Does this Python code do what the user needs?\n\n\
         Purpose: {}\nExpected inputs: {}\nExpected outputs: {}\n\n\
         Code:\n{}\n\n\
         Reply PASS if it looks correct, or FAIL: [specific issue]",
        spec.purpose,
        spec.inputs,
        spec.outputs,
        crate::trunc(code, 2000)
    );

    let implementation_verify = format!(
        "Review this Python tool for bugs, edge cases, and correctness.\n\n\
         Purpose: {}\nApproach: {}\n\n\
         Code:\n{}\n\n\
         Reply PASS if the code is solid, or FAIL: [specific bug or issue]",
        spec.purpose,
        spec.approach,
        crate::trunc(code, 2000)
    );

    let (intent_result, implementation_result) = tokio::join!(
        chat_forge_intent_lane_traced(llm, &intent_verify, 80),
        llm.chat_auto_with_fallback_traced(&implementation_verify, 80)
    );

    let intent_pass = intent_result
        .as_ref()
        .map(|response| {
            llm_usage.record("verify_intent", &response.trace);
            response.text.to_uppercase().contains("PASS")
        })
        .unwrap_or(true);
    let implementation_pass = implementation_result
        .as_ref()
        .map(|response| {
            llm_usage.record("verify_implementation", &response.trace);
            response.text.to_uppercase().contains("PASS")
        })
        .unwrap_or(true);

    if intent_pass && implementation_pass {
        VerifyResult::Pass
    } else {
        let mut feedback = Vec::new();
        if !intent_pass {
            feedback.push(format!(
                "Intent review ({}): {}",
                llm.preferred_chat_model_label(),
                intent_result
                    .map(|response| response.text)
                    .unwrap_or_default()
            ));
        }
        if !implementation_pass {
            feedback.push(format!(
                "Implementation review ({}): {}",
                llm.preferred_autonomous_model_label(),
                implementation_result
                    .map(|response| response.text)
                    .unwrap_or_default()
            ));
        }
        VerifyResult::Fail(feedback.join("\n"))
    }
}

fn validate_bootstrap_imports(code: &str) -> Result<()> {
    let allowed = crate::BOOTSTRAP_ALLOWED_IMPORTS;
    for line in code.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("import ") || trimmed.starts_with("from ") {
            let module = if trimmed.starts_with("from ") {
                trimmed.split_whitespace().nth(1).unwrap_or("")
            } else {
                trimmed
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .split('.')
                    .next()
                    .unwrap_or("")
            };
            let base = module.split('.').next().unwrap_or("");
            if !allowed
                .iter()
                .any(|allowed| allowed.split('.').next().unwrap_or(allowed) == base)
            {
                bail!("disallowed import: {}", module);
            }
        }
    }
    Ok(())
}

async fn deploy_fix_and_register_tool(
    llm: &Arc<LlmGate>,
    user_request: &str,
    spec: &TaskSpec,
    initial_code: &str,
    initial_feedback: Option<String>,
    mut telemetry: ForgeTelemetry,
) -> ForgeResult {
    let mut current_code = initial_code.to_string();
    let mut feedback = initial_feedback;
    let mut allow_overwrite = false;
    let mut seen_states = HashSet::new();
    let mut repair_attempts = 0usize;
    let mut executed = false;

    loop {
        let state_fingerprint = format!(
            "{}\n---feedback---\n{}",
            normalize_compare_text(&current_code),
            feedback.as_deref().unwrap_or("")
        );
        if !seen_states.insert(state_fingerprint) {
            telemetry.set_execution_status(executed, executed.then_some(false));
            return ForgeResult::Failed {
                reason: "repair loop stalled: repeated state with no progress".into(),
                telemetry,
            };
        }

        if let Some(issues) = feedback.as_ref() {
            if repair_attempts >= MAX_REPAIR_ATTEMPTS {
                telemetry.set_execution_status(executed, executed.then_some(false));
                return ForgeResult::Failed {
                    reason: format!(
                        "repair budget exhausted after {} rounds: {}",
                        MAX_REPAIR_ATTEMPTS,
                        crate::trunc(issues, 160)
                    ),
                    telemetry,
                };
            }
            telemetry.repair_rounds += 1;
            current_code = match repair_tool_code(
                llm,
                spec,
                &current_code,
                issues,
                repair_attempts,
                &mut telemetry.llm_usage,
            )
            .await
            {
                Ok(code) => code,
                Err(error) => {
                    telemetry.set_execution_status(executed, executed.then_some(false));
                    return ForgeResult::Failed {
                        reason: format!(
                            "fix failed after {}: {}",
                            crate::trunc(issues, 160),
                            error
                        ),
                        telemetry,
                    };
                }
            };
            repair_attempts += 1;

            match verify_code(llm, spec, &current_code, &mut telemetry.llm_usage).await {
                VerifyResult::Pass => {}
                VerifyResult::Fail(next_feedback) => {
                    feedback = Some(next_feedback);
                    telemetry.verification_failures += 1;
                    continue;
                }
            }
        }

        match write_tool_file(spec, &current_code, allow_overwrite).await {
            Ok(()) => {
                executed = true;
            }
            Err(error) => {
                telemetry.set_execution_status(executed, executed.then_some(false));
                return ForgeResult::Failed {
                    reason: error.to_string(),
                    telemetry,
                };
            }
        }

        match smoke_test_tool(spec).await {
            Ok(()) => {
                if let Err(error) = write_built_tool_manifest(spec) {
                    remove_built_tool_artifacts(spec);
                    telemetry.set_execution_status(true, Some(false));
                    return ForgeResult::Failed {
                        reason: format!("registration failed: {}", error),
                        telemetry,
                    };
                }
                let auto_run = match maybe_auto_run_built_tool(spec, user_request).await {
                    Ok(preview) => preview,
                    Err(error) => {
                        tracing::warn!(
                            "forge: built {} but skipped auto-run preview: {}",
                            spec.filename,
                            error
                        );
                        None
                    }
                };
                tracing::info!("forge: deployed {} — {}", spec.filename, spec.purpose);
                telemetry.set_execution_status(true, Some(true));
                return ForgeResult::Success {
                    filename: spec.filename.clone(),
                    description: spec.purpose.clone(),
                    auto_run,
                    telemetry,
                };
            }
            Err(runtime_feedback) => {
                tracing::info!("forge: runtime test failed, fixing");
                feedback = Some(runtime_feedback.to_string());
                allow_overwrite = true;
                telemetry.runtime_failures += 1;
            }
        }
    }
}

async fn repair_tool_code(
    llm: &Arc<LlmGate>,
    spec: &TaskSpec,
    current_code: &str,
    feedback: &str,
    repair_round: usize,
    llm_usage: &mut ForgeLlmUsage,
) -> Result<String> {
    let fix_prompt = format!(
        "Fix this Python tool based on the feedback.\n\n\
         Original spec: {} — {}\n\
         Inputs: {}\nOutputs: {}\n\
         Example test input: {}\nExpected signal: {}\n\n\
         Repair round: {}\n\
         Issues found:\n{}\n\n\
         Current code:\n{}\n\n\
         Reply with ONLY the fixed Python code, no markdown fences.",
        spec.tool_name,
        spec.purpose,
        spec.inputs,
        spec.outputs,
        spec.test_input,
        spec.test_expected,
        repair_round + 1,
        feedback,
        crate::trunc(current_code, 3000)
    );
    let current_norm = normalize_compare_text(current_code);
    let stage = format!("repair_tool_round_{}", repair_round + 1);

    if llm.has_ollama() {
        if let Ok(candidate) = llm.chat_ollama_direct_traced(&fix_prompt, 700).await {
            llm_usage.record(stage.clone(), &candidate.trace);
            if let Some(code) = accept_repair_candidate(&candidate.text, &current_norm) {
                return Ok(code);
            }
        }
    }

    if llm.has_nim() {
        if let Ok(candidate) = llm.chat_nim_direct_traced(&fix_prompt, 700).await {
            llm_usage.record(stage.clone(), &candidate.trace);
            if let Some(code) = accept_repair_candidate(&candidate.text, &current_norm) {
                return Ok(code);
            }
        }
    }

    if llm.has_anthropic() {
        if let Ok(candidate) = llm.chat_anthropic_direct_traced(&fix_prompt, 700).await {
            llm_usage.record(stage.clone(), &candidate.trace);
            if let Some(code) = accept_repair_candidate(&candidate.text, &current_norm) {
                return Ok(code);
            }
        }
    }

    if let Ok(candidate) = llm.chat_auto_with_fallback_traced(&fix_prompt, 700).await {
        llm_usage.record(stage.clone(), &candidate.trace);
        if let Some(code) = accept_repair_candidate(&candidate.text, &current_norm) {
            return Ok(code);
        }
    }

    let fallback = chat_forge_intent_lane_traced(llm, &fix_prompt, 700).await?;
    llm_usage.record(stage, &fallback.trace);
    accept_repair_candidate(&fallback.text, &current_norm)
        .ok_or_else(|| anyhow!("all repair providers returned unchanged or invalid code"))
}

async fn write_tool_file(spec: &TaskSpec, code: &str, allow_overwrite: bool) -> Result<()> {
    validate_bootstrap_imports(code)?;
    let filename = &spec.filename;

    if !allow_overwrite && Path::new(filename).exists() {
        bail!("{} already exists", filename);
    }

    match crate::tools::run(
        "file_ops",
        &serde_json::json!({
            "action": "write",
            "path": filename,
            "content": code
        }),
    )
    .await
    {
        Ok(result) if result["success"].as_bool() == Some(true) => Ok(()),
        Ok(result) => bail!("{}", result["error"].as_str().unwrap_or("write failed")),
        Err(error) => Err(error),
    }
}

async fn smoke_test_tool(spec: &TaskSpec) -> Result<()> {
    let tool_path = sanitize_relative_repo_path(&spec.filename)?;
    let input = parse_tool_input_json(&spec.test_input)?;
    run_tool_with_expectation(&tool_path, &input, &spec.test_expected).await?;
    Ok(())
}

async fn maybe_auto_run_built_tool(
    spec: &TaskSpec,
    user_request: &str,
) -> Result<Option<ToolRunPreview>> {
    let Some(input) = derive_auto_run_input(user_request, spec) else {
        return Ok(None);
    };
    let tool_path = sanitize_relative_repo_path(&spec.filename)?;
    let output = run_built_tool_file(&tool_path, &input).await?;
    Ok(Some(ToolRunPreview { input, output }))
}

pub(super) fn derive_auto_run_input(
    user_request: &str,
    spec: &TaskSpec,
) -> Option<serde_json::Value> {
    if let Some(explicit_json) = extract_json_object_from_text(user_request) {
        return Some(explicit_json);
    }

    let inputs = serde_json::from_str::<serde_json::Value>(&spec.inputs).ok()?;
    let input_fields = inputs.as_object()?;
    if input_fields.len() != 1 {
        return None;
    }

    let (field_name, field_schema) = input_fields.iter().next()?;
    let schema_text = match field_schema {
        serde_json::Value::String(value) => value.to_lowercase(),
        other => other.to_string().to_lowercase(),
    };
    if !schema_text.contains("string") {
        return None;
    }

    let quoted = extract_quoted_string(user_request)?;
    Some(serde_json::json!({ field_name: quoted }))
}

fn extract_json_object_from_text(text: &str) -> Option<serde_json::Value> {
    let start = text.find('{')?;
    let mut depth = 0usize;

    for (offset, ch) in text[start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let candidate = &text[start..start + offset + ch.len_utf8()];
                    let parsed = serde_json::from_str::<serde_json::Value>(candidate).ok()?;
                    if parsed.is_object() {
                        return Some(parsed);
                    }
                    return None;
                }
            }
            _ => {}
        }
    }

    None
}

fn extract_quoted_string(text: &str) -> Option<String> {
    let mut current_quote = None;
    let mut buffer = String::new();

    for ch in text.chars() {
        match current_quote {
            Some(quote) if ch == quote => {
                let trimmed = buffer.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
                buffer.clear();
                current_quote = None;
            }
            Some(_) => buffer.push(ch),
            None if ch == '"' || ch == '\'' => {
                current_quote = Some(ch);
                buffer.clear();
            }
            None => {}
        }
    }

    None
}
