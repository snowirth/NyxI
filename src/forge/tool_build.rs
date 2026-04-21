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
    let raw_filename = implementation_spec["FILENAME"]
        .as_str()
        .or(implementation_spec["filename"].as_str())
        .unwrap_or(&default_filename);
    let filename = normalize_tool_filename(raw_filename, &tool_name, user_request);

    let synthesized = synthesize_tool_contract(user_request, &tool_name);

    let purpose = select_spec_field(
        intent_spec["PURPOSE"]
            .as_str()
            .or(intent_spec["purpose"].as_str())
            .unwrap_or(""),
        &synthesized.purpose,
    );
    let inputs = select_spec_field(
        intent_spec["INPUTS"]
            .as_str()
            .or(intent_spec["inputs"].as_str())
            .unwrap_or("{}"),
        &synthesized.inputs,
    );
    let outputs = select_spec_field(
        intent_spec["OUTPUTS"]
            .as_str()
            .or(intent_spec["outputs"].as_str())
            .unwrap_or("{}"),
        &synthesized.outputs,
    );
    let test_input = select_spec_field(
        intent_spec["TEST_INPUT"]
            .as_str()
            .or(intent_spec["test_input"].as_str())
            .unwrap_or("{}"),
        &synthesized.test_input,
    );
    let test_expected = select_spec_field(
        intent_spec["TEST_EXPECTED"]
            .as_str()
            .or(intent_spec["test_expected"].as_str())
            .unwrap_or(""),
        &synthesized.test_expected,
    );
    let approach = implementation_spec["APPROACH"]
        .as_str()
        .or(implementation_spec["approach"].as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| synthesize_tool_approach(user_request, &purpose, &inputs, &outputs));

    Ok(TaskSpec {
        tool_name,
        filename,
        purpose,
        inputs,
        outputs,
        approach,
        test_input,
        test_expected,
    })
}

fn normalize_tool_name(raw_tool_name: &str, user_request: &str) -> String {
    let request_candidate = request_tool_name_candidate(user_request);

    if let Some(candidate) = cleaned_tool_name_candidate(raw_tool_name) {
        if let Some(request_candidate) = request_candidate.as_ref() {
            let raw_lower = raw_tool_name.to_ascii_lowercase();
            let raw_token_count = candidate.split('_').count();
            let request_token_count = request_candidate.split('_').count();
            if (raw_lower.contains("named")
                || raw_lower.contains("called")
                || REQUEST_LEADING_VERBS
                    .iter()
                    .any(|verb| raw_lower.starts_with(verb)))
                && request_token_count > raw_token_count
            {
                return request_candidate.clone();
            }
        }
        return candidate;
    }

    if let Some(candidate) = request_candidate {
        return candidate;
    }

    let fallback = sanitize_tool_name_tokens(
        user_request
            .split(|ch: char| !ch.is_ascii_alphanumeric())
            .filter_map(|part| {
                let token = part.trim().to_ascii_lowercase();
                if token.len() < 3 { None } else { Some(token) }
            })
            .take(4),
    )
    .join("_");
    if fallback.is_empty() {
        "unnamed_tool".to_string()
    } else {
        format!("{}_tool", fallback)
    }
}

fn normalize_tool_filename(raw_filename: &str, tool_name: &str, user_request: &str) -> String {
    let trimmed = raw_filename.trim();
    if trimmed.is_empty() {
        return format!("tools/{}.py", tool_name);
    }

    let candidate = std::path::Path::new(trimmed);
    let directory = candidate
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(|parent| parent.to_string_lossy().trim_matches('/').to_string())
        .filter(|parent| !parent.is_empty())
        .unwrap_or_else(|| "tools".to_string());
    let stem = candidate
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(tool_name);
    let normalized_stem = normalize_tool_name(stem, user_request);
    format!("{}/{}.py", directory, normalized_stem)
}

fn cleaned_tool_name_candidate(raw_tool_name: &str) -> Option<String> {
    let parts =
        sanitize_tool_name_tokens(raw_tool_name.split(|ch: char| !ch.is_ascii_alphanumeric()));
    let simplified = simplify_tool_name_parts(parts);
    if simplified.is_empty() {
        None
    } else {
        Some(simplified.join("_"))
    }
}

fn request_tool_name_candidate(user_request: &str) -> Option<String> {
    let lower = user_request.to_ascii_lowercase();

    for marker in [" named ", " called "] {
        if let Some((_, tail)) = lower.split_once(marker) {
            let explicit = sanitize_tool_name_tokens(
                tail.split(|ch: char| !ch.is_ascii_alphanumeric())
                    .take_while(|token| {
                        let token = token.trim().to_ascii_lowercase();
                        !token.is_empty() && !REQUEST_NAME_STOP_WORDS.contains(&token.as_str())
                    }),
            );
            let simplified = simplify_tool_name_parts(explicit);
            if !simplified.is_empty() {
                return Some(simplified.join("_"));
            }
        }
    }

    let mut parts = sanitize_tool_name_tokens(lower.split(|ch: char| !ch.is_ascii_alphanumeric()));
    while parts.len() > 1 && REQUEST_LEADING_VERBS.contains(&parts[0].as_str()) {
        parts.remove(0);
    }
    while parts.len() > 1 && REQUEST_ARTICLES.contains(&parts[0].as_str()) {
        parts.remove(0);
    }

    let descriptive = parts
        .into_iter()
        .take_while(|part| !REQUEST_NAME_STOP_WORDS.contains(&part.as_str()))
        .collect::<Vec<_>>();
    let simplified = simplify_tool_name_parts(descriptive);
    if simplified.is_empty() {
        None
    } else {
        Some(simplified.join("_"))
    }
}

fn sanitize_tool_name_tokens<T>(tokens: impl IntoIterator<Item = T>) -> Vec<String>
where
    T: AsRef<str>,
{
    tokens
        .into_iter()
        .filter_map(|token| {
            let normalized = token.as_ref().trim().to_ascii_lowercase();
            if normalized.is_empty() {
                None
            } else {
                Some(normalized)
            }
        })
        .collect()
}

fn simplify_tool_name_parts(parts: Vec<String>) -> Vec<String> {
    if parts.is_empty() {
        return parts;
    }

    let mut parts = parts;
    while parts.len() > 1 && REQUEST_LEADING_VERBS.contains(&parts[0].as_str()) {
        parts.remove(0);
    }
    while parts.len() > 1 && REQUEST_ARTICLES.contains(&parts[0].as_str()) {
        parts.remove(0);
    }
    while parts.len() > 2 && parts[0] == "tool" && (parts[1] == "named" || parts[1] == "called") {
        parts.drain(0..2);
    }
    while parts.len() > 1 && GENERIC_TOOL_PREFIX_WORDS.contains(&parts[0].as_str()) {
        parts.remove(0);
    }
    while parts.len() > 1 && GENERIC_TOOL_SUFFIX_WORDS.contains(&parts[parts.len() - 1].as_str()) {
        parts.pop();
    }

    if parts.len() > 1 && parts[0] == "unnamed" && parts[1] == "tool" {
        return Vec::new();
    }

    parts
}

const REQUEST_LEADING_VERBS: &[&str] = &[
    "make", "build", "create", "generate", "write", "craft", "add", "forge",
];
const REQUEST_ARTICLES: &[&str] = &["a", "an", "the", "new"];
const GENERIC_TOOL_PREFIX_WORDS: &[&str] = &["tool", "helper", "utility", "named", "called"];
const GENERIC_TOOL_SUFFIX_WORDS: &[&str] = &["tool", "helper", "utility"];
const REQUEST_NAME_STOP_WORDS: &[&str] = &[
    "that", "which", "who", "to", "from", "for", "using", "with", "reads", "read", "returns",
    "return", "turns", "turn", "parses", "parse", "extracts", "extract", "converts", "convert",
    "into", "based", "by",
];

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

#[derive(Debug, Clone)]
struct SynthesizedToolContract {
    purpose: String,
    inputs: String,
    outputs: String,
    test_input: String,
    test_expected: String,
}

fn select_spec_field(raw: &str, synthesized: &str) -> String {
    if spec_field_needs_synthesis(raw) {
        synthesized.to_string()
    } else {
        raw.trim().to_string()
    }
}

fn spec_field_needs_synthesis(raw: &str) -> bool {
    matches!(raw.trim(), "" | "{}" | "[]" | "null" | "\"\"")
}

fn synthesize_tool_contract(user_request: &str, tool_name: &str) -> SynthesizedToolContract {
    let request_lower = user_request.to_ascii_lowercase();

    if looks_like_slugifier_request(&request_lower, tool_name) {
        return synthesize_slugifier_contract(user_request);
    }

    if looks_like_markdown_todo_request(&request_lower, tool_name) {
        return synthesize_markdown_todo_contract(user_request);
    }

    if looks_like_summary_request(&request_lower, tool_name) {
        return synthesize_summary_contract(user_request);
    }

    if looks_like_extractor_request(&request_lower, tool_name) {
        return synthesize_generic_extractor_contract(user_request);
    }

    synthesize_generic_text_contract(user_request)
}

fn looks_like_slugifier_request(request_lower: &str, tool_name: &str) -> bool {
    request_lower.contains("slug") || tool_name.contains("slug")
}

fn looks_like_markdown_todo_request(request_lower: &str, tool_name: &str) -> bool {
    tool_name.contains("todo")
        || (request_lower.contains("todo") || request_lower.contains("task list"))
        || request_lower.contains("checkbox")
        || (request_lower.contains("markdown") && request_lower.contains("checklist"))
}

fn looks_like_summary_request(request_lower: &str, tool_name: &str) -> bool {
    tool_name.contains("summary")
        || tool_name.contains("summar")
        || request_lower.contains("summar")
        || request_lower.contains("digest")
}

fn looks_like_extractor_request(request_lower: &str, tool_name: &str) -> bool {
    tool_name.contains("extract") || request_lower.contains("extract")
}

fn synthesize_slugifier_contract(user_request: &str) -> SynthesizedToolContract {
    let source = extract_quoted_string(user_request).unwrap_or_else(|| "Launch Plan 2026".into());
    let slug = slugify_text(&source);
    let inputs = serde_json::json!({
        "text": "string"
    })
    .to_string();
    let outputs = serde_json::json!({
        "success": "boolean",
        "slug": "string"
    })
    .to_string();
    let test_input = serde_json::json!({
        "text": source
    })
    .to_string();

    SynthesizedToolContract {
        purpose: "Convert input text into a URL-safe lowercase slug.".into(),
        inputs,
        outputs,
        test_input,
        test_expected: field_expectation("slug", &slug),
    }
}

fn synthesize_markdown_todo_contract(user_request: &str) -> SynthesizedToolContract {
    let source = extract_quoted_string(user_request)
        .unwrap_or_else(|| "- [ ] ship docs\n- [x] add tests".into());
    let normalized_source = normalize_multiline_sample(&source);
    let first_item = extract_first_markdown_checkbox_text(&normalized_source)
        .unwrap_or_else(|| "ship docs".into());
    let inputs = serde_json::json!({
        "text": "string"
    })
    .to_string();
    let outputs = serde_json::json!({
        "success": "boolean",
        "items": [{
            "text": "string",
            "done": "boolean"
        }]
    })
    .to_string();
    let test_input = serde_json::json!({
        "text": normalized_source
    })
    .to_string();

    SynthesizedToolContract {
        purpose: "Extract markdown checkbox items into structured todo entries.".into(),
        inputs,
        outputs,
        test_input,
        test_expected: field_expectation("text", &first_item),
    }
}

fn synthesize_summary_contract(user_request: &str) -> SynthesizedToolContract {
    let input = extract_request_object(user_request).unwrap_or_else(|| {
        serde_json::json!({
            "text": extract_quoted_string(user_request)
                .unwrap_or_else(|| "Summarize the weekly project update".into())
        })
    });

    SynthesizedToolContract {
        purpose: "Produce a concise structured summary of the provided input.".into(),
        inputs: infer_json_schema(&input).to_string(),
        outputs: serde_json::json!({
            "success": "boolean",
            "summary": "string"
        })
        .to_string(),
        test_input: input.to_string(),
        test_expected: "\"summary\":".into(),
    }
}

fn synthesize_generic_extractor_contract(user_request: &str) -> SynthesizedToolContract {
    let sample = extract_quoted_string(user_request)
        .unwrap_or_else(|| "Follow up with Alex tomorrow.\nShip docs by Friday.".into());
    SynthesizedToolContract {
        purpose: "Extract structured items from the provided text input.".into(),
        inputs: serde_json::json!({
            "text": "string"
        })
        .to_string(),
        outputs: serde_json::json!({
            "success": "boolean",
            "items": ["string"]
        })
        .to_string(),
        test_input: serde_json::json!({
            "text": sample
        })
        .to_string(),
        test_expected: "\"items\":".into(),
    }
}

fn synthesize_generic_text_contract(user_request: &str) -> SynthesizedToolContract {
    let input = extract_request_object(user_request).unwrap_or_else(|| {
        serde_json::json!({
            "text": extract_quoted_string(user_request)
                .unwrap_or_else(|| "example".into())
        })
    });

    SynthesizedToolContract {
        purpose: format!(
            "Handle the user request by transforming the provided input: {}.",
            crate::trunc(user_request.trim(), 180)
        ),
        inputs: infer_json_schema(&input).to_string(),
        outputs: serde_json::json!({
            "success": "boolean",
            "result": "string"
        })
        .to_string(),
        test_input: input.to_string(),
        test_expected: "\"success\":true".into(),
    }
}

fn extract_request_object(text: &str) -> Option<serde_json::Value> {
    extract_json_object_from_text(text)
}

fn infer_json_schema(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Null => serde_json::Value::String("null".into()),
        serde_json::Value::Bool(_) => serde_json::Value::String("boolean".into()),
        serde_json::Value::Number(_) => serde_json::Value::String("number".into()),
        serde_json::Value::String(_) => serde_json::Value::String("string".into()),
        serde_json::Value::Array(items) => {
            if let Some(first) = items.first() {
                serde_json::Value::Array(vec![infer_json_schema(first)])
            } else {
                serde_json::Value::String("array".into())
            }
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), infer_json_schema(value)))
                .collect(),
        ),
    }
}

fn field_expectation(field: &str, value: &str) -> String {
    format!(
        "\"{}\":{}",
        field,
        serde_json::Value::String(value.to_string())
    )
}

fn slugify_text(text: &str) -> String {
    let mut output = String::new();
    let mut last_was_dash = false;

    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !output.is_empty() && !last_was_dash {
            output.push('-');
            last_was_dash = true;
        }
    }

    output.trim_matches('-').to_string()
}

fn extract_first_markdown_checkbox_text(text: &str) -> Option<String> {
    text.lines().find_map(|line| {
        let trimmed = line.trim_start();
        let remainder = trimmed
            .strip_prefix("- [ ] ")
            .or_else(|| trimmed.strip_prefix("- [x] "))
            .or_else(|| trimmed.strip_prefix("* [ ] "))
            .or_else(|| trimmed.strip_prefix("* [x] "))?;
        let item = remainder.trim();
        if item.is_empty() {
            None
        } else {
            Some(item.to_string())
        }
    })
}

fn normalize_multiline_sample(text: &str) -> String {
    text.replace("\\n", "\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn markdown_todo_spec() -> TaskSpec {
        TaskSpec {
            tool_name: "markdown_todo_extractor".into(),
            filename: "tools/markdown_todo_extractor.py".into(),
            purpose: "Extract markdown checkbox items into structured todo entries.".into(),
            inputs: "{\"text\":\"string\"}".into(),
            outputs:
                "{\"items\":[{\"text\":\"string\",\"done\":\"boolean\"}],\"success\":\"boolean\"}"
                    .into(),
            approach: "Parse markdown checklist lines and return structured todo items.".into(),
            test_input: "{\"text\":\"- [ ] ship docs\\n- [x] add tests\"}".into(),
            test_expected: "\"text\":\"ship docs\"".into(),
        }
    }

    #[test]
    fn parser_tool_tolerates_minor_regex_review_feedback() {
        let spec = markdown_todo_spec();
        let review =
            "FAIL: The regex is overly permissive regarding whitespace inside the brackets.";
        assert!(is_non_blocking_verification_feedback(&spec, review));
        assert!(is_passing_or_non_blocking_review(&spec, review));
    }

    #[test]
    fn parser_tool_still_blocks_real_schema_failures() {
        let spec = markdown_todo_spec();
        let review = "FAIL: Wrong output schema and missing required success field.";
        assert!(!is_non_blocking_verification_feedback(&spec, review));
        assert!(!is_passing_or_non_blocking_review(&spec, review));
    }

    #[test]
    fn normalize_tool_name_strips_command_words_from_generated_names() {
        let name = normalize_tool_name(
            "make_markdown_todo_extractor_tool",
            "make a markdown todo extractor that turns markdown into structured todo items",
        );
        assert_eq!(name, "markdown_todo_extractor");
    }

    #[test]
    fn normalize_tool_name_uses_request_shape_when_raw_name_is_missing() {
        let name = normalize_tool_name(
            "",
            "make a markdown todo extractor that turns markdown into structured todo items",
        );
        assert_eq!(name, "markdown_todo_extractor");
    }

    #[test]
    fn normalize_tool_filename_rewrites_generic_generated_filename() {
        let filename = normalize_tool_filename(
            "tools/make_markdown_todo_extractor_tool.py",
            "markdown_todo_extractor",
            "make a markdown todo extractor that turns markdown into structured todo items",
        );
        assert_eq!(filename, "tools/markdown_todo_extractor.py");
    }

    #[test]
    fn normalize_tool_name_prefers_explicit_named_request_over_awkward_raw_name() {
        let name = normalize_tool_name(
            "build_tool_named_markdown_tool",
            "build a tool named markdown_todo_extractor that turns markdown into structured todo items",
        );
        assert_eq!(name, "markdown_todo_extractor");
    }
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
    file_provenance_operation_id: &str,
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
         - Handle errors by returning JSON that still matches the declared output shape.\n\
         - If the output schema includes \"success\", set it to false on errors.\n\
         - Keep the primary output fields present with sensible empty/default values and include an \"error\" string.\n\
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
            deploy_fix_and_register_tool(
                llm,
                user_request,
                spec,
                &code,
                None,
                file_provenance_operation_id,
                telemetry,
            )
            .await
        }
        VerifyResult::Fail(feedback) => {
            tracing::info!("forge: first verify failed, fixing");
            telemetry.verification_failures += 1;
            deploy_fix_and_register_tool(
                llm,
                user_request,
                spec,
                &code,
                Some(feedback),
                file_provenance_operation_id,
                telemetry,
            )
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
         Only FAIL for likely user-visible wrong behavior: wrong output shape, missing required fields, \
         sample input not matching the expected signal, likely runtime errors, or disallowed behavior.\n\
         Do not FAIL for style, naming, alternative but valid parsing choices, or minor implementation preferences.\n\
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
         Only FAIL for likely user-visible correctness issues: wrong output schema, likely crashes, invalid sample handling, \
         missing required fields, broken control flow, or disallowed imports.\n\
         Do not FAIL for style, naming, comment quality, alternative regex or parsing choices, minor permissiveness, \
         or other non-fatal implementation preferences.\n\
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
            is_passing_or_non_blocking_review(spec, &response.text)
        })
        .unwrap_or(true);
    let implementation_pass = implementation_result
        .as_ref()
        .map(|response| {
            llm_usage.record("verify_implementation", &response.trace);
            is_passing_or_non_blocking_review(spec, &response.text)
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

fn is_passing_or_non_blocking_review(spec: &TaskSpec, review: &str) -> bool {
    let review_upper = review.to_uppercase();
    if review_upper.contains("PASS") {
        return true;
    }

    is_non_blocking_verification_feedback(spec, review)
}

fn is_non_blocking_verification_feedback(spec: &TaskSpec, review: &str) -> bool {
    let review_lower = review.to_ascii_lowercase();

    if contains_any(
        &review_lower,
        &[
            "wrong output",
            "output shape",
            "output schema",
            "missing required",
            "missing field",
            "crash",
            "throws",
            "exception",
            "traceback",
            "syntax error",
            "runtime error",
            "does not parse",
            "parse error",
            "sample input",
            "expected signal",
            "disallowed import",
            "unsafe",
            "infinite loop",
            "never returns",
            "always fails",
            "cannot handle",
            "can't handle",
            "fails to",
            "does not",
            "doesn't",
            "incorrect output",
        ],
    ) {
        return false;
    }

    if !tool_looks_like_parser_or_extractor(spec) {
        return contains_any(
            &review_lower,
            &["style", "naming", "readability", "comment quality"],
        );
    }

    contains_any(
        &review_lower,
        &[
            "overly permissive",
            "minor permissiveness",
            "alternative regex",
            "alternative parsing",
            "regex",
            "whitespace inside the brackets",
            "style",
            "naming",
            "readability",
            "comment quality",
            "could be simpler",
            "could be more precise",
            "preference",
        ],
    )
}

fn tool_looks_like_parser_or_extractor(spec: &TaskSpec) -> bool {
    let combined =
        format!("{} {} {}", spec.tool_name, spec.purpose, spec.approach).to_ascii_lowercase();

    contains_any(
        &combined,
        &[
            "extract",
            "parser",
            "parse",
            "markdown",
            "todo",
            "checklist",
            "slug",
            "transform",
            "summar",
            "digest",
        ],
    )
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
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
    file_provenance_operation_id: &str,
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

        match write_tool_file(
            spec,
            &current_code,
            allow_overwrite,
            file_provenance_operation_id,
        )
        .await
        {
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
                if let Err(error) =
                    write_built_tool_manifest(spec, Some(file_provenance_operation_id))
                {
                    remove_built_tool_artifacts(spec, Some(file_provenance_operation_id));
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
         Keep the output schema consistent even on errors: include \"success\": false when applicable,\n\
         preserve the primary output keys with empty/default values, and include an \"error\" string.\n\n\
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

async fn write_tool_file(
    spec: &TaskSpec,
    code: &str,
    allow_overwrite: bool,
    file_provenance_operation_id: &str,
) -> Result<()> {
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
            "content": code,
            "provenance": {
                "actor": "nyx",
                "source": "forge.tool_build",
                "action_kind": "tool_code_write",
                "operation_id": file_provenance_operation_id,
                "description": spec.purpose,
                "metadata": {
                    "tool_name": spec.tool_name,
                    "filename": spec.filename,
                    "path_kind": "tool_code",
                    "test_expected": spec.test_expected,
                }
            }
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
                    return Some(normalize_multiline_sample(trimmed));
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
