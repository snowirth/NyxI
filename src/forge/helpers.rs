use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};

use super::sandbox::{SandboxSpec, StagedFile, shared_sandbox};
use crate::llm::LlmGate;

pub(super) async fn chat_forge_intent_lane_traced(
    llm: &Arc<LlmGate>,
    prompt: &str,
    max_tokens: u32,
) -> Result<crate::llm::LlmTextResponse> {
    match llm.chat_traced(prompt, max_tokens).await {
        Ok(response) => Ok(response),
        Err(primary_error) => llm
            .chat_auto_with_fallback_traced(prompt, max_tokens)
            .await
            .with_context(|| format!("forge intent lane primary call failed: {}", primary_error)),
    }
}

pub(super) fn parse_tool_input_json(raw: &str) -> Result<serde_json::Value> {
    if raw.trim().is_empty() {
        Ok(serde_json::json!({}))
    } else {
        serde_json::from_str::<serde_json::Value>(raw)
            .map_err(|e| anyhow!("invalid test input JSON: {}", e))
    }
}

pub(super) async fn run_tool_with_expectation(
    path: &Path,
    input: &serde_json::Value,
    expected: &str,
) -> Result<serde_json::Value> {
    let result = run_built_tool_file(path, input).await?;
    if result.get("success").and_then(|v| v.as_bool()) == Some(false) {
        let err = result["error"].as_str().unwrap_or("tool runtime failure");
        bail!("tool runtime failure: {}", err);
    }

    if !expected.trim().is_empty() {
        let normalized_output = normalize_compare_text(&result.to_string());
        let normalized_expected = normalize_compare_text(expected);
        if !normalized_output.contains(&normalized_expected) {
            bail!(
                "runtime output mismatch [expected]{}[/expected] [actual]{}[/actual]",
                expected,
                result
            );
        }
    }

    Ok(result)
}

pub(super) fn normalize_compare_text(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace()).collect()
}

pub(super) fn accept_repair_candidate(candidate: &str, current_norm: &str) -> Option<String> {
    let code = strip_code_fences(candidate);
    if code.len() < 30 {
        return None;
    }
    if normalize_compare_text(&code) == current_norm {
        return None;
    }
    Some(code)
}

pub(super) async fn run_built_tool_file(
    path: &Path,
    args: &serde_json::Value,
) -> Result<serde_json::Value> {
    let cwd = std::env::current_dir()?;
    let script = cwd.join(path);
    if !script.exists() {
        bail!("built tool missing: {}", path.display());
    }

    let sandbox = shared_sandbox();
    // Keep the host-path in argv for HostSandbox compatibility (which runs
    // python3 directly and sees the host filesystem). Also declare the file
    // as "staged" so DockerSandbox copies it into /work and rewrites the
    // matching arg to `/work/<script_name>` before launching the container.
    let script_name = script
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "tool.py".to_string());
    // No `-I`: the Sandbox trait is the real isolation boundary. Python's
    // isolate mode would block user site-packages (e.g. pip-installed
    // playwright for the browser tool) while giving no defense the sandbox
    // does not already provide.
    let spec = SandboxSpec::new([script.to_string_lossy().to_string()])
        .cwd(&cwd)
        .stdin_bytes(serde_json::to_vec(args)?)
        .timeout(std::time::Duration::from_secs(30))
        .stage(StagedFile {
            host_path: script.clone(),
            container_name: script_name,
        });
    let output = sandbox.run_python(&spec).await?;

    tracing::debug!(
        backend = output.backend.as_str(),
        script = %path.display(),
        "built tool executed via sandbox"
    );

    let stdout = output.stdout_str().trim().to_string();
    let stderr = output.stderr_str().trim().to_string();
    serde_json::from_str::<serde_json::Value>(&stdout).map_err(|_| {
        if !stderr.is_empty() {
            anyhow!("built tool parse error: {}", stderr)
        } else if !stdout.is_empty() {
            anyhow!("built tool parse error: {}", stdout)
        } else {
            anyhow!("built tool parse error")
        }
    })
}

pub(super) fn parse_json_response(text: &str) -> serde_json::Value {
    let clean = strip_code_fences(text);
    let parsed: serde_json::Value = serde_json::from_str(&clean).unwrap_or_else(|_| {
        if let Some(start) = clean.find('{') {
            if let Some(end) = clean.rfind('}') {
                return serde_json::from_str(&clean[start..=end]).unwrap_or_default();
            }
        }
        serde_json::Value::Object(serde_json::Map::new())
    });

    match parsed {
        serde_json::Value::Array(items) => items
            .into_iter()
            .find(|item| item.is_object())
            .unwrap_or_default(),
        other => other,
    }
}

pub(super) fn strip_code_fences(text: &str) -> String {
    text.trim()
        .trim_start_matches("```python")
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim()
        .to_string()
}

pub(super) fn sanitize_relative_repo_path(path: &str) -> Result<PathBuf> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        bail!("path required");
    }

    let candidate = Path::new(trimmed);
    if candidate.is_absolute() {
        bail!("absolute paths not allowed");
    }

    let mut clean = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => clean.push(part),
            _ => bail!("path traversal not allowed"),
        }
    }

    if clean.as_os_str().is_empty() {
        bail!("path required");
    }

    Ok(clean)
}
