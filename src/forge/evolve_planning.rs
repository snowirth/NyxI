//! Evolve planning — extracting and grounding code-change plans.
//!
//! This submodule owns the LLM-driven flow that turns a natural-language
//! evolve request into a concrete `PlannedChange` that `protected_evolve`
//! can apply. That includes:
//!   • the intent + implementation lane split used to draft the plan
//!   • retry/repair calls that re-ground the plan against the real repo
//!   • repo-text scanning heuristics (tokens, markdown anchors, doc notes)
//!   • the guardrail prompt + delta previews shown to the intent lane
//!
//! Everything here is `pub(super)` so only the parent `forge` module sees it.

use anyhow::{Result, anyhow, bail};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::llm::LlmGate;

use super::protected_evolve::PlannedChange;
use super::{
    ForgeLlmUsage, chat_forge_intent_lane_traced, normalize_compare_text, parse_json_response,
    sanitize_relative_repo_path,
};

pub(super) async fn plan_evolve_change(
    llm: &Arc<LlmGate>,
    user_request: &str,
    llm_usage: &mut ForgeLlmUsage,
) -> Result<PlannedChange> {
    let repo_context = build_evolve_repo_context(user_request);
    let intent_prompt = format!(
        "The user wants to modify code. Extract:\n\
         1. PATH: which file\n\
         2. WHAT: what to change (in plain English)\n\
         3. WHY: why they want this change\n\n\
         Request: \"{}\"\n\n\
         Choose PATH from an existing repo file when possible.\n{}\n\
         Reply as JSON only.",
        user_request, repo_context
    );

    let impl_prompt = format!(
        "The user wants to modify code. Extract the exact code changes:\n\
         1. PATH: which file\n\
         2. SEARCH: exact text to find\n\
         3. REPLACE: exact text to replace with\n\
         4. DESCRIPTION: what this change does\n\n\
         Request: \"{}\"\n\n\
         PATH must refer to an existing repo file.\n\
         SEARCH must be exact existing text from the chosen file.\n{}\n\
         Reply as JSON only.",
        user_request, repo_context
    );

    let (intent_result, impl_result) = tokio::join!(
        chat_forge_intent_lane_traced(llm, &intent_prompt, 150),
        llm.chat_auto_with_fallback_traced(&impl_prompt, 500)
    );

    let implementation_response = impl_result.map_err(|e| {
        anyhow!(
            "forge implementation lane ({}) failed: {}",
            llm_usage.predicted_implementation_model,
            e
        )
    })?;
    llm_usage.record("plan_evolve_implementation", &implementation_response.trace);
    let impl_json = parse_json_response(&implementation_response.text);

    let Some(mut plan) = planned_change_from_json(&impl_json).or(retry_extract_evolve_change(
        llm,
        user_request,
        llm_usage,
    )
    .await?) else {
        bail!("couldn't extract code changes");
    };
    stabilize_evolve_plan(user_request, &mut plan);
    if let Some(issue) = validate_evolve_plan_against_repo(&plan, user_request) {
        if let Some(corrected) = repair_evolve_plan_against_repo(
            llm,
            user_request,
            &plan,
            &issue,
            &repo_context,
            llm_usage,
        )
        .await?
        {
            plan = corrected;
        }
    }

    let intent_response = intent_result.ok();
    if let Some(response) = &intent_response {
        llm_usage.record("plan_evolve_intent", &response.trace);
    }
    let intent_json = intent_response
        .as_ref()
        .map(|response| parse_json_response(&response.text));
    if let Some(intent) = &intent_json {
        let what = intent["WHAT"]
            .as_str()
            .or(intent["what"].as_str())
            .unwrap_or("");
        if !what.is_empty() {
            let mut verify_prompt = build_evolve_guardrail_prompt(what, &plan);
            if let Ok(verdict) = chat_forge_intent_lane_traced(llm, &verify_prompt, 50).await {
                llm_usage.record("plan_evolve_guardrail", &verdict.trace);
                if verdict.text.to_uppercase().contains("FAIL") {
                    if let Some(corrected) = repair_evolve_plan_against_repo(
                        llm,
                        user_request,
                        &plan,
                        verdict.text.trim(),
                        &repo_context,
                        llm_usage,
                    )
                    .await?
                    {
                        plan = corrected;
                        verify_prompt = build_evolve_guardrail_prompt(what, &plan);
                        if let Ok(retry_verdict) =
                            chat_forge_intent_lane_traced(llm, &verify_prompt, 50).await
                        {
                            llm_usage.record("plan_evolve_guardrail_retry", &retry_verdict.trace);
                            if retry_verdict.text.to_uppercase().contains("FAIL") {
                                bail!(
                                    "forge intent lane ({}) rejected plan: {}",
                                    llm.preferred_chat_model_label(),
                                    retry_verdict.text.trim()
                                );
                            }
                        }
                    } else {
                        bail!(
                            "forge intent lane ({}) rejected plan: {}",
                            llm.preferred_chat_model_label(),
                            verdict.text.trim()
                        );
                    }
                }
            }
        }
    }

    Ok(plan)
}

fn planned_change_from_json(value: &serde_json::Value) -> Option<PlannedChange> {
    let path = value["PATH"]
        .as_str()
        .or(value["path"].as_str())
        .unwrap_or("")
        .trim();
    let search = value["SEARCH"]
        .as_str()
        .or(value["search"].as_str())
        .unwrap_or("")
        .trim();
    let replace = value["REPLACE"]
        .as_str()
        .or(value["replace"].as_str())
        .unwrap_or("")
        .trim();
    let description = value["DESCRIPTION"]
        .as_str()
        .or(value["description"].as_str())
        .unwrap_or("")
        .trim();

    if path.is_empty() || search.is_empty() || replace.is_empty() {
        return None;
    }

    Some(PlannedChange {
        path: path.to_string(),
        search: search.to_string(),
        replace: replace.to_string(),
        description: description.to_string(),
    })
}

async fn repair_evolve_plan_against_repo(
    llm: &Arc<LlmGate>,
    user_request: &str,
    plan: &PlannedChange,
    issue: &str,
    repo_context: &str,
    llm_usage: &mut ForgeLlmUsage,
) -> Result<Option<PlannedChange>> {
    let correction_prompt = format!(
        "The previous evolve plan does not line up with the real repository.\n\n\
         User request: \"{}\"\n\
         Invalid plan:\n\
         PATH: {}\n\
         SEARCH: {}\n\
         REPLACE: {}\n\
         DESCRIPTION: {}\n\n\
         Problem: {}\n\n\
         Return a corrected JSON object with PATH, SEARCH, REPLACE, DESCRIPTION.\n\
         PATH must be one of the existing repo files below.\n\
         SEARCH must be exact existing text from the chosen file.\n{}\n\
         Reply as JSON only.",
        user_request,
        crate::trunc(&plan.path, 160),
        crate::trunc(&plan.search, 220),
        crate::trunc(&plan.replace, 220),
        crate::trunc(&plan.description, 180),
        issue,
        repo_context,
    );

    let response = match llm
        .chat_auto_with_fallback_traced(&correction_prompt, 700)
        .await
    {
        Ok(response) => response,
        Err(_) => return Ok(None),
    };
    llm_usage.record("plan_evolve_grounding", &response.trace);
    let mut corrected = match planned_change_from_json(&parse_json_response(&response.text)) {
        Some(plan) => plan,
        None => return Ok(None),
    };
    stabilize_evolve_plan(user_request, &mut corrected);
    Ok(validate_evolve_plan_against_repo(&corrected, user_request)
        .is_none()
        .then_some(corrected))
}

fn stabilize_evolve_plan(user_request: &str, plan: &mut PlannedChange) {
    if plan.description.trim().is_empty() {
        plan.description = user_request.trim().to_string();
    }
    if let Some(grounded) = ground_evolve_path_by_search_match(plan, user_request) {
        *plan = grounded;
    }
    reroute_markdown_note_plan(user_request, plan);
    ensure_requested_doc_note(plan, user_request);
}

fn validate_evolve_plan_against_repo(plan: &PlannedChange, user_request: &str) -> Option<String> {
    let rel_path = match sanitize_relative_repo_path(&plan.path) {
        Ok(path) => path,
        Err(error) => return Some(error.to_string()),
    };
    if request_prefers_markdown(user_request)
        && rel_path.extension().and_then(|ext| ext.to_str()) != Some("md")
    {
        return Some(format!(
            "request targets docs but planned path is not markdown: {}",
            rel_path.display()
        ));
    }
    let full_path = std::env::current_dir().ok()?.join(&rel_path);
    if !full_path.exists() {
        return Some(format!("file not found: {}", rel_path.display()));
    }
    let content = match std::fs::read_to_string(&full_path) {
        Ok(content) => content,
        Err(error) => return Some(format!("failed to read {}: {}", rel_path.display(), error)),
    };
    if !content.contains(&plan.search) {
        return Some(format!("search text not found in {}", rel_path.display()));
    }
    if let Some(note_sentence) = requested_doc_note_sentence(user_request) {
        if !text_semantically_covers(&plan.replace, &note_sentence) {
            return Some("replacement text does not include the requested doc note".to_string());
        }
    }
    None
}

fn ground_evolve_path_by_search_match(
    plan: &PlannedChange,
    user_request: &str,
) -> Option<PlannedChange> {
    if plan.search.trim().is_empty() {
        return None;
    }

    let request_terms = request_tokens(user_request);
    let path_terms = request_tokens(&plan.path);
    let desired_markdown = plan.path.ends_with(".md")
        || user_request.to_lowercase().contains("docs")
        || user_request.to_lowercase().contains("readme")
        || user_request.to_lowercase().contains("design");

    let mut candidates = collect_repo_text_files();
    candidates.retain(|path| {
        if desired_markdown {
            path.extension().and_then(|ext| ext.to_str()) == Some("md")
        } else {
            true
        }
    });

    let mut matches = candidates
        .into_iter()
        .filter_map(|candidate| {
            let content = std::fs::read_to_string(&candidate).ok()?;
            if !content.contains(&plan.search) {
                return None;
            }
            let rel = candidate.to_string_lossy().to_string();
            let rel_tokens = request_tokens(&rel);
            let overlap = rel_tokens
                .iter()
                .filter(|token| request_terms.contains(*token) || path_terms.contains(*token))
                .count();
            let score = overlap
                + usize::from(
                    desired_markdown
                        && candidate.extension().and_then(|ext| ext.to_str()) == Some("md"),
                );
            Some((score, rel))
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.len().cmp(&right.1.len()))
            .then_with(|| left.1.cmp(&right.1))
    });

    matches.first().map(|candidate| PlannedChange {
        path: candidate.1.clone(),
        search: plan.search.clone(),
        replace: plan.replace.clone(),
        description: plan.description.clone(),
    })
}

fn build_evolve_repo_context(user_request: &str) -> String {
    let candidates = collect_repo_candidates(user_request, 4);
    if candidates.is_empty() {
        return "Existing repo file candidates: (none found)".to_string();
    }

    let mut out = String::from("Existing repo file candidates:\n");
    for candidate in candidates {
        out.push_str(&format!("- {}\n", candidate.path));
        for snippet in candidate.snippets {
            out.push_str(&format!("  snippet: {}\n", crate::trunc(&snippet, 180)));
        }
    }
    out
}

async fn retry_extract_evolve_change(
    llm: &Arc<LlmGate>,
    user_request: &str,
    llm_usage: &mut ForgeLlmUsage,
) -> Result<Option<PlannedChange>> {
    let concise_context = build_evolve_repo_context(user_request);
    let retry_prompt = format!(
        "Return ONLY one JSON object with these exact keys: PATH, SEARCH, REPLACE, DESCRIPTION.\n\
         Do not add markdown fences.\n\
         Do not return an array.\n\
         PATH must be one of the existing repo files below.\n\
         SEARCH must be exact existing text from that file.\n\n\
         Request: \"{}\"\n\n\
         JSON shape example:\n\
         {{\"PATH\":\"DESIGN.md\",\"SEARCH\":\"exact old text\",\"REPLACE\":\"exact new text\",\"DESCRIPTION\":\"what the change does\"}}\n\n\
         {}\n",
        user_request, concise_context,
    );

    let response = match llm.chat_auto_with_fallback_traced(&retry_prompt, 320).await {
        Ok(response) => response,
        Err(_) => return Ok(None),
    };
    llm_usage.record("plan_evolve_implementation_retry", &response.trace);
    Ok(planned_change_from_json(&parse_json_response(
        &response.text,
    )))
}

#[derive(Debug)]
struct RepoGroundingCandidate {
    path: String,
    snippets: Vec<String>,
    score: usize,
}

fn collect_repo_candidates(user_request: &str, limit: usize) -> Vec<RepoGroundingCandidate> {
    let tokens = request_tokens(user_request);
    let docs_bias = request_prefers_markdown(user_request);

    let mut candidates = collect_repo_text_files()
        .into_iter()
        .filter(|path| !docs_bias || path.extension().and_then(|ext| ext.to_str()) == Some("md"))
        .filter_map(|path| {
            let content = std::fs::read_to_string(&path).ok()?;
            let snippets = repo_candidate_snippets(&content, &tokens);
            let path_str = path.to_string_lossy().to_string();
            let path_overlap = request_tokens(&path_str)
                .iter()
                .filter(|token| tokens.contains(*token))
                .count();
            let snippet_overlap = snippets
                .iter()
                .map(|snippet| {
                    request_tokens(snippet)
                        .iter()
                        .filter(|token| tokens.contains(*token))
                        .count()
                })
                .sum::<usize>();
            let markdown_bonus = usize::from(
                docs_bias && path.extension().and_then(|ext| ext.to_str()) == Some("md"),
            );
            let score = path_overlap * 3 + snippet_overlap + markdown_bonus;
            (score > 0).then_some(RepoGroundingCandidate {
                path: path_str,
                snippets,
                score,
            })
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.path.len().cmp(&right.path.len()))
            .then_with(|| left.path.cmp(&right.path))
    });
    candidates.truncate(limit);
    candidates
}

fn repo_candidate_snippets(content: &str, tokens: &[String]) -> Vec<String> {
    let mut snippets = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.len() > 220 {
            continue;
        }
        let overlap = request_tokens(trimmed)
            .iter()
            .filter(|token| tokens.contains(*token))
            .count();
        if overlap > 0 || trimmed.starts_with('#') {
            let candidate = trimmed.to_string();
            if !snippets.contains(&candidate) {
                snippets.push(candidate);
            }
        }
        if snippets.len() >= 2 {
            break;
        }
    }
    if snippets.is_empty() {
        content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .take(1)
            .map(str::to_string)
            .collect()
    } else {
        snippets
    }
}

fn collect_repo_text_files() -> Vec<PathBuf> {
    let root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut files = Vec::new();
    collect_repo_text_files_recursive(&root, &root, 0, &mut files);
    files
}

fn collect_repo_text_files_recursive(
    root: &Path,
    dir: &Path,
    depth: usize,
    files: &mut Vec<PathBuf>,
) {
    if depth > 4 || files.len() >= 400 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if path.is_dir() {
            if matches!(
                name,
                ".git" | "target" | "__pycache__" | ".venv" | "node_modules" | ".idea"
            ) {
                continue;
            }
            collect_repo_text_files_recursive(root, &path, depth + 1, files);
            continue;
        }
        if !is_repo_text_candidate(&path) {
            continue;
        }
        if let Ok(relative) = path.strip_prefix(root) {
            files.push(relative.to_path_buf());
        }
    }
}

fn is_repo_text_candidate(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("md" | "rs" | "py" | "toml" | "json" | "html" | "css" | "js")
    )
}

fn request_tokens(input: &str) -> Vec<String> {
    input
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(|token| {
            let token = token.trim().to_ascii_lowercase();
            if token.len() < 3 { None } else { Some(token) }
        })
        .collect()
}

fn request_prefers_markdown(user_request: &str) -> bool {
    let lower = user_request.to_lowercase();
    lower.contains("docs")
        || lower.contains("documentation")
        || lower.contains("readme")
        || lower.contains("design")
        || lower.contains("roadmap")
        || lower.contains("memory system")
}

#[derive(Debug, Clone)]
pub(super) struct MarkdownAnchorCandidate {
    pub(super) path: String,
    pub(super) anchor: String,
    pub(super) score: isize,
}

pub(super) fn reroute_markdown_note_plan(user_request: &str, plan: &mut PlannedChange) {
    let Some(note_sentence) = requested_doc_note_sentence(user_request) else {
        return;
    };
    let Some(best) = best_markdown_anchor_candidate(user_request) else {
        return;
    };

    let current_score = score_markdown_anchor_candidate(user_request, &plan.path, &plan.search);
    let should_reroute = current_score
        .map(|score| best.score >= score + 5)
        .unwrap_or(true);
    if !should_reroute {
        return;
    }

    plan.path = best.path;
    plan.search = best.anchor.clone();
    plan.replace = format!("{}\n\nNote: {}", best.anchor.trim_end(), note_sentence);
    if plan.description.trim().is_empty() {
        plan.description = user_request.trim().to_string();
    }
}

pub(super) fn ensure_requested_doc_note(plan: &mut PlannedChange, user_request: &str) {
    let Some(note_sentence) = requested_doc_note_sentence(user_request) else {
        return;
    };
    if text_semantically_covers(&plan.replace, &note_sentence) {
        return;
    }

    let note_line = format!("Note: {}", note_sentence);
    let search_trimmed = plan.search.trim_end();
    let replace_trimmed = plan.replace.trim();

    plan.replace = if replace_trimmed.is_empty()
        || replace_trimmed.eq_ignore_ascii_case("note")
        || replace_trimmed.eq_ignore_ascii_case("note:")
        || replace_trimmed.len() <= 8
    {
        format!("{}\n\n{}", search_trimmed, note_line)
    } else if replace_trimmed.contains(search_trimmed) {
        format!("{}\n\n{}", replace_trimmed.trim_end(), note_line)
    } else {
        format!("{}\n\n{}", search_trimmed, note_line)
    };
}

pub(super) fn requested_doc_note_sentence(user_request: &str) -> Option<String> {
    if !request_prefers_markdown(user_request) {
        return None;
    }
    let trimmed = user_request.trim();
    let trimmed = trimmed.trim_end_matches(['.', '!', '?']);
    let lower = trimmed.to_ascii_lowercase();
    if !lower.contains("note") {
        return None;
    }

    let raw_clause = if let Some(index) = lower.rfind(" that ") {
        trimmed[index + 6..].trim()
    } else if let Some(index) = lower.rfind(" saying ") {
        trimmed[index + 8..].trim()
    } else if let Some(index) = lower.rfind(" about ") {
        trimmed[index + 7..].trim()
    } else {
        return None;
    };

    let clause = raw_clause
        .trim()
        .trim_matches(|ch: char| ch == '"' || ch == '\'');
    if clause.is_empty() {
        return None;
    }

    let mut chars = clause.chars();
    let first = chars.next()?;
    let mut sentence = first.to_uppercase().collect::<String>();
    sentence.push_str(chars.as_str());
    if !sentence.ends_with('.') && !sentence.ends_with('!') && !sentence.ends_with('?') {
        sentence.push('.');
    }
    Some(sentence)
}

pub(super) fn best_markdown_anchor_candidate(
    user_request: &str,
) -> Option<MarkdownAnchorCandidate> {
    let request_terms = request_tokens(user_request);
    collect_repo_text_files()
        .into_iter()
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("md"))
        .filter_map(|path| {
            let content = std::fs::read_to_string(&path).ok()?;
            let anchor = best_markdown_anchor_line(user_request, &content, &request_terms)?;
            let path_str = path.to_string_lossy().to_string();
            let score = score_markdown_anchor_candidate(user_request, &path_str, &anchor)?;
            (score > 0).then_some(MarkdownAnchorCandidate {
                path: path_str,
                anchor,
                score,
            })
        })
        .max_by(|left, right| {
            left.score
                .cmp(&right.score)
                .then_with(|| right.anchor.len().cmp(&left.anchor.len()))
                .then_with(|| left.path.cmp(&right.path))
        })
}

fn best_markdown_anchor_line(
    user_request: &str,
    content: &str,
    request_terms: &[String],
) -> Option<String> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && line.len() <= 220)
        .map(|line| {
            let line_terms = request_tokens(line);
            let overlap = line_terms
                .iter()
                .filter(|token| request_terms.contains(*token))
                .count() as isize;
            let heading_bonus = if line.starts_with('#') { 2 } else { 0 };
            let topic_bonus = markdown_anchor_topic_bonus(user_request, line);
            let shape_bonus = markdown_anchor_shape_bonus(user_request, line);
            let score = overlap + heading_bonus + topic_bonus + shape_bonus;
            (score, line.to_string())
        })
        .filter(|(score, _)| *score > 0)
        .max_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| right.1.len().cmp(&left.1.len()))
                .then_with(|| left.1.cmp(&right.1))
        })
        .map(|(_, line)| line)
}

fn markdown_anchor_shape_bonus(user_request: &str, line: &str) -> isize {
    let trimmed = line.trim();
    let lower_request = user_request.to_ascii_lowercase();
    let is_numbered =
        trimmed.chars().next().is_some_and(|ch| ch.is_ascii_digit()) && trimmed.contains(". ");
    let is_bullet = trimmed.starts_with("- ") || trimmed.starts_with("* ");
    let is_heading = trimmed.starts_with('#');

    let mut bonus = 0;
    if is_numbered || is_bullet {
        let build_order_request = lower_request.contains("phase")
            || lower_request.contains("build order")
            || lower_request.contains("roadmap")
            || lower_request.contains("step");
        if !build_order_request {
            bonus -= 4;
        }
    }
    if !is_heading && !is_numbered && !is_bullet && trimmed.ends_with('.') {
        bonus += 4;
    }
    if !is_heading && !is_numbered && !is_bullet && trimmed.len() >= 24 && trimmed.len() <= 140 {
        bonus += 2;
    }
    if trimmed
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_uppercase())
    {
        bonus += 1;
    }

    bonus
}

fn markdown_anchor_topic_bonus(user_request: &str, line: &str) -> isize {
    let request_lower = user_request.to_ascii_lowercase();
    let line_lower = line.to_ascii_lowercase();
    let mut bonus = 0;

    if request_lower.contains("planner") && line_lower.contains("planner") {
        bonus += 3;
    }
    if request_lower.contains("lane") && line_lower.contains("lane") {
        bonus += 2;
    }
    if request_lower.contains("default") && line_lower.contains("default") {
        bonus += 1;
    }
    if request_lower.contains("fast") && line_lower.contains("fast") {
        bonus += 1;
    }
    if request_lower.contains("runtime") && line_lower.contains("runtime") {
        bonus += 1;
    }

    bonus
}

fn score_markdown_anchor_candidate(user_request: &str, path: &str, anchor: &str) -> Option<isize> {
    if path.trim().is_empty() || anchor.trim().is_empty() {
        return None;
    }

    let request_terms = request_tokens(user_request);
    let lower = user_request.to_ascii_lowercase();
    let lower_path = path.to_ascii_lowercase();
    let path_overlap = request_tokens(path)
        .iter()
        .filter(|token| request_terms.contains(*token))
        .count() as isize;
    let anchor_overlap = request_tokens(anchor)
        .iter()
        .filter(|token| request_terms.contains(*token))
        .count() as isize;

    let mut score = path_overlap * 3 + anchor_overlap;

    if lower.contains("runtime") {
        if lower_path.contains("design") {
            score += 10;
        }
        if lower_path.contains("readme") {
            score += 3;
        }
        if lower_path.contains("soul") || lower_path.contains("identity") {
            score -= 8;
        }
    }
    if lower.contains("memory") {
        if lower_path.ends_with("memory.md") {
            score += 10;
        }
        if lower_path.contains("memory_benchmark") || lower_path.contains("longmemeval_benchmark") {
            score += 5;
        }
        if lower_path.contains("design") {
            score += 3;
        }
    }
    if lower.contains("roadmap") || lower.contains("phase") {
        if lower_path.contains("system_refinement_roadmap") {
            score += 12;
        }
    }
    if lower.contains("benchmark") || lower.contains("longmemeval") {
        if lower_path.contains("benchmark") {
            score += 12;
        }
    }
    if lower.contains("soul") {
        if lower_path.contains("soul") {
            score += 15;
        }
    } else if lower_path.contains("soul") {
        score -= 2;
    }
    if lower.contains("identity") {
        if lower_path.contains("identity") {
            score += 15;
        }
    } else if lower_path.contains("identity") {
        score -= 2;
    }

    Some(score)
}

fn text_semantically_covers(text: &str, target: &str) -> bool {
    let text_lower = text.to_ascii_lowercase();
    let tokens = request_tokens(target)
        .into_iter()
        .filter(|token| {
            !matches!(
                token.as_str(),
                "that"
                    | "note"
                    | "docs"
                    | "doc"
                    | "short"
                    | "about"
                    | "saying"
                    | "with"
                    | "from"
                    | "into"
            )
        })
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return true;
    }
    let matches = tokens
        .iter()
        .filter(|token| text_lower.contains(token.as_str()))
        .count();
    let required = usize::min(tokens.len(), usize::max(2, (tokens.len() + 1) / 2));
    matches >= required
}

fn build_evolve_guardrail_prompt(what: &str, plan: &PlannedChange) -> String {
    let search_preview = collapsed_preview(&plan.search, 100);
    let replace_preview = collapsed_preview(&plan.replace, 160);
    let mut prompt = format!(
        "User wanted: \"{}\"\nPlanned change in {}:\n- SEARCH: \"{}\"\n- REPLACE: \"{}\"",
        what, plan.path, search_preview, replace_preview
    );
    if let Some(delta_preview) = replacement_delta_preview(&plan.search, &plan.replace, 160) {
        prompt.push_str(&format!("\n- CHANGED CONTENT FOCUS: \"{}\"", delta_preview));
    }
    prompt.push_str(
        "\n\nDoes this change match what the user asked? Reply PASS or FAIL with one line explanation.",
    );
    prompt
}

pub(super) fn replacement_delta_preview(search: &str, replace: &str, max: usize) -> Option<String> {
    let trimmed_search = search.trim();
    let trimmed_replace = replace.trim();
    if trimmed_search.is_empty() || trimmed_replace.is_empty() {
        return None;
    }
    if let Some(index) = trimmed_replace.find(trimmed_search) {
        let mut delta = String::new();
        let before = trimmed_replace[..index].trim();
        let after = trimmed_replace[index + trimmed_search.len()..].trim();
        if !before.is_empty() {
            delta.push_str(before);
        }
        if !before.is_empty() && !after.is_empty() {
            delta.push(' ');
        }
        if !after.is_empty() {
            delta.push_str(after);
        }
        let delta = delta.trim();
        if !delta.is_empty() {
            return Some(collapsed_preview(delta, max));
        }
    }
    (!normalize_compare_text(trimmed_search).eq(&normalize_compare_text(trimmed_replace)))
        .then(|| collapsed_preview(trimmed_replace, max))
}

fn collapsed_preview(text: &str, max: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return "(empty)".to_string();
    }
    if collapsed.len() <= max {
        return collapsed;
    }
    let keep = max.saturating_sub(3);
    format!("{}...", crate::trunc(&collapsed, keep))
}
