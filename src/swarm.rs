//! Swarm — parallel agent execution for complex tasks.
//!
//! Coordinator is Rust code (heuristics), not an LLM. Agents are just
//! parallel LLM calls. Merge is one cheap call or simple concatenation.
//!
//! Pattern: split → fan-out → fan-in → merge
//! When NOT to split: short queries, conversational, sequential dependency.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::task::JoinSet;

use crate::llm::LlmGate;

/// How to combine results from parallel agents.
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    /// Just join outputs with headers
    Concatenate,
    /// LLM synthesizes into one coherent response
    Synthesize,
}

/// A subtask for one agent.
#[derive(Debug, Clone)]
pub struct SubTask {
    pub id: String,
    pub prompt: String,
}

/// Result from one agent.
#[derive(Debug, Clone)]
struct AgentResult {
    id: String,
    output: String,
}

/// A split plan — what to parallelize and how to merge.
pub struct TaskSplit {
    pub subtasks: Vec<SubTask>,
    pub strategy: MergeStrategy,
    pub merge_context: String, // original query for synthesis
    pub merge_guidance: String,
}

fn should_avoid_split(lower: &str) -> bool {
    let speculative_or_narrative = [
        "i saw ",
        "he said ",
        "she said ",
        "they said ",
        "what gonna happen",
        "what will happen",
        "grows up",
    ];
    let high_risk_terms = ["nigger", "nigga", "faggot", "kike", "spic"];
    speculative_or_narrative
        .iter()
        .any(|needle| lower.contains(needle))
        || high_risk_terms.iter().any(|needle| lower.contains(needle))
}

fn looks_like_internal_synthesis_leak(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("multiple researchers found this")
        || lower.contains("synthesize into one coherent response")
        || lower.contains("don't mention multiple agents")
        || lower.contains("the user asked:")
        || lower.contains("turn>user")
        || lower.contains("<box>")
}

fn looks_like_unusable_policy_stub(text: &str) -> bool {
    text.to_ascii_lowercase()
        .starts_with("based on user safety guidelines")
}

fn looks_like_reasoning_trace(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "here's a thinking process",
        "thinking process that leads",
        "deconstruct the request",
        "initial brainstorming",
        "structuring the response",
        "let's think step by step",
        "my thought process",
        "scratchpad",
        "internal monologue",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn looks_like_task_echo_line(line: &str) -> bool {
    let lower = line.trim().to_ascii_lowercase();
    [
        "research ",
        "explain ",
        "describe ",
        "summarize ",
        "tell me about ",
        "give a high-level overview of ",
        "high-level overview of ",
        "technical details of ",
        "practical aspects of ",
        "task:",
    ]
    .iter()
    .any(|prefix| lower.starts_with(prefix))
        || lower.contains("be thorough")
        || lower.contains("key features, pros, cons, use cases")
}

fn strip_known_control_tokens(text: &str) -> String {
    let mut without_pipe_tokens = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'<' && bytes[i + 1] == b'|' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'|' && bytes[i + 1] == b'>') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        without_pipe_tokens.push(bytes[i] as char);
        i += 1;
    }

    let mut cleaned = without_pipe_tokens.trim().to_string();
    for token in [
        "<bos>",
        "<eos>",
        "<turn|>",
        "<|assistant|>",
        "<assistant>",
        "</assistant>",
        "<|begin_of_text|>",
        "<|end_of_text|>",
    ] {
        cleaned = cleaned.replace(token, " ");
    }
    cleaned
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

fn normalize_agent_output(text: &str) -> String {
    let mut candidate = strip_known_control_tokens(text);

    for marker in ["<turn|>", "<|assistant|>", "<assistant>", "<|end|>", "</assistant>"] {
        if let Some(index) = candidate.rfind(marker) {
            let after = candidate[index + marker.len()..].trim();
            if !after.is_empty() {
                candidate = after.to_string();
            }
        }
    }

    loop {
        let Some(first_line) = candidate.lines().next() else {
            break;
        };
        if !looks_like_task_echo_line(first_line) {
            break;
        }

        let remaining = candidate
            .lines()
            .skip(1)
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_string();
        if remaining.is_empty() {
            break;
        }
        candidate = remaining;
    }

    candidate.trim().to_string()
}

fn sanitize_agent_output(text: &str) -> Option<String> {
    let normalized = normalize_agent_output(text);
    let trimmed = normalized.trim();
    if trimmed.is_empty()
        || looks_like_internal_synthesis_leak(trimmed)
        || looks_like_reasoning_trace(trimmed)
        || trimmed.contains("<turn|")
        || trimmed.contains("<|assistant|>")
    {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn sanitize_user_visible_synthesis(text: &str) -> Option<String> {
    let normalized = strip_known_control_tokens(text);
    let trimmed = normalized.trim();
    if trimmed.is_empty() {
        return None;
    }
    if !looks_like_internal_synthesis_leak(trimmed)
        && !looks_like_reasoning_trace(trimmed)
        && !looks_like_unusable_policy_stub(trimmed)
    {
        return Some(trimmed.to_string());
    }

    let mut candidate = trimmed.to_string();
    for marker in [
        "Be concise and direct.",
        "Don't mention multiple agents.",
        "Remove redundancy. Resolve contradictions.",
    ] {
        if let Some(index) = candidate.rfind(marker) {
            let after = candidate[index + marker.len()..].trim();
            if !after.is_empty() {
                candidate = after.to_string();
            }
        }
    }

    let candidate = candidate.trim();
    if candidate.is_empty()
        || looks_like_internal_synthesis_leak(candidate)
        || looks_like_reasoning_trace(candidate)
        || looks_like_unusable_policy_stub(candidate)
    {
        None
    } else {
        Some(candidate.to_string())
    }
}

fn humanize_agent_id(id: &str) -> String {
    id.trim_start_matches("item_")
        .replace('_', " ")
        .replace('-', " ")
        .trim()
        .to_string()
}

fn fallback_merge_output(results: &[AgentResult]) -> String {
    let cleaned = results
        .iter()
        .filter_map(|result| {
            sanitize_agent_output(&result.output).map(|output| (result.id.clone(), output))
        })
        .collect::<Vec<_>>();
    if cleaned.is_empty() {
        "I couldn't merge the parallel research into a clean final answer this time. Please retry that request.".into()
    } else if cleaned.len() == 1 {
        cleaned[0].1.clone()
    } else {
        cleaned
            .iter()
            .map(|(id, output)| format!("### {}\n{}", humanize_agent_id(id), output))
            .collect::<Vec<_>>()
            .join("\n\n")
    }
}

fn clean_topic_fragment(fragment: &str) -> String {
    let mut text = fragment.trim();
    for prefix in [
        "compare ",
        "comparing ",
        "difference between ",
        "differences between ",
        "between ",
        "should i use ",
        "should i choose ",
    ] {
        if let Some(rest) = text.strip_prefix(prefix) {
            text = rest.trim();
            break;
        }
    }
    text.trim_matches(|c: char| ",.;:?!".contains(c)).to_string()
}

fn split_trailing_scope(fragment: &str) -> (String, Option<String>) {
    let trimmed = fragment.trim();
    for marker in [
        " for ", " in ", " when ", " on ", " under ", " with ", " as ", " at ",
    ] {
        if let Some(index) = trimmed.find(marker) {
            let topic = clean_topic_fragment(&trimmed[..index]);
            let scope = trimmed[index + marker.len()..]
                .trim()
                .trim_matches(|c: char| ",.;:?!".contains(c))
                .to_string();
            if !topic.is_empty() && !scope.is_empty() {
                return (topic, Some(scope));
            }
        }
    }
    (clean_topic_fragment(trimmed), None)
}

fn compare_subtask_prompt(topic: &str, scope: Option<&str>) -> String {
    if let Some(scope) = scope {
        format!(
            "Evaluate {} for {}.\nFocus on strengths, weaknesses, deployment fit, operational tradeoffs, and when to choose it over alternatives. Keep notes concise and user-facing.",
            topic, scope
        )
    } else {
        format!(
            "Evaluate {}.\nFocus on strengths, weaknesses, operational tradeoffs, and best use cases. Keep notes concise and user-facing.",
            topic
        )
    }
}

/// Try to split a user message into parallel subtasks.
/// Returns None if the task shouldn't be split.
/// Hybrid: heuristics first (free, instant), LLM classifier for ambiguous cases.
pub fn try_split(input: &str) -> Option<TaskSplit> {
    let lower = input.to_lowercase();
    let words: Vec<&str> = lower.split_whitespace().collect();

    // Too short — not worth splitting
    if words.len() < 8 {
        return None;
    }

    // Conversational — splitting kills coherence
    let conversational = [
        "how are you",
        "what's up",
        "hey",
        "thanks",
        "ok",
        "good morning",
        "goodnight",
        "going to sleep",
        "remind me",
    ];
    if conversational.iter().any(|c| lower.starts_with(c)) {
        return None;
    }

    if should_avoid_split(&lower) {
        return None;
    }

    // Questions about Nyx herself — don't split
    if lower.contains("what do you") || lower.contains("can you") || lower.contains("are you") {
        return None;
    }

    // === HEURISTIC PATTERNS (instant, free) ===

    if let Some(split) = try_comparison_split(input, &lower) {
        return Some(split);
    }
    if let Some(split) = try_list_split(input, &lower) {
        return Some(split);
    }
    if let Some(split) = try_breadth_split(input, &lower) {
        return Some(split);
    }

    // === LLM CLASSIFIER (only for long ambiguous queries) ===
    // Deferred — call try_split_llm() from the async handler
    None
}

/// LLM-based split classifier for queries that heuristics can't decide.
/// One cheap DeepSeek call, ~20 output tokens. Only called for 15+ word queries
/// that didn't match any heuristic pattern.
pub async fn try_split_llm(input: &str, llm: &Arc<LlmGate>) -> Option<TaskSplit> {
    let words: Vec<&str> = input.split_whitespace().collect();
    if words.len() < 15 {
        return None;
    }
    let lower = input.to_ascii_lowercase();
    if should_avoid_split(&lower) {
        return None;
    }

    let prompt = format!(
        "Classify this query. Reply with EXACTLY one line:\n\
         COMPARE: [topic A] | [topic B] — if comparing two things\n\
         LIST: [item1] | [item2] | [item3] — if asking about multiple items\n\
         BREADTH: [topic] — if asking for a deep/comprehensive overview\n\
         SINGLE — if a simple question that doesn't need splitting\n\n\
         Query: {}\n\n\
         Reply with one line only.",
        crate::trunc(input, 200)
    );

    let response = llm.chat_auto(&prompt, 30).await.ok()?;
    let trimmed = response.trim();

    if trimmed.starts_with("COMPARE:") {
        let rest = trimmed.trim_start_matches("COMPARE:").trim();
        let parts: Vec<&str> = rest.split('|').map(|s| s.trim()).collect();
        if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
            return Some(TaskSplit {
                subtasks: vec![
                    SubTask {
                        id: "side_a".into(),
                        prompt: format!(
                            "Research {} — features, strengths, weaknesses, use cases.",
                            parts[0]
                        ),
                    },
                    SubTask {
                        id: "side_b".into(),
                        prompt: format!(
                            "Research {} — features, strengths, weaknesses, use cases.",
                            parts[1]
                        ),
                    },
                ],
                strategy: MergeStrategy::Synthesize,
                merge_context: input.to_string(),
                merge_guidance:
                    "Answer as a direct comparison. Start with a short verdict, then compare each side and end with when to choose which.".into(),
            });
        }
    } else if trimmed.starts_with("LIST:") {
        let rest = trimmed.trim_start_matches("LIST:").trim();
        let items: Vec<&str> = rest
            .split('|')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        if items.len() >= 2 && items.len() <= 6 {
            return Some(TaskSplit {
                subtasks: items
                    .iter()
                    .map(|item| SubTask {
                        id: format!(
                            "item_{}",
                            item.replace(' ', "_").chars().take(20).collect::<String>()
                        ),
                        prompt: format!("Research and explain: {}", item),
                    })
                    .collect(),
                strategy: if items.len() <= 3 {
                    MergeStrategy::Concatenate
                } else {
                    MergeStrategy::Synthesize
                },
                merge_context: input.to_string(),
                merge_guidance:
                    "Answer with a short overview followed by a clearly separated section for each item.".into(),
            });
        }
    } else if trimmed.starts_with("BREADTH:") {
        let topic = trimmed.trim_start_matches("BREADTH:").trim();
        if topic.len() > 2 {
            return Some(TaskSplit {
                subtasks: vec![
                    SubTask {
                        id: "overview".into(),
                        prompt: format!(
                            "High-level overview of {}: what it is, why it matters. Focus only on the overview and avoid implementation detail overlap.",
                            topic
                        ),
                    },
                    SubTask {
                        id: "technical".into(),
                        prompt: format!(
                            "Technical details of {}: architecture, memory model, data flow, and key concepts. Avoid repeating the basic overview.",
                            topic
                        ),
                    },
                    SubTask {
                        id: "practical".into(),
                        prompt: format!(
                            "Practical aspects of {}: how to use it, operator workflows, patterns, gotchas, and rollout advice. Avoid repeating the general definition.",
                            topic
                        ),
                    },
                ],
                strategy: MergeStrategy::Synthesize,
                merge_context: format!("Tell me about {}", topic),
                merge_guidance:
                    "Give one cohesive answer with sections for overview, technical details, and practical guidance. Avoid repeating the same definitions.".into(),
            });
        }
    }

    None // SINGLE or unparseable
}

/// Detect "compare X and Y" / "X vs Y" patterns.
fn try_comparison_split(input: &str, lower: &str) -> Option<TaskSplit> {
    let patterns = [
        (" vs ", " vs "),
        (" versus ", " versus "),
        (" compared to ", " compared to "),
        (" or ", " or "), // "should I use X or Y"
    ];

    // "compare X and Y"
    if (lower.contains("compare") || lower.contains("difference"))
        && !lower.contains(" vs ")
        && !lower.contains(" versus ")
        && !lower.contains(" compared to ")
    {
        if let Some(idx) = lower.find(" and ") {
            let before = &lower[..idx];
            let after = &lower[idx + 5..];
            let a = clean_topic_fragment(before);
            let (b, scope) = split_trailing_scope(after);
            if !a.is_empty() && !b.is_empty() {
                let scope_ref = scope.as_deref();
                return Some(TaskSplit {
                    subtasks: vec![
                        SubTask {
                            id: a.replace(' ', "_"),
                            prompt: compare_subtask_prompt(&a, scope_ref),
                        },
                        SubTask {
                            id: b.replace(' ', "_"),
                            prompt: compare_subtask_prompt(&b, scope_ref),
                        },
                    ],
                    strategy: MergeStrategy::Synthesize,
                    merge_context: input.to_string(),
                    merge_guidance:
                        "Answer as a direct comparison. Start with a short verdict, then compare each side and end with when to choose which.".into(),
                });
            }
        }
    }

    // "X vs Y" pattern
    for (pattern, _) in &patterns {
        if lower.contains(pattern) {
            let parts: Vec<&str> = lower.splitn(2, pattern).collect();
            if parts.len() == 2 {
                let a = clean_topic_fragment(parts[0]);
                let (b, scope) = split_trailing_scope(parts[1]);
                if a.len() > 1 && b.len() > 1 {
                    let scope_ref = scope.as_deref();
                    return Some(TaskSplit {
                        subtasks: vec![
                            SubTask {
                                id: a.replace(' ', "_"),
                                prompt: compare_subtask_prompt(&a, scope_ref),
                            },
                            SubTask {
                                id: b.replace(' ', "_"),
                                prompt: compare_subtask_prompt(&b, scope_ref),
                            },
                        ],
                        strategy: MergeStrategy::Synthesize,
                        merge_context: input.to_string(),
                        merge_guidance:
                            "Answer as a direct comparison. Start with a short verdict, then compare each side and end with when to choose which.".into(),
                    });
                }
            }
        }
    }

    None
}

/// Detect list patterns: "explain X, Y, and Z"
fn try_list_split(input: &str, lower: &str) -> Option<TaskSplit> {
    // Look for comma-separated items with "and" before the last
    if !lower.contains(',') {
        return None;
    }

    let action_verbs = [
        "explain",
        "research",
        "look up",
        "tell me about",
        "what are",
        "describe",
        "summarize",
    ];
    let verb = action_verbs.iter().find(|v| lower.starts_with(*v))?;

    let rest = &input[verb.len()..].trim_start();
    let items: Vec<&str> = rest
        .split(',')
        .flat_map(|s| s.split(" and "))
        .map(|s| s.trim().trim_end_matches('?').trim_end_matches('.'))
        .filter(|s| s.len() > 1)
        .collect();

    if items.len() >= 2 && items.len() <= 6 {
        Some(TaskSplit {
            subtasks: items
                .iter()
                .map(|item| SubTask {
                    id: format!(
                        "item_{}",
                        item.replace(' ', "_").chars().take(20).collect::<String>()
                    ),
                    prompt: format!("{} {}", verb, item),
                })
                .collect(),
            strategy: if items.len() <= 3 {
                MergeStrategy::Concatenate
            } else {
                MergeStrategy::Synthesize
            },
            merge_context: input.to_string(),
            merge_guidance:
                "Answer with a short overview followed by a clearly separated section for each item.".into(),
        })
    } else {
        None
    }
}

/// Detect "tell me everything about X" — split into aspects.
fn try_breadth_split(_input: &str, lower: &str) -> Option<TaskSplit> {
    let breadth_signals = [
        "everything about",
        "deep dive into",
        "full breakdown of",
        "comprehensive overview of",
        "all about",
    ];
    let signal = breadth_signals.iter().find(|s| lower.contains(*s))?;

    let topic = lower
        .split(signal)
        .last()?
        .trim()
        .trim_end_matches('?')
        .to_string();
    if topic.len() < 3 {
        return None;
    }

    Some(TaskSplit {
        subtasks: vec![
            SubTask {
                id: "overview".into(),
                prompt: format!(
                    "Give a high-level overview of {}: what it is, why it matters. Focus only on the overview and avoid implementation detail overlap.",
                    topic
                ),
            },
            SubTask {
                id: "technical".into(),
                prompt: format!(
                    "Explain the technical details of {}: architecture, memory model, and key concepts. Avoid repeating the basic overview.",
                    topic
                ),
            },
            SubTask {
                id: "practical".into(),
                prompt: format!(
                    "Practical aspects of {}: how to use it, operator workflows, common patterns, gotchas, and best practices. Avoid repeating the general definition.",
                    topic
                ),
            },
        ],
        strategy: MergeStrategy::Synthesize,
        merge_context: format!("Tell me everything about {}", topic),
        merge_guidance:
            "Give one cohesive answer with sections for overview, technical details, and practical guidance. Avoid repeating the same definitions.".into(),
    })
}

/// Execute a swarm — fan out subtasks, fan in results, merge.
pub async fn execute(llm: &Arc<LlmGate>, split: TaskSplit) -> String {
    let agent_count = split.subtasks.len();
    tracing::info!("swarm: splitting into {} agents", agent_count);

    let cost = Arc::new(AtomicU32::new(0));
    let mut set = JoinSet::new();

    // Fan out — all agents run in parallel on DeepSeek (free)
    for task in split.subtasks {
        let llm = llm.clone();
        let cost = cost.clone();
        set.spawn(async move {
            let worker_prompt = format!(
                "You are a background research worker helping produce a user-facing answer.\n\
                 Return only concise factual notes that can safely be shown to the user.\n\
                 Focus only on the assigned slice and avoid repeating content owned by other workers unless comparison requires it.\n\
                 Prefer compact bullets or short sections over long essays.\n\
                 Do not include chain-of-thought, scratchpad, planning, or meta commentary.\n\
                 Do not say phrases like \"here's a thinking process\" or \"deconstruct the request\".\n\n\
                 Task:\n{}",
                task.prompt
            );
            let result = tokio::time::timeout(Duration::from_secs(30), llm.chat_auto(&worker_prompt, 300))
                .await;

            match result {
                Ok(Ok(output)) => {
                    cost.fetch_add(300, Ordering::Relaxed); // rough estimate
                    Some(AgentResult {
                        id: task.id,
                        output,
                    })
                }
                Ok(Err(e)) => {
                    tracing::warn!("swarm: agent {} failed: {}", task.id, e);
                    None
                }
                Err(_) => {
                    tracing::warn!("swarm: agent {} timed out", task.id);
                    None
                }
            }
        });
    }

    // Fan in — collect results as they finish
    let mut results: Vec<AgentResult> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    loop {
        tokio::select! {
            Some(join_result) = set.join_next() => {
                if let Ok(Some(r)) = join_result {
                    results.push(r);
                }
                if set.is_empty() { break; }
            }
            _ = tokio::time::sleep_until(deadline) => {
                tracing::warn!("swarm: group timeout, {} agents still running", set.len());
                set.abort_all();
                break;
            }
        }
    }

    let succeeded = results.len();
    tracing::info!("swarm: {}/{} agents completed", succeeded, agent_count);

    if results.is_empty() {
        return "couldn't complete the research — all agents timed out.".into();
    }

    // Merge
    match split.strategy {
        MergeStrategy::Concatenate => results
            .iter()
            .map(|r| format!("**{}:**\n{}", r.id.replace('_', " "), r.output))
            .collect::<Vec<_>>()
            .join("\n\n---\n\n"),
        MergeStrategy::Synthesize => {
            let clean_results = results
                .iter()
                .filter_map(|result| {
                    sanitize_agent_output(&result.output).map(|output| AgentResult {
                        id: result.id.clone(),
                        output,
                    })
                })
                .collect::<Vec<_>>();

            if clean_results.is_empty() {
                tracing::warn!("swarm: no clean agent notes available for synthesis");
                return fallback_merge_output(&results);
            }

            let combined = clean_results
                .iter()
                .map(|r| format!("[{}]\n{}", r.id, crate::trunc(&r.output, 500)))
                .collect::<Vec<_>>()
                .join("\n\n");

            let synthesis_prompt = format!(
                "Merge these research notes into one direct final answer for the user.\n\
                 Desired answer shape:\n{}\n\n\
                 Return only the final answer.\n\
                 Do not mention internal notes, agents, or researchers.\n\n\
                 User question:\n{}\n\n\
                 Research notes:\n{}",
                crate::trunc(&split.merge_guidance, 300),
                crate::trunc(&split.merge_context, 200),
                crate::trunc(&combined, 2400),
            );

            let synthesis = if llm.has_anthropic() {
                tokio::time::timeout(
                    Duration::from_secs(25),
                    llm.chat_anthropic_direct(&synthesis_prompt, 650),
                )
                .await
            } else {
                tokio::time::timeout(Duration::from_secs(25), llm.chat(&synthesis_prompt, 650))
                    .await
            };

            match synthesis {
                Ok(Ok(synthesis)) => sanitize_user_visible_synthesis(&synthesis)
                    .unwrap_or_else(|| fallback_merge_output(&clean_results)),
                Ok(Err(_)) | Err(_) => fallback_merge_output(&clean_results),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_split_avoids_speculative_story_queries() {
        assert!(try_split(
            "nyx what gonna happen when harry grows up, i saw he used a pistol wand and said nigger"
        )
        .is_none());
    }

    #[test]
    fn sanitize_user_visible_synthesis_rejects_internal_prompt_echo() {
        let leaked = "The user asked: test\n\nMultiple researchers found this. Synthesize into ONE coherent response. Don't mention multiple agents.";
        assert!(sanitize_user_visible_synthesis(leaked).is_none());
    }

    #[test]
    fn sanitize_user_visible_synthesis_rejects_policy_stub() {
        assert!(sanitize_user_visible_synthesis("Based on user safety guidelines:").is_none());
    }

    #[test]
    fn sanitize_user_visible_synthesis_strips_control_tokens() {
        assert_eq!(
            sanitize_user_visible_synthesis("<bos>\nhello there").as_deref(),
            Some("hello there")
        );
        assert_eq!(
            sanitize_user_visible_synthesis("<|end_system_prompt|><|end_user_prompt|>hello")
                .as_deref(),
            Some("hello")
        );
    }

    #[test]
    fn sanitize_agent_output_rejects_reasoning_trace() {
        let leaked = "Here's a thinking process that leads to the answer.\n\n1. Deconstruct the request.";
        assert!(sanitize_agent_output(leaked).is_none());
    }

    #[test]
    fn sanitize_agent_output_strips_task_echo_and_control_tokens() {
        let raw = "Research deployment pipelines — key features, pros, cons, use cases. Be thorough.<turn|>\n\nKey Features\nGo:\nFast builds.";
        assert_eq!(
            sanitize_agent_output(raw).as_deref(),
            Some("Key Features\nGo:\nFast builds.")
        );
    }

    #[test]
    fn comparison_split_keeps_shared_scope_and_clean_topics() {
        let split =
            try_split("compare rust vs go for backend systems programming and deployment pipelines")
                .unwrap();
        assert_eq!(split.subtasks.len(), 2);
        assert_eq!(split.subtasks[0].id, "rust");
        assert_eq!(split.subtasks[1].id, "go");
        assert!(split.subtasks[0]
            .prompt
            .contains("for backend systems programming and deployment pipelines"));
        assert!(split.subtasks[1]
            .prompt
            .contains("for backend systems programming and deployment pipelines"));
    }

    #[test]
    fn fallback_merge_output_never_returns_reasoning_trace() {
        let results = vec![AgentResult {
            id: "side_a".into(),
            output: "Here's a thinking process that leads to the answer.".into(),
        }];
        assert!(
            fallback_merge_output(&results)
                .contains("couldn't merge the parallel research into a clean final answer")
        );
    }
}
