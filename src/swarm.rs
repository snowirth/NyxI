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
                            "High-level overview of {}: what it is, why it matters.",
                            topic
                        ),
                    },
                    SubTask {
                        id: "technical".into(),
                        prompt: format!(
                            "Technical details of {}: how it works, architecture, key concepts.",
                            topic
                        ),
                    },
                    SubTask {
                        id: "practical".into(),
                        prompt: format!(
                            "Practical aspects of {}: how to use it, patterns, gotchas.",
                            topic
                        ),
                    },
                ],
                strategy: MergeStrategy::Synthesize,
                merge_context: format!("Tell me about {}", topic),
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
    if lower.contains("compare") || lower.contains("difference") {
        if let Some(idx) = lower.find(" and ") {
            let before = &lower[..idx];
            let after = &lower[idx + 5..];
            // Extract the two things being compared
            let a = before
                .split_whitespace()
                .rev()
                .take_while(|w| !["compare", "between", "differences", "difference"].contains(w))
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join(" ");
            let b = after
                .split_whitespace()
                .take_while(|w| !["for", "in", "when", "?"].contains(w))
                .collect::<Vec<_>>()
                .join(" ");
            if !a.is_empty() && !b.is_empty() {
                return Some(TaskSplit {
                    subtasks: vec![
                        SubTask {
                            id: format!("research_{}", a.replace(' ', "_")),
                            prompt: format!(
                                "Research {} — key features, pros, cons, use cases. Be thorough.",
                                a
                            ),
                        },
                        SubTask {
                            id: format!("research_{}", b.replace(' ', "_")),
                            prompt: format!(
                                "Research {} — key features, pros, cons, use cases. Be thorough.",
                                b
                            ),
                        },
                    ],
                    strategy: MergeStrategy::Synthesize,
                    merge_context: input.to_string(),
                });
            }
        }
    }

    // "X vs Y" pattern
    for (pattern, _) in &patterns {
        if lower.contains(pattern) {
            let parts: Vec<&str> = lower.splitn(2, pattern).collect();
            if parts.len() == 2 {
                let a = parts[0]
                    .trim()
                    .split_whitespace()
                    .rev()
                    .take(4)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join(" ");
                let b = parts[1]
                    .trim()
                    .split_whitespace()
                    .take(4)
                    .collect::<Vec<_>>()
                    .join(" ");
                if a.len() > 1 && b.len() > 1 {
                    return Some(TaskSplit {
                        subtasks: vec![
                            SubTask {
                                id: "side_a".into(),
                                prompt: format!(
                                    "Research {} — features, strengths, weaknesses.",
                                    a
                                ),
                            },
                            SubTask {
                                id: "side_b".into(),
                                prompt: format!(
                                    "Research {} — features, strengths, weaknesses.",
                                    b
                                ),
                            },
                        ],
                        strategy: MergeStrategy::Synthesize,
                        merge_context: input.to_string(),
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
                    "Give a high-level overview of {}: what it is, why it matters.",
                    topic
                ),
            },
            SubTask {
                id: "technical".into(),
                prompt: format!(
                    "Explain the technical details of {}: how it works, architecture, key concepts.",
                    topic
                ),
            },
            SubTask {
                id: "practical".into(),
                prompt: format!(
                    "Practical aspects of {}: how to use it, common patterns, gotchas, best practices.",
                    topic
                ),
            },
        ],
        strategy: MergeStrategy::Synthesize,
        merge_context: format!("Tell me everything about {}", topic),
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
            let result =
                tokio::time::timeout(Duration::from_secs(30), llm.chat_auto(&task.prompt, 300))
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
            let combined = results
                .iter()
                .map(|r| format!("[{}]\n{}", r.id, crate::trunc(&r.output, 500)))
                .collect::<Vec<_>>()
                .join("\n\n");

            let synthesis_prompt = format!(
                "The user asked: {}\n\n\
                 Multiple researchers found this. Synthesize into ONE coherent response. \
                 Remove redundancy. Resolve contradictions. Don't mention multiple agents. \
                 Be concise and direct.\n\n{}",
                crate::trunc(&split.merge_context, 200),
                crate::trunc(&combined, 2000),
            );

            // Synthesis uses Haiku (user will see this)
            match llm.chat(&synthesis_prompt, 400).await {
                Ok(synthesis) => synthesis,
                Err(_) => {
                    // Fallback to concatenation
                    results
                        .iter()
                        .map(|r| r.output.clone())
                        .collect::<Vec<_>>()
                        .join("\n\n")
                }
            }
        }
    }
}
