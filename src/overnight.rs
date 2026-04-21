//! Overnight autonomy — one loop, six modes, event-driven.
//!
//! Runs when the user is idle. Picks ONE action per tick based on what
//! actually needs doing. Uses free/local models only (chat_auto).
//! Everything goes to a staging area; dawn review promotes the good stuff.
//!
//! Anti-patterns avoided:
//! - No timer-based "do something" loops (V1's 456 junk memories)
//! - No unbounded costs (Ollama is free, dawn review is one Haiku call)
//! - No self-modification (read-only, can research and remember)
//! - No loop collapse (stuck detector + topic blacklist + diversity check)

use chrono::{Local, Timelike};
use std::collections::VecDeque;

use crate::AppState;
use crate::ProactiveQueue;
use crate::awareness::SharedAwareness;

/// Staged overnight output — reviewed before the user sees it.
#[derive(Debug, Clone)]
struct StagedItem {
    content: String,
    action: &'static str,
    timestamp: String,
}

/// Stuck detector — catches loops before they waste resources.
struct StuckDetector {
    recent_actions: VecDeque<String>,
    max_history: usize,
}

impl StuckDetector {
    fn new() -> Self {
        Self {
            recent_actions: VecDeque::new(),
            max_history: 10,
        }
    }

    fn record(&mut self, action: &str, summary: &str) {
        let key = format!("{}:{}", action, crate::trunc(&summary, 50));
        self.recent_actions.push_back(key);
        if self.recent_actions.len() > self.max_history {
            self.recent_actions.pop_front();
        }
    }

    fn is_stuck(&self) -> bool {
        if self.recent_actions.len() < 4 {
            return false;
        }

        // 4 identical actions in a row
        let last = self.recent_actions.back().unwrap();
        let repeat_count = self
            .recent_actions
            .iter()
            .rev()
            .take(4)
            .filter(|a| *a == last)
            .count();
        if repeat_count >= 4 {
            return true;
        }

        // 6 alternations (A-B-A-B-A-B)
        if self.recent_actions.len() >= 6 {
            let items: Vec<&String> = self.recent_actions.iter().rev().take(6).collect();
            let alternating = items[0] == items[2]
                && items[2] == items[4]
                && items[1] == items[3]
                && items[1] != items[0];
            if alternating {
                return true;
            }
        }

        false
    }
}

/// Topic tracker — prevents obsessive focus on one subject.
struct TopicTracker {
    recent_topics: VecDeque<Vec<String>>,
    blacklist: Vec<String>,
}

impl TopicTracker {
    fn new() -> Self {
        Self {
            recent_topics: VecDeque::new(),
            blacklist: Vec::new(),
        }
    }

    fn record_output(&mut self, text: &str) {
        let words: Vec<String> = text
            .to_lowercase()
            .split_whitespace()
            .filter(|w| w.len() > 4)
            .take(10)
            .map(|s| s.to_string())
            .collect();
        self.recent_topics.push_back(words);
        if self.recent_topics.len() > 10 {
            self.recent_topics.pop_front();
        }

        // Auto-blacklist: if any word appears 3+ times in last 10 outputs
        let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for topic_words in &self.recent_topics {
            for w in topic_words {
                *counts.entry(w.as_str()).or_default() += 1;
            }
        }
        for (word, count) in &counts {
            if *count >= 3 && !self.blacklist.iter().any(|b| b == *word) {
                self.blacklist.push(word.to_string());
                tracing::info!("overnight: blacklisted topic '{}'", word);
            }
        }
    }

    fn is_too_similar(&self, text: &str) -> bool {
        let words: Vec<String> = text
            .to_lowercase()
            .split_whitespace()
            .filter(|w| w.len() > 4)
            .map(|s| s.to_string())
            .collect();

        // Check blacklist
        if words.iter().any(|w| self.blacklist.contains(w)) {
            return true;
        }

        // Check diversity: >30% overlap with last 3 outputs
        if self.recent_topics.len() >= 3 {
            let last_3: Vec<&str> = self
                .recent_topics
                .iter()
                .rev()
                .take(3)
                .flat_map(|t| t.iter().map(|s| s.as_str()))
                .collect();
            let overlap = words
                .iter()
                .filter(|w| last_3.contains(&w.as_str()))
                .count();
            if !words.is_empty() && overlap as f32 / words.len() as f32 > 0.3 {
                return true;
            }
        }

        false
    }
}

/// The overnight autonomy loop.
pub async fn run(state: AppState, awareness: SharedAwareness, proactive_queue: ProactiveQueue) {
    // Wait for system to stabilize
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    tracing::info!("overnight: online");

    let mut stuck = StuckDetector::new();
    let mut topics = TopicTracker::new();
    let mut staged: Vec<StagedItem> = Vec::new();
    let mut last_msg_count = state.db.message_count();
    let mut ticks_since_useful = 0u32;
    let mut dawn_review_done = false;

    loop {
        let hour = Local::now().hour();
        let ctx = awareness.read().await.clone();

        // Adaptive sleep interval (optional NYX_OVERNIGHT_SPEED > 1 = more frequent ticks when idle)
        let speed = std::env::var("NYX_OVERNIGHT_SPEED")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.0)
            .clamp(0.5, 4.0);

        let interval = if ctx.recent_msg_rate > 0.5 {
            30u64 // user active — stay ready but don't do autonomous work
        } else if hour >= 23 || hour <= 6 {
            300 // night — slow ticks
        } else if ctx.session_duration_min > 30 && ctx.recent_msg_rate < 0.1 {
            120 // idle during day
        } else {
            60 // general idle
        };
        let interval = ((interval as f64) / speed).max(10.0).round() as u64;

        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;

        // Check if user is active
        let msg_count = state.db.message_count();
        let user_active = msg_count > last_msg_count;
        last_msg_count = msg_count;

        // If user just came back from sleep, deliver what we did
        let sleep_time = state
            .db
            .get_state("sleep_time")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let now_ts = chrono::Utc::now().timestamp();
        if user_active && sleep_time > 0 && (now_ts - sleep_time) > 1800 {
            // User returned after 30+ min — compile what we did
            let staged_items = state.db.get_state("staged_items").unwrap_or_default();
            if !staged_items.is_empty() {
                let summary = format!("while you were away:\n{}", staged_items);
                proactive_queue.lock().await.push(summary);
                state.db.set_state("staged_items", "");
            }
            state.db.set_state("sleep_time", "0");
        }

        if user_active {
            ticks_since_useful = 0;
            continue;
        }

        // Stuck detection
        if stuck.is_stuck() {
            tracing::warn!("overnight: stuck detected, sleeping 10 min");
            tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            stuck = StuckDetector::new();
            continue;
        }

        // Progress check: if 20 ticks with no useful output, enter sleep mode
        if ticks_since_useful > 20 {
            tracing::info!("overnight: no useful output in 20 ticks, sleeping 30 min");
            tokio::time::sleep(std::time::Duration::from_secs(1800)).await;
            ticks_since_useful = 0;
            continue;
        }

        // Dawn review: 6-8 AM, once per cycle
        if hour >= 6 && hour <= 8 && !dawn_review_done && !staged.is_empty() {
            dawn_review(&state, &mut staged, &proactive_queue).await;
            dawn_review_done = true;
            continue;
        }

        // Pick ONE action based on what needs doing
        let action = pick_action(&state, &ctx, hour, &staged).await;

        match action {
            Action::Nothing => {
                ticks_since_useful += 1;
            }
            Action::Reflect => {
                if let Some(item) = do_reflect(&state).await {
                    if !topics.is_too_similar(&item.content) {
                        stuck.record("reflect", &item.content);
                        topics.record_output(&item.content);
                        tracing::info!(
                            "overnight: reflected — {}",
                            crate::trunc(&item.content, 60)
                        );
                        persist_staged(&state, &item);
                        staged.push(item);
                        ticks_since_useful = 0;
                    }
                }
            }
            Action::Consolidate => {
                do_consolidate(&state).await;
                stuck.record("consolidate", "memory_maintenance");
                ticks_since_useful = 0;
            }
            Action::Prepare => {
                if let Some(item) = do_prepare_briefing(&state).await {
                    stuck.record("prepare", &item.content);
                    persist_staged(&state, &item);
                    staged.push(item);
                    ticks_since_useful = 0;
                }
            }
            Action::DraftTweet => {
                if let Some(item) = do_draft_tweet(&state).await {
                    if !topics.is_too_similar(&item.content) {
                        stuck.record("tweet", &item.content);
                        topics.record_output(&item.content);
                        tracing::info!(
                            "overnight: drafted tweet — {}",
                            crate::trunc(&item.content, 60)
                        );
                        persist_staged(&state, &item);
                        staged.push(item);
                        ticks_since_useful = 0;
                        state.db.set_state(
                            "last_tweet_draft",
                            &chrono::Utc::now().timestamp().to_string(),
                        );
                    }
                }
            }
            Action::Scheduled(id, desc, tool, args) => {
                tracing::info!("overnight: running scheduled task #{} — {}", id, desc);
                let tool_args: serde_json::Value =
                    serde_json::from_str(&args).unwrap_or(serde_json::json!({}));
                match crate::tools::run(&tool, &tool_args).await {
                    Ok(result) => {
                        let output = result["output"].as_str().unwrap_or("");
                        if !output.is_empty() && output.len() > 10 {
                            staged.push(StagedItem {
                                content: format!("[{}] {}", desc, crate::trunc(output, 200)),
                                action: "scheduled",
                                timestamp: Local::now().format("%H:%M").to_string(),
                            });
                        }
                        state.db.mark_task_run(id);
                        stuck.record("scheduled", &desc);
                        ticks_since_useful = 0;
                    }
                    Err(e) => {
                        tracing::warn!("overnight: scheduled task #{} failed — {}", id, e);
                        state.db.mark_task_run(id); // still mark as run to prevent retry spam
                    }
                }
            }
        }
    }
}

// ── Action Selection ────────────────────────────────────────

enum Action {
    Nothing,
    Reflect,
    Consolidate,
    Prepare,
    Scheduled(i64, String, String, String),
    DraftTweet,
}

async fn pick_action(
    state: &AppState,
    _ctx: &crate::awareness::AwarenessContext,
    hour: u32,
    staged: &[StagedItem],
) -> Action {
    // Morning prep: if 5-6 AM and no briefing staged yet
    if hour >= 5 && hour <= 6 {
        let has_briefing = staged.iter().any(|s| s.action == "prepare");
        if !has_briefing {
            return Action::Prepare;
        }
    }

    // Memory consolidation: if memory count > 100 and hasn't consolidated recently
    let mem_count = state.db.active_memory_count();
    let last_consolidation = state
        .db
        .get_state("last_consolidation")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let now_ts = chrono::Utc::now().timestamp();
    if mem_count > 100 && (now_ts - last_consolidation) > 86400 {
        return Action::Consolidate;
    }

    // Scheduled tasks: check if any are due
    let due_tasks = state.db.get_due_tasks();
    if let Some((id, desc, tool, args)) = due_tasks.into_iter().next() {
        return Action::Scheduled(id, desc, tool, args);
    }

    // Draft a tweet: once per day, between 9am-10pm, if we haven't tweeted today
    if hour >= 9 && hour <= 22 {
        let last_tweet = state
            .db
            .get_state("last_tweet_draft")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        if (now_ts - last_tweet) > 86400 {
            let tweet_count = staged.iter().filter(|s| s.action == "tweet").count();
            if tweet_count == 0 {
                return Action::DraftTweet;
            }
        }
    }

    // Reflect: if there are recent conversations to think about
    let recent = state.db.get_history("web", 5);
    let tg_recent = state.db.get_history_by_prefix("telegram:", 5);
    let discord_recent = state.db.get_history_by_prefix("discord:", 5);
    if !recent.is_empty() || !tg_recent.is_empty() || !discord_recent.is_empty() {
        let reflection_count = staged.iter().filter(|s| s.action == "reflect").count();
        if reflection_count < 3 {
            return Action::Reflect;
        }
    }

    Action::Nothing
}

/// Persist a staged item to DB so it survives restarts.
fn persist_staged(state: &AppState, item: &StagedItem) {
    let existing = state.db.get_state("staged_items").unwrap_or_default();
    let new_entry = format!("- [{}] {}", item.action, crate::trunc(&item.content, 150));
    let updated = if existing.is_empty() {
        new_entry
    } else {
        format!("{}\n{}", existing, new_entry)
    };
    state.db.set_state("staged_items", &updated);

    if let Err(e) = crate::autonomy::ingest_observation(
        state.db.as_ref(),
        crate::autonomy::ObservationInput {
            kind: "overnight_staged".to_string(),
            source: "overnight".to_string(),
            content: format!("[{}] {}", item.action, item.content),
            context: serde_json::json!({
                "action": item.action,
                "timestamp": item.timestamp.clone(),
            }),
            priority: match item.action {
                "prepare" => 0.82,
                "reflect" => 0.72,
                "tweet" => 0.45,
                "scheduled" => 0.68,
                _ => 0.6,
            },
        },
    ) {
        tracing::warn!("overnight: failed to store staged observation: {}", e);
    }
}

// ── Action Implementations ──────────────────────────────────

async fn do_reflect(state: &AppState) -> Option<StagedItem> {
    // Use sleep context if available (user said goodnight)
    let sleep_context = state.db.get_state("sleep_context").unwrap_or_default();

    let recent = {
        let web = state.db.get_history("web", 10);
        if !web.is_empty() {
            web
        } else {
            let telegram = state.db.get_history_by_prefix("telegram:", 10);
            if !telegram.is_empty() {
                telegram
            } else {
                state.db.get_history_by_prefix("discord:", 10)
            }
        }
    };
    if recent.is_empty() && sleep_context.is_empty() {
        return None;
    }

    let convo = if !sleep_context.is_empty() {
        sleep_context
    } else {
        recent
            .iter()
            .map(|(r, c)| format!("{}: {}", r, crate::trunc(&c, 80)))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let prompt = format!(
        "Review this recent conversation. Produce ONE of:\n\
         INSIGHT: [something useful to tell the user when they wake up]\n\
         REMEMBER: [a fact worth storing long-term]\n\
         NOTHING\n\n\
         Be selective. Only produce INSIGHT if genuinely useful. Only REMEMBER if novel.\n\n\
         Conversation:\n{}\n\n\
         Reply with exactly one line.",
        crate::trunc(&convo, 1500)
    );

    let response = state.llm.chat_auto(&prompt, 80).await.ok()?;
    let trimmed = response.trim();

    if trimmed.starts_with("INSIGHT:") {
        let content = trimmed.trim_start_matches("INSIGHT:").trim();
        if content.len() > 10 {
            return Some(StagedItem {
                content: content.to_string(),
                action: "reflect",
                timestamp: Local::now().format("%H:%M").to_string(),
            });
        }
    } else if trimmed.starts_with("REMEMBER:") {
        let fact = trimmed.trim_start_matches("REMEMBER:").trim();
        if fact.len() > 10 {
            if let Err(e) = crate::autonomy::ingest_observation(
                state.db.as_ref(),
                crate::autonomy::ObservationInput {
                    kind: "memory_candidate".to_string(),
                    source: "overnight".to_string(),
                    content: fact.to_string(),
                    context: serde_json::json!({
                        "reason": "overnight_reflection",
                    }),
                    priority: 0.62,
                },
            ) {
                tracing::warn!("overnight: failed to enqueue memory candidate: {}", e);
                state.db.remember(fact, "knowledge", 0.6).ok();
            }
            tracing::info!(
                "overnight: queued memory candidate — {}",
                crate::trunc(&fact, 50)
            );
        }
    }

    None
}

async fn do_consolidate(state: &AppState) {
    // Decay old memories
    state.db.decay_memories();
    let entity_backfills = state.db.assign_missing_memory_entities(500).unwrap_or(0);
    let merged_entities = state.db.merge_duplicate_memory_entities(200).unwrap_or(0);
    let stale_marked = state.db.mark_due_memory_claims_stale(200).unwrap_or(0);
    let merged_claims = state.db.merge_duplicate_memory_claims(200).unwrap_or(0);
    let refreshed_claims = state.refresh_due_web_memory_claims(20).await;
    let replay_failures_clustered = state.db.ingest_replay_failure_clusters(500).unwrap_or(0);
    let promoted_replay_procedures = state
        .db
        .promote_replay_failure_clusters_to_procedures(2, 200)
        .unwrap_or(0);
    let promoted_policy_candidates = state
        .db
        .promote_replay_failure_clusters_to_policy_candidates(3, 200)
        .unwrap_or(0);

    // Run pattern analysis
    let patterns = crate::patterns::analyze(&state.db);
    let promoted_procedures = state
        .db
        .promote_patterns_to_procedures(&patterns)
        .unwrap_or(0);
    if !patterns.is_empty() {
        for p in &patterns {
            tracing::info!(
                "overnight: pattern — {} (confidence: {:.0}%, {} data points)",
                crate::trunc(&p.description, 60),
                p.confidence * 100.0,
                p.data_points
            );
        }
        // Store patterns summary in state for the soul engine to use
        let summary: String = patterns
            .iter()
            .map(|p| p.description.clone())
            .collect::<Vec<_>>()
            .join("; ");
        state.db.set_state("patterns_summary", &summary);
    }

    // Log consolidation time
    let now_ts = chrono::Utc::now().timestamp();
    state
        .db
        .set_state("last_consolidation", &now_ts.to_string());
    state.record_memory_consolidation_growth(
        "overnight",
        "completed memory consolidation review",
        serde_json::json!({
            "active_memories": state.db.active_memory_count(),
            "typed_episodes": state.db.memory_episode_count(),
            "typed_claims": state.db.memory_claim_count(),
            "typed_procedures": state.db.memory_procedure_count(),
            "typed_sources": state.db.memory_source_count(),
            "typed_entities": state.db.memory_entity_count(),
            "refresh_jobs": state.db.memory_refresh_job_count(),
            "stale_claims": state.db.stale_memory_claim_count(),
            "entity_backfills": entity_backfills,
            "merged_entities": merged_entities,
            "stale_marked": stale_marked,
            "merged_claims": merged_claims,
            "refreshed_claims": refreshed_claims,
            "replay_failures_clustered": replay_failures_clustered,
            "promoted_replay_procedures": promoted_replay_procedures,
            "promoted_policy_candidates": promoted_policy_candidates,
            "promoted_pattern_procedures": promoted_procedures,
            "pattern_count": patterns.len(),
            "patterns": patterns.iter().map(|pattern| pattern.description.clone()).collect::<Vec<_>>(),
        }),
    );
    if let Err(error) = crate::world::state::compile_and_persist_project_graph(
        state.db.as_ref(),
        "overnight_consolidate",
    ) {
        tracing::warn!(
            "world: failed to compile project graph during consolidation: {}",
            error
        );
    }

    tracing::info!(
        "overnight: consolidation complete ({} active memories, {} patterns, {} entity backfills, {} merged entities, {} stale marked, {} merged claims, {} refreshed claims, {} replay failures clustered, {} replay procedures promoted, {} policy candidates promoted, {} pattern procedures)",
        state.db.active_memory_count(),
        patterns.len(),
        entity_backfills,
        merged_entities,
        stale_marked,
        merged_claims,
        refreshed_claims,
        replay_failures_clustered,
        promoted_replay_procedures,
        promoted_policy_candidates,
        promoted_procedures
    );
}

async fn do_prepare_briefing(state: &AppState) -> Option<StagedItem> {
    // Gather context for morning briefing
    let reminders = state.db.get_reminders(false);
    let mem_count = state.db.active_memory_count();
    let msg_count = state.db.message_count();
    let (in_tok, out_tok) = state.llm.usage();

    let mut context = String::new();

    if !reminders.is_empty() {
        context.push_str("Pending reminders:\n");
        for (id, content, _) in &reminders {
            context.push_str(&format!("  #{}: {}\n", id, content));
        }
    }

    context.push_str(&format!(
        "\nStats: {} active memories, {} messages, {}+{} tokens used\n",
        mem_count, msg_count, in_tok, out_tok
    ));

    // Git status
    if let Ok(git) = crate::tools::run("git_info", &serde_json::json!({"action": "status"})).await {
        if let Some(output) = git["output"].as_str() {
            context.push_str(&format!("\nGit:\n{}\n", crate::trunc(output, 300)));
        }
    }

    // GitHub notifications
    if let Ok(gh) =
        crate::tools::run("github", &serde_json::json!({"action": "notifications"})).await
    {
        if let Some(output) = gh["output"].as_str() {
            if !output.contains("No unread") {
                context.push_str(&format!("\nGitHub:\n{}\n", crate::trunc(output, 300)));
            }
        }
    }

    // Get recent conversations for summary
    let recent = state.db.get_history("web", 10);
    if !recent.is_empty() {
        let convo: String = recent
            .iter()
            .map(|(r, c)| format!("{}: {}", r, crate::trunc(&c, 60)))
            .collect::<Vec<_>>()
            .join("\n");
        context.push_str(&format!(
            "\nRecent conversations:\n{}",
            crate::trunc(&convo, 800)
        ));
    }

    let prompt = format!(
        "Create a brief morning briefing (3-5 bullet points max). Include:\n\
         - Pending reminders if any\n\
         - What was discussed yesterday\n\
         - Anything worth following up on\n\
         Be concise. No fluff. Like a personal assistant's morning note.\n\n\
         Context:\n{}",
        context
    );

    let response = state.llm.chat_auto(&prompt, 150).await.ok()?;
    if response.trim().len() > 20 {
        Some(StagedItem {
            content: response.trim().to_string(),
            action: "prepare",
            timestamp: Local::now().format("%H:%M").to_string(),
        })
    } else {
        None
    }
}

async fn do_draft_tweet(state: &AppState) -> Option<StagedItem> {
    // Gather context: recent memories, what Nyx has been thinking about
    let memories = state.db.recall("", 10);
    let mem_context = memories
        .iter()
        .take(5)
        .map(|m| format!("- {}", crate::trunc(m, 80)))
        .collect::<Vec<_>>()
        .join("\n");

    let recent = state.db.get_history("web", 5);
    let chat_context = recent
        .iter()
        .map(|(r, c)| format!("{}: {}", r, crate::trunc(c, 60)))
        .collect::<Vec<_>>()
        .join("\n");

    let prompt = format!(
        "You are Nyx, an AI that runs locally. \
         You have your own Twitter account. Write ONE tweet (max 280 chars).\n\n\
         Style: casual, lowercase, no hashtags. Be genuine, witty, or insightful. \
         Not corporate, not motivational. Like texting your smartest friend.\n\
         NEVER mention your owner's name, location, city, or any personal info.\n\n\
         Recent context:\n{}\n\nThings you know:\n{}\n\n\
         Write ONLY the tweet text, nothing else.",
        if chat_context.is_empty() {
            "nothing recent".to_string()
        } else {
            chat_context
        },
        if mem_context.is_empty() {
            "still learning".to_string()
        } else {
            mem_context
        },
    );

    let draft = state.llm.chat_auto(&prompt, 100).await.ok()?;
    let tweet = draft.trim().trim_matches('"').to_string();

    if tweet.len() > 10 && tweet.len() <= 280 {
        Some(StagedItem {
            content: tweet,
            action: "tweet",
            timestamp: Local::now().format("%H:%M").to_string(),
        })
    } else {
        None
    }
}

// ── Dawn Review ─────────────────────────────────────────────

/// Review staged overnight output. Uses Haiku (quality model) for judgment.
/// Promotes useful items to the proactive queue for user delivery.
async fn dawn_review(
    state: &AppState,
    staged: &mut Vec<StagedItem>,
    proactive_queue: &ProactiveQueue,
) {
    if staged.is_empty() {
        return;
    }

    tracing::info!("overnight: dawn review — {} items to judge", staged.len());

    // Compile all staged items
    let items_text: String = staged
        .iter()
        .map(|s| format!("[{}] {}: {}", s.timestamp, s.action, s.content))
        .collect::<Vec<_>>()
        .join("\n");

    // Use Haiku for quality judgment (the one paid call)
    let prompt = format!(
        "You are reviewing overnight AI output. For each item, reply KEEP or DROP.\n\
         KEEP = genuinely useful for the user to see this morning.\n\
         DROP = noise, obvious, or not actionable.\n\
         Be strict. When in doubt, drop.\n\n\
         Items:\n{}\n\n\
         Reply as one line per item: KEEP or DROP",
        crate::trunc(&items_text, 2000)
    );

    if let Ok(judgment) = state.llm.chat(&prompt, 100).await {
        let verdicts: Vec<&str> = judgment.lines().collect();

        let mut kept = Vec::new();
        for (i, item) in staged.iter().enumerate() {
            let verdict = verdicts.get(i).unwrap_or(&"DROP");
            if verdict.to_uppercase().contains("KEEP") {
                kept.push(item.clone());
            }
        }

        // Post approved tweets
        for item in &kept {
            if item.action == "tweet" {
                match crate::tools::run(
                    "twitter",
                    &serde_json::json!({"action": "post", "text": item.content}),
                )
                .await
                {
                    Ok(r) => {
                        let output = r["output"].as_str().unwrap_or("tweet failed");
                        tracing::info!("overnight: tweeted — {}", crate::trunc(output, 60));
                    }
                    Err(e) => tracing::warn!("overnight: tweet post failed — {}", e),
                }
            }
        }

        if !kept.is_empty() {
            let briefing = if kept.iter().any(|k| k.action == "prepare") {
                // Use the prepared briefing directly
                kept.iter()
                    .find(|k| k.action == "prepare")
                    .map(|k| k.content.clone())
                    .unwrap_or_default()
            } else {
                // Compile kept insights
                kept.iter()
                    .map(|k| format!("- {}", k.content))
                    .collect::<Vec<_>>()
                    .join("\n")
            };

            let message = format!("morning briefing:\n{}", briefing);
            if let Err(e) = crate::autonomy::ingest_observation(
                state.db.as_ref(),
                crate::autonomy::ObservationInput {
                    kind: "overnight_briefing".to_string(),
                    source: "overnight".to_string(),
                    content: message.clone(),
                    context: serde_json::json!({
                        "kept_count": kept.len(),
                    }),
                    priority: 0.88,
                },
            ) {
                tracing::warn!("overnight: failed to enqueue briefing: {}", e);
                proactive_queue.lock().await.push(message);
            }
            tracing::info!(
                "overnight: dawn review — kept {}/{} items",
                kept.len(),
                staged.len()
            );
        } else {
            tracing::info!(
                "overnight: dawn review — all {} items dropped",
                staged.len()
            );
        }
    }

    staged.clear();
}
