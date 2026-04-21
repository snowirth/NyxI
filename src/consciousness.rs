//! Background consciousness — targeted thinking between messages.
//!
//! Nyx thinks when the user is idle. Not on a timer, not in a loop —
//! only when there's meaningful silence and something worth reflecting on.
//!
//! Constraints (learned from V1):
//! - Read-only: cannot modify code, only read + remember + message
//! - Budget-capped: max 10% of total LLM spend
//! - Triggered by idle, not periodic
//! - Quality gate: must produce actionable insight
//! - Max 3 thoughts per idle period

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;

use crate::AppState;
use crate::ProactiveQueue;
use crate::awareness::SharedAwareness;

/// Shared consciousness state
pub struct ConsciousnessState {
    /// Total tokens spent on background thinking
    pub tokens_spent: AtomicU64,
    /// Whether consciousness is currently active
    pub active: AtomicBool,
    /// Last time consciousness ran
    pub last_run: RwLock<Instant>,
    /// Thoughts produced this session
    pub thought_count: AtomicU64,
}

impl ConsciousnessState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tokens_spent: AtomicU64::new(0),
            active: AtomicBool::new(false),
            last_run: RwLock::new(Instant::now()),
            thought_count: AtomicU64::new(0),
        })
    }
}

pub type SharedConsciousness = Arc<ConsciousnessState>;

/// Run the background consciousness loop.
/// This is NOT a periodic thinker. It waits for idle, then reflects.
pub async fn run(
    state: AppState,
    awareness: SharedAwareness,
    proactive_queue: ProactiveQueue,
    consciousness: SharedConsciousness,
) {
    // Wait for system to stabilize
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    tracing::info!("consciousness: online");

    let mut last_msg_count = state.db.message_count();
    let mut idle_start: Option<Instant> = None;
    let mut thoughts_this_idle = 0u8;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;

        let msg_count = state.db.message_count();
        let user_active = msg_count > last_msg_count;
        last_msg_count = msg_count;

        // User is active — reset idle tracking
        if user_active {
            idle_start = None;
            thoughts_this_idle = 0;
            continue;
        }

        // User is idle — track how long
        if idle_start.is_none() {
            idle_start = Some(Instant::now());
        }

        let idle_duration = idle_start.unwrap().elapsed();

        // Don't think until 5 minutes of idle
        if idle_duration.as_secs() < 300 {
            continue;
        }

        // Max 3 thoughts per idle period
        if thoughts_this_idle >= 3 {
            continue;
        }

        // Budget cap: stop if consciousness has spent too much
        // (rough estimate: 10% of total, assuming ~500 tokens per thought at ~$0.003)
        let (total_in, total_out) = state.llm.usage();
        let total_spend = total_in + total_out;
        let consciousness_spend = consciousness.tokens_spent.load(Ordering::Relaxed);
        if total_spend > 0 && consciousness_spend > total_spend / 10 {
            continue; // Over 10% budget
        }

        // Don't think again too soon (min 10 min between thoughts)
        let last_run = *consciousness.last_run.read().await;
        if last_run.elapsed().as_secs() < 600 {
            continue;
        }

        // ── Time to think ──────────────────────────────────

        consciousness.active.store(true, Ordering::Relaxed);
        *consciousness.last_run.write().await = Instant::now();

        let thought = think(&state, &awareness).await;

        consciousness.active.store(false, Ordering::Relaxed);
        consciousness.tokens_spent.fetch_add(300, Ordering::Relaxed); // rough estimate

        if let Some(thought) = thought {
            thoughts_this_idle += 1;
            consciousness.thought_count.fetch_add(1, Ordering::Relaxed);

            match thought {
                Thought::Remember(fact) => {
                    let fact_for_log = fact.clone();
                    if let Err(e) = crate::autonomy::ingest_observation(
                        state.db.as_ref(),
                        crate::autonomy::ObservationInput {
                            kind: "memory_candidate".to_string(),
                            source: "consciousness".to_string(),
                            content: fact,
                            context: serde_json::json!({
                                "idle_seconds": idle_duration.as_secs(),
                                "thoughts_this_idle": thoughts_this_idle,
                            }),
                            priority: 0.64,
                        },
                    ) {
                        tracing::warn!("consciousness: failed to enqueue memory candidate: {}", e);
                        state.db.remember(&fact_for_log, "knowledge", 0.6).ok();
                    }
                    tracing::info!(
                        "consciousness: queued memory candidate — {}",
                        crate::trunc(&fact_for_log, 60)
                    );
                }
                Thought::Insight(insight) => {
                    let insight_for_log = insight.clone();
                    if let Err(e) = crate::autonomy::ingest_observation(
                        state.db.as_ref(),
                        crate::autonomy::ObservationInput {
                            kind: "consciousness_insight".to_string(),
                            source: "consciousness".to_string(),
                            content: insight,
                            context: serde_json::json!({
                                "idle_seconds": idle_duration.as_secs(),
                                "thoughts_this_idle": thoughts_this_idle,
                            }),
                            priority: 0.78,
                        },
                    ) {
                        tracing::warn!("consciousness: failed to enqueue insight: {}", e);
                        proactive_queue.lock().await.push(insight_for_log.clone());
                    }
                    tracing::info!(
                        "consciousness: queued insight — {}",
                        crate::trunc(&insight_for_log, 60)
                    );
                }
                Thought::Nothing => {}
            }
        }
    }
}

enum Thought {
    Remember(String),
    Insight(String),
    Nothing,
}

/// The actual thinking process. Examines recent context and produces a thought.
async fn think(state: &AppState, awareness: &SharedAwareness) -> Option<Thought> {
    let ctx = awareness.read().await.clone();

    // Gather recent conversation for reflection
    let recent = {
        let web = state.db.get_history("web", 10);
        if !web.is_empty() {
            web
        } else {
            let telegram = state.db.get_history_by_prefix("telegram:", 10);
            if !telegram.is_empty() {
                telegram
            } else {
                let discord = state.db.get_history_by_prefix("discord:", 10);
                if discord.is_empty() {
                    return None;
                }
                discord
            }
        }
    };

    let recent_text: String = recent
        .iter()
        .map(|(r, c)| format!("{}: {}", r, crate::trunc(&c, 80)))
        .collect::<Vec<_>>()
        .join("\n");

    // Get some existing memories for cross-referencing
    let memories = state.db.recall("", 10);
    let mem_text = if memories.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nExisting memories:\n{}",
            memories
                .iter()
                .take(5)
                .map(|m| format!("- {}", crate::trunc(&m, 80)))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };

    // Get pending reminders
    let reminders = state.db.get_reminders(false);
    let reminder_text = if reminders.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nPending reminders:\n{}",
            reminders
                .iter()
                .map(|(_, c, _)| format!("- {}", c))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };

    let prompt = format!(
        "You are Nyx's background consciousness. The user has been idle for a while.\n\
         Reflect briefly on recent context and decide ONE of:\n\n\
         1. REMEMBER: [fact] — if you notice something worth storing long-term\n\
         2. INSIGHT: [message] — if you have something useful to tell the user next time they return\n\
         3. NOTHING — if nothing interesting to note\n\n\
         Keep it SHORT. One sentence max. No fluff. Quality over quantity.\n\
         Only produce REMEMBER if the fact isn't already in existing memories.\n\
         Only produce INSIGHT if it's genuinely useful (not \"welcome back!\").\n\n\
         Current context: {} in {}, {}min session\n\
         Recent conversation:\n{}{}{}\n\n\
         Reply with exactly one line: REMEMBER: ... or INSIGHT: ... or NOTHING",
        if ctx.current_app.is_empty() {
            "unknown app"
        } else {
            &ctx.current_app
        },
        match ctx.time_of_day {
            crate::awareness::TimeOfDay::Morning => "morning",
            crate::awareness::TimeOfDay::Afternoon => "afternoon",
            crate::awareness::TimeOfDay::Evening => "evening",
            crate::awareness::TimeOfDay::LateNight => "late night",
        },
        ctx.session_duration_min,
        recent_text,
        mem_text,
        reminder_text,
    );

    let response = state.llm.chat_auto(&prompt, 60).await.ok()?;
    let trimmed = response.trim();

    if trimmed.starts_with("REMEMBER:") {
        let fact = trimmed.trim_start_matches("REMEMBER:").trim();
        if fact.len() > 10 {
            Some(Thought::Remember(fact.to_string()))
        } else {
            Some(Thought::Nothing)
        }
    } else if trimmed.starts_with("INSIGHT:") {
        let insight = trimmed.trim_start_matches("INSIGHT:").trim();
        if insight.len() > 10 {
            Some(Thought::Insight(insight.to_string()))
        } else {
            Some(Thought::Nothing)
        }
    } else {
        Some(Thought::Nothing)
    }
}
