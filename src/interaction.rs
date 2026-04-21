//! Interaction scoring — measures how the user responds to Nyx.
//!
//! No LLM calls. Pure signal detection from message timing, length, and content.
//! Scores the PREVIOUS interaction when a new message arrives, because you can
//! only know the outcome of response N when message N+1 comes in.
//!
//! This is the foundation for emergent personality — the soul engine reads
//! accumulated scores to learn what works, instead of following hardcoded rules.

use std::time::Instant;

/// Outcome of an interaction — how the user responded to Nyx's last message.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Outcome {
    Engaged,      // continued conversation naturally
    Expanded,     // asked a follow-up or "tell me more"
    Corrected,    // said "no", "wrong", corrected Nyx
    Ignored,      // didn't respond for 30+ minutes
    Acknowledged, // short response like "ok", "thanks", "yeah"
}

impl Outcome {
    /// Numeric score: positive = good, negative = bad.
    pub fn score(&self) -> f32 {
        match self {
            Outcome::Expanded => 1.0,
            Outcome::Engaged => 0.5,
            Outcome::Acknowledged => 0.0,
            Outcome::Ignored => -0.3,
            Outcome::Corrected => -0.5,
        }
    }
}

/// A recorded interaction with its context.
#[derive(Debug, Clone)]
pub struct Interaction {
    pub timestamp: i64,
    pub channel: String,
    pub user_msg_len: usize,
    pub response_len: usize,
    pub response_time_ms: u64,
    pub outcome: Outcome,
    /// Snapshot of soul state when this response was generated
    pub warmth: f32,
    pub verbosity: f32,
    pub assertiveness: f32,
    /// Time of day when this happened (0-23)
    pub hour: u32,
}

/// Tracker that sits in the message handler.
/// Call `on_message()` at the start of every message to score the previous one,
/// then `on_response()` after generating a response to record the new pending interaction.
pub struct InteractionTracker {
    /// The pending interaction (waiting for the next message to determine outcome)
    pending: Option<PendingInteraction>,
}

struct PendingInteraction {
    channel: String,
    response_len: usize,
    response_time_ms: u64,
    sent_at: Instant,
    warmth: f32,
    verbosity: f32,
    assertiveness: f32,
    hour: u32,
}

impl InteractionTracker {
    pub fn new() -> Self {
        Self { pending: None }
    }

    /// Called when a new user message arrives.
    /// Scores the previous interaction based on how the user responded.
    /// Returns the scored interaction if there was a pending one.
    pub fn on_message(&mut self, channel: &str, text: &str) -> Option<Interaction> {
        let pending = self.pending.take()?;

        // Only score if same channel
        if pending.channel != channel {
            self.pending = Some(pending); // put it back
            return None;
        }

        let elapsed = pending.sent_at.elapsed();
        let lower = text.to_lowercase();

        // Determine outcome from user's response
        let outcome = classify_outcome(text, &lower, elapsed);

        Some(Interaction {
            timestamp: chrono::Utc::now().timestamp(),
            channel: channel.to_string(),
            user_msg_len: text.len(),
            response_len: pending.response_len,
            response_time_ms: pending.response_time_ms,
            outcome,
            warmth: pending.warmth,
            verbosity: pending.verbosity,
            assertiveness: pending.assertiveness,
            hour: pending.hour,
        })
    }

    /// Called after generating a response. Records it as pending for scoring.
    pub fn on_response(
        &mut self,
        channel: &str,
        response_len: usize,
        response_time_ms: u64,
        warmth: f32,
        verbosity: f32,
        assertiveness: f32,
        hour: u32,
    ) {
        self.pending = Some(PendingInteraction {
            channel: channel.to_string(),
            response_len,
            response_time_ms,
            sent_at: Instant::now(),
            warmth,
            verbosity,
            assertiveness,
            hour,
        });
    }
}

/// Classify the outcome of the previous interaction based on the new message.
fn classify_outcome(text: &str, lower: &str, elapsed: std::time::Duration) -> Outcome {
    let secs = elapsed.as_secs();

    // Ignored: no response for 30+ minutes
    if secs > 1800 {
        return Outcome::Ignored;
    }

    // Corrected: strong correction signals
    let corrections = [
        "that's not right",
        "thats wrong",
        "that's wrong",
        "you got it wrong",
        "you misunderstood",
        "no that's",
        "no, that's",
        "wrong",
        "not what i",
    ];
    if corrections.iter().any(|c| lower.contains(c)) {
        return Outcome::Corrected;
    }

    // Acknowledged: very short, low-engagement responses
    let ack_phrases = [
        "ok", "thanks", "thx", "yeah", "yep", "sure", "cool", "got it", "k", "ty", "alright",
        "right",
    ];
    if text.len() < 15
        && ack_phrases
            .iter()
            .any(|a| lower.trim() == *a || lower.starts_with(a))
    {
        return Outcome::Acknowledged;
    }

    // Expanded: follow-up questions or requests for more
    let expand_signals = [
        "tell me more",
        "what about",
        "how does",
        "why",
        "can you explain",
        "what do you mean",
        "elaborate",
        "go on",
        "interesting",
        "and then",
        "what else",
    ];
    if expand_signals.iter().any(|s| lower.contains(s)) || text.contains('?') {
        return Outcome::Expanded;
    }

    // Default: engaged (continued conversation naturally)
    Outcome::Engaged
}

/// Aggregate interaction data for the soul engine.
/// Computes what trait weights correlate with positive outcomes.
#[derive(Debug, Clone, Default)]
pub struct InteractionInsights {
    pub total: u32,
    /// Average outcome score (higher = user engages more)
    pub avg_score: f32,
    /// Optimal warmth (weighted avg of warmth when user engaged/expanded)
    pub optimal_warmth: f32,
    /// Optimal verbosity
    pub optimal_verbosity: f32,
    /// Optimal assertiveness
    pub optimal_assertiveness: f32,
    /// Does the user engage more at night? (score diff: night - day)
    pub night_preference: f32,
}

impl InteractionInsights {
    /// Compute insights from a list of recent interactions.
    pub fn from_interactions(interactions: &[Interaction]) -> Self {
        if interactions.is_empty() {
            return Self::default();
        }

        let total = interactions.len() as u32;
        let avg_score = interactions.iter().map(|i| i.outcome.score()).sum::<f32>() / total as f32;

        // Weighted averages: weight by outcome score (positive outcomes count more)
        let positive: Vec<&Interaction> = interactions
            .iter()
            .filter(|i| i.outcome.score() > 0.0)
            .collect();

        let (optimal_warmth, optimal_verbosity, optimal_assertiveness) = if positive.is_empty() {
            (0.5, 0.3, 0.6) // defaults
        } else {
            let total_weight: f32 = positive.iter().map(|i| i.outcome.score()).sum();
            let w = positive
                .iter()
                .map(|i| i.warmth * i.outcome.score())
                .sum::<f32>()
                / total_weight;
            let v = positive
                .iter()
                .map(|i| i.verbosity * i.outcome.score())
                .sum::<f32>()
                / total_weight;
            let a = positive
                .iter()
                .map(|i| i.assertiveness * i.outcome.score())
                .sum::<f32>()
                / total_weight;
            (w, v, a)
        };

        // Night preference: avg score for night (22-5) vs day
        let night: Vec<&Interaction> = interactions
            .iter()
            .filter(|i| i.hour >= 22 || i.hour <= 5)
            .collect();
        let day: Vec<&Interaction> = interactions
            .iter()
            .filter(|i| i.hour > 5 && i.hour < 22)
            .collect();
        let night_avg = if night.is_empty() {
            0.0
        } else {
            night.iter().map(|i| i.outcome.score()).sum::<f32>() / night.len() as f32
        };
        let day_avg = if day.is_empty() {
            0.0
        } else {
            day.iter().map(|i| i.outcome.score()).sum::<f32>() / day.len() as f32
        };

        Self {
            total,
            avg_score,
            optimal_warmth,
            optimal_verbosity,
            optimal_assertiveness,
            night_preference: night_avg - day_avg,
        }
    }
}
