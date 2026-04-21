//! Awareness engine — event-driven screen tracking + proactive intelligence.
//! Exposes AwarenessContext: a shared snapshot of what the user is doing,
//! how they're feeling (inferred from behavior), and how Nyx should adapt.

use crate::AppState;
use chrono::{Local, Timelike};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Time-of-day classification
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeOfDay {
    Morning,   // 05:00-11:59
    Afternoon, // 12:00-16:59
    Evening,   // 17:00-22:59
    LateNight, // 23:00-04:59
}

/// How much energy the user likely has based on session behavior
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SessionEnergy {
    Fresh,     // just started or came back from break
    Sustained, // working steadily, normal pace
    Grinding,  // long session, high focus, fast messages
    Winding,   // slowing down, longer gaps between messages
}

/// Shared awareness state — written by awareness loop, read by message handler
#[derive(Debug, Clone)]
pub struct AwarenessContext {
    /// Current foreground application
    pub current_app: String,
    /// How long they've been in the current app (minutes)
    pub app_duration_min: u64,
    /// Time of day classification
    pub time_of_day: TimeOfDay,
    /// Current hour (0-23)
    pub hour: u32,
    /// Messages in the last 5 minutes (pace indicator)
    pub recent_msg_rate: f64,
    /// Whether the user just came back from being idle
    pub just_returned: bool,
    /// How long they were idle before returning (seconds, 0 if not just returned)
    pub idle_before_return_secs: u64,
    /// Session energy estimate
    pub energy: SessionEnergy,
    /// How long since Nyx started (minutes) — proxy for session length
    pub session_duration_min: u64,
    /// Whether a message burst was detected (frustration/excitement)
    pub burst_detected: bool,
    /// Active project/workstream focus derived from the persisted world graph
    pub world_focus: crate::world::projects::WorldFocusSummary,
}

impl Default for AwarenessContext {
    fn default() -> Self {
        Self {
            current_app: String::new(),
            app_duration_min: 0,
            time_of_day: TimeOfDay::Evening,
            hour: 0,
            recent_msg_rate: 0.0,
            just_returned: false,
            idle_before_return_secs: 0,
            energy: SessionEnergy::Fresh,
            burst_detected: false,
            session_duration_min: 0,
            world_focus: crate::world::projects::WorldFocusSummary::default(),
        }
    }
}

impl AwarenessContext {
    /// Generate a tone directive for the LLM system prompt
    pub fn tone_directive(&self) -> String {
        let mut parts = Vec::new();

        // Time-based tone
        match self.time_of_day {
            TimeOfDay::LateNight => {
                parts.push("It's late night. Be brief, no fluff. If they've been going a while, gently acknowledge it but don't nag.".to_string());
            }
            TimeOfDay::Morning => {
                parts.push("Morning session. Clean energy — match it.".to_string());
            }
            _ => {}
        }

        // Energy-based tone
        match self.energy {
            SessionEnergy::Fresh => {
                parts.push(
                    "User just started or returned. Be warm but not over-the-top.".to_string(),
                );
            }
            SessionEnergy::Grinding => {
                parts.push("Deep work mode. Ultra-concise. Don't waste their flow state with unnecessary words.".to_string());
            }
            SessionEnergy::Winding => {
                parts.push(
                    "They're slowing down. It's okay to be a bit more conversational.".to_string(),
                );
            }
            SessionEnergy::Sustained => {}
        }

        // Welcome back
        if self.just_returned {
            let mins_away = self.idle_before_return_secs / 60;
            if mins_away >= 30 {
                parts.push(format!("They just came back after ~{}min away. Brief welcome, maybe surface what they were working on.", mins_away));
            } else if mins_away >= 5 {
                parts.push("Short break, they're back. No need to acknowledge it.".to_string());
            }
        }

        // Burst detection
        if self.burst_detected {
            parts.push("Message burst detected — they might be frustrated or excited. Read the tone carefully.".to_string());
        }

        // App context
        if !self.current_app.is_empty() && self.app_duration_min >= 10 {
            parts.push(format!(
                "They're in {} ({}min). Tailor responses to that context.",
                self.current_app, self.app_duration_min
            ));
        }

        if let Some(workstream_title) = self.world_focus.active_workstream_title.as_deref() {
            let workstream_status = self
                .world_focus
                .active_workstream_status
                .as_deref()
                .unwrap_or("active");
            let mut focus_note = if let Some(resume_title) =
                self.world_focus.resume_focus_title.as_deref()
            {
                let reason = self
                    .world_focus
                    .resume_focus_reason
                    .as_deref()
                    .unwrap_or("keep that thread moving");
                format!(
                    "Active workstream: {} ({}). Current resume focus: {}. Bias toward {}.",
                    workstream_title, workstream_status, resume_title, reason
                )
            } else {
                format!(
                    "Active workstream: {} ({}). Keep responses anchored to that project context.",
                    workstream_title, workstream_status
                )
            };

            if self.world_focus.resume_focus_blocker_count > 0 {
                focus_note.push_str(&format!(
                    " There are {} blockers touching that focus.",
                    self.world_focus.resume_focus_blocker_count
                ));
                if let Some(summary) = self.world_focus.top_blocker_summary.as_deref() {
                    focus_note.push_str(&format!(" Top blocker: {}.", crate::trunc(summary, 160)));
                }
            }

            parts.push(focus_note);
        } else if let Some(project_title) = self.world_focus.active_project_title.as_deref() {
            let project_status = self
                .world_focus
                .active_project_status
                .as_deref()
                .unwrap_or("active");
            parts.push(format!(
                "Current project focus is {} ({}). Keep replies grounded in that thread.",
                project_title, project_status
            ));
        }

        if parts.is_empty() {
            return String::new();
        }

        format!("<awareness>\n{}\n</awareness>", parts.join("\n"))
    }
}

pub type SharedAwareness = Arc<RwLock<AwarenessContext>>;

pub fn new_shared() -> SharedAwareness {
    Arc::new(RwLock::new(AwarenessContext::default()))
}

pub async fn run(
    state: AppState,
    awareness: SharedAwareness,
    proactive_queue: crate::ProactiveQueue,
) {
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    tracing::info!("awareness: online");

    let mut last_app = String::new();
    let mut app_since = Instant::now();
    let mut was_idle = true;
    let mut idle_since = Instant::now();
    let mut late_night_sent = false;
    let mut focus_sent = false;
    let mut last_msg_count: i64 = state.db.message_count();
    let session_start = Instant::now();

    // Rolling message timestamps for rate calculation
    let mut msg_timestamps: Vec<Instant> = Vec::new();

    // Track just_returned for 2 ticks so the handler catches it (Fix #7: race condition)
    let mut just_returned_ttl: u8 = 0;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Get foreground app
        let app = tokio::process::Command::new("osascript")
            .args(["-e", "tell application \"System Events\" to get name of first application process whose frontmost is true"])
            .output().await
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();

        if app.is_empty() {
            continue;
        }

        // App switch
        if app != last_app && !last_app.is_empty() {
            app_since = Instant::now();
            focus_sent = false;
        }
        last_app = app.clone();

        let app_duration_min = app_since.elapsed().as_secs() / 60;

        // Focus alert (2+ hours) — now pushes to proactive queue
        if app_duration_min >= 120 && !focus_sent {
            let msg = format!("you've been in {} for {}h", app, app_duration_min / 60);
            tracing::info!("proactive: {}", msg);
            proactive_queue.lock().await.push(msg);
            if let Err(e) = crate::autonomy::ingest_observation(
                state.db.as_ref(),
                crate::autonomy::ObservationInput {
                    kind: "attention_prompt".to_string(),
                    source: "awareness".to_string(),
                    content: format!("you've been in {} for {}h", app, app_duration_min / 60),
                    context: serde_json::json!({
                        "app": app.clone(),
                        "app_duration_min": app_duration_min,
                        "reason": "focus_alert",
                    }),
                    priority: 0.72,
                },
            ) {
                tracing::warn!("awareness: failed to store focus observation: {}", e);
            }
            focus_sent = true;
        }

        // Time
        let now = Local::now();
        let hour = now.hour();
        let msg_count = state.db.message_count();
        let new_msgs = msg_count - last_msg_count;
        let user_active = new_msgs > 0;

        // Track message rate (rolling 5-minute window)
        if user_active {
            for _ in 0..new_msgs {
                msg_timestamps.push(Instant::now());
            }
        }
        let five_min_ago = Instant::now() - std::time::Duration::from_secs(300);
        msg_timestamps.retain(|t| *t > five_min_ago);
        let recent_msg_rate = msg_timestamps.len() as f64 / 5.0;

        last_msg_count = msg_count;

        // Late night proactive — pushes to queue
        if (hour >= 23 || hour <= 4) && user_active && !late_night_sent {
            let msg = format!("it's {}:{:02}, maybe time to wrap up?", hour, now.minute());
            tracing::info!("proactive: {}", msg);
            proactive_queue.lock().await.push(msg);
            if let Err(e) = crate::autonomy::ingest_observation(
                state.db.as_ref(),
                crate::autonomy::ObservationInput {
                    kind: "attention_prompt".to_string(),
                    source: "awareness".to_string(),
                    content: format!("it's {}:{:02}, maybe time to wrap up?", hour, now.minute()),
                    context: serde_json::json!({
                        "hour": hour,
                        "minute": now.minute(),
                        "reason": "late_night_prompt",
                    }),
                    priority: 0.68,
                },
            ) {
                tracing::warn!("awareness: failed to store late-night observation: {}", e);
            }
            late_night_sent = true;
        }
        if hour >= 5 && hour <= 22 {
            late_night_sent = false;
        }

        // Welcome-back detection
        let idle = !user_active;
        let just_returned_now = was_idle && user_active;
        let idle_before_return = if just_returned_now {
            idle_since.elapsed().as_secs()
        } else {
            0
        };

        if just_returned_now {
            tracing::info!(
                "awareness: user returned after {}s idle",
                idle_before_return
            );
            just_returned_ttl = 3; // Keep just_returned true for 3 ticks (15s) so handler catches it
            if idle_before_return > 1800 {
                // 30+ min away
                proactive_queue
                    .lock()
                    .await
                    .push("welcome back".to_string());
                if let Err(e) = crate::autonomy::ingest_observation(
                    state.db.as_ref(),
                    crate::autonomy::ObservationInput {
                        kind: "return_event".to_string(),
                        source: "awareness".to_string(),
                        content: "welcome back".to_string(),
                        context: serde_json::json!({
                            "idle_before_return_secs": idle_before_return,
                        }),
                        priority: 0.55,
                    },
                ) {
                    tracing::warn!("awareness: failed to store return observation: {}", e);
                }
            }
        }
        if just_returned_ttl > 0 {
            just_returned_ttl -= 1;
        }

        if idle && !was_idle {
            idle_since = Instant::now();
        }

        // Burst detection (3+ user messages in 5s, accounting for 2 rows per message)
        let burst = new_msgs > 6;
        if burst {
            tracing::info!("awareness: message burst detected ({} new in 5s)", new_msgs);
        }

        // Time of day
        let time_of_day = match hour {
            5..=11 => TimeOfDay::Morning,
            12..=16 => TimeOfDay::Afternoon,
            17..=22 => TimeOfDay::Evening,
            _ => TimeOfDay::LateNight,
        };

        // Session energy estimation
        let session_min = session_start.elapsed().as_secs() / 60;
        let energy = if (just_returned_now || just_returned_ttl > 0) && idle_before_return > 300 {
            SessionEnergy::Fresh
        } else if recent_msg_rate > 3.0 && session_min > 60 {
            SessionEnergy::Grinding
        } else if recent_msg_rate < 0.3 && session_min > 30 {
            SessionEnergy::Winding
        } else {
            SessionEnergy::Sustained
        };
        let world_focus = crate::world::state::load_world_focus(state.db.as_ref());

        // Update shared context
        {
            let mut ctx = awareness.write().await;
            ctx.current_app = app.clone();
            ctx.app_duration_min = app_duration_min;
            ctx.time_of_day = time_of_day;
            ctx.hour = hour;
            ctx.recent_msg_rate = recent_msg_rate;
            ctx.just_returned = just_returned_now || just_returned_ttl > 0;
            ctx.idle_before_return_secs = idle_before_return;
            ctx.energy = energy;
            ctx.session_duration_min = session_min;
            ctx.burst_detected = burst;
            ctx.world_focus = world_focus;
        }

        was_idle = idle;
    }
}

#[cfg(test)]
#[path = "../tests/unit/awareness.rs"]
mod tests;
