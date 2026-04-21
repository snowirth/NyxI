//! Soul engine — emergent personality from interaction data.
//!
//! Starts with baseline traits. Adapts based on two inputs:
//! 1. Awareness context (time of day, energy, app) — immediate adjustment
//! 2. Interaction history (what the user responded to) — long-term learning
//!
//! The personality isn't prescribed. It grows from accumulated evidence
//! of what works. Like a person who learns to read the room.

use crate::awareness::{AwarenessContext, SessionEnergy, TimeOfDay};
use crate::interaction::InteractionInsights;

/// A personality trait with a dynamic weight (0.0 to 1.0).
#[derive(Debug, Clone)]
pub struct Trait {
    pub name: &'static str,
    pub description: &'static str,
    pub weight: f32,
    /// Baseline weight — where the trait started before any learning
    pub baseline: f32,
}

/// The living soul — personality that emerges from experience.
#[derive(Debug, Clone)]
pub struct Soul {
    pub traits: Vec<Trait>,
    pub warmth: f32,
    pub verbosity: f32,
    pub assertiveness: f32,
    pub interactions: u32,
    /// Whether learning data has been applied this session
    pub learned: bool,
}

impl Default for Soul {
    fn default() -> Self {
        Self {
            traits: vec![
                Trait {
                    name: "direct",
                    description: "Says things plainly, no filler",
                    weight: 0.5,
                    baseline: 0.5,
                },
                Trait {
                    name: "genuine",
                    description: "Honest even when uncomfortable",
                    weight: 0.5,
                    baseline: 0.5,
                },
                Trait {
                    name: "casual",
                    description: "Talks like texting a friend",
                    weight: 0.5,
                    baseline: 0.5,
                },
                Trait {
                    name: "attentive",
                    description: "Remembers and connects details",
                    weight: 0.5,
                    baseline: 0.5,
                },
                Trait {
                    name: "witty",
                    description: "Dry humor, never forced",
                    weight: 0.3,
                    baseline: 0.3,
                },
                Trait {
                    name: "protective",
                    description: "Looks out for the user's wellbeing",
                    weight: 0.4,
                    baseline: 0.4,
                },
            ],
            warmth: 0.5,
            verbosity: 0.3,
            assertiveness: 0.5,
            interactions: 0,
            learned: false,
        }
    }
}

impl Soul {
    fn load_identity_text() -> String {
        std::fs::read_to_string("IDENTITY.md")
            .or_else(|_| std::fs::read_to_string("SOUL.md"))
            .unwrap_or_default()
    }

    /// Apply long-term learning from interaction history.
    /// Call once per session (or after enough new data accumulates).
    /// This is the Chappie part — personality shaped by experience, not prescription.
    pub fn learn_from(&mut self, insights: &InteractionInsights) {
        if insights.total < 10 {
            return; // not enough data to learn from
        }

        // Gradually shift toward what works (slow learning, 20% per application)
        let rate = 0.2;

        self.warmth = lerp(self.warmth, insights.optimal_warmth, rate);
        self.verbosity = lerp(self.verbosity, insights.optimal_verbosity, rate);
        self.assertiveness = lerp(self.assertiveness, insights.optimal_assertiveness, rate);

        // Night preference: if user engages more at night, don't reduce verbosity at night
        // (overrides the default "late night = terse" assumption)
        if insights.night_preference > 0.2 {
            // User is MORE engaged at night — trait for that
            self.trait_weight(
                "protective",
                (self.trait_by_name("protective").baseline - 0.1).max(0.2),
            );
        }

        self.learned = true;
        tracing::info!(
            "soul: learned from {} interactions — warmth={:.2} verbosity={:.2} assertiveness={:.2}",
            insights.total,
            self.warmth,
            self.verbosity,
            self.assertiveness
        );
    }

    /// Adapt personality based on immediate awareness context.
    /// Learning provides the baseline; awareness provides moment-to-moment adjustment.
    pub fn adapt(&mut self, ctx: &AwarenessContext) {
        // Time-of-day shifts — gentler if we've learned from data
        let strength = if self.learned { 0.05 } else { 0.1 };

        match ctx.time_of_day {
            TimeOfDay::LateNight => {
                self.verbosity = (self.verbosity - strength).max(0.1);
                self.trait_nudge("protective", strength);
                self.trait_nudge("witty", -strength);
            }
            TimeOfDay::Morning => {
                self.warmth = (self.warmth + strength).min(0.8);
            }
            _ => {}
        }

        // Energy-based shifts
        match ctx.energy {
            SessionEnergy::Fresh => {
                self.warmth = lerp(self.warmth, 0.7, strength * 2.0);
                self.trait_nudge("casual", strength);
            }
            SessionEnergy::Grinding => {
                self.verbosity = lerp(self.verbosity, 0.1, strength * 2.0);
                self.trait_nudge("direct", strength);
            }
            SessionEnergy::Winding => {
                self.warmth = lerp(self.warmth, 0.7, strength);
                self.trait_nudge("witty", strength);
            }
            SessionEnergy::Sustained => {
                // Drift back toward learned baseline
                if self.learned {
                    // already at learned values, small drift
                } else {
                    self.warmth = lerp(self.warmth, 0.5, 0.05);
                    self.verbosity = lerp(self.verbosity, 0.3, 0.05);
                }
            }
        }

        if ctx.just_returned && ctx.idle_before_return_secs > 1800 {
            self.warmth = (self.warmth + 0.2).min(0.9);
            self.trait_nudge("attentive", 0.15);
        }

        if ctx.burst_detected {
            self.assertiveness = (self.assertiveness - 0.1).max(0.2);
        }

        self.interactions += 1;

        // Familiarity: warmth increases naturally over many interactions
        if self.interactions > 20 {
            self.warmth = (self.warmth + 0.01).min(0.85);
        }
    }

    /// Generate the identity/soul directive for the LLM prompt.
    pub fn to_prompt(&self) -> String {
        let base = Self::load_identity_text();

        let active_traits: Vec<String> = self
            .traits
            .iter()
            .filter(|t| t.weight > 0.3)
            .map(|t| {
                let intensity = if t.weight > 0.8 {
                    "strongly"
                } else if t.weight > 0.6 {
                    "notably"
                } else {
                    "somewhat"
                };
                format!("- {} {}: {}", intensity, t.name, t.description)
            })
            .collect();

        let verbosity_hint = if self.verbosity < 0.2 {
            "Extremely terse. One sentence max when possible."
        } else if self.verbosity < 0.4 {
            "Short and direct. 1-3 sentences."
        } else if self.verbosity < 0.7 {
            "Normal conversational length."
        } else {
            "Can be more expansive. Share thoughts."
        };

        let warmth_hint = if self.warmth < 0.3 {
            "Clinical and efficient."
        } else if self.warmth < 0.6 {
            "Friendly but focused."
        } else {
            "Warm and personable. Like a close friend."
        };

        format!(
            "{}\n\n<soul_state>\nActive traits:\n{}\n\nTone: {} {}\n</soul_state>",
            base,
            active_traits.join("\n"),
            warmth_hint,
            verbosity_hint,
        )
    }

    /// Nudge a trait weight up or down (clamped 0.1 to 1.0).
    fn trait_nudge(&mut self, name: &str, delta: f32) {
        if let Some(t) = self.traits.iter_mut().find(|t| t.name == name) {
            t.weight = (t.weight + delta).clamp(0.1, 1.0);
        }
    }

    fn trait_weight(&mut self, name: &str, weight: f32) {
        if let Some(t) = self.traits.iter_mut().find(|t| t.name == name) {
            t.weight = weight.clamp(0.1, 1.0);
        }
    }

    fn trait_by_name(&self, name: &str) -> &Trait {
        self.traits.iter().find(|t| t.name == name).unwrap()
    }
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a + (b - a) * t
}
