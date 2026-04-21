//! Intent router — classifies user messages into actions.
//!
//! Two-pass system:
//! 1. Fast heuristics (instant, free) — catches obvious patterns
//! 2. LLM classifier (DeepSeek, ~10 tokens) — catches everything else
//!
//! Replaces 15+ keyword blocks with one clean routing layer.

use crate::llm::LlmGate;
use std::sync::Arc;

/// What the user wants to do.
#[derive(Debug, Clone)]
pub enum Intent {
    Time,
    Weather {
        city: Option<String>,
    },
    Remember {
        fact: String,
    },
    Recall,
    Search {
        query: String,
    },
    Remind {
        task: String,
    },
    ListReminders,
    Schedule {
        text: String,
    },
    ListSchedule,
    Git {
        action: String,
    },
    GitHub,
    Tweet {
        text: String,
    },
    Timeline,
    Mentions,
    Gif {
        query: String,
    },
    ImageGen {
        prompt: String,
        style: Option<String>,
    },
    ReadFile {
        path: String,
    },
    Evolve,
    Vision,
    Sleep,
    Chat, // default — goes to LLM
}

/// Try to classify intent with heuristics first (instant, free).
pub fn classify_fast(text: &str) -> Option<Intent> {
    let lower = text.to_lowercase();

    // Time — very specific patterns only
    if (lower == "time" || lower == "what time is it" || lower == "what's the time")
        && !lower.contains("times")
    {
        return Some(Intent::Time);
    }

    // Sleep signals
    let sleep_signals = [
        "going to sleep",
        "going to bed",
        "goodnight",
        "good night",
        "gonna sleep",
        "time to sleep",
        "night night",
        "gotta sleep",
    ];
    if sleep_signals.iter().any(|s| lower.contains(s)) || lower == "gn" {
        return Some(Intent::Sleep);
    }

    // Explicit commands (starts with keyword)
    if lower.starts_with("remember that ") || lower.starts_with("remember this ") {
        let fact = text
            .split("remember that ")
            .last()
            .or(text.split("remember this ").last())
            .unwrap_or(text)
            .trim()
            .to_string();
        return Some(Intent::Remember { fact });
    }

    if lower.starts_with("remind me to ") || lower.starts_with("remind me ") {
        let task = lower
            .replace("remind me to ", "")
            .replace("remind me ", "")
            .trim()
            .to_string();
        return Some(Intent::Remind { task });
    }

    if lower.starts_with("todo ") || lower.starts_with("todo:") {
        let task = lower
            .replace("todo ", "")
            .replace("todo:", "")
            .trim()
            .to_string();
        return Some(Intent::Remind { task });
    }

    if lower.starts_with("tweet:") || lower.starts_with("tweet ") && !lower.contains("?") {
        let tweet = text
            .replace("tweet:", "")
            .replace("tweet ", "")
            .trim()
            .to_string();
        if !tweet.is_empty() {
            return Some(Intent::Tweet { text: tweet });
        }
    }

    if lower.starts_with("search for ") || lower.starts_with("look up ") {
        let query = text
            .replace("search for ", "")
            .replace("look up ", "")
            .trim()
            .to_string();
        return Some(Intent::Search { query });
    }

    if lower.starts_with("gif:") || lower.starts_with("gif ") {
        let query = lower
            .replace("gif:", "")
            .replace("gif ", "")
            .trim()
            .to_string();
        return Some(Intent::Gif {
            query: if query.is_empty() {
                "funny".into()
            } else {
                query
            },
        });
    }

    if lower.starts_with("generate ") || lower.starts_with("draw ") || lower.starts_with("imagine ")
    {
        let prompt = text
            .replace("generate ", "")
            .replace("draw ", "")
            .replace("imagine ", "")
            .trim()
            .to_string();
        let style = detect_style(&lower);
        return Some(Intent::ImageGen { prompt, style });
    }

    // Exact matches
    if lower == "reminders" || lower == "todos" || lower == "my reminders" || lower == "my todos" {
        return Some(Intent::ListReminders);
    }
    if lower == "schedule" || lower == "my schedule" || lower == "scheduled tasks" {
        return Some(Intent::ListSchedule);
    }
    if lower == "my timeline" || lower == "twitter timeline" {
        return Some(Intent::Timeline);
    }
    if lower == "my mentions" || lower == "twitter mentions" {
        return Some(Intent::Mentions);
    }
    // Vision
    if lower.contains("screenshot")
        || lower.contains("my screen")
        || lower.contains("look at")
        || lower.contains("what do you see")
        || lower.contains("whats on screen")
    {
        return Some(Intent::Vision);
    }

    if lower == "git status" || lower == "git log" {
        let action = if lower.contains("log") {
            "log"
        } else {
            "status"
        };
        return Some(Intent::Git {
            action: action.into(),
        });
    }

    None
}

/// LLM-based intent classification for messages that heuristics can't handle.
pub async fn classify_llm(text: &str, llm: &Arc<LlmGate>) -> Intent {
    // Only classify messages under 200 chars — longer ones are probably chat
    if text.len() > 200 {
        return Intent::Chat;
    }

    let prompt = format!(
        "Classify this message into ONE category. Reply with ONLY the category name.\n\n\
         TIME — asking what time it is\n\
         WEATHER [city] — asking about weather (include city if mentioned)\n\
         REMEMBER [fact] — asking to remember something\n\
         RECALL — asking what you know about them\n\
         SEARCH [query] — wanting to search the web\n\
         REMIND [task] — setting a reminder or todo\n\
         REMINDERS — listing reminders/todos\n\
         SCHEDULE [text] — setting up a recurring task\n\
         SCHEDULES — listing scheduled tasks\n\
         GIT [action] — git status, log, diff, todos\n\
         GITHUB — checking github notifications/PRs\n\
         TWEET [text] — posting a tweet\n\
         TIMELINE — reading twitter timeline\n\
         MENTIONS — checking twitter mentions\n\
         GIF [query] — wanting a GIF\n\
         IMAGE [prompt] — wanting to generate an image\n\
         VISION — looking at screen, taking screenshot, seeing what's on the display\n\
         SLEEP — going to bed/sleep\n\
         EVOLVE — modifying code\n\
         CHAT — just chatting, none of the above\n\n\
         Message: \"{}\"\n\
         Category:",
        crate::trunc(text, 150)
    );

    let response = match llm.chat_auto(&prompt, 20).await {
        Ok(r) => r,
        Err(_) => return Intent::Chat,
    };

    let trimmed = response.trim().to_uppercase();

    // Parse the response
    if trimmed.starts_with("TIME") {
        return Intent::Time;
    }
    if trimmed.starts_with("WEATHER") {
        let city = trimmed
            .strip_prefix("WEATHER")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        return Intent::Weather { city };
    }
    if trimmed.starts_with("REMEMBER") {
        let fact = trimmed
            .strip_prefix("REMEMBER")
            .unwrap_or("")
            .trim()
            .to_string();
        if !fact.is_empty() {
            return Intent::Remember { fact };
        }
    }
    if trimmed.starts_with("RECALL") {
        return Intent::Recall;
    }
    if trimmed.starts_with("SEARCH") {
        let query = trimmed
            .strip_prefix("SEARCH")
            .unwrap_or("")
            .trim()
            .to_string();
        return Intent::Search {
            query: if query.is_empty() {
                text.to_string()
            } else {
                query
            },
        };
    }
    if trimmed.starts_with("REMIND") && !trimmed.starts_with("REMINDERS") {
        let task = trimmed
            .strip_prefix("REMIND")
            .unwrap_or("")
            .trim()
            .to_string();
        return Intent::Remind {
            task: if task.is_empty() {
                text.to_string()
            } else {
                task
            },
        };
    }
    if trimmed.starts_with("REMINDERS") {
        return Intent::ListReminders;
    }
    if trimmed.starts_with("SCHEDULE") && !trimmed.starts_with("SCHEDULES") {
        return Intent::Schedule {
            text: text.to_string(),
        };
    }
    if trimmed.starts_with("SCHEDULES") {
        return Intent::ListSchedule;
    }
    if trimmed.starts_with("GIT") && !trimmed.starts_with("GITHUB") {
        let action = if trimmed.contains("LOG") {
            "log"
        } else if trimmed.contains("DIFF") {
            "diff"
        } else if trimmed.contains("TODO") {
            "todos"
        } else {
            "status"
        };
        return Intent::Git {
            action: action.into(),
        };
    }
    if trimmed.starts_with("GITHUB") {
        return Intent::GitHub;
    }
    if trimmed.starts_with("TWEET") {
        let tweet_text = trimmed
            .strip_prefix("TWEET")
            .unwrap_or("")
            .trim()
            .to_string();
        return Intent::Tweet {
            text: if tweet_text.is_empty() {
                text.to_string()
            } else {
                tweet_text
            },
        };
    }
    if trimmed.starts_with("TIMELINE") {
        return Intent::Timeline;
    }
    if trimmed.starts_with("MENTIONS") {
        return Intent::Mentions;
    }
    if trimmed.starts_with("GIF") {
        let query = trimmed.strip_prefix("GIF").unwrap_or("").trim().to_string();
        return Intent::Gif {
            query: if query.is_empty() {
                "funny".into()
            } else {
                query.to_lowercase()
            },
        };
    }
    if trimmed.starts_with("IMAGE") {
        let prompt = trimmed
            .strip_prefix("IMAGE")
            .unwrap_or("")
            .trim()
            .to_string();
        return Intent::ImageGen {
            prompt: if prompt.is_empty() {
                text.to_string()
            } else {
                prompt
            },
            style: None,
        };
    }
    if trimmed.starts_with("VISION") {
        return Intent::Vision;
    }
    if trimmed.starts_with("SLEEP") {
        return Intent::Sleep;
    }
    if trimmed.starts_with("EVOLVE") {
        return Intent::Evolve;
    }

    Intent::Chat
}

fn detect_style(lower: &str) -> Option<String> {
    if lower.contains("anime") {
        Some("anime".into())
    } else if lower.contains("realistic") {
        Some("realistic".into())
    } else if lower.contains("cinematic") {
        Some("cinematic".into())
    } else if lower.contains("pixel") {
        Some("pixel".into())
    } else if lower.contains("artistic") {
        Some("artistic".into())
    } else {
        None
    }
}
