//! Pattern learning — detects behavioral patterns from interaction history.
//! No LLM calls. Pure data analysis over the interactions table.

use crate::db::Db;
use crate::interaction::Interaction;

#[derive(Debug, Clone, PartialEq)]
pub enum Category {
    TimePattern,
    SessionPattern,
    TopicPattern,
    ResponsePattern,
}

impl std::fmt::Display for Category {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimePattern => write!(f, "time"),
            Self::SessionPattern => write!(f, "session"),
            Self::TopicPattern => write!(f, "topic"),
            Self::ResponsePattern => write!(f, "response"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Pattern {
    pub category: Category,
    pub description: String,
    pub confidence: f32, // 0.0–1.0
    pub data_points: u32,
}

/// Analyze all interactions and return detected patterns.
/// Needs >= 20 interactions to produce anything useful.
pub fn analyze(db: &Db) -> Vec<Pattern> {
    let interactions = db.get_interactions(1000);
    if interactions.len() < 20 {
        return vec![];
    }
    let mut p = Vec::new();
    p.extend(time_patterns(&interactions));
    p.extend(session_patterns(&interactions));
    p.extend(topic_patterns(db));
    p.extend(response_patterns(&interactions));
    p
}

/// Group interactions by hour, find peak/dead engagement windows (3h sliding).
fn time_patterns(interactions: &[Interaction]) -> Vec<Pattern> {
    let mut by_hour: [Vec<f32>; 24] = Default::default();
    for i in interactions {
        by_hour[i.hour as usize].push(i.outcome.score());
    }

    let mut out = Vec::new();
    let (mut best_h, mut best_s, mut best_n) = (0u32, f32::MIN, 0u32);
    let (mut worst_h, mut worst_s, mut worst_n) = (0u32, f32::MAX, 0u32);

    for h in 0..24u32 {
        let mut s = Vec::new();
        for off in 0..3u32 {
            s.extend_from_slice(&by_hour[((h + off) % 24) as usize]);
        }
        if s.is_empty() {
            continue;
        }
        let avg = s.iter().sum::<f32>() / s.len() as f32;
        let w = avg * (s.len() as f32).sqrt();
        if s.len() >= 5 && w > best_s {
            best_s = w;
            best_h = h;
            best_n = s.len() as u32;
        }
        if s.len() >= 3 && avg < worst_s {
            worst_s = avg;
            worst_h = h;
            worst_n = s.len() as u32;
        }
    }

    if best_n >= 5 {
        out.push(Pattern {
            category: Category::TimePattern,
            description: format!(
                "user is most engaged between {}:00\u{2013}{}:00",
                best_h,
                (best_h + 3) % 24
            ),
            confidence: (best_n as f32 / interactions.len() as f32).min(1.0),
            data_points: best_n,
        });
    }
    if worst_n >= 3 && worst_s < 0.0 {
        out.push(Pattern {
            category: Category::TimePattern,
            description: format!(
                "user is least engaged between {}:00\u{2013}{}:00",
                worst_h,
                (worst_h + 3) % 24
            ),
            confidence: 0.5,
            data_points: worst_n,
        });
    }
    out
}

/// Detect session lengths from message timestamp gaps (>30min = new session).
fn session_patterns(interactions: &[Interaction]) -> Vec<Pattern> {
    if interactions.len() < 10 {
        return vec![];
    }
    let mut sorted: Vec<&Interaction> = interactions.iter().collect();
    sorted.sort_by_key(|i| i.timestamp);

    let mut lengths: Vec<f64> = Vec::new();
    let mut start = sorted[0].timestamp;
    for pair in sorted.windows(2) {
        if pair[1].timestamp - pair[0].timestamp > 1800 {
            let hrs = (pair[0].timestamp - start) as f64 / 3600.0;
            if hrs > 0.1 {
                lengths.push(hrs);
            }
            start = pair[1].timestamp;
        }
    }
    if let Some(last) = sorted.last() {
        let hrs = (last.timestamp - start) as f64 / 3600.0;
        if hrs > 0.1 {
            lengths.push(hrs);
        }
    }
    if lengths.len() < 3 {
        return vec![];
    }

    let avg = lengths.iter().sum::<f64>() / lengths.len() as f64;
    let n = lengths.len() as u32;
    let range = match avg {
        x if x < 1.0 => "under 1 hour",
        x if x < 2.0 => "1\u{2013}2 hour",
        x if x < 3.5 => "2\u{2013}3 hour",
        x if x < 5.0 => "3\u{2013}5 hour",
        _ => "5+ hour",
    };
    vec![Pattern {
        category: Category::SessionPattern,
        description: format!(
            "user typically works in {} stretches (avg {:.1}h, {} sessions)",
            range, avg, n
        ),
        confidence: (n as f32 / 20.0).min(1.0),
        data_points: n,
    }]
}

/// Count topic keywords from recent user messages.
fn topic_patterns(db: &Db) -> Vec<Pattern> {
    let topics = db.get_topic_counts(500);
    if topics.is_empty() {
        return vec![];
    }
    let total: u32 = topics.iter().map(|(_, c)| *c).sum();
    let mut out = Vec::new();
    for (i, (topic, count)) in topics.iter().take(2).enumerate() {
        let share = *count as f32 / total as f32;
        if *count >= 5 && share > if i == 0 { 0.15 } else { 0.10 } {
            let desc = if i == 0 {
                format!(
                    "user asks about {} most frequently ({}/{})",
                    topic, count, total
                )
            } else {
                format!("user frequently asks about {} ({} mentions)", topic, count)
            };
            out.push(Pattern {
                category: Category::TopicPattern,
                description: desc,
                confidence: share.min(1.0),
                data_points: *count,
            });
        }
    }
    out
}

/// Correlate soul state (warmth/verbosity/assertiveness) with outcome scores.
fn response_patterns(interactions: &[Interaction]) -> Vec<Pattern> {
    let pos: Vec<&Interaction> = interactions
        .iter()
        .filter(|i| i.outcome.score() > 0.0)
        .collect();
    let neg: Vec<&Interaction> = interactions
        .iter()
        .filter(|i| i.outcome.score() < 0.0)
        .collect();
    if pos.len() < 5 || neg.len() < 3 {
        return vec![];
    }

    let avg = |items: &[&Interaction], f: fn(&Interaction) -> f32| -> f32 {
        items.iter().map(|i| f(i)).sum::<f32>() / items.len() as f32
    };
    let pv = avg(&pos, |i| i.verbosity);
    let nv = avg(&neg, |i| i.verbosity);
    let pw = avg(&pos, |i| i.warmth);
    let nw = avg(&neg, |i| i.warmth);
    let pa = avg(&pos, |i| i.assertiveness);
    let na = avg(&neg, |i| i.assertiveness);
    let total = (pos.len() + neg.len()) as u32;
    let mut out = Vec::new();

    let vd = pv - nv;
    if vd.abs() > 0.1 {
        let pref = if pv < 0.3 {
            "short"
        } else if pv < 0.6 {
            "moderate"
        } else {
            "detailed"
        };
        out.push(Pattern {
            category: Category::ResponsePattern,
            description: format!(
                "user prefers {} responses (verbosity {:.2} in positive outcomes)",
                pref, pv
            ),
            confidence: vd.abs().min(1.0),
            data_points: total,
        });
    }
    let wd = pw - nw;
    if wd.abs() > 0.1 {
        let dir = if wd > 0.0 { "warmer" } else { "cooler" };
        out.push(Pattern {
            category: Category::ResponsePattern,
            description: format!(
                "user responds better to {} tone ({:.2} vs {:.2})",
                dir, pw, nw
            ),
            confidence: wd.abs().min(1.0),
            data_points: total,
        });
    }
    let ad = pa - na;
    if ad.abs() > 0.1 {
        let dir = if ad > 0.0 {
            "more assertive"
        } else {
            "less assertive"
        };
        out.push(Pattern {
            category: Category::ResponsePattern,
            description: format!("user prefers {} responses ({:.2} vs {:.2})", dir, pa, na),
            confidence: ad.abs().min(1.0),
            data_points: total,
        });
    }
    out
}
