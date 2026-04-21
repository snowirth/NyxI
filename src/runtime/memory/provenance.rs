use super::*;

const PROVENANCE_HISTORY_LIMIT: usize = 12;
const PROVENANCE_ITEM_LIMIT: usize = 6;
const LOW_SIGNAL_HISTORY_SUBJECTS: &[&str] = &[
    "ok",
    "okay",
    "yeah",
    "yep",
    "yup",
    "sure",
    "thanks",
    "thank you",
    "cool",
    "nice",
    "got it",
];

impl AppState {
    pub(crate) async fn memory_provenance_brief(
        &self,
        channel: &str,
        requested_query: &str,
        limit: usize,
        source: &str,
    ) -> MemoryProvenanceBrief {
        let (resolved_query, inferred_from_history, inferred_from_surface) =
            resolve_memory_provenance_query(self.db.as_ref(), channel, requested_query);
        if resolved_query.is_empty() {
            return unresolved_memory_provenance_brief(requested_query, source);
        }

        let working_set = self
            .memory_working_set_for_query(&resolved_query, limit.clamp(3, PROVENANCE_ITEM_LIMIT))
            .await;
        let coverage_gaps = working_set
            .negative_evidence
            .iter()
            .map(|gap| gap.summary.clone())
            .take(2)
            .collect::<Vec<_>>();
        let mut brief = MemoryProvenanceBrief {
            schema_version: MEMORY_PROVENANCE_BRIEF_SCHEMA_VERSION.to_string(),
            generated_at: working_set.assembled_at.clone(),
            source: source.to_string(),
            requested_query: requested_query.trim().to_string(),
            resolved_query,
            inferred_from_history,
            inferred_from_surface,
            basis_summary: working_set.explanation.lane_summary.clone(),
            evidence_items: working_set
                .explanation
                .surfaced_items
                .iter()
                .filter(|item| item.kind != "negative_evidence")
                .take(limit.clamp(1, PROVENANCE_ITEM_LIMIT))
                .cloned()
                .collect(),
            supporting_source_count: working_set.supporting_sources.len(),
            evidence_cluster_count: working_set.aggregated_evidence.len(),
            coverage_gaps,
            reply: String::new(),
        };
        brief.reply = render_memory_provenance_reply(&brief);
        brief
    }
}

pub(crate) fn is_memory_provenance_request(lower: &str) -> bool {
    lower.contains("where did you get that")
        || lower.contains("what is that based on")
        || lower.contains("what's that based on")
        || lower.contains("whats that based on")
        || lower.contains("why do you think that")
        || lower.contains("what evidence do you have for that")
        || lower.contains("what is that grounded in")
}

fn resolve_memory_provenance_query(
    db: &db::Db,
    channel: &str,
    requested_query: &str,
) -> (String, bool, Option<String>) {
    if let Some(explicit) = explicit_memory_provenance_subject(requested_query) {
        return (explicit, false, None);
    }

    let trimmed = requested_query.trim();
    if !trimmed.is_empty() && !is_memory_provenance_request(&trimmed.to_ascii_lowercase()) {
        return (trimmed.to_string(), false, None);
    }

    let history = db.get_history(channel, PROVENANCE_HISTORY_LIMIT);
    let prior_user = history.iter().rev().find_map(|(role, content)| {
        if role == "user" && is_meaningful_history_subject(content) {
            Some(content.trim().to_string())
        } else {
            None
        }
    });
    let prior_assistant = history.iter().rev().find_map(|(role, content)| {
        if role == "assistant" && is_meaningful_history_subject(content) {
            Some(content.trim().to_string())
        } else {
            None
        }
    });

    match prior_user.or(prior_assistant) {
        Some(query) => (query, true, None),
        None => recent_cross_surface_subject(db, channel)
            .map(|(query, surface)| (query, true, Some(surface)))
            .unwrap_or_else(|| (String::new(), false, None)),
    }
}

fn explicit_memory_provenance_subject(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let lower = trimmed.to_ascii_lowercase();
    if let Some((prefix, rest)) = trimmed.split_once(':') {
        if is_memory_provenance_request(&prefix.trim().to_lowercase()) {
            let subject = rest.trim();
            if !subject.is_empty() {
                return Some(subject.to_string());
            }
        }
    }

    if !is_memory_provenance_request(&lower) {
        return Some(trimmed.to_string());
    }

    for marker in [" about ", " regarding "] {
        if let Some(index) = lower.rfind(marker) {
            let subject = trimmed[index + marker.len()..].trim();
            if !subject.is_empty() && !is_memory_provenance_request(&subject.to_ascii_lowercase()) {
                return Some(subject.to_string());
            }
        }
    }

    None
}

fn is_meaningful_history_subject(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.len() < 8 {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    if is_memory_provenance_request(&lower) {
        return false;
    }
    !LOW_SIGNAL_HISTORY_SUBJECTS
        .iter()
        .any(|signal| lower == *signal)
}

fn unresolved_memory_provenance_brief(
    requested_query: &str,
    source: &str,
) -> MemoryProvenanceBrief {
    MemoryProvenanceBrief {
        schema_version: MEMORY_PROVENANCE_BRIEF_SCHEMA_VERSION.to_string(),
        generated_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        source: source.to_string(),
        requested_query: requested_query.trim().to_string(),
        resolved_query: String::new(),
        inferred_from_history: false,
        inferred_from_surface: None,
        basis_summary:
            "No clear topic could be resolved from the request or recent channel history."
                .to_string(),
        evidence_items: Vec::new(),
        supporting_source_count: 0,
        evidence_cluster_count: 0,
        coverage_gaps: Vec::new(),
        reply: "I couldn't tell what 'that' referred to, so I can't trace the memory basis yet."
            .to_string(),
    }
}

fn render_memory_provenance_reply(brief: &MemoryProvenanceBrief) -> String {
    if brief.resolved_query.is_empty() {
        return "I couldn't tell what 'that' referred to, so I can't trace the memory basis yet."
            .to_string();
    }

    let mut sentences = Vec::new();
    if brief.inferred_from_history {
        if let Some(surface) = brief.inferred_from_surface.as_deref() {
            sentences.push(format!(
                "I treated 'that' as your earlier topic from {}: '{}'.",
                surface,
                crate::trunc(&brief.resolved_query, 96)
            ));
        } else {
            sentences.push(format!(
                "I treated 'that' as your earlier topic: '{}'.",
                crate::trunc(&brief.resolved_query, 96)
            ));
        }
    }

    let evidence = brief
        .evidence_items
        .iter()
        .take(2)
        .map(describe_provenance_item)
        .collect::<Vec<_>>();
    if let Some(first) = evidence.first() {
        if let Some(second) = evidence.get(1) {
            sentences.push(format!("That came mainly from {}, plus {}.", first, second));
        } else {
            sentences.push(format!("That came mainly from {}.", first));
        }
    } else {
        sentences.push("I couldn't find a strong direct memory trace for that yet.".to_string());
    }

    if brief.supporting_source_count > 0 {
        let verb = if brief.supporting_source_count == 1 {
            "is"
        } else {
            "are"
        };
        let noun = if brief.supporting_source_count == 1 {
            "source"
        } else {
            "sources"
        };
        sentences.push(format!(
            "There {} {} supporting {} attached to that recall.",
            verb, brief.supporting_source_count, noun
        ));
    }

    if brief.evidence_cluster_count > 0 {
        let noun = if brief.evidence_cluster_count == 1 {
            "cluster"
        } else {
            "clusters"
        };
        sentences.push(format!(
            "I also merged {} evidence {} around that topic.",
            brief.evidence_cluster_count, noun
        ));
    }

    if let Some(gap) = brief.coverage_gaps.first() {
        sentences.push(format!(
            "One gap I still see is {}.",
            crate::trunc(gap, 160)
        ));
    }

    sentences.join(" ")
}

fn recent_cross_surface_subject(db: &db::Db, current_channel: &str) -> Option<(String, String)> {
    let mut candidates = Vec::new();

    push_cross_surface_subject(
        &mut candidates,
        db.get_history_with_meta("web", PROVENANCE_HISTORY_LIMIT),
        current_channel,
    );
    push_cross_surface_subject(
        &mut candidates,
        db.get_history_with_meta("voice", PROVENANCE_HISTORY_LIMIT),
        current_channel,
    );
    push_cross_surface_subject(
        &mut candidates,
        db.get_history_with_meta("mcp", PROVENANCE_HISTORY_LIMIT),
        current_channel,
    );
    push_cross_surface_subject(
        &mut candidates,
        db.get_history_with_meta_by_prefix("telegram:", PROVENANCE_HISTORY_LIMIT),
        current_channel,
    );
    push_cross_surface_subject(
        &mut candidates,
        db.get_history_with_meta_by_prefix("discord:", PROVENANCE_HISTORY_LIMIT),
        current_channel,
    );

    candidates.sort_by(|(left_id, _, _), (right_id, _, _)| right_id.cmp(left_id));
    candidates
        .into_iter()
        .next()
        .map(|(_, query, surface)| (query, surface))
}

fn push_cross_surface_subject(
    out: &mut Vec<(i64, String, String)>,
    entries: Vec<(i64, String, String, String, String)>,
    current_channel: &str,
) {
    let Some((message_id, channel, _role, content, _timestamp)) =
        select_meaningful_cross_surface_entry(&entries, current_channel)
    else {
        return;
    };

    out.push((
        *message_id,
        content.trim().to_string(),
        provenance_surface_label(channel),
    ));
}

fn select_meaningful_cross_surface_entry<'a>(
    entries: &'a [(i64, String, String, String, String)],
    current_channel: &str,
) -> Option<&'a (i64, String, String, String, String)> {
    entries
        .iter()
        .rev()
        .filter(|(_, channel, _, _, _)| channel != current_channel)
        .find(|(_, _, role, content, _)| role == "user" && is_meaningful_history_subject(content))
        .or_else(|| {
            entries
                .iter()
                .rev()
                .filter(|(_, channel, _, _, _)| channel != current_channel)
                .find(|(_, _, _, content, _)| is_meaningful_history_subject(content))
        })
}

fn provenance_surface_label(channel: &str) -> String {
    if channel.starts_with("telegram:") {
        "Telegram".to_string()
    } else if channel.starts_with("discord:") {
        "Discord".to_string()
    } else if channel == "voice" {
        "voice".to_string()
    } else if channel == "mcp" {
        "MCP".to_string()
    } else {
        "web".to_string()
    }
}

fn describe_provenance_item(item: &MemoryExplanationItem) -> String {
    let label = crate::trunc(item.label.trim(), 120);
    match item.kind.as_str() {
        "claim" => format!("a remembered claim: '{}'", label),
        "aggregate" => format!("merged evidence: '{}'", label),
        "capsule" => format!("a recent conversation capsule: '{}'", label),
        "source" => format!("a supporting source: '{}'", label),
        "episode" => format!("a recent episode: '{}'", label),
        "procedure" => format!("a learned procedure: '{}'", label),
        "resource" => format!("a runtime resource signal: '{}'", label),
        _ => format!("'{}'", label),
    }
}
