use std::collections::{HashMap, HashSet};

use super::explain::build_memory_explanation;
use super::planner::{
    memory_lane_label, memory_request_mode_label, normalize_semantic_token, semantic_token_set,
    surface_section_order,
};
use super::*;

pub(super) fn assemble_memory_working_set(
    query: &str,
    plan: &MemoryRetrievalPlan,
    context: &db::RecallContext,
    capsules: &[db::MemorySessionCapsuleRecord],
    resources: &[MemoryResourceCard],
    limit: usize,
) -> MemoryWorkingSet {
    let (aggregated_evidence, negative_evidence) =
        synthesize_memory_evidence(query, plan, context, capsules, limit);
    let memory_items = memory_surface_items(
        context,
        capsules,
        resources,
        &aggregated_evidence,
        &negative_evidence,
        plan,
    )
    .into_iter()
    .take(limit)
    .collect::<Vec<_>>();
    let prompt_context = render_memory_surface(&memory_items, plan);
    let explanation = build_memory_explanation(
        query,
        plan,
        context,
        capsules,
        resources,
        &aggregated_evidence,
        &negative_evidence,
        limit,
    );

    MemoryWorkingSet {
        schema_version: MEMORY_WORKING_SET_SCHEMA_VERSION.to_string(),
        query: query.to_string(),
        assembled_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        intent: infer_memory_working_set_intent(query, plan),
        plan: plan.clone(),
        entity_focus: derive_memory_entity_focus(query, context, limit),
        action_notes: derive_memory_action_notes(
            query,
            plan,
            context,
            capsules,
            resources,
            &aggregated_evidence,
            &negative_evidence,
        ),
        explanation,
        aggregated_evidence,
        negative_evidence,
        prompt_context,
        capsules: capsules.to_vec(),
        resource_cards: resources.to_vec(),
        profile: context.profile.clone(),
        active_claims: context.active_claims.clone(),
        procedures: context.procedures.clone(),
        recent_episodes: context.recent_episodes.clone(),
        supporting_sources: context.supporting_sources.clone(),
        uncertainties: context.uncertainties.clone(),
    }
}

pub(super) fn infer_memory_working_set_intent(query: &str, plan: &MemoryRetrievalPlan) -> String {
    let lower = query.to_lowercase();
    if lower.contains("build")
        || lower.contains("fix")
        || lower.contains("implement")
        || lower.contains("refactor")
        || lower.contains("evolve")
    {
        "code_change".to_string()
    } else if lower.contains("why")
        || lower.contains("explain")
        || lower.contains("compare")
        || lower.contains("analyze")
    {
        "analysis".to_string()
    } else {
        memory_request_mode_label(&plan.mode).to_string()
    }
}

pub(super) fn derive_memory_entity_focus(
    query: &str,
    context: &db::RecallContext,
    limit: usize,
) -> Vec<String> {
    let mut focus = Vec::new();
    let mut seen = HashSet::new();
    let lower = query.to_lowercase();

    if lower.contains("nyx") && seen.insert("Nyx".to_string()) {
        focus.push("Nyx".to_string());
    }
    if ["user", "my", "me", "i "]
        .iter()
        .any(|needle| lower.contains(needle))
        && seen.insert("User".to_string())
    {
        focus.push("User".to_string());
    }

    for claim in &context.active_claims {
        let candidate =
            entity_focus_name_from_text(&claim.statement).unwrap_or_else(|| {
                match claim.subject.as_str() {
                    "project" => "Nyx".to_string(),
                    "user" => "User".to_string(),
                    "general" => String::new(),
                    other => other.trim().to_string(),
                }
            });
        if candidate.is_empty() {
            continue;
        }
        let normalized = candidate.to_lowercase();
        if seen.insert(normalized) {
            focus.push(candidate);
        }
        if focus.len() >= limit {
            break;
        }
    }

    for episode in &context.recent_episodes {
        let Some(candidate) = entity_focus_name_from_text(&episode.content) else {
            continue;
        };
        let normalized = candidate.to_lowercase();
        if seen.insert(normalized) {
            focus.push(candidate);
        }
        if focus.len() >= limit {
            break;
        }
    }

    focus
}

fn entity_focus_name_from_text(text: &str) -> Option<String> {
    let lower = text.to_lowercase();
    if lower.contains("project:nyx") {
        Some("Nyx".to_string())
    } else if lower.contains("person:user") {
        Some("User".to_string())
    } else if lower.contains("nyx") || lower.contains(" project ") || lower.starts_with("project ")
    {
        Some("Nyx".to_string())
    } else if lower.contains("user") || lower.contains(" owner ") || lower.starts_with("vd ") {
        Some("User".to_string())
    } else {
        None
    }
}

pub(super) fn derive_memory_action_notes(
    query: &str,
    plan: &MemoryRetrievalPlan,
    context: &db::RecallContext,
    capsules: &[db::MemorySessionCapsuleRecord],
    resources: &[MemoryResourceCard],
    aggregates: &[MemoryEvidenceAggregate],
    negative_evidence: &[MemoryNegativeEvidence],
) -> Vec<String> {
    let mut notes = Vec::new();
    let has_stale_claims = context.active_claims.iter().any(|claim| claim.is_stale);
    let lower = query.to_lowercase();

    notes.push(format!(
        "Planner selected the {} lane for this memory lookup.",
        memory_lane_label(&plan.lane)
    ));
    if !context.procedures.is_empty() {
        notes.push("Apply learned procedures before answering or acting.".to_string());
    }
    if has_stale_claims || !context.uncertainties.is_empty() {
        notes.push(
            "Some recalled claims are stale or uncertain; verify before relying on them."
                .to_string(),
        );
    }
    if !context.supporting_sources.is_empty() {
        notes.push(
            "Supporting sources are available if evidence or provenance is needed.".to_string(),
        );
    }
    if !capsules.is_empty() {
        notes.push(
            "Conversation capsules are available for fast recall of recent evidence windows."
                .to_string(),
        );
    }
    if let Some(max_sessions) = aggregates
        .iter()
        .map(|aggregate| aggregate.session_count)
        .max()
        .filter(|count| *count > 1)
    {
        notes.push(format!(
            "Evidence synthesis merged related recall across {} sessions.",
            max_sessions
        ));
    }
    if !negative_evidence.is_empty() {
        notes.push(
            "Related evidence exists for some query terms even where no direct mention was recalled."
                .to_string(),
        );
    }
    if !resources.is_empty() {
        notes.push("Resource cards are available for readiness-aware tool, provider, and action selection.".to_string());
    }
    if matches!(
        plan.lane,
        MemoryRetrievalLane::CapsuleRecall | MemoryRetrievalLane::TemporalRecall
    ) {
        notes.push(
            "Start with capsule anchors before widening to full historical recall.".to_string(),
        );
    }
    if lower.contains("latest")
        || lower.contains("today")
        || lower.contains("current")
        || lower.contains("version")
        || lower.contains("price")
    {
        notes.push("Prefer fresh verification for time-sensitive facts.".to_string());
    }

    notes
}

const LOW_SIGNAL_THEME_TOKENS: &[&str] = &["daily", "duration", "time"];
const LOW_SIGNAL_NEGATIVE_TERMS: &[&str] = &[
    "what",
    "when",
    "which",
    "said",
    "remember",
    "recall",
    "mention",
    "mentioned",
    "session",
    "latest",
    "current",
    "today",
    "much",
    "time",
    "every",
    "day",
    "daily",
];

#[derive(Clone, Debug)]
struct WorkingSetEvidenceCandidate {
    kind: &'static str,
    text: String,
    summary: String,
    session_key: Option<String>,
    lexical_tokens: HashSet<String>,
    semantic_tokens: HashSet<String>,
}

#[derive(Clone, Debug)]
struct EvidenceAggregateAccumulator {
    topic: String,
    theme_tokens: Vec<String>,
    support_count: usize,
    session_keys: HashSet<String>,
    evidence_kinds: HashSet<String>,
    examples: Vec<String>,
    direct_hit_count: usize,
}

impl EvidenceAggregateAccumulator {
    fn new(topic: String, theme_tokens: Vec<String>) -> Self {
        Self {
            topic,
            theme_tokens,
            support_count: 0,
            session_keys: HashSet::new(),
            evidence_kinds: HashSet::new(),
            examples: Vec::new(),
            direct_hit_count: 0,
        }
    }

    fn register(&mut self, candidate: &WorkingSetEvidenceCandidate, direct_hit: bool) {
        self.support_count += 1;
        if let Some(session_key) = &candidate.session_key {
            self.session_keys.insert(session_key.clone());
        }
        self.evidence_kinds.insert(candidate.kind.to_string());
        if direct_hit {
            self.direct_hit_count += 1;
        }
        if self.examples.len() < 3 && !self.examples.contains(&candidate.summary) {
            self.examples.push(candidate.summary.clone());
        }
    }

    fn into_aggregate(self) -> MemoryEvidenceAggregate {
        let mut evidence_kinds = self.evidence_kinds.into_iter().collect::<Vec<_>>();
        evidence_kinds.sort();
        let session_count = self.session_keys.len();
        let summary = if session_count > 1 {
            format!(
                "merged {} supporting hits across {} sessions: {}",
                self.support_count,
                session_count,
                self.examples.join(" | ")
            )
        } else if self.support_count > 1 {
            format!(
                "merged {} supporting hits: {}",
                self.support_count,
                self.examples.join(" | ")
            )
        } else {
            format!("supporting hit: {}", self.examples.join(" | "))
        };

        MemoryEvidenceAggregate {
            topic: self.topic,
            summary,
            support_count: self.support_count,
            session_count,
            evidence_kinds,
            examples: self.examples,
        }
    }
}

fn synthesize_memory_evidence(
    query: &str,
    plan: &MemoryRetrievalPlan,
    context: &db::RecallContext,
    capsules: &[db::MemorySessionCapsuleRecord],
    limit: usize,
) -> (Vec<MemoryEvidenceAggregate>, Vec<MemoryNegativeEvidence>) {
    let candidates = collect_memory_evidence_candidates(context, capsules);
    if candidates.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let query_text = if plan.expanded_query.trim().is_empty() {
        query
    } else {
        plan.expanded_query.as_str()
    };
    let query_semantic_tokens = semantic_token_set(query_text);
    let mut grouped = HashMap::<String, EvidenceAggregateAccumulator>::new();
    let mut direct_focus_hits = HashMap::<String, usize>::new();
    let mut semantic_focus_hits = HashMap::<String, usize>::new();

    for candidate in &candidates {
        let matched_terms = matched_focus_terms_for_candidate(candidate, plan);
        for term in &matched_terms {
            *direct_focus_hits.entry(term.clone()).or_insert(0) += 1;
        }
        for term in &plan.focus_terms {
            let normalized = normalize_semantic_token(term);
            if candidate.semantic_tokens.contains(&normalized) {
                *semantic_focus_hits.entry(term.clone()).or_insert(0) += 1;
            }
        }

        let semantic_hits = candidate
            .semantic_tokens
            .intersection(&query_semantic_tokens)
            .cloned()
            .collect::<Vec<_>>();
        let theme_tokens = derive_candidate_theme_tokens(&matched_terms, &semantic_hits);
        if theme_tokens.is_empty() {
            continue;
        }

        let topic = theme_label_from_tokens(&theme_tokens, &matched_terms);
        let key = theme_tokens.join("|");
        let aggregate = grouped
            .entry(key)
            .or_insert_with(|| EvidenceAggregateAccumulator::new(topic, theme_tokens));
        aggregate.register(candidate, !matched_terms.is_empty());
    }

    if grouped.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let mut ranked_aggregates = grouped.into_values().collect::<Vec<_>>();
    ranked_aggregates.sort_by(|left, right| {
        right
            .session_keys
            .len()
            .cmp(&left.session_keys.len())
            .then_with(|| right.support_count.cmp(&left.support_count))
            .then_with(|| right.direct_hit_count.cmp(&left.direct_hit_count))
            .then_with(|| left.topic.cmp(&right.topic))
    });

    let mut negative_evidence = derive_negative_evidence(
        plan,
        &ranked_aggregates,
        &direct_focus_hits,
        &semantic_focus_hits,
    );

    let aggregate_limit = limit.min(4).max(1);
    let aggregates = ranked_aggregates
        .into_iter()
        .take(aggregate_limit)
        .map(EvidenceAggregateAccumulator::into_aggregate)
        .collect::<Vec<_>>();

    negative_evidence.retain(|gap| {
        gap.related_topics
            .iter()
            .any(|topic| aggregates.iter().any(|aggregate| &aggregate.topic == topic))
    });
    negative_evidence.truncate(limit.min(3).max(1));

    (aggregates, negative_evidence)
}

fn collect_memory_evidence_candidates(
    context: &db::RecallContext,
    capsules: &[db::MemorySessionCapsuleRecord],
) -> Vec<WorkingSetEvidenceCandidate> {
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();

    for claim in &context.active_claims {
        push_evidence_candidate(
            &mut candidates,
            &mut seen,
            "claim",
            claim.statement.clone(),
            format!("claim: {}", crate::trunc(&claim.statement, 100)),
            None,
        );
    }
    for procedure in &context.procedures {
        push_evidence_candidate(
            &mut candidates,
            &mut seen,
            "procedure",
            format!("{} {}", procedure.title, procedure.content),
            format!("procedure: {}", crate::trunc(&procedure.content, 100)),
            None,
        );
    }
    for episode in &context.recent_episodes {
        push_evidence_candidate(
            &mut candidates,
            &mut seen,
            "episode",
            format!("{} {}", episode.summary, episode.content),
            format!("episode: {}", crate::trunc(&episode.summary, 100)),
            None,
        );
    }
    for capsule in capsules {
        let anchor_text = capsule
            .anchors
            .iter()
            .map(|anchor| anchor.content.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        push_evidence_candidate(
            &mut candidates,
            &mut seen,
            "capsule",
            format!("{} {}", capsule.summary, anchor_text),
            format!("capsule: {}", crate::trunc(&capsule.summary, 100)),
            Some(capsule.session_key.clone()),
        );
    }

    candidates
}

fn push_evidence_candidate(
    out: &mut Vec<WorkingSetEvidenceCandidate>,
    seen: &mut HashSet<String>,
    kind: &'static str,
    text: String,
    summary: String,
    session_key: Option<String>,
) {
    let normalized_key = format!("{}:{}", kind, text.to_lowercase());
    if !seen.insert(normalized_key) {
        return;
    }

    out.push(WorkingSetEvidenceCandidate {
        kind,
        lexical_tokens: super::planner::content_tokens(&text).into_iter().collect(),
        semantic_tokens: semantic_token_set(&text),
        text,
        summary,
        session_key,
    });
}

fn matched_focus_terms_for_candidate(
    candidate: &WorkingSetEvidenceCandidate,
    plan: &MemoryRetrievalPlan,
) -> Vec<String> {
    let lower = candidate.text.to_lowercase();
    plan.focus_terms
        .iter()
        .filter(|term| {
            lower.contains(term.as_str())
                || (candidate
                    .lexical_tokens
                    .contains(&normalize_semantic_token(term))
                    && is_morphological_variant(term, &normalize_semantic_token(term)))
        })
        .cloned()
        .collect()
}

fn is_morphological_variant(term: &str, normalized: &str) -> bool {
    if term == normalized {
        return false;
    }

    if term.starts_with(normalized) || normalized.starts_with(term) {
        return true;
    }

    for suffix in ["ing", "ed", "es", "s"] {
        if let Some(stem) = term.strip_suffix(suffix) {
            let stem = stem.trim_end_matches('e');
            if !stem.is_empty() && normalized.starts_with(stem) {
                return true;
            }
        }
    }

    false
}

fn derive_candidate_theme_tokens(
    matched_terms: &[String],
    semantic_hits: &[String],
) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut seen = HashSet::new();

    for term in matched_terms {
        let normalized = normalize_semantic_token(term);
        if seen.insert(normalized.clone()) {
            tokens.push(normalized);
        }
    }
    for term in semantic_hits {
        if seen.insert(term.clone()) {
            tokens.push(term.clone());
        }
    }

    tokens.sort_by(|left, right| {
        theme_token_priority(right)
            .cmp(&theme_token_priority(left))
            .then_with(|| left.cmp(right))
    });
    if tokens.len() > 1 {
        let filtered = tokens
            .iter()
            .filter(|token| !LOW_SIGNAL_THEME_TOKENS.contains(&token.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !filtered.is_empty() {
            tokens = filtered;
        }
    }
    tokens.truncate(3);
    tokens
}

fn theme_token_priority(token: &str) -> usize {
    match token {
        "doctor" => 100,
        "project" => 95,
        "sibling" => 92,
        "repair" | "refresh" => 90,
        "family" => 88,
        "accessory" => 86,
        "practice" => 85,
        "purchase" | "delivery" => 84,
        "commute" => 83,
        "instrument" | "music" => 82,
        "audio" => 80,
        "lead" | "ownership" => 78,
        "fish" | "aquarium" => 76,
        "history" | "science" => 74,
        "visit" | "appointment" => 70,
        "duration" => 48,
        "daily" => 42,
        _ => 20 + token.len(),
    }
}

fn theme_label_from_tokens(tokens: &[String], matched_terms: &[String]) -> String {
    let token_set = tokens.iter().map(String::as_str).collect::<HashSet<_>>();
    if token_set.contains("doctor")
        && (token_set.contains("visit") || token_set.contains("appointment"))
    {
        "medical visits".to_string()
    } else if token_set.contains("sibling") || token_set.contains("family") {
        "family relationships".to_string()
    } else if token_set.contains("commute")
        && (token_set.contains("audio")
            || token_set.contains("history")
            || token_set.contains("science"))
    {
        "commute audio preferences".to_string()
    } else if token_set.contains("accessory")
        && (token_set.contains("purchase") || token_set.contains("delivery"))
    {
        "accessory purchases".to_string()
    } else if token_set.contains("doctor") {
        "doctor history".to_string()
    } else if token_set.contains("project")
        && (token_set.contains("lead") || token_set.contains("ownership"))
    {
        "project ownership".to_string()
    } else if token_set.contains("garden")
        && (token_set.contains("produce")
            || token_set.contains("herb")
            || token_set.contains("tomato")
            || token_set.contains("recipe"))
    {
        "garden cooking".to_string()
    } else if token_set.contains("school")
        && (token_set.contains("memory") || token_set.contains("friend"))
    {
        "school memories".to_string()
    } else if token_set.contains("practice")
        && (token_set.contains("music") || token_set.contains("instrument"))
    {
        if token_set.contains("duration") || token_set.contains("daily") {
            "music practice routine".to_string()
        } else {
            "music practice".to_string()
        }
    } else if token_set.contains("fish") || token_set.contains("aquarium") {
        "aquarium care".to_string()
    } else if token_set.contains("refresh")
        || matched_terms
            .iter()
            .any(|term| matches!(term.as_str(), "refresh" | "maintenance" | "job" | "jobs"))
    {
        "refresh jobs".to_string()
    } else if let Some(term) = matched_terms.first() {
        format!("{} evidence", term)
    } else if let Some(token) = tokens.first() {
        format!("{} evidence", token.replace('_', " "))
    } else {
        "related evidence".to_string()
    }
}

fn derive_negative_evidence(
    plan: &MemoryRetrievalPlan,
    aggregates: &[EvidenceAggregateAccumulator],
    direct_focus_hits: &HashMap<String, usize>,
    semantic_focus_hits: &HashMap<String, usize>,
) -> Vec<MemoryNegativeEvidence> {
    let mut grouped_terms = HashMap::<String, Vec<String>>::new();

    for term in &plan.focus_terms {
        if !should_surface_negative_gap(term) {
            continue;
        }
        if direct_focus_hits.get(term).copied().unwrap_or(0) > 0 {
            continue;
        }
        if semantic_focus_hits.get(term).copied().unwrap_or(0) == 0 {
            continue;
        }
        grouped_terms
            .entry(normalize_semantic_token(term))
            .or_default()
            .push(term.clone());
    }

    let mut gaps = Vec::new();
    for (normalized, mut missing_terms) in grouped_terms {
        missing_terms.sort();
        missing_terms.dedup();

        let mut related = aggregates
            .iter()
            .filter(|aggregate| {
                aggregate.theme_tokens.contains(&normalized)
                    || aggregate.topic.to_lowercase().contains(&normalized)
            })
            .collect::<Vec<_>>();
        related.sort_by(|left, right| {
            right
                .session_keys
                .len()
                .cmp(&left.session_keys.len())
                .then_with(|| right.support_count.cmp(&left.support_count))
                .then_with(|| left.topic.cmp(&right.topic))
        });
        let related_topics = related
            .iter()
            .map(|aggregate| aggregate.topic.clone())
            .take(3)
            .collect::<Vec<_>>();
        if related_topics.is_empty() {
            continue;
        }

        gaps.push(MemoryNegativeEvidence {
            summary: format!(
                "No direct memory hit mentioned {}, but related {} evidence was recalled.",
                missing_terms.join(", "),
                related_topics[0]
            ),
            missing_terms,
            related_topics,
        });
    }

    gaps.sort_by(|left, right| {
        right
            .related_topics
            .len()
            .cmp(&left.related_topics.len())
            .then_with(|| {
                right
                    .missing_terms
                    .first()
                    .map(|term| term.len())
                    .unwrap_or(0)
                    .cmp(
                        &left
                            .missing_terms
                            .first()
                            .map(|term| term.len())
                            .unwrap_or(0),
                    )
            })
            .then_with(|| left.summary.cmp(&right.summary))
    });
    gaps
}

fn should_surface_negative_gap(term: &str) -> bool {
    term.len() >= 4 && !LOW_SIGNAL_NEGATIVE_TERMS.contains(&term)
}

pub(super) fn memory_surface_items(
    context: &db::RecallContext,
    capsules: &[db::MemorySessionCapsuleRecord],
    resources: &[MemoryResourceCard],
    aggregates: &[MemoryEvidenceAggregate],
    negative_evidence: &[MemoryNegativeEvidence],
    plan: &MemoryRetrievalPlan,
) -> Vec<MemorySurfaceItem> {
    let mut items = Vec::new();
    let mut seen = HashSet::new();
    let mut synthesis_items = Vec::new();
    let mut profile_items = Vec::new();
    let mut fact_items = Vec::new();
    let mut procedure_items = Vec::new();
    let mut capsule_items = Vec::new();
    let mut resource_items = Vec::new();
    let mut episode_items = Vec::new();
    let mut coverage_gap_items = Vec::new();
    let mut uncertainty_items = Vec::new();

    for fact in &context.profile {
        let display = format!("profile: {}", crate::trunc(fact, 120));
        if seen.insert(display.clone()) {
            profile_items.push(MemorySurfaceItem {
                section: "profile",
                display,
            });
        }
    }

    for claim in &context.active_claims {
        let display = if claim.is_stale {
            format!(
                "fact (possibly stale): {}",
                crate::trunc(&claim.statement, 120)
            )
        } else {
            format!("fact: {}", crate::trunc(&claim.statement, 120))
        };
        if seen.insert(display.clone()) {
            fact_items.push(MemorySurfaceItem {
                section: "facts",
                display,
            });
        }
    }

    for aggregate in aggregates {
        let display = format!(
            "synthesis: {}: {}",
            aggregate.topic,
            crate::trunc(&aggregate.summary, 160)
        );
        if seen.insert(display.clone()) {
            synthesis_items.push(MemorySurfaceItem {
                section: "evidence_synthesis",
                display,
            });
        }
    }

    for procedure in &context.procedures {
        let display = format!("procedure: {}", crate::trunc(&procedure.content, 120));
        if seen.insert(display.clone()) {
            procedure_items.push(MemorySurfaceItem {
                section: "procedures",
                display,
            });
        }
    }

    for episode in &context.recent_episodes {
        let display = format!("episode: {}", crate::trunc(&episode.summary, 120));
        if seen.insert(display.clone()) {
            episode_items.push(MemorySurfaceItem {
                section: "recent_events",
                display,
            });
        }
    }

    for capsule in capsules {
        let anchor_hint = capsule
            .anchors
            .iter()
            .take(2)
            .map(|anchor| format!("{}: {}", anchor.role, crate::trunc(&anchor.content, 80)))
            .collect::<Vec<_>>()
            .join(" | ");
        let display = if anchor_hint.is_empty() {
            format!("capsule: {}", crate::trunc(&capsule.summary, 120))
        } else {
            format!(
                "capsule: {} [{}]",
                crate::trunc(&capsule.summary, 100),
                anchor_hint
            )
        };
        if seen.insert(display.clone()) {
            capsule_items.push(MemorySurfaceItem {
                section: "capsules",
                display,
            });
        }
    }

    for resource in resources {
        let readiness = if resource.ready {
            resource.status.clone()
        } else {
            format!("{} (attention)", resource.status)
        };
        let handle = resource
            .handle
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!(" @ {}", crate::trunc(value, 72)))
            .unwrap_or_default();
        let display = format!(
            "resource: {} {} [{}] - {}{}",
            resource.kind,
            resource.name,
            readiness,
            crate::trunc(&resource.summary, 100),
            handle
        );
        if seen.insert(display.clone()) {
            resource_items.push(MemorySurfaceItem {
                section: "resources",
                display,
            });
        }
    }

    for uncertainty in &context.uncertainties {
        let display = format!("uncertainty: {}", crate::trunc(uncertainty, 120));
        if seen.insert(display.clone()) {
            uncertainty_items.push(MemorySurfaceItem {
                section: "uncertainties",
                display,
            });
        }
    }

    for gap in negative_evidence {
        let display = format!("gap: {}", crate::trunc(&gap.summary, 160));
        if seen.insert(display.clone()) {
            coverage_gap_items.push(MemorySurfaceItem {
                section: "coverage_gaps",
                display,
            });
        }
    }

    for &section in surface_section_order(plan) {
        match section {
            "evidence_synthesis" => items.extend(synthesis_items.iter().cloned()),
            "profile" => items.extend(profile_items.iter().cloned()),
            "facts" => items.extend(fact_items.iter().cloned()),
            "procedures" => items.extend(procedure_items.iter().cloned()),
            "resources" => items.extend(resource_items.iter().cloned()),
            "capsules" => items.extend(capsule_items.iter().cloned()),
            "recent_events" => items.extend(episode_items.iter().cloned()),
            "coverage_gaps" => items.extend(coverage_gap_items.iter().cloned()),
            "uncertainties" => items.extend(uncertainty_items.iter().cloned()),
            _ => {}
        }
    }

    items
}

pub(super) fn render_memory_surface(
    items: &[MemorySurfaceItem],
    plan: &MemoryRetrievalPlan,
) -> String {
    if items.is_empty() {
        return String::new();
    }

    let mut out = String::new();

    for &section in surface_section_order(plan) {
        let section_items: Vec<&MemorySurfaceItem> = items
            .iter()
            .filter(|item| item.section == section)
            .collect();
        if section_items.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(match section {
            "evidence_synthesis" => "Evidence Synthesis:\n",
            "profile" => "Profile:\n",
            "facts" => "Facts:\n",
            "procedures" => "Procedures:\n",
            "resources" => "Resources:\n",
            "capsules" => "Capsules:\n",
            "recent_events" => "Recent Events:\n",
            "coverage_gaps" => "Coverage Gaps:\n",
            "uncertainties" => "Uncertainties:\n",
            _ => "Memory:\n",
        });
        for item in section_items {
            out.push_str("- ");
            out.push_str(strip_surface_prefix(&item.display));
            out.push('\n');
        }
    }

    out.trim().to_string()
}

pub(super) fn strip_surface_prefix(display: &str) -> &str {
    for prefix in [
        "synthesis: ",
        "profile: ",
        "fact: ",
        "fact (possibly stale): ",
        "procedure: ",
        "resource: ",
        "capsule: ",
        "episode: ",
        "gap: ",
        "uncertainty: ",
    ] {
        if let Some(rest) = display.strip_prefix(prefix) {
            return rest;
        }
    }
    display
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/memory/working_set.rs"]
mod tests;
