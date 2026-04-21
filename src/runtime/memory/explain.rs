use super::planner::{memory_lane_label, memory_request_mode_label};
use super::*;

pub(super) fn build_memory_explanation(
    query: &str,
    plan: &MemoryRetrievalPlan,
    context: &db::RecallContext,
    capsules: &[db::MemorySessionCapsuleRecord],
    resources: &[MemoryResourceCard],
    aggregates: &[MemoryEvidenceAggregate],
    negative_evidence: &[MemoryNegativeEvidence],
    limit: usize,
) -> MemoryExplanationPacket {
    let focus_terms = if plan.focus_terms.is_empty() {
        "no strong focus terms were extracted".to_string()
    } else {
        format!(
            "focus terms: {}",
            plan.focus_terms
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let query_suffix = if query.trim().is_empty() {
        String::new()
    } else {
        format!(" for query '{}'", crate::trunc(query.trim(), 96))
    };

    let mut surfaced_items = Vec::new();
    let item_limit = limit.min(8).max(3);

    for claim in context.active_claims.iter().take(limit.min(3)) {
        surfaced_items.push(MemoryExplanationItem {
            kind: "claim".to_string(),
            label: crate::trunc(&claim.statement, 120).to_string(),
            reason: explain_claim_surface_reason(claim, plan),
        });
    }
    for aggregate in aggregates.iter().take(limit.min(2)) {
        if surfaced_items.len() >= item_limit {
            break;
        }
        surfaced_items.push(MemoryExplanationItem {
            kind: "aggregate".to_string(),
            label: crate::trunc(&aggregate.summary, 120).to_string(),
            reason: explain_aggregate_surface_reason(aggregate, plan),
        });
    }
    for procedure in context.procedures.iter().take(limit.min(2)) {
        if surfaced_items.len() >= item_limit {
            break;
        }
        surfaced_items.push(MemoryExplanationItem {
            kind: "procedure".to_string(),
            label: crate::trunc(&procedure.content, 120).to_string(),
            reason: explain_procedure_surface_reason(procedure, plan),
        });
    }
    for capsule in capsules.iter().take(limit.min(2)) {
        if surfaced_items.len() >= item_limit {
            break;
        }
        surfaced_items.push(MemoryExplanationItem {
            kind: "capsule".to_string(),
            label: crate::trunc(&capsule.summary, 120).to_string(),
            reason: explain_capsule_surface_reason(capsule, plan),
        });
    }
    for resource in resources.iter().take(limit.min(2)) {
        if surfaced_items.len() >= item_limit {
            break;
        }
        surfaced_items.push(MemoryExplanationItem {
            kind: "resource".to_string(),
            label: crate::trunc(&format!("{} {}", resource.kind, resource.name), 120).to_string(),
            reason: explain_resource_surface_reason(resource),
        });
    }
    for source in context.supporting_sources.iter().take(limit.min(2)) {
        if surfaced_items.len() >= item_limit {
            break;
        }
        surfaced_items.push(MemoryExplanationItem {
            kind: "source".to_string(),
            label: crate::trunc(
                source
                    .title
                    .as_deref()
                    .filter(|title| !title.trim().is_empty())
                    .unwrap_or(&source.url_or_ref),
                120,
            )
            .to_string(),
            reason: explain_source_surface_reason(source),
        });
    }
    for episode in context.recent_episodes.iter().take(limit.min(1)) {
        if surfaced_items.len() >= item_limit {
            break;
        }
        surfaced_items.push(MemoryExplanationItem {
            kind: "episode".to_string(),
            label: crate::trunc(&episode.summary, 120).to_string(),
            reason: explain_episode_surface_reason(episode, plan),
        });
    }
    for gap in negative_evidence.iter().take(1) {
        if surfaced_items.len() >= item_limit {
            break;
        }
        surfaced_items.push(MemoryExplanationItem {
            kind: "negative_evidence".to_string(),
            label: crate::trunc(&gap.summary, 120).to_string(),
            reason: explain_negative_evidence_surface_reason(gap),
        });
    }

    let synthesis_suffix = match (aggregates.len(), negative_evidence.len()) {
        (0, 0) => String::new(),
        (aggregate_count, 0) => format!("; synthesized {} evidence clusters", aggregate_count),
        (0, gap_count) => format!("; surfaced {} coverage gaps", gap_count),
        (aggregate_count, gap_count) => format!(
            "; synthesized {} evidence clusters and {} coverage gaps",
            aggregate_count, gap_count
        ),
    };

    MemoryExplanationPacket {
        lane_summary: format!(
            "Planner selected the {} lane in {} mode{}; {}{}.",
            memory_lane_label(&plan.lane),
            memory_request_mode_label(&plan.mode),
            query_suffix,
            focus_terms,
            synthesis_suffix,
        ),
        surfaced_items,
    }
}

pub(super) fn explain_resource_surface_reason(resource: &MemoryResourceCard) -> String {
    let mut reasons = vec![resource.reason.clone()];
    if resource.ready {
        reasons.push("currently ready".to_string());
    } else {
        reasons.push("currently blocked or degraded".to_string());
    }
    if let Some(handle) = resource
        .handle
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        reasons.push(format!("handle {}", crate::trunc(handle, 60)));
    }
    reasons.join("; ")
}

pub(super) fn explain_claim_surface_reason(
    claim: &db::MemoryClaimRecord,
    plan: &MemoryRetrievalPlan,
) -> String {
    let mut reasons = vec![format!(
        "matched {} lane scoring",
        memory_lane_label(&plan.lane)
    )];
    let matched = matched_focus_terms(&claim.statement, plan);
    if !matched.is_empty() {
        reasons.push(format!("matched focus terms {}", matched.join(", ")));
    }
    if claim.scope == "personal" || claim.kind == "preference" {
        reasons.push("tagged as personal or preference state".to_string());
    }
    if claim.source_id.is_some() {
        reasons.push("backed by a supporting source".to_string());
    }
    if claim.is_stale {
        reasons.push("kept visible because it may need refresh".to_string());
    }
    reasons.join("; ")
}

pub(super) fn explain_procedure_surface_reason(
    procedure: &db::MemoryProcedureRecord,
    plan: &MemoryRetrievalPlan,
) -> String {
    let mut reasons = vec![format!(
        "ranked for the {} lane",
        memory_lane_label(&plan.lane)
    )];
    let matched = matched_focus_terms(&procedure.content, plan);
    if !matched.is_empty() {
        reasons.push(format!("matched focus terms {}", matched.join(", ")));
    }
    reasons.push("kept as reusable action guidance".to_string());
    reasons.join("; ")
}

pub(super) fn explain_capsule_surface_reason(
    capsule: &db::MemorySessionCapsuleRecord,
    plan: &MemoryRetrievalPlan,
) -> String {
    let mut reasons = vec!["kept as a fast evidence window".to_string()];
    let matched = matched_focus_terms(&capsule.summary, plan);
    if !matched.is_empty() {
        reasons.push(format!(
            "summary matched focus terms {}",
            matched.join(", ")
        ));
    } else {
        let anchor_text = capsule
            .anchors
            .iter()
            .map(|anchor| anchor.content.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        let anchor_matches = matched_focus_terms(&anchor_text, plan);
        if !anchor_matches.is_empty() {
            reasons.push(format!(
                "anchors matched focus terms {}",
                anchor_matches.join(", ")
            ));
        }
    }
    reasons.push(format!(
        "planner can widen from capsules inside the {} lane",
        memory_lane_label(&plan.lane)
    ));
    reasons.join("; ")
}

pub(super) fn explain_source_surface_reason(source: &db::MemorySourceRecord) -> String {
    let mut reasons = vec!["retained because a surfaced claim cites this source".to_string()];
    reasons.push(format!("trust tier {:.2}", source.trust_tier));
    if let Some(publisher) = source
        .publisher
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        reasons.push(format!("publisher {}", publisher));
    }
    reasons.join("; ")
}

pub(super) fn explain_episode_surface_reason(
    episode: &db::MemoryEpisodeRecord,
    plan: &MemoryRetrievalPlan,
) -> String {
    let mut reasons = vec![format!(
        "recent event matched the {} lane",
        memory_lane_label(&plan.lane)
    )];
    let matched = matched_focus_terms(&episode.summary, plan);
    if !matched.is_empty() {
        reasons.push(format!(
            "summary matched focus terms {}",
            matched.join(", ")
        ));
    }
    reasons.join("; ")
}

pub(super) fn explain_aggregate_surface_reason(
    aggregate: &MemoryEvidenceAggregate,
    plan: &MemoryRetrievalPlan,
) -> String {
    let mut reasons = vec![format!(
        "synthesized for the {} lane",
        memory_lane_label(&plan.lane)
    )];
    if aggregate.session_count > 1 {
        reasons.push(format!(
            "merged evidence across {} sessions",
            aggregate.session_count
        ));
    }
    reasons.push(format!("{} supporting hits", aggregate.support_count));
    if !aggregate.evidence_kinds.is_empty() {
        reasons.push(format!("combined {}", aggregate.evidence_kinds.join(", ")));
    }
    reasons.join("; ")
}

pub(super) fn explain_negative_evidence_surface_reason(gap: &MemoryNegativeEvidence) -> String {
    let mut reasons = vec!["direct lexical evidence was not found".to_string()];
    if !gap.missing_terms.is_empty() {
        reasons.push(format!(
            "missing direct hits for {}",
            gap.missing_terms.join(", ")
        ));
    }
    if !gap.related_topics.is_empty() {
        reasons.push(format!(
            "related evidence surfaced under {}",
            gap.related_topics.join(", ")
        ));
    }
    reasons.join("; ")
}

pub(super) fn matched_focus_terms(text: &str, plan: &MemoryRetrievalPlan) -> Vec<String> {
    let lower = text.to_lowercase();
    plan.focus_terms
        .iter()
        .filter(|term| lower.contains(term.as_str()))
        .take(3)
        .cloned()
        .collect()
}
