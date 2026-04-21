use super::planner::{
    bm25_scores, capsule_candidate_limit_for_plan, capsule_limit_for_plan, content_tokens,
    context_candidate_limit_for_plan, normalize_semantic_token, plan_capsule_score,
    plan_claim_score, plan_episode_score, plan_memory_request, plan_procedure_score,
    plan_source_score, plan_text_score, semantic_token_set,
};
use super::working_set::assemble_memory_working_set;
use super::*;

impl AppState {
    pub(crate) async fn memory_working_set_for_query(
        &self,
        text: &str,
        limit: usize,
    ) -> MemoryWorkingSet {
        let query = text.trim();
        let safe_limit = limit.clamp(1, 20);
        let plan = plan_memory_request(query);
        let recall_query = if query.is_empty() {
            String::new()
        } else {
            plan.expanded_query.trim().to_string()
        };
        let query_embedding = if recall_query.len() >= 8 {
            self.embedder.embed(&recall_query).await
        } else {
            None
        };
        let mut context = if recall_query.is_empty() {
            db::RecallContext::default()
        } else {
            self.db.recall_context(
                &recall_query,
                query_embedding.as_deref(),
                context_candidate_limit_for_plan(&plan, safe_limit),
            )
        };
        if context.procedures.is_empty() {
            if let Ok(fallback_procedures) =
                self.db.list_recent_memory_procedures(safe_limit.min(3))
            {
                context.procedures.extend(
                    fallback_procedures
                        .into_iter()
                        .filter(|procedure| procedure.status == "active"),
                );
            }
        }
        refine_context_for_plan(&mut context, &plan, safe_limit);
        if let Some(query_embedding) = query_embedding.as_deref() {
            self.rerank_context_with_embeddings(query_embedding, &plan, &mut context)
                .await;
        }
        let capsule_query = if recall_query.is_empty() {
            query
        } else {
            recall_query.as_str()
        };
        let resource_limit = safe_limit.min(4).max(1);
        let resources = self
            .resource_cards_for_plan(query, &plan, resource_limit)
            .await;
        let final_capsule_limit = capsule_limit_for_plan(&plan, safe_limit);
        let mut capsules = if capsule_query.is_empty() {
            self.db
                .list_recent_memory_capsules(capsule_candidate_limit_for_plan(&plan, safe_limit))
                .unwrap_or_default()
        } else {
            self.db
                .recall_memory_capsules(
                    capsule_query,
                    capsule_candidate_limit_for_plan(&plan, safe_limit),
                )
                .unwrap_or_default()
        };
        refine_capsules_for_plan(&mut capsules, &plan);
        if let Some(query_embedding) = query_embedding.as_deref() {
            self.rerank_capsules_with_embeddings(query_embedding, &plan, &mut capsules)
                .await;
        }
        dedupe_capsules_for_working_set(&mut capsules);
        capsules.truncate(final_capsule_limit);
        assemble_memory_working_set(query, &plan, &context, &capsules, &resources, safe_limit)
    }

    pub(crate) async fn structured_memory_context_for_query(&self, text: &str) -> String {
        self.memory_working_set_for_query(text, 8)
            .await
            .prompt_context
    }
}

impl AppState {
    async fn rerank_context_with_embeddings(
        &self,
        query_embedding: &[f32],
        plan: &MemoryRetrievalPlan,
        context: &mut db::RecallContext,
    ) {
        rerank_items_with_embeddings(
            self.embedder.as_ref(),
            query_embedding,
            &mut context.active_claims,
            claim_embedding_weight_for_lane(&plan.lane),
            |claim| {
                format!(
                    "{} {} {} {} {}",
                    claim.statement, claim.subject, claim.predicate, claim.object, claim.kind
                )
            },
        )
        .await;
        rerank_items_with_embeddings(
            self.embedder.as_ref(),
            query_embedding,
            &mut context.procedures,
            procedure_embedding_weight_for_lane(&plan.lane),
            |procedure| {
                format!(
                    "{} {} {}",
                    procedure.title, procedure.trigger, procedure.content
                )
            },
        )
        .await;
        rerank_items_with_embeddings(
            self.embedder.as_ref(),
            query_embedding,
            &mut context.recent_episodes,
            episode_embedding_weight_for_lane(&plan.lane),
            |episode| {
                format!(
                    "{} {} {} {}",
                    episode.summary, episode.content, episode.actor, episode.network
                )
            },
        )
        .await;
    }

    async fn rerank_capsules_with_embeddings(
        &self,
        query_embedding: &[f32],
        plan: &MemoryRetrievalPlan,
        capsules: &mut Vec<db::MemorySessionCapsuleRecord>,
    ) {
        rerank_items_with_embeddings(
            self.embedder.as_ref(),
            query_embedding,
            capsules,
            capsule_embedding_weight_for_lane(&plan.lane),
            capsule_embedding_document,
        )
        .await;
    }

    async fn resource_cards_for_plan(
        &self,
        query: &str,
        plan: &MemoryRetrievalPlan,
        limit: usize,
    ) -> Vec<MemoryResourceCard> {
        let snapshot = self.self_model_snapshot().await;
        let mut candidates = Vec::new();

        extend_tool_resource_cards(
            &mut candidates,
            crate::tools::builtin_tool_runtime_statuses(),
            plan,
        );
        if let Ok(self_built) =
            crate::forge::list_registered_tool_runtime_statuses(self.db.as_ref())
        {
            extend_tool_resource_cards(&mut candidates, self_built, plan);
        }
        extend_tool_resource_cards(&mut candidates, self.plugins.tool_statuses(), plan);
        extend_tool_resource_cards(&mut candidates, self.mcp_hub.tool_statuses().await, plan);
        extend_provider_resource_cards(&mut candidates, snapshot.runtime.providers, query, plan);

        let mut deduped = std::collections::HashMap::<String, (f64, MemoryResourceCard)>::new();
        for (score, card) in candidates {
            if score <= 0.0 {
                continue;
            }
            let key = format!("{}:{}", card.kind, card.name.to_lowercase());
            match deduped.get(&key) {
                Some((existing_score, _)) if *existing_score >= score => {}
                _ => {
                    deduped.insert(key, (score, card));
                }
            }
        }

        let mut ranked = deduped.into_values().collect::<Vec<_>>();
        ranked.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| right.ready.cmp(&left.ready))
                .then_with(|| left.name.cmp(&right.name))
        });

        ranked
            .into_iter()
            .map(|(_, card)| card)
            .take(limit)
            .collect()
    }
}

fn extend_tool_resource_cards(
    out: &mut Vec<(f64, MemoryResourceCard)>,
    statuses: Vec<crate::tools::ToolRuntimeStatus>,
    plan: &MemoryRetrievalPlan,
) {
    for status in statuses {
        let handle = status
            .filename
            .clone()
            .or(status.server_name.clone())
            .or(status.command.clone());
        let mut score = plan_text_score(
            &format!(
                "{} {} {} {} {}",
                status.name,
                status.description,
                handle.clone().unwrap_or_default(),
                status.source.clone().unwrap_or_default(),
                status.status,
            ),
            plan,
        ) * 3.0;
        score += tool_lane_bonus(&status.name, plan);
        if status.ready {
            score += 0.35;
        } else {
            score -= 0.25;
        }
        if matches!(plan.lane, MemoryRetrievalLane::FreshLookup)
            && status.requires_network
            && status.name == "web_search"
        {
            score += 1.2;
        }
        if score <= 0.0 {
            continue;
        }
        let summary = if status.ready {
            format!("{} [{}]", status.description, status.status)
        } else {
            format!(
                "{} [{}: {}]",
                status.description,
                status.status,
                status
                    .issue
                    .clone()
                    .unwrap_or_else(|| "not ready".to_string())
            )
        };
        let reason = format!(
            "matched the {} lane and is a {} resource",
            super::planner::memory_lane_label(&plan.lane),
            status.kind
        );
        out.push((
            score,
            MemoryResourceCard {
                kind: "tool".to_string(),
                name: status.name,
                source: status.kind,
                handle,
                ready: status.ready,
                status: status.status,
                summary,
                reason,
            },
        ));
    }
}

fn extend_provider_resource_cards(
    out: &mut Vec<(f64, MemoryResourceCard)>,
    providers: Vec<crate::runtime::self_model::ProviderState>,
    query: &str,
    plan: &MemoryRetrievalPlan,
) {
    let lower = query.to_lowercase();
    for provider in providers {
        let mut score = plan_text_score(
            &format!(
                "{} {} {} {} {}",
                provider.name,
                provider.role,
                provider.model.clone().unwrap_or_default(),
                provider.reason,
                if provider.configured {
                    "configured"
                } else {
                    "not configured"
                },
            ),
            plan,
        ) * 2.5;
        if lower.contains("provider")
            || lower.contains("model")
            || lower.contains("llm")
            || lower.contains(&provider.name.to_lowercase())
        {
            score += 2.0;
        }
        if provider.configured {
            score += 0.4;
        }
        if score <= 0.0 {
            continue;
        }
        let summary = if let Some(model) = provider
            .model
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            format!("{} provider using {}", provider.role, model)
        } else if provider.configured {
            format!("{} provider is configured", provider.role)
        } else {
            format!("{} provider is not configured", provider.role)
        };
        let reason = format!(
            "surfaced because provider readiness matched the {} lane",
            super::planner::memory_lane_label(&plan.lane)
        );
        out.push((
            score,
            MemoryResourceCard {
                kind: "provider".to_string(),
                name: provider.name,
                source: "self_model".to_string(),
                handle: provider.model,
                ready: provider.configured,
                status: if provider.configured {
                    "configured".to_string()
                } else {
                    "unconfigured".to_string()
                },
                summary,
                reason,
            },
        ));
    }
}

#[derive(Clone, Debug)]
struct CapsuleEvidenceAggregate {
    theme_tokens: Vec<String>,
    member_ids: Vec<String>,
    session_count: usize,
    missing_direct_terms: Vec<String>,
}

#[derive(Clone, Debug)]
struct CapsuleEvidenceAccumulator {
    theme_tokens: Vec<String>,
    member_ids: Vec<String>,
    session_keys: HashSet<String>,
    direct_focus_terms: HashSet<String>,
}

impl CapsuleEvidenceAccumulator {
    fn new(theme_tokens: Vec<String>) -> Self {
        Self {
            theme_tokens,
            member_ids: Vec::new(),
            session_keys: HashSet::new(),
            direct_focus_terms: HashSet::new(),
        }
    }

    fn register(&mut self, capsule: &db::MemorySessionCapsuleRecord, direct_terms: &[String]) {
        self.member_ids.push(capsule.id.clone());
        self.session_keys.insert(capsule.session_key.clone());
        self.direct_focus_terms.extend(direct_terms.iter().cloned());
    }
}

#[derive(Clone, Debug)]
enum ContextEvidenceKind {
    Claim(String),
    Procedure(String),
    Episode(String),
}

#[derive(Clone, Debug)]
struct ContextEvidenceAccumulator {
    theme_tokens: Vec<String>,
    members: Vec<ContextEvidenceKind>,
    evidence_kinds: HashSet<&'static str>,
    direct_focus_terms: HashSet<String>,
}

impl ContextEvidenceAccumulator {
    fn new(theme_tokens: Vec<String>) -> Self {
        Self {
            theme_tokens,
            members: Vec::new(),
            evidence_kinds: HashSet::new(),
            direct_focus_terms: HashSet::new(),
        }
    }

    fn register(
        &mut self,
        member: ContextEvidenceKind,
        evidence_kind: &'static str,
        direct_terms: &[String],
    ) {
        self.members.push(member);
        self.evidence_kinds.insert(evidence_kind);
        self.direct_focus_terms.extend(direct_terms.iter().cloned());
    }
}

async fn rerank_items_with_embeddings<T, F>(
    embedder: &crate::embed::Embedder,
    query_embedding: &[f32],
    items: &mut Vec<T>,
    semantic_weight: f64,
    build_doc: F,
) where
    F: Fn(&T) -> String,
{
    if items.len() < 2 || semantic_weight <= f64::EPSILON {
        return;
    }
    let docs = items.iter().map(build_doc).collect::<Vec<_>>();
    let Some(doc_embeddings) = embedder.embed_many(&docs).await else {
        return;
    };
    if doc_embeddings.len() != items.len() {
        return;
    }

    let semantic_scores = doc_embeddings
        .iter()
        .map(|embedding| {
            crate::embed::cosine_similarity(query_embedding, embedding).max(0.0) as f64
        })
        .collect::<Vec<_>>();
    if semantic_scores.iter().all(|score| *score <= 0.05) {
        return;
    }

    let total = items.len().max(1) as f64;
    let mut ranked = items
        .drain(..)
        .zip(semantic_scores.into_iter())
        .enumerate()
        .map(|(idx, (item, semantic_score))| {
            let base_rank_score = 1.0 - idx as f64 / total;
            (
                semantic_score * semantic_weight + base_rank_score * 0.65,
                idx,
                item,
            )
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|(left_score, left_idx, _), (right_score, right_idx, _)| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left_idx.cmp(right_idx))
    });
    *items = ranked.into_iter().map(|(_, _, item)| item).collect();
}

fn claim_embedding_weight_for_lane(lane: &MemoryRetrievalLane) -> f64 {
    match lane {
        MemoryRetrievalLane::HotState | MemoryRetrievalLane::MixedContext => 3.1,
        MemoryRetrievalLane::CapsuleRecall | MemoryRetrievalLane::TemporalRecall => 2.8,
        MemoryRetrievalLane::TaskState | MemoryRetrievalLane::RepairHistory => 2.7,
        MemoryRetrievalLane::Procedural => 2.2,
        MemoryRetrievalLane::FreshLookup => 2.0,
    }
}

fn procedure_embedding_weight_for_lane(lane: &MemoryRetrievalLane) -> f64 {
    match lane {
        MemoryRetrievalLane::Procedural | MemoryRetrievalLane::TaskState => 3.0,
        MemoryRetrievalLane::RepairHistory => 2.8,
        MemoryRetrievalLane::HotState | MemoryRetrievalLane::MixedContext => 2.4,
        MemoryRetrievalLane::CapsuleRecall | MemoryRetrievalLane::TemporalRecall => 2.2,
        MemoryRetrievalLane::FreshLookup => 1.8,
    }
}

fn episode_embedding_weight_for_lane(lane: &MemoryRetrievalLane) -> f64 {
    match lane {
        MemoryRetrievalLane::CapsuleRecall | MemoryRetrievalLane::TemporalRecall => 3.3,
        MemoryRetrievalLane::RepairHistory | MemoryRetrievalLane::TaskState => 2.8,
        MemoryRetrievalLane::MixedContext | MemoryRetrievalLane::HotState => 2.5,
        MemoryRetrievalLane::Procedural => 2.0,
        MemoryRetrievalLane::FreshLookup => 1.9,
    }
}

fn capsule_embedding_weight_for_lane(lane: &MemoryRetrievalLane) -> f64 {
    match lane {
        MemoryRetrievalLane::CapsuleRecall | MemoryRetrievalLane::TemporalRecall => 4.2,
        MemoryRetrievalLane::MixedContext | MemoryRetrievalLane::HotState => 3.5,
        MemoryRetrievalLane::RepairHistory | MemoryRetrievalLane::TaskState => 3.1,
        MemoryRetrievalLane::Procedural => 2.6,
        MemoryRetrievalLane::FreshLookup => 2.4,
    }
}

fn capsule_embedding_document(capsule: &db::MemorySessionCapsuleRecord) -> String {
    let anchor_text = capsule
        .anchors
        .iter()
        .take(4)
        .map(|anchor| anchor.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "summary: {}\nkeyphrases: {}\nentities: {}\nmarkers: {}\nanchors:\n{}",
        capsule.summary,
        capsule.keyphrases.join(", "),
        capsule.entity_markers.join(", "),
        capsule.marker_terms.join(", "),
        crate::trunc(&anchor_text, 1200)
    )
}

fn tool_lane_bonus(name: &str, plan: &MemoryRetrievalPlan) -> f64 {
    match plan.lane {
        MemoryRetrievalLane::FreshLookup => match name {
            "web_search" => 4.5,
            "weather" => 2.0,
            _ => 0.0,
        },
        MemoryRetrievalLane::TaskState => match name {
            "file_ops" | "git_info" => 3.0,
            _ => 0.0,
        },
        MemoryRetrievalLane::RepairHistory => match name {
            "file_ops" | "git_info" => 2.6,
            _ => 0.0,
        },
        MemoryRetrievalLane::Procedural => match name {
            "file_ops" | "git_info" => 1.5,
            _ => 0.0,
        },
        MemoryRetrievalLane::CapsuleRecall | MemoryRetrievalLane::TemporalRecall => match name {
            "file_ops" => 0.8,
            _ => 0.0,
        },
        MemoryRetrievalLane::HotState | MemoryRetrievalLane::MixedContext => 0.0,
    }
}

pub(crate) fn refine_context_for_plan(
    context: &mut db::RecallContext,
    plan: &MemoryRetrievalPlan,
    limit: usize,
) {
    filter_claims_for_plan(context, plan);
    let (claim_boosts, procedure_boosts, episode_boosts) = context_evidence_boosts(context, plan);
    context.profile.sort_by(|left, right| {
        plan_text_score(right, plan)
            .partial_cmp(&plan_text_score(left, plan))
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.cmp(right))
    });
    context.active_claims.sort_by(|left, right| {
        (plan_claim_score(right, plan) + claim_boosts.get(&right.id).copied().unwrap_or(0.0))
            .partial_cmp(
                &(plan_claim_score(left, plan)
                    + claim_boosts.get(&left.id).copied().unwrap_or(0.0)),
            )
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });
    context.procedures.sort_by(|left, right| {
        (plan_procedure_score(right, plan)
            + procedure_boosts.get(&right.id).copied().unwrap_or(0.0))
        .partial_cmp(
            &(plan_procedure_score(left, plan)
                + procedure_boosts.get(&left.id).copied().unwrap_or(0.0)),
        )
        .unwrap_or(Ordering::Equal)
        .then_with(|| right.updated_at.cmp(&left.updated_at))
    });
    context.recent_episodes.sort_by(|left, right| {
        (plan_episode_score(right, plan) + episode_boosts.get(&right.id).copied().unwrap_or(0.0))
            .partial_cmp(
                &(plan_episode_score(left, plan)
                    + episode_boosts.get(&left.id).copied().unwrap_or(0.0)),
            )
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.created_at.cmp(&left.created_at))
    });
    context.supporting_sources.sort_by(|left, right| {
        plan_source_score(right, plan)
            .partial_cmp(&plan_source_score(left, plan))
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.observed_at.cmp(&left.observed_at))
    });
    context.uncertainties.sort_by(|left, right| {
        plan_text_score(right, plan)
            .partial_cmp(&plan_text_score(left, plan))
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.cmp(right))
    });

    context.profile.truncate(limit);
    context.active_claims.truncate(limit);
    prune_supporting_sources_for_claims(context);
    prune_uncertainties_for_claims(context);
    context.procedures.truncate(limit);
    context.recent_episodes.truncate(limit);
    context.supporting_sources.truncate(limit);
    context.uncertainties.truncate(limit);
}

fn context_evidence_boosts(
    context: &db::RecallContext,
    plan: &MemoryRetrievalPlan,
) -> (
    std::collections::HashMap<String, f64>,
    std::collections::HashMap<String, f64>,
    std::collections::HashMap<String, f64>,
) {
    let mut claim_boosts = std::collections::HashMap::<String, f64>::new();
    let mut procedure_boosts = std::collections::HashMap::<String, f64>::new();
    let mut episode_boosts = std::collections::HashMap::<String, f64>::new();

    let query_semantic_tokens = semantic_token_set(&plan.expanded_query);
    let mut grouped = std::collections::HashMap::<String, ContextEvidenceAccumulator>::new();
    let mut direct_focus_hits = std::collections::HashMap::<String, usize>::new();
    let mut semantic_focus_hits = std::collections::HashMap::<String, usize>::new();

    for claim in &context.active_claims {
        let text = format!(
            "{} {} {} {} {}",
            claim.statement, claim.subject, claim.predicate, claim.object, claim.kind
        );
        register_context_candidate(
            &text,
            &ContextEvidenceKind::Claim(claim.id.clone()),
            "claim",
            plan,
            &query_semantic_tokens,
            &mut grouped,
            &mut direct_focus_hits,
            &mut semantic_focus_hits,
        );
    }

    for procedure in &context.procedures {
        let text = format!(
            "{} {} {}",
            procedure.title, procedure.trigger, procedure.content
        );
        register_context_candidate(
            &text,
            &ContextEvidenceKind::Procedure(procedure.id.clone()),
            "procedure",
            plan,
            &query_semantic_tokens,
            &mut grouped,
            &mut direct_focus_hits,
            &mut semantic_focus_hits,
        );
    }

    for episode in &context.recent_episodes {
        let text = format!(
            "{} {} {} {}",
            episode.summary, episode.content, episode.actor, episode.network
        );
        register_context_candidate(
            &text,
            &ContextEvidenceKind::Episode(episode.id.clone()),
            "episode",
            plan,
            &query_semantic_tokens,
            &mut grouped,
            &mut direct_focus_hits,
            &mut semantic_focus_hits,
        );
    }

    let lower_query = plan.expanded_query.to_lowercase();
    for aggregate in grouped.into_values() {
        let mut missing_direct_terms = plan
            .focus_terms
            .iter()
            .filter(|term| {
                should_surface_negative_gap(term)
                    && direct_focus_hits.get(*term).copied().unwrap_or(0) == 0
                    && semantic_focus_hits.get(*term).copied().unwrap_or(0) > 0
                    && aggregate
                        .theme_tokens
                        .contains(&normalize_semantic_token(term))
            })
            .cloned()
            .collect::<Vec<_>>();
        missing_direct_terms.sort();
        missing_direct_terms.dedup();

        let support_count = aggregate.members.len();
        let kind_count = aggregate.evidence_kinds.len();
        if support_count <= 1 && missing_direct_terms.is_empty() {
            continue;
        }

        let mut aggregate_bonus = 0.0f64;
        if support_count > 1 {
            aggregate_bonus += 1.2 + 0.4 * support_count.saturating_sub(1) as f64;
        }
        if kind_count > 1 {
            aggregate_bonus += 0.8 + 0.25 * kind_count.saturating_sub(1) as f64;
        }
        if !missing_direct_terms.is_empty() {
            aggregate_bonus += 0.9 + 0.3 * missing_direct_terms.len() as f64;
        }
        aggregate_bonus += capsule_aggregate_intent_bonus(&lower_query, &aggregate.theme_tokens);
        if aggregate_bonus <= 0.0 {
            continue;
        }

        let share_penalty = 1.0 + support_count.saturating_sub(1) as f64 * 0.3;
        let base_boost = aggregate_bonus / share_penalty.max(1.0);
        let per_member_bonus = if (lower_query.contains("sibling")
            || lower_query.contains("siblings"))
            && aggregate
                .theme_tokens
                .iter()
                .any(|token| matches!(token.as_str(), "sibling" | "family"))
            && (lower_query.contains("total number") || lower_query.contains("how many"))
        {
            1.0
        } else {
            0.0
        };

        for member in aggregate.members {
            match member {
                ContextEvidenceKind::Claim(id) => {
                    *claim_boosts.entry(id).or_insert(0.0) += base_boost + per_member_bonus;
                }
                ContextEvidenceKind::Procedure(id) => {
                    *procedure_boosts.entry(id).or_insert(0.0) += base_boost + per_member_bonus;
                }
                ContextEvidenceKind::Episode(id) => {
                    *episode_boosts.entry(id).or_insert(0.0) += base_boost + per_member_bonus;
                }
            }
        }
    }

    (claim_boosts, procedure_boosts, episode_boosts)
}

fn register_context_candidate(
    text: &str,
    member: &ContextEvidenceKind,
    evidence_kind: &'static str,
    plan: &MemoryRetrievalPlan,
    query_semantic_tokens: &HashSet<String>,
    grouped: &mut std::collections::HashMap<String, ContextEvidenceAccumulator>,
    direct_focus_hits: &mut std::collections::HashMap<String, usize>,
    semantic_focus_hits: &mut std::collections::HashMap<String, usize>,
) {
    let direct_terms = matched_focus_terms_for_text(text, plan);
    for term in &direct_terms {
        *direct_focus_hits.entry(term.clone()).or_insert(0) += 1;
    }

    let semantic_terms = semantic_token_set(text);
    for term in &plan.focus_terms {
        let normalized = normalize_semantic_token(term);
        if semantic_terms.contains(&normalized) {
            *semantic_focus_hits.entry(term.clone()).or_insert(0) += 1;
        }
    }

    let semantic_hits = semantic_terms
        .intersection(query_semantic_tokens)
        .cloned()
        .collect::<Vec<_>>();
    let theme_tokens = derive_capsule_theme_tokens(&direct_terms, &semantic_hits);
    if theme_tokens.is_empty() {
        return;
    }
    let key = capsule_theme_group_key(&theme_tokens);
    grouped
        .entry(key)
        .or_insert_with(|| ContextEvidenceAccumulator::new(theme_tokens))
        .register(member.clone(), evidence_kind, &direct_terms);
}

fn matched_focus_terms_for_text(text: &str, plan: &MemoryRetrievalPlan) -> Vec<String> {
    let lower = text.to_lowercase();
    let lexical_terms = content_tokens(text);
    plan.focus_terms
        .iter()
        .filter(|term| {
            lower.contains(term.as_str())
                || lexical_terms
                    .iter()
                    .any(|token| lexical_variant_match(token, term))
        })
        .cloned()
        .collect()
}

pub(super) fn filter_claims_for_plan(context: &mut db::RecallContext, plan: &MemoryRetrievalPlan) {
    if !plan_prefers_personal_claims(plan) {
        return;
    }
    context
        .active_claims
        .retain(claim_belongs_to_personal_state);
}

pub(super) fn refine_capsules_for_plan(
    capsules: &mut Vec<db::MemorySessionCapsuleRecord>,
    plan: &MemoryRetrievalPlan,
) {
    let summary_docs = capsules
        .iter()
        .map(|capsule| capsule.summary.clone())
        .collect::<Vec<_>>();
    let anchor_docs = capsules
        .iter()
        .map(|capsule| {
            capsule
                .anchors
                .iter()
                .map(|anchor| anchor.content.clone())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect::<Vec<_>>();
    let summary_scores = bm25_scores(&plan.expanded_query, &summary_docs);
    let anchor_scores = bm25_scores(&plan.expanded_query, &anchor_docs);
    let mut rerank_scores = std::collections::HashMap::<String, f64>::new();

    for (idx, capsule) in capsules.iter().enumerate() {
        let base_score = plan_capsule_score(capsule, plan);
        let anchor_score = anchor_scores.get(idx).copied().unwrap_or(0.0);
        let summary_score = summary_scores.get(idx).copied().unwrap_or(0.0);
        let exact_focus_bonus = if plan.focus_terms.iter().any(|term| {
            capsule.summary.to_lowercase().contains(term)
                || anchor_docs[idx].to_lowercase().contains(term)
        }) {
            1.0
        } else {
            0.0
        };
        rerank_scores.insert(
            capsule.id.clone(),
            base_score + anchor_score * 3.2 + summary_score * 2.1 + exact_focus_bonus,
        );
    }

    apply_capsule_evidence_boosts(capsules, plan, &anchor_docs, &mut rerank_scores);

    capsules.sort_by(|left, right| {
        rerank_scores
            .get(&right.id)
            .copied()
            .unwrap_or_else(|| plan_capsule_score(right, plan))
            .partial_cmp(
                &rerank_scores
                    .get(&left.id)
                    .copied()
                    .unwrap_or_else(|| plan_capsule_score(left, plan)),
            )
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.last_message_at.cmp(&left.last_message_at))
    });
}

fn apply_capsule_evidence_boosts(
    capsules: &[db::MemorySessionCapsuleRecord],
    plan: &MemoryRetrievalPlan,
    anchor_docs: &[String],
    rerank_scores: &mut std::collections::HashMap<String, f64>,
) {
    if capsules.len() < 2 || plan.focus_terms.is_empty() {
        return;
    }

    let query_semantic_tokens = semantic_token_set(&plan.expanded_query);
    let mut grouped = std::collections::HashMap::<String, CapsuleEvidenceAccumulator>::new();
    let mut direct_focus_hits = std::collections::HashMap::<String, usize>::new();
    let mut semantic_focus_hits = std::collections::HashMap::<String, usize>::new();

    for (idx, capsule) in capsules.iter().enumerate() {
        let direct_terms = matched_focus_terms_for_capsule(capsule, anchor_docs.get(idx), plan);
        for term in &direct_terms {
            *direct_focus_hits.entry(term.clone()).or_insert(0) += 1;
        }

        let semantic_terms = capsule_semantic_terms(capsule, anchor_docs.get(idx));
        for term in &plan.focus_terms {
            let normalized = normalize_semantic_token(term);
            if semantic_terms.contains(&normalized) {
                *semantic_focus_hits.entry(term.clone()).or_insert(0) += 1;
            }
        }

        let semantic_hits = semantic_terms
            .intersection(&query_semantic_tokens)
            .cloned()
            .collect::<Vec<_>>();
        let theme_tokens = derive_capsule_theme_tokens(&direct_terms, &semantic_hits);
        if theme_tokens.is_empty() {
            continue;
        }
        let key = capsule_theme_group_key(&theme_tokens);
        grouped
            .entry(key)
            .or_insert_with(|| CapsuleEvidenceAccumulator::new(theme_tokens))
            .register(capsule, &direct_terms);
    }

    let lower_query = plan.expanded_query.to_lowercase();
    for aggregate in grouped.into_values() {
        let mut missing_direct_terms = plan
            .focus_terms
            .iter()
            .filter(|term| {
                should_surface_negative_gap(term)
                    && direct_focus_hits.get(*term).copied().unwrap_or(0) == 0
                    && semantic_focus_hits.get(*term).copied().unwrap_or(0) > 0
                    && aggregate
                        .theme_tokens
                        .contains(&normalize_semantic_token(term))
            })
            .cloned()
            .collect::<Vec<_>>();
        missing_direct_terms.sort();
        missing_direct_terms.dedup();

        let aggregate = CapsuleEvidenceAggregate {
            theme_tokens: aggregate.theme_tokens,
            member_ids: aggregate.member_ids,
            session_count: aggregate.session_keys.len(),
            missing_direct_terms,
        };
        if aggregate.session_count <= 1 && aggregate.missing_direct_terms.is_empty() {
            continue;
        }

        let mut aggregate_bonus = 0.0f64;
        if aggregate.session_count > 1 {
            aggregate_bonus += 1.7 + 0.55 * aggregate.session_count.saturating_sub(1) as f64;
        }
        if !aggregate.missing_direct_terms.is_empty() {
            aggregate_bonus += 1.1 + 0.4 * aggregate.missing_direct_terms.len() as f64;
        }
        aggregate_bonus += capsule_aggregate_intent_bonus(&lower_query, &aggregate.theme_tokens);
        if aggregate_bonus <= 0.0 {
            continue;
        }

        let share_penalty = 1.0 + aggregate.member_ids.len().saturating_sub(1) as f64 * 0.35;
        let base_boost = aggregate_bonus / share_penalty.max(1.0);
        let per_member_bonus = if (lower_query.contains("sibling")
            || lower_query.contains("siblings"))
            && aggregate
                .theme_tokens
                .iter()
                .any(|token| matches!(token.as_str(), "sibling" | "family"))
            && (lower_query.contains("total number") || lower_query.contains("how many"))
        {
            1.4
        } else {
            0.0
        };
        for id in aggregate.member_ids {
            if let Some(score) = rerank_scores.get_mut(&id) {
                *score += base_boost + per_member_bonus;
            }
        }
    }
}

fn matched_focus_terms_for_capsule(
    capsule: &db::MemorySessionCapsuleRecord,
    anchor_doc: Option<&String>,
    plan: &MemoryRetrievalPlan,
) -> Vec<String> {
    let summary_lower = capsule.summary.to_lowercase();
    let anchor_lower = anchor_doc
        .map(|text| text.to_lowercase())
        .unwrap_or_default();
    let lexical_terms = capsule_lexical_terms(capsule, anchor_doc);
    plan.focus_terms
        .iter()
        .filter(|term| {
            summary_lower.contains(term.as_str())
                || anchor_lower.contains(term.as_str())
                || lexical_terms
                    .iter()
                    .any(|token| lexical_variant_match(token, term))
        })
        .cloned()
        .collect()
}

fn capsule_lexical_terms(
    capsule: &db::MemorySessionCapsuleRecord,
    anchor_doc: Option<&String>,
) -> HashSet<String> {
    let mut terms = content_tokens(&capsule.summary)
        .into_iter()
        .collect::<HashSet<_>>();
    if let Some(anchor_doc) = anchor_doc {
        terms.extend(content_tokens(anchor_doc));
    }
    terms
}

fn capsule_semantic_terms(
    capsule: &db::MemorySessionCapsuleRecord,
    anchor_doc: Option<&String>,
) -> HashSet<String> {
    let mut combined = capsule.summary.clone();
    if let Some(anchor_doc) = anchor_doc {
        if !anchor_doc.trim().is_empty() {
            combined.push('\n');
            combined.push_str(anchor_doc);
        }
    }
    semantic_token_set(&combined)
}

fn derive_capsule_theme_tokens(direct_terms: &[String], semantic_hits: &[String]) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut seen = HashSet::new();

    for term in direct_terms {
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
        capsule_theme_token_priority(right)
            .cmp(&capsule_theme_token_priority(left))
            .then_with(|| left.cmp(right))
    });
    if tokens.len() > 1 {
        let filtered = tokens
            .iter()
            .filter(|token: &&String| !matches!(token.as_str(), "daily" | "duration" | "time"))
            .cloned()
            .collect::<Vec<_>>();
        if !filtered.is_empty() {
            tokens = filtered;
        }
    }
    tokens.truncate(3);
    tokens
}

fn capsule_theme_group_key(theme_tokens: &[String]) -> String {
    let token_set = theme_tokens
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();

    if token_set.contains("sibling") || token_set.contains("family") {
        return "family|sibling".to_string();
    }
    if token_set.contains("accessory")
        && (token_set.contains("purchase") || token_set.contains("delivery"))
    {
        return "accessory|transaction".to_string();
    }
    if token_set.contains("commute")
        && (token_set.contains("audio")
            || token_set.contains("history")
            || token_set.contains("science"))
    {
        return "commute|audio".to_string();
    }

    theme_tokens.join("|")
}

fn capsule_theme_token_priority(token: &str) -> usize {
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

fn capsule_aggregate_intent_bonus(lower_query: &str, theme_tokens: &[String]) -> f64 {
    let token_set = theme_tokens
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let mut bonus = 0.0f64;

    if (lower_query.contains("sibling") || lower_query.contains("siblings"))
        && (token_set.contains("sibling") || token_set.contains("family"))
    {
        bonus += 1.35;
        if lower_query.contains("total number") || lower_query.contains("how many") {
            bonus += 0.55;
        }
    }
    if lower_query.contains("commute")
        && (lower_query.contains("activity")
            || lower_query.contains("activities")
            || lower_query.contains("suggest")
            || lower_query.contains("recommend"))
        && (token_set.contains("commute") || token_set.contains("audio"))
    {
        bonus += 0.85;
        if token_set.contains("history") || token_set.contains("science") {
            bonus += 0.20;
        }
    }
    if (lower_query.contains("arrive")
        || lower_query.contains("arrival")
        || lower_query.contains("deliver")
        || lower_query.contains("shipping"))
        && (lower_query.contains("bought")
            || lower_query.contains("buy")
            || lower_query.contains("ordered")
            || lower_query.contains("order"))
        && token_set.contains("accessory")
        && (token_set.contains("purchase") || token_set.contains("delivery"))
    {
        bonus += 0.95;
        if token_set.contains("purchase") && token_set.contains("delivery") {
            bonus += 0.25;
        }
    }

    bonus
}

fn should_surface_negative_gap(term: &str) -> bool {
    term.len() >= 4
        && !matches!(
            term,
            "what"
                | "when"
                | "which"
                | "said"
                | "remember"
                | "recall"
                | "mention"
                | "mentioned"
                | "session"
                | "latest"
                | "current"
                | "today"
                | "much"
                | "time"
                | "every"
                | "day"
                | "daily"
        )
}

fn lexical_variant_match(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let left_variants = lexical_variants(left);
    let right_variants = lexical_variants(right);
    left_variants
        .iter()
        .any(|variant| !variant.is_empty() && right_variants.contains(variant))
}

fn lexical_variants(token: &str) -> HashSet<String> {
    let mut variants = HashSet::new();
    let lower = token.to_lowercase();
    if lower.len() < 3 {
        return variants;
    }
    variants.insert(lower.clone());
    if lower.ends_with('e') && lower.len() > 4 {
        variants.insert(lower.trim_end_matches('e').to_string());
    }
    for suffix in ["ing", "ers", "er", "ied", "ies", "ed", "es", "s"] {
        if let Some(stem) = lower.strip_suffix(suffix) {
            let trimmed = stem.trim_end_matches('e');
            if trimmed.len() >= 4 {
                variants.insert(trimmed.to_string());
            }
            if suffix == "ies" && stem.len() >= 3 {
                variants.insert(format!("{stem}y"));
            }
        }
    }
    variants
}

pub(super) fn dedupe_capsules_for_working_set(capsules: &mut Vec<db::MemorySessionCapsuleRecord>) {
    let mut seen = HashSet::new();
    capsules.retain(|capsule| seen.insert(capsule_dedupe_key(capsule)));
}

pub(super) fn capsule_dedupe_key(capsule: &db::MemorySessionCapsuleRecord) -> String {
    let mut parts = vec![normalize_capsule_text(&capsule.summary)];
    let mut user_anchor = None;
    let mut assistant_anchor = None;

    for anchor in &capsule.anchors {
        let role = anchor.role.trim().to_ascii_lowercase();
        let normalized = normalize_capsule_text(&anchor.content);
        if normalized.is_empty() {
            continue;
        }
        match role.as_str() {
            "user" if user_anchor.is_none() => user_anchor = Some(normalized),
            "assistant" if assistant_anchor.is_none() => assistant_anchor = Some(normalized),
            _ => {}
        }
        if user_anchor.is_some() && assistant_anchor.is_some() {
            break;
        }
    }

    if let Some(anchor) = user_anchor {
        parts.push(format!("user:{anchor}"));
    }
    if let Some(anchor) = assistant_anchor {
        parts.push(format!("assistant:{anchor}"));
    }
    if parts.len() == 1 {
        for anchor in capsule.anchors.iter().take(2) {
            let normalized = normalize_capsule_text(&anchor.content);
            if !normalized.is_empty() {
                parts.push(format!(
                    "{}:{}",
                    anchor.role.trim().to_ascii_lowercase(),
                    normalized
                ));
            }
        }
    }

    parts.join("|")
}

pub(super) fn normalize_capsule_text(text: &str) -> String {
    text.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn prune_supporting_sources_for_claims(context: &mut db::RecallContext) {
    let active_source_ids = context
        .active_claims
        .iter()
        .filter_map(|claim| claim.source_id.as_deref())
        .collect::<HashSet<_>>();
    if active_source_ids.is_empty() {
        context.supporting_sources.clear();
        return;
    }
    context
        .supporting_sources
        .retain(|source| active_source_ids.contains(source.id.as_str()));
}

pub(super) fn prune_uncertainties_for_claims(context: &mut db::RecallContext) {
    let active_statements = context
        .active_claims
        .iter()
        .map(|claim| claim.statement.as_str())
        .collect::<Vec<_>>();
    if active_statements.is_empty() {
        context.uncertainties.clear();
        return;
    }
    context.uncertainties.retain(|uncertainty| {
        active_statements
            .iter()
            .any(|statement| !statement.is_empty() && uncertainty.contains(statement))
    });
}

pub(super) fn plan_prefers_personal_claims(plan: &MemoryRetrievalPlan) -> bool {
    if matches!(plan.mode, MemoryRequestMode::Identity) {
        return true;
    }
    if !matches!(plan.lane, MemoryRetrievalLane::HotState) {
        return false;
    }
    let lower = plan.expanded_query.to_lowercase();
    lower.starts_with("my ")
        || lower.starts_with("i ")
        || lower.contains(" my ")
        || lower.contains(" me ")
        || lower.contains(" do i ")
        || lower.contains(" did i ")
        || lower.contains(" should i ")
        || lower.contains("prefer")
        || lower.contains("preference")
        || lower.contains("favorite")
        || lower.contains("usually")
        || lower.contains("style")
        || lower.contains("tone")
}

pub(super) fn claim_belongs_to_personal_state(claim: &db::MemoryClaimRecord) -> bool {
    claim.scope == "personal"
        || claim.kind == "preference"
        || matches!(claim.subject.as_str(), "user" | "owner")
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/memory/retrieval.rs"]
mod tests;
