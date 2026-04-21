use rusqlite::{Connection, OptionalExtension};

use super::memory_model::{
    claim_record_is_stale, clean_memory_query, make_like_patterns, query_claim_matches,
    query_entity_match_ids, query_episode_matches, query_procedure_matches,
};
use super::shared::{
    map_memory_claim_row, map_memory_episode_row, map_memory_procedure_row, map_memory_source_row,
};
use super::{Db, MemoryClaimRecord, MemoryEpisodeRecord, RecallContext};

impl Db {
    pub fn store_embedding(&self, memory_id: &str, embedding: &[f32]) {
        let conn = self.conn.lock().unwrap();
        let bytes = crate::embed::vec_to_bytes(embedding);
        conn.execute(
            "INSERT OR REPLACE INTO memory_embeddings (memory_id, embedding) VALUES (?1, ?2)",
            rusqlite::params![memory_id, bytes],
        )
        .ok();
    }

    pub fn recall_semantic(&self, query_embedding: &[f32], limit: usize) -> Vec<(String, f32)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT m.content, e.embedding FROM memory_embeddings e \
                 JOIN memories m ON m.id = e.memory_id \
                 ORDER BY m.importance DESC",
            )
            .unwrap();

        let mut results: Vec<(String, f32)> = stmt
            .query_map([], |row| {
                let content: String = row.get(0)?;
                let blob: Vec<u8> = row.get(1)?;
                Ok((content, blob))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .map(|(content, blob)| {
                let emb = crate::embed::bytes_to_vec(&blob);
                let sim = crate::embed::cosine_similarity(query_embedding, &emb);
                (content, sim)
            })
            .collect();

        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        results
    }

    pub fn recall_hybrid(
        &self,
        query: &str,
        query_embedding: Option<&[f32]>,
        limit: usize,
    ) -> Vec<String> {
        let fts_results = self.recall(query, limit * 2);

        let query_emb = match query_embedding {
            Some(emb) if !emb.is_empty() => emb,
            _ => return fts_results.into_iter().take(limit).collect(),
        };

        let semantic_results = self.recall_semantic(query_emb, limit * 2);
        let mut scores: std::collections::HashMap<String, f32> = std::collections::HashMap::new();

        for (i, mem) in fts_results.iter().enumerate() {
            let fts_score = 1.0 / (1.0 + i as f32);
            *scores.entry(mem.clone()).or_default() += fts_score * 0.4;
        }

        for (mem, sim) in &semantic_results {
            *scores.entry(mem.clone()).or_default() += sim * 0.6;
        }

        let mut ranked: Vec<(String, f32)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.into_iter().take(limit).map(|(s, _)| s).collect()
    }

    pub fn last_memory_id(&self) -> Option<String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id FROM memories ORDER BY rowid DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .ok()
    }

    pub fn recall_context(
        &self,
        query: &str,
        query_embedding: Option<&[f32]>,
        limit: usize,
    ) -> RecallContext {
        let mut context = RecallContext::default();
        let candidate_memories = self.recall_hybrid(query, query_embedding, limit.max(1) * 2);
        let conn = self.conn.lock().unwrap();
        let clean_query = clean_memory_query(query);
        let like_patterns = make_like_patterns(&clean_query);

        let mut seen_claims = std::collections::HashSet::new();
        let mut seen_procedures = std::collections::HashSet::new();
        let mut seen_episodes = std::collections::HashSet::new();
        let mut seen_sources = std::collections::HashSet::new();

        for content in candidate_memories {
            let memory_row = conn
                .query_row(
                    "SELECT id FROM memories WHERE content = ?1 AND superseded_by IS NULL LIMIT 1",
                    rusqlite::params![content],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .unwrap_or(None);
            let Some(memory_id) = memory_row else {
                continue;
            };

            if let Some(episode) = conn
                .query_row(
                    "SELECT * FROM memory_episodes WHERE legacy_memory_id = ?1 LIMIT 1",
                    rusqlite::params![memory_id],
                    map_memory_episode_row,
                )
                .optional()
                .unwrap_or(None)
            {
                if seen_episodes.insert(episode.id.clone()) {
                    context.recent_episodes.push(episode);
                }
            }

            if let Some(procedure) = conn
                .query_row(
                    "SELECT * FROM memory_procedures WHERE legacy_memory_id = ?1 AND status = 'active' LIMIT 1",
                    rusqlite::params![memory_id],
                    map_memory_procedure_row,
                )
                .optional()
                .unwrap_or(None)
            {
                if seen_procedures.insert(procedure.id.clone()) {
                    context.procedures.push(procedure);
                }
            }

            let mut stmt = conn
                .prepare(
                    "SELECT * FROM memory_claims WHERE legacy_memory_id = ?1 AND status IN ('active', 'stale')
                     ORDER BY importance DESC, confidence DESC LIMIT 4",
                )
                .unwrap();
            let claims: Vec<MemoryClaimRecord> = stmt
                .query_map(rusqlite::params![memory_id], map_memory_claim_row)
                .unwrap()
                .filter_map(|row| row.ok())
                .collect();
            for mut claim in claims {
                claim.is_stale = claim_record_is_stale(&claim);
                if seen_claims.insert(claim.id.clone()) {
                    if claim.is_stale {
                        context
                            .uncertainties
                            .push(format!("stale claim may need refresh: {}", claim.statement));
                    }
                    if claim.scope == "personal" || claim.kind == "preference" {
                        context.profile.push(claim.statement.clone());
                    }
                    if let Some(source_id) = &claim.source_id {
                        if seen_sources.insert(source_id.clone()) {
                            if let Some(source) = conn
                                .query_row(
                                    "SELECT * FROM memory_sources WHERE id = ?1",
                                    rusqlite::params![source_id],
                                    map_memory_source_row,
                                )
                                .optional()
                                .unwrap_or(None)
                            {
                                context.supporting_sources.push(source);
                            }
                        }
                    }
                    context.active_claims.push(claim);
                }
            }
        }

        self.extend_context_from_direct_typed_matches(
            &conn,
            &mut context,
            &like_patterns,
            limit,
            &mut seen_claims,
            &mut seen_procedures,
            &mut seen_episodes,
            &mut seen_sources,
        );
        self.extend_context_from_entity_matches(
            &conn,
            &mut context,
            query,
            &like_patterns,
            limit,
            &mut seen_claims,
            &mut seen_procedures,
            &mut seen_episodes,
            &mut seen_sources,
        );
        let seeded_entity_ids = derive_seed_entity_ids_from_context(&context, limit);
        self.extend_context_from_entity_ids(
            &conn,
            &mut context,
            &seeded_entity_ids,
            limit,
            &mut seen_claims,
            &mut seen_procedures,
            &mut seen_episodes,
            &mut seen_sources,
        );

        context.profile.sort();
        context.profile.dedup();
        context.active_claims.sort_by(|a, b| {
            b.importance
                .partial_cmp(&a.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        context.procedures.sort_by(|a, b| {
            b.importance
                .partial_cmp(&a.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        context
            .recent_episodes
            .sort_by(|a, b| b.created_at.cmp(&a.created_at));
        context
            .supporting_sources
            .sort_by(|a, b| b.observed_at.cmp(&a.observed_at));
        context.uncertainties.sort();
        context.uncertainties.dedup();

        context.active_claims.truncate(limit);
        context.procedures.truncate(limit);
        context.recent_episodes.truncate(limit);
        context.supporting_sources.truncate(limit);
        context.profile.truncate(limit);
        context.uncertainties.truncate(limit);
        context
    }

    fn extend_context_from_direct_typed_matches(
        &self,
        conn: &Connection,
        context: &mut RecallContext,
        like_patterns: &[String],
        limit: usize,
        seen_claims: &mut std::collections::HashSet<String>,
        seen_procedures: &mut std::collections::HashSet<String>,
        seen_episodes: &mut std::collections::HashSet<String>,
        seen_sources: &mut std::collections::HashSet<String>,
    ) {
        let claims = query_claim_matches(conn, like_patterns, limit);
        for mut claim in claims {
            claim.is_stale = claim_record_is_stale(&claim);
            if seen_claims.insert(claim.id.clone()) {
                if claim.is_stale {
                    context
                        .uncertainties
                        .push(format!("stale claim may need refresh: {}", claim.statement));
                }
                if claim.scope == "personal" || claim.kind == "preference" {
                    context.profile.push(claim.statement.clone());
                }
                if let Some(source_id) = &claim.source_id {
                    if seen_sources.insert(source_id.clone()) {
                        if let Some(source) = conn
                            .query_row(
                                "SELECT * FROM memory_sources WHERE id = ?1",
                                rusqlite::params![source_id],
                                map_memory_source_row,
                            )
                            .optional()
                            .unwrap_or(None)
                        {
                            context.supporting_sources.push(source);
                        }
                    }
                }
                context.active_claims.push(claim);
            }
        }

        let procedures = query_procedure_matches(conn, like_patterns, limit);
        for procedure in procedures {
            if seen_procedures.insert(procedure.id.clone()) {
                context.procedures.push(procedure);
            }
        }

        let episodes = query_episode_matches(conn, like_patterns, limit);
        for episode in episodes {
            if seen_episodes.insert(episode.id.clone()) {
                context.recent_episodes.push(episode);
            }
        }
    }

    fn extend_context_from_entity_matches(
        &self,
        conn: &Connection,
        context: &mut RecallContext,
        query: &str,
        like_patterns: &[String],
        limit: usize,
        seen_claims: &mut std::collections::HashSet<String>,
        seen_procedures: &mut std::collections::HashSet<String>,
        seen_episodes: &mut std::collections::HashSet<String>,
        seen_sources: &mut std::collections::HashSet<String>,
    ) {
        let entity_ids = query_entity_match_ids(conn, query, like_patterns, limit);
        self.extend_context_from_entity_ids(
            conn,
            context,
            &entity_ids,
            limit,
            seen_claims,
            seen_procedures,
            seen_episodes,
            seen_sources,
        );
    }

    fn extend_context_from_entity_ids(
        &self,
        conn: &Connection,
        context: &mut RecallContext,
        entity_ids: &[String],
        limit: usize,
        seen_claims: &mut std::collections::HashSet<String>,
        seen_procedures: &mut std::collections::HashSet<String>,
        seen_episodes: &mut std::collections::HashSet<String>,
        seen_sources: &mut std::collections::HashSet<String>,
    ) {
        for entity_id in entity_ids {
            let mut claim_stmt = conn
                .prepare(
                    "SELECT * FROM memory_claims
                     WHERE entity_id = ?1
                       AND status IN ('active', 'stale')
                     ORDER BY importance DESC, confidence DESC, updated_at DESC
                     LIMIT ?2",
                )
                .unwrap();
            let claims: Vec<MemoryClaimRecord> = claim_stmt
                .query_map(
                    rusqlite::params![entity_id, limit as i64],
                    map_memory_claim_row,
                )
                .unwrap()
                .filter_map(|row| row.ok())
                .collect();
            for mut claim in claims {
                merge_claim_into_context(conn, context, &mut claim, seen_claims, seen_sources);
            }

            let mut procedure_stmt = conn
                .prepare(
                    "SELECT p.*
                     FROM memory_procedures p
                     JOIN memory_episodes e ON e.id = p.episode_id
                     WHERE e.entity_id = ?1
                       AND p.status = 'active'
                     ORDER BY p.importance DESC, p.confidence DESC, p.updated_at DESC
                     LIMIT ?2",
                )
                .unwrap();
            let procedures = procedure_stmt
                .query_map(
                    rusqlite::params![entity_id, limit as i64],
                    map_memory_procedure_row,
                )
                .unwrap()
                .filter_map(|row| row.ok())
                .collect::<Vec<_>>();
            for procedure in procedures {
                if seen_procedures.insert(procedure.id.clone()) {
                    context.procedures.push(procedure);
                }
            }

            let mut episode_stmt = conn
                .prepare(
                    "SELECT * FROM memory_episodes
                     WHERE entity_id = ?1
                     ORDER BY updated_at DESC, created_at DESC
                     LIMIT ?2",
                )
                .unwrap();
            let episodes: Vec<MemoryEpisodeRecord> = episode_stmt
                .query_map(
                    rusqlite::params![entity_id, limit as i64],
                    map_memory_episode_row,
                )
                .unwrap()
                .filter_map(|row| row.ok())
                .collect();
            for episode in episodes {
                if seen_episodes.insert(episode.id.clone()) {
                    context.recent_episodes.push(episode);
                }
            }
        }
    }
}

fn merge_claim_into_context(
    conn: &Connection,
    context: &mut RecallContext,
    claim: &mut MemoryClaimRecord,
    seen_claims: &mut std::collections::HashSet<String>,
    seen_sources: &mut std::collections::HashSet<String>,
) {
    claim.is_stale = claim_record_is_stale(claim);
    if !seen_claims.insert(claim.id.clone()) {
        return;
    }
    if claim.is_stale {
        context
            .uncertainties
            .push(format!("stale claim may need refresh: {}", claim.statement));
    }
    if claim.scope == "personal" || claim.kind == "preference" {
        context.profile.push(claim.statement.clone());
    }
    if let Some(source_id) = &claim.source_id {
        if seen_sources.insert(source_id.clone()) {
            if let Some(source) = conn
                .query_row(
                    "SELECT * FROM memory_sources WHERE id = ?1",
                    rusqlite::params![source_id],
                    map_memory_source_row,
                )
                .optional()
                .unwrap_or(None)
            {
                context.supporting_sources.push(source);
            }
        }
    }
    context.active_claims.push(claim.clone());
}

fn derive_seed_entity_ids_from_context(context: &RecallContext, limit: usize) -> Vec<String> {
    let mut counts = std::collections::HashMap::<String, usize>::new();
    for claim in &context.active_claims {
        if let Some(entity_id) = claim
            .entity_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            *counts.entry(entity_id.clone()).or_insert(0) += 3;
        }
    }
    for episode in &context.recent_episodes {
        if let Some(entity_id) = episode
            .entity_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            *counts.entry(entity_id.clone()).or_insert(0) += 1;
        }
    }

    let mut ranked = counts.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    ranked
        .into_iter()
        .take(limit.min(4).max(1))
        .map(|(entity_id, _)| entity_id)
        .collect()
}
