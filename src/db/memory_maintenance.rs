use anyhow::Result;
use rusqlite::OptionalExtension;
use std::collections::{HashMap, HashSet};

use super::autonomy_store::map_replay_failure_cluster_row;
use super::memory_model::{
    DerivedClaim, db_timestamp_now, entity_identity_signatures, entity_kinds_compatible,
    infer_entity_seed_from_claim, infer_entity_seed_from_text, offset_db_timestamp,
    preferred_entity_kind,
};
use super::shared::{
    map_memory_claim_row, map_memory_entity_row, map_memory_procedure_row,
    map_memory_refresh_job_row, map_memory_source_row,
};
use super::{
    Db, MemoryClaimRecord, MemoryEntityRecord, MemoryProcedureRecord, MemoryRefreshJobRecord,
    MemorySourceRecord,
};

impl Db {
    pub fn decay_memories(&self) {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "UPDATE memories SET importance = MAX(0.3, importance * 0.95) \
             WHERE superseded_by IS NULL \
             AND last_accessed < datetime('now', '-7 days') \
             AND importance > 0.3",
        )
        .ok();
    }

    pub fn mark_due_memory_claims_stale(&self, limit: usize) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT mc.id, mc.source_id, mc.entity_id, COALESCE(ms.refresh_query, mc.statement)
             FROM memory_claims mc
             LEFT JOIN memory_sources ms ON ms.id = mc.source_id
             WHERE mc.status = 'active'
               AND mc.freshness_ttl_secs IS NOT NULL
               AND mc.freshness_ttl_secs > 0
               AND datetime(COALESCE(mc.valid_from, mc.created_at), '+' || mc.freshness_ttl_secs || ' seconds') <= datetime('now')
             ORDER BY mc.updated_at ASC
             LIMIT ?1",
        )?;
        let due_rows: Vec<(String, Option<String>, Option<String>, Option<String>)> = stmt
            .query_map(rusqlite::params![limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .filter_map(|row| row.ok())
            .collect();
        let mut updated = 0usize;
        for (id, source_id, entity_id, refresh_query) in due_rows {
            updated += conn.execute(
                "UPDATE memory_claims
                 SET status = 'stale', updated_at = datetime('now')
                 WHERE id = ?1 AND status = 'active'",
                rusqlite::params![id],
            )?;
            if let Some(query) = refresh_query.filter(|query| !query.trim().is_empty()) {
                Self::enqueue_memory_refresh_job_static(
                    &conn,
                    &id,
                    source_id.as_deref(),
                    entity_id.as_deref(),
                    &query,
                    None,
                )?;
            }
        }
        Ok(updated)
    }

    pub fn merge_duplicate_memory_claims(&self, limit: usize) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source_id, entity_id, kind, scope, subject, predicate, object, statement,
                    confidence, importance, freshness_ttl_secs
             FROM memory_claims
             WHERE status IN ('active', 'stale')
             ORDER BY kind ASC, scope ASC, subject ASC, predicate ASC, object ASC, statement ASC,
                      importance DESC, confidence DESC, COALESCE(valid_from, created_at) DESC,
                      updated_at DESC, created_at DESC",
        )?;
        let rows: Vec<ClaimMergeRow> = stmt
            .query_map([], |row| {
                Ok(ClaimMergeRow {
                    id: row.get(0)?,
                    source_id: row.get(1)?,
                    entity_id: row.get(2)?,
                    kind: row.get(3)?,
                    scope: row.get(4)?,
                    subject: row.get(5)?,
                    predicate: row.get(6)?,
                    object: row.get(7)?,
                    statement: row.get(8)?,
                    confidence: row.get(9)?,
                    importance: row.get(10)?,
                    freshness_ttl_secs: row.get(11)?,
                })
            })?
            .filter_map(|row| row.ok())
            .collect();

        let mut merged = 0usize;
        let mut current_fingerprint = String::new();
        let mut winner: Option<ClaimMergeRow> = None;
        let now = db_timestamp_now();

        for row in rows {
            let fingerprint = claim_dedupe_fingerprint(&row);
            if fingerprint != current_fingerprint {
                current_fingerprint = fingerprint;
                winner = Some(row);
                continue;
            }
            let Some(winner_row) = winner.as_ref() else {
                winner = Some(row);
                continue;
            };
            if merged >= limit {
                break;
            }
            let winner_id = winner_row.id.clone();
            let changed = conn.execute(
                "UPDATE memory_claims
                 SET status = 'superseded',
                     valid_to = COALESCE(valid_to, ?2),
                     superseded_by = ?1,
                     updated_at = datetime('now')
                 WHERE id = ?3 AND status IN ('active', 'stale')",
                rusqlite::params![winner_id, now, row.id],
            )?;
            if changed > 0 {
                merged += 1;
                if winner_row.source_id.is_none() && row.source_id.is_some() {
                    conn.execute(
                        "UPDATE memory_claims
                         SET source_id = COALESCE(source_id, ?2),
                             updated_at = datetime('now')
                         WHERE id = ?1",
                        rusqlite::params![winner_id, row.source_id],
                    )?;
                }
                if winner_row.entity_id.is_none() && row.entity_id.is_some() {
                    conn.execute(
                        "UPDATE memory_claims
                         SET entity_id = COALESCE(entity_id, ?2),
                             updated_at = datetime('now')
                         WHERE id = ?1",
                        rusqlite::params![winner_id, row.entity_id],
                    )?;
                }
                conn.execute(
                    "UPDATE memory_claims
                     SET confidence = MAX(confidence, ?2),
                         importance = MAX(importance, ?3),
                         freshness_ttl_secs = COALESCE(MAX(freshness_ttl_secs, ?4), freshness_ttl_secs),
                         updated_at = datetime('now')
                     WHERE id = ?1",
                    rusqlite::params![
                        winner_id,
                        row.confidence,
                        row.importance,
                        row.freshness_ttl_secs,
                    ],
                )?;
                conn.execute(
                    "UPDATE memory_refresh_jobs
                     SET claim_id = ?2,
                         entity_id = COALESCE(entity_id, ?3),
                         updated_at = datetime('now')
                     WHERE claim_id = ?1",
                    rusqlite::params![
                        row.id,
                        winner_id,
                        winner_row.entity_id.as_ref().or(row.entity_id.as_ref())
                    ],
                )?;
                if let Some(source_id) = row.source_id.as_deref() {
                    Self::insert_memory_edge_static(
                        &conn, "claim", &winner_id, "source", source_id, "supports",
                    );
                }
                Self::insert_memory_edge_static(
                    &conn,
                    "claim",
                    &winner_id,
                    "claim",
                    &row.id,
                    "supersedes",
                );
                if let Some(winner_row) = winner.as_mut() {
                    if winner_row.source_id.is_none() {
                        winner_row.source_id = row.source_id.clone();
                    }
                    if winner_row.entity_id.is_none() {
                        winner_row.entity_id = row.entity_id.clone();
                    }
                    winner_row.confidence = winner_row.confidence.max(row.confidence);
                    winner_row.importance = winner_row.importance.max(row.importance);
                    winner_row.freshness_ttl_secs =
                        match (winner_row.freshness_ttl_secs, row.freshness_ttl_secs) {
                            (Some(left), Some(right)) => Some(left.max(right)),
                            (None, other) => other,
                            (current, None) => current,
                        };
                }
            }
        }
        if merged > 0 {
            dedupe_memory_refresh_jobs_static(&conn)?;
        }
        Ok(merged)
    }

    pub fn merge_duplicate_memory_entities(&self, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM memory_entities
             ORDER BY updated_at DESC, created_at DESC",
        )?;
        let entities = stmt
            .query_map([], map_memory_entity_row)?
            .filter_map(|row| row.ok())
            .collect::<Vec<_>>();
        if entities.len() < 2 {
            return Ok(0);
        }

        let reference_counts = entity_reference_counts(&conn)?;
        let mut candidates = entities
            .into_iter()
            .map(|entity| {
                let reference_count = *reference_counts.get(&entity.id).unwrap_or(&0);
                EntityMergeCandidate::new(entity, reference_count)
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .quality()
                .cmp(&left.quality())
                .then_with(|| left.entity.id.cmp(&right.entity.id))
        });

        let mut merged = 0usize;
        for index in 0..candidates.len() {
            if candidates[index].absorbed {
                continue;
            }
            let (prefix, suffix) = candidates.split_at_mut(index + 1);
            let winner = &mut prefix[index];
            for loser in suffix.iter_mut() {
                if merged >= limit {
                    break;
                }
                if loser.absorbed || !entities_should_merge(winner, loser) {
                    continue;
                }
                merge_entity_candidate_static(&conn, winner, loser)?;
                loser.absorbed = true;
                merged += 1;
            }
            if merged >= limit {
                break;
            }
        }

        if merged > 0 {
            dedupe_memory_refresh_jobs_static(&conn)?;
            dedupe_memory_edges_static(&conn)?;
        }
        Ok(merged)
    }

    pub fn assign_missing_memory_entities(&self, limit: usize) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, episode_id, source_id, kind, scope, subject, predicate, object, statement
             FROM memory_claims
             WHERE entity_id IS NULL
               AND status IN ('active', 'stale')
             ORDER BY updated_at DESC
             LIMIT ?1",
        )?;
        let rows: Vec<(
            String,
            Option<String>,
            Option<String>,
            String,
            String,
            String,
            String,
            String,
            String,
        )> = stmt
            .query_map(rusqlite::params![limit as i64], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            })?
            .filter_map(|row| row.ok())
            .collect();

        let mut updated = 0usize;
        for (claim_id, episode_id, source_id, kind, scope, subject, predicate, object, statement) in
            rows
        {
            let derived = DerivedClaim {
                kind,
                scope,
                subject,
                predicate,
                object,
            };
            let Some(seed) = infer_entity_seed_from_claim(&statement, &derived) else {
                continue;
            };
            let entity_id = Self::ensure_memory_entity_static(&conn, &seed)?;
            updated += conn.execute(
                "UPDATE memory_claims
                 SET entity_id = ?2, updated_at = datetime('now')
                 WHERE id = ?1 AND entity_id IS NULL",
                rusqlite::params![claim_id, entity_id],
            )?;
            if let Some(episode_id) = episode_id.as_deref() {
                conn.execute(
                    "UPDATE memory_episodes
                     SET entity_id = COALESCE(entity_id, ?2), updated_at = datetime('now')
                     WHERE id = ?1",
                    rusqlite::params![episode_id, entity_id],
                )?;
                Self::insert_memory_edge_static(
                    &conn, "episode", episode_id, "entity", &entity_id, "about",
                );
            }
            Self::insert_memory_edge_static(
                &conn, "claim", &claim_id, "entity", &entity_id, "about",
            );
            if let Some(source_id) = source_id.as_deref() {
                Self::insert_memory_edge_static(
                    &conn, "source", source_id, "entity", &entity_id, "about",
                );
            }
            conn.execute(
                "UPDATE memory_refresh_jobs
                 SET entity_id = COALESCE(entity_id, ?2), updated_at = datetime('now')
                 WHERE claim_id = ?1",
                rusqlite::params![claim_id, entity_id],
            )?;
        }

        let episode_limit = limit.saturating_sub(updated).max(1);
        let mut episode_stmt = conn.prepare(
            "SELECT id, source_id, content
             FROM memory_episodes
             WHERE entity_id IS NULL
             ORDER BY updated_at DESC
             LIMIT ?1",
        )?;
        let episode_rows: Vec<(String, Option<String>, String)> = episode_stmt
            .query_map(rusqlite::params![episode_limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .filter_map(|row| row.ok())
            .collect();

        for (episode_id, source_id, content) in episode_rows {
            let Some(seed) = infer_entity_seed_from_text(&content) else {
                continue;
            };
            let entity_id = Self::ensure_memory_entity_static(&conn, &seed)?;
            let changed = conn.execute(
                "UPDATE memory_episodes
                 SET entity_id = ?2, updated_at = datetime('now')
                 WHERE id = ?1 AND entity_id IS NULL",
                rusqlite::params![episode_id, entity_id],
            )?;
            if changed == 0 {
                continue;
            }
            updated += changed;
            Self::insert_memory_edge_static(
                &conn,
                "episode",
                &episode_id,
                "entity",
                &entity_id,
                "about",
            );
            if let Some(source_id) = source_id.as_deref() {
                Self::insert_memory_edge_static(
                    &conn, "source", source_id, "entity", &entity_id, "about",
                );
            }
        }
        Ok(updated)
    }

    pub fn list_due_memory_refresh_jobs(
        &self,
        limit: usize,
    ) -> Result<Vec<MemoryRefreshJobRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM memory_refresh_jobs
             WHERE status = 'pending'
               AND scheduled_for <= datetime('now')
             ORDER BY scheduled_for ASC, created_at ASC
             LIMIT ?1",
        )?;
        let jobs = stmt
            .query_map(rusqlite::params![limit as i64], map_memory_refresh_job_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(jobs)
    }

    pub fn claim_memory_refresh_job(&self, job_id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE memory_refresh_jobs
             SET status = 'running',
                 started_at = datetime('now'),
                 updated_at = datetime('now'),
                 last_error = NULL
             WHERE id = ?1 AND status = 'pending'",
            rusqlite::params![job_id],
        )?;
        Ok(changed > 0)
    }

    pub fn complete_memory_refresh_job(&self, job_id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE memory_refresh_jobs
             SET status = 'completed',
                 completed_at = datetime('now'),
                 updated_at = datetime('now')
             WHERE id = ?1",
            rusqlite::params![job_id],
        )?;
        Ok(changed > 0)
    }

    pub fn reschedule_memory_refresh_job(
        &self,
        job_id: &str,
        error: &str,
        delay_secs: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let delay = format!("+{} seconds", delay_secs.max(60));
        let changed = conn.execute(
            "UPDATE memory_refresh_jobs
             SET status = 'pending',
                 attempt_count = attempt_count + 1,
                 scheduled_for = datetime('now', ?2),
                 started_at = NULL,
                 completed_at = NULL,
                 last_error = ?3,
                 updated_at = datetime('now')
             WHERE id = ?1",
            rusqlite::params![job_id, delay, error],
        )?;
        Ok(changed > 0)
    }

    pub fn get_memory_claim(&self, claim_id: &str) -> Result<Option<MemoryClaimRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT * FROM memory_claims WHERE id = ?1 LIMIT 1",
            rusqlite::params![claim_id],
            map_memory_claim_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_memory_claim_lineage(&self, claim_id: &str) -> Result<Vec<MemoryClaimRecord>> {
        let conn = self.conn.lock().unwrap();
        let Some(version_root_id) = claim_version_root_static(&conn, claim_id)? else {
            return Ok(Vec::new());
        };
        let mut stmt = conn.prepare(
            "SELECT * FROM memory_claims
             WHERE COALESCE(version_root_id, id) = ?1
             ORDER BY COALESCE(valid_from, created_at) ASC, created_at ASC, updated_at ASC",
        )?;
        let claims = stmt
            .query_map(rusqlite::params![version_root_id], map_memory_claim_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(claims)
    }

    pub fn get_memory_source(&self, source_id: &str) -> Result<Option<MemorySourceRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT * FROM memory_sources WHERE id = ?1 LIMIT 1",
            rusqlite::params![source_id],
            map_memory_source_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_memory_entity_by_key(&self, entity_key: &str) -> Result<Option<MemoryEntityRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT * FROM memory_entities WHERE entity_key = ?1 LIMIT 1",
            rusqlite::params![entity_key],
            map_memory_entity_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn mark_memory_source_checked(
        &self,
        source_id: &str,
        checked_at: Option<&str>,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE memory_sources
             SET last_checked_at = COALESCE(?2, datetime('now'))
             WHERE id = ?1",
            rusqlite::params![source_id, checked_at],
        )?;
        Ok(changed > 0)
    }

    pub fn get_memory_refresh_job(&self, job_id: &str) -> Result<Option<MemoryRefreshJobRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT * FROM memory_refresh_jobs WHERE id = ?1 LIMIT 1",
            rusqlite::params![job_id],
            map_memory_refresh_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn reactivate_memory_claim(
        &self,
        claim_id: &str,
        source_id: Option<&str>,
        observed_at: Option<&str>,
        freshness_ttl_secs: Option<i64>,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let observed = observed_at
            .map(str::to_string)
            .unwrap_or_else(db_timestamp_now);
        let changed = conn.execute(
            "UPDATE memory_claims
             SET status = 'active',
                 source_id = COALESCE(?2, source_id),
                 valid_from = ?3,
                 valid_to = NULL,
                 superseded_by = NULL,
                 visibility = 'default',
                 disputed_at = NULL,
                 dispute_note = NULL,
                 freshness_ttl_secs = COALESCE(?4, freshness_ttl_secs),
                 updated_at = datetime('now')
             WHERE id = ?1 AND status IN ('active', 'stale', 'invalidated', 'disputed')",
            rusqlite::params![claim_id, source_id, observed, freshness_ttl_secs],
        )?;
        if changed == 0 {
            return Ok(false);
        }

        let refresh_info = conn
            .query_row(
                "SELECT COALESCE(ms.refresh_query, mc.statement), COALESCE(?2, mc.source_id), mc.entity_id, mc.freshness_ttl_secs
                 FROM memory_claims mc
                 LEFT JOIN memory_sources ms ON ms.id = COALESCE(?2, mc.source_id)
                 WHERE mc.id = ?1",
                rusqlite::params![claim_id, source_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .optional()?;
        if let Some((Some(refresh_query), next_source_id, entity_id, ttl_secs)) = refresh_info {
            if let Some(ttl_secs) = ttl_secs.filter(|ttl| *ttl > 0) {
                let scheduled_for = offset_db_timestamp(&observed, ttl_secs);
                Self::enqueue_memory_refresh_job_static(
                    &conn,
                    claim_id,
                    next_source_id.as_deref(),
                    entity_id.as_deref(),
                    &refresh_query,
                    scheduled_for.as_deref(),
                )?;
            }
        }
        Ok(true)
    }

    pub fn invalidate_memory_claim(&self, claim_id: &str, valid_to: Option<&str>) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let invalidated_at = valid_to
            .map(str::to_string)
            .unwrap_or_else(db_timestamp_now);
        let changed = conn.execute(
            "UPDATE memory_claims
             SET status = 'invalidated',
                 valid_to = COALESCE(valid_to, ?2),
                 visibility = 'history',
                 updated_at = datetime('now')
             WHERE id = ?1 AND status IN ('active', 'stale', 'disputed')",
            rusqlite::params![claim_id, invalidated_at],
        )?;
        Ok(changed > 0)
    }

    pub fn supersede_memory_claim(
        &self,
        old_claim_id: &str,
        new_claim_id: &str,
        valid_to: Option<&str>,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let superseded_at = valid_to
            .map(str::to_string)
            .unwrap_or_else(db_timestamp_now);
        let root_id = claim_version_root_static(&conn, old_claim_id)?
            .unwrap_or_else(|| old_claim_id.to_string());
        let changed = conn.execute(
            "UPDATE memory_claims
             SET status = 'superseded',
                 valid_to = COALESCE(valid_to, ?2),
                 superseded_by = ?1,
                 visibility = 'history',
                 updated_at = datetime('now')
             WHERE id = ?3 AND status IN ('active', 'stale')",
            rusqlite::params![new_claim_id, superseded_at, old_claim_id],
        )?;
        if changed > 0 {
            conn.execute(
                "UPDATE memory_claims
                 SET version_root_id = ?2,
                     supersedes_claim_id = ?3,
                     visibility = 'default',
                     disputed_at = NULL,
                     dispute_note = NULL,
                     updated_at = datetime('now')
                 WHERE id = ?1",
                rusqlite::params![new_claim_id, root_id, old_claim_id],
            )?;
            Self::insert_memory_edge_static(
                &conn,
                "claim",
                new_claim_id,
                "claim",
                old_claim_id,
                "supersedes",
            );
        }
        Ok(changed > 0)
    }

    pub fn dispute_memory_claims(
        &self,
        left_claim_id: &str,
        right_claim_id: &str,
        note: Option<&str>,
        disputed_at: Option<&str>,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let disputed_at = disputed_at
            .map(str::to_string)
            .unwrap_or_else(db_timestamp_now);
        let left_root = claim_version_root_static(&conn, left_claim_id)?
            .unwrap_or_else(|| left_claim_id.to_string());
        let right_root = claim_version_root_static(&conn, right_claim_id)?
            .unwrap_or_else(|| right_claim_id.to_string());
        let target_root = left_root.clone();

        if left_root != target_root {
            conn.execute(
                "UPDATE memory_claims
                 SET version_root_id = ?2, updated_at = datetime('now')
                 WHERE COALESCE(version_root_id, id) = ?1",
                rusqlite::params![left_root, target_root],
            )?;
        }
        if right_root != target_root {
            conn.execute(
                "UPDATE memory_claims
                 SET version_root_id = ?2, updated_at = datetime('now')
                 WHERE COALESCE(version_root_id, id) = ?1",
                rusqlite::params![right_root, target_root],
            )?;
        }

        let mut changed_any = false;
        for claim_id in [left_claim_id, right_claim_id] {
            let changed = conn.execute(
                "UPDATE memory_claims
                 SET status = 'disputed',
                     version_root_id = ?2,
                     disputed_at = ?3,
                     dispute_note = COALESCE(?4, dispute_note),
                     visibility = 'history',
                     updated_at = datetime('now')
                 WHERE id = ?1 AND status IN ('active', 'stale', 'invalidated', 'disputed')",
                rusqlite::params![claim_id, target_root, disputed_at, note],
            )?;
            changed_any |= changed > 0;
        }

        if changed_any {
            Self::insert_memory_edge_static(
                &conn,
                "claim",
                left_claim_id,
                "claim",
                right_claim_id,
                "contradicts",
            );
            Self::insert_memory_edge_static(
                &conn,
                "claim",
                right_claim_id,
                "claim",
                left_claim_id,
                "contradicts",
            );
        }

        Ok(changed_any)
    }

    pub fn promote_patterns_to_procedures(
        &self,
        patterns: &[crate::patterns::Pattern],
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut created = 0usize;
        for pattern in patterns {
            if pattern.category != crate::patterns::Category::ResponsePattern {
                continue;
            }
            if pattern.confidence < 0.25 {
                continue;
            }
            let content = format!(
                "When replying, adapt to this learned preference: {}",
                pattern.description
            );
            let exists: bool = conn.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM memory_procedures
                    WHERE content = ?1 AND status = 'active'
                )",
                rusqlite::params![content],
                |row| row.get(0),
            )?;
            if exists {
                continue;
            }

            let episode_id = uuid::Uuid::new_v4().to_string();
            conn.execute(
                "INSERT INTO memory_episodes (
                    id, legacy_memory_id, source_id, actor, channel, network, summary, content, importance, event_at
                ) VALUES (?1, NULL, NULL, 'nyx', NULL, 'knowledge', ?2, ?3, ?4, datetime('now'))",
                rusqlite::params![
                    episode_id,
                    format!("response pattern detected: {}", pattern.description),
                    pattern.description,
                    (pattern.confidence as f64).clamp(0.3, 0.95),
                ],
            )?;

            let procedure_id = uuid::Uuid::new_v4().to_string();
            conn.execute(
                "INSERT INTO memory_procedures (
                    id, legacy_memory_id, episode_id, title, content, trigger, confidence, importance, status
                ) VALUES (?1, NULL, ?2, ?3, ?4, 'pattern_response', ?5, ?6, 'active')",
                rusqlite::params![
                    procedure_id,
                    episode_id,
                    "adapt replies to learned preference",
                    content,
                    (pattern.confidence as f64).clamp(0.3, 0.95),
                    (pattern.confidence as f64).clamp(0.3, 0.95),
                ],
            )?;
            Self::insert_memory_edge_static(
                &conn,
                "procedure",
                &procedure_id,
                "episode",
                &episode_id,
                "derived_from",
            );
            created += 1;
        }
        Ok(created)
    }

    pub fn promote_replay_failure_clusters_to_procedures(
        &self,
        min_occurrences: usize,
        limit: usize,
    ) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, fingerprint, task_kind, tool, failure_class, failure_stage, latest_outcome,
                    issue_signature, exemplar_summary, exemplar_error, target, provider, model, route,
                    occurrence_count, first_seen_at, last_seen_at, sample_action_run_ids_json,
                    latest_action_run_id, latest_task_id, created_at, updated_at
             FROM replay_failure_clusters
             WHERE occurrence_count >= ?1
             ORDER BY occurrence_count DESC, last_seen_at DESC
             LIMIT ?2",
        )?;
        let clusters = stmt
            .query_map(
                rusqlite::params![min_occurrences.max(2) as i64, limit as i64],
                map_replay_failure_cluster_row,
            )?
            .filter_map(|row| row.ok())
            .collect::<Vec<_>>();

        let mut promoted = 0usize;
        for cluster in clusters {
            let Some(candidate) =
                crate::improvement::distill::procedure_candidate_from_failure_cluster(&cluster)
            else {
                continue;
            };
            if upsert_replay_failure_cluster_procedure_static(&conn, &cluster, &candidate)? {
                promoted += 1;
            }
        }

        Ok(promoted)
    }
}

fn claim_version_root_static(
    conn: &rusqlite::Connection,
    claim_id: &str,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT COALESCE(version_root_id, id) FROM memory_claims WHERE id = ?1 LIMIT 1",
        rusqlite::params![claim_id],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(Into::into)
}

#[derive(Debug, Clone)]
struct ClaimMergeRow {
    id: String,
    source_id: Option<String>,
    entity_id: Option<String>,
    kind: String,
    scope: String,
    subject: String,
    predicate: String,
    object: String,
    statement: String,
    confidence: f64,
    importance: f64,
    freshness_ttl_secs: Option<i64>,
}

#[derive(Debug, Clone)]
struct EntityMergeCandidate {
    entity: MemoryEntityRecord,
    signatures: HashSet<String>,
    reference_count: usize,
    absorbed: bool,
}

impl EntityMergeCandidate {
    fn new(entity: MemoryEntityRecord, reference_count: usize) -> Self {
        let mut candidate = Self {
            entity,
            signatures: HashSet::new(),
            reference_count,
            absorbed: false,
        };
        candidate.refresh_signatures();
        candidate
    }

    fn quality(&self) -> usize {
        self.reference_count * 100
            + self.entity.aliases.len() * 10
            + canonical_name_quality(&self.entity.canonical_name, &self.entity.entity_kind)
    }

    fn refresh_signatures(&mut self) {
        self.signatures = entity_identity_signatures(
            &self.entity.entity_key,
            &self.entity.entity_kind,
            &self.entity.canonical_name,
            &self.entity.aliases,
        )
        .into_iter()
        .collect();
    }
}

fn claim_dedupe_fingerprint(row: &ClaimMergeRow) -> String {
    let kind = normalize_claim_component(&row.kind);
    let scope = normalize_claim_component(&row.scope);
    let subject = normalize_claim_component(&row.subject);
    let predicate = normalize_claim_component(&row.predicate);
    let object = normalize_claim_component(&row.object);
    if !subject.is_empty() && subject != "general" && !predicate.is_empty() && !object.is_empty() {
        format!("{kind}|{scope}|{subject}|{predicate}|{object}")
    } else {
        format!(
            "{kind}|{scope}|{}",
            normalize_claim_component(&row.statement)
        )
    }
}

fn normalize_claim_component(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .skip_while(|token| matches!(*token, "a" | "an" | "the"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn entity_reference_counts(conn: &rusqlite::Connection) -> Result<HashMap<String, usize>> {
    let mut counts = HashMap::new();
    for sql in [
        "SELECT entity_id, COUNT(*) FROM memory_claims WHERE entity_id IS NOT NULL GROUP BY entity_id",
        "SELECT entity_id, COUNT(*) FROM memory_episodes WHERE entity_id IS NOT NULL GROUP BY entity_id",
        "SELECT entity_id, COUNT(*) FROM memory_refresh_jobs WHERE entity_id IS NOT NULL GROUP BY entity_id",
    ] {
        let mut stmt = conn.prepare(sql)?;
        let rows: Vec<(String, i64)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|row| row.ok())
            .collect();
        for (entity_id, count) in rows {
            *counts.entry(entity_id).or_insert(0) += count.max(0) as usize;
        }
    }
    Ok(counts)
}

fn entities_should_merge(left: &EntityMergeCandidate, right: &EntityMergeCandidate) -> bool {
    if !entity_kinds_compatible(&left.entity.entity_kind, &right.entity.entity_kind) {
        return false;
    }
    left.signatures
        .intersection(&right.signatures)
        .any(|signature| signature.len() >= 3)
}

fn merge_entity_candidate_static(
    conn: &rusqlite::Connection,
    winner: &mut EntityMergeCandidate,
    loser: &EntityMergeCandidate,
) -> Result<()> {
    let canonical_name = preferred_canonical_name(&winner.entity, &loser.entity);
    let entity_kind = preferred_entity_kind(&winner.entity.entity_kind, &loser.entity.entity_kind);
    let aliases = merged_entity_aliases(&winner.entity, &loser.entity);

    conn.execute(
        "UPDATE memory_entities
         SET entity_kind = ?2,
             canonical_name = ?3,
             aliases_json = ?4,
             updated_at = datetime('now')
         WHERE id = ?1",
        rusqlite::params![
            winner.entity.id,
            entity_kind,
            canonical_name,
            serde_json::to_string(&aliases)?,
        ],
    )?;
    conn.execute(
        "UPDATE memory_claims
         SET entity_id = ?2, updated_at = datetime('now')
         WHERE entity_id = ?1",
        rusqlite::params![loser.entity.id, winner.entity.id],
    )?;
    conn.execute(
        "UPDATE memory_episodes
         SET entity_id = ?2, updated_at = datetime('now')
         WHERE entity_id = ?1",
        rusqlite::params![loser.entity.id, winner.entity.id],
    )?;
    conn.execute(
        "UPDATE memory_refresh_jobs
         SET entity_id = ?2, updated_at = datetime('now')
         WHERE entity_id = ?1",
        rusqlite::params![loser.entity.id, winner.entity.id],
    )?;
    conn.execute(
        "UPDATE memory_edges
         SET source_id = ?2
         WHERE source_kind = 'entity' AND source_id = ?1",
        rusqlite::params![loser.entity.id, winner.entity.id],
    )?;
    conn.execute(
        "UPDATE memory_edges
         SET target_id = ?2
         WHERE target_kind = 'entity' AND target_id = ?1",
        rusqlite::params![loser.entity.id, winner.entity.id],
    )?;
    conn.execute(
        "DELETE FROM memory_entities WHERE id = ?1",
        rusqlite::params![loser.entity.id],
    )?;

    winner.entity.entity_kind = entity_kind;
    winner.entity.canonical_name = canonical_name;
    winner.entity.aliases = aliases;
    winner.reference_count += loser.reference_count;
    winner.refresh_signatures();
    Ok(())
}

fn merged_entity_aliases(left: &MemoryEntityRecord, right: &MemoryEntityRecord) -> Vec<String> {
    let mut aliases = left.aliases.clone();
    for candidate in [left.canonical_name.as_str(), right.canonical_name.as_str()] {
        if !candidate.trim().is_empty()
            && !aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(candidate))
        {
            aliases.push(candidate.to_string());
        }
    }
    for alias in &right.aliases {
        if alias.trim().is_empty() {
            continue;
        }
        if !aliases
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(alias))
        {
            aliases.push(alias.clone());
        }
    }
    aliases.sort_by_key(|alias| alias.to_lowercase());
    aliases.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
    aliases
}

fn preferred_canonical_name(left: &MemoryEntityRecord, right: &MemoryEntityRecord) -> String {
    let left_score = canonical_name_quality(&left.canonical_name, &left.entity_kind);
    let right_score = canonical_name_quality(&right.canonical_name, &right.entity_kind);
    if right_score > left_score {
        right.canonical_name.clone()
    } else {
        left.canonical_name.clone()
    }
}

fn canonical_name_quality(name: &str, entity_kind: &str) -> usize {
    let normalized = normalize_claim_component(name);
    if normalized.is_empty() {
        return 0;
    }
    let generic_penalty = usize::from(matches!(
        normalized.as_str(),
        "entity" | "organization" | "person" | "project" | "general"
    ));
    normalized.split_whitespace().count() + entity_kind.trim().len().saturating_mul(2)
        - generic_penalty
}

fn upsert_replay_failure_cluster_procedure_static(
    conn: &rusqlite::Connection,
    cluster: &super::ReplayFailureClusterRecord,
    candidate: &crate::improvement::distill::DistilledProcedureCandidate,
) -> Result<bool> {
    let existing = conn
        .query_row(
            "SELECT * FROM memory_procedures
             WHERE trigger = ?1
             LIMIT ?2",
            rusqlite::params![candidate.trigger.as_str(), 1],
            map_memory_procedure_row,
        )
        .optional()?;

    if let Some(existing_procedure) = existing {
        let mut changed = false;
        if procedure_needs_update(&existing_procedure, candidate) {
            conn.execute(
                "UPDATE memory_procedures
                 SET title = ?2,
                     content = ?3,
                     trigger = ?4,
                     confidence = ?5,
                     importance = ?6,
                     status = 'active',
                     updated_at = datetime('now')
                WHERE id = ?1",
                rusqlite::params![
                    existing_procedure.id,
                    candidate.title.as_str(),
                    candidate.content.as_str(),
                    candidate.trigger.as_str(),
                    candidate.confidence,
                    candidate.importance,
                ],
            )?;
            changed = true;
        }
        if let Some(episode_id) = existing_procedure.episode_id.as_deref() {
            let episode_changed = conn
                .query_row(
                    "SELECT summary, content, importance, event_at
                     FROM memory_episodes
                     WHERE id = ?1
                     LIMIT 1",
                    rusqlite::params![episode_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, f64>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .optional()?
                .map(|(summary, content, importance, event_at)| {
                    summary != candidate.episode_summary
                        || content != candidate.episode_content
                        || (importance - candidate.importance).abs() > 0.0001
                        || event_at.as_deref() != Some(cluster.last_seen_at.as_str())
                })
                .unwrap_or(true);
            if episode_changed {
                conn.execute(
                    "UPDATE memory_episodes
                     SET summary = ?2,
                         content = ?3,
                         importance = ?4,
                         event_at = ?5,
                         updated_at = datetime('now')
                     WHERE id = ?1",
                    rusqlite::params![
                        episode_id,
                        candidate.episode_summary.as_str(),
                        candidate.episode_content.as_str(),
                        candidate.importance,
                        cluster.last_seen_at.as_str(),
                    ],
                )?;
                changed = true;
            }
        }
        return Ok(changed);
    }

    let episode_id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO memory_episodes (
            id, legacy_memory_id, source_id, entity_id, actor, channel, network, summary, content, importance, event_at
        ) VALUES (?1, NULL, NULL, NULL, 'nyx', NULL, 'knowledge', ?2, ?3, ?4, ?5)",
        rusqlite::params![
            episode_id,
            candidate.episode_summary.as_str(),
            candidate.episode_content.as_str(),
            candidate.importance,
            cluster.last_seen_at.as_str(),
        ],
    )?;

    let procedure_id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO memory_procedures (
            id, legacy_memory_id, episode_id, title, content, trigger, confidence, importance, status
        ) VALUES (?1, NULL, ?2, ?3, ?4, ?5, ?6, ?7, 'active')",
        rusqlite::params![
            procedure_id,
            episode_id,
            candidate.title.as_str(),
            candidate.content.as_str(),
            candidate.trigger.as_str(),
            candidate.confidence,
            candidate.importance,
        ],
    )?;
    Db::insert_memory_edge_static(
        conn,
        "procedure",
        &procedure_id,
        "episode",
        &episode_id,
        "derived_from",
    );
    Ok(true)
}

fn procedure_needs_update(
    existing: &MemoryProcedureRecord,
    candidate: &crate::improvement::distill::DistilledProcedureCandidate,
) -> bool {
    existing.title != candidate.title
        || existing.content != candidate.content
        || existing.trigger != candidate.trigger
        || (existing.confidence - candidate.confidence).abs() > 0.0001
        || (existing.importance - candidate.importance).abs() > 0.0001
        || existing.status != "active"
}

fn dedupe_memory_edges_static(conn: &rusqlite::Connection) -> Result<usize> {
    let deleted = conn.execute(
        "DELETE FROM memory_edges
         WHERE id NOT IN (
            SELECT MIN(id)
            FROM memory_edges
            GROUP BY source_kind, source_id, target_kind, target_id, edge_type
         )",
        [],
    )?;
    Ok(deleted)
}

fn dedupe_memory_refresh_jobs_static(conn: &rusqlite::Connection) -> Result<usize> {
    let deleted_active = conn.execute(
        "DELETE FROM memory_refresh_jobs
         WHERE id IN (
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY claim_id
                           ORDER BY
                               CASE status WHEN 'running' THEN 0 ELSE 1 END,
                               CASE WHEN COALESCE(source_id, '') = '' THEN 1 ELSE 0 END,
                               CASE WHEN COALESCE(entity_id, '') = '' THEN 1 ELSE 0 END,
                               scheduled_for ASC,
                               created_at ASC,
                               updated_at ASC,
                               id ASC
                       ) AS rn
                FROM memory_refresh_jobs
                WHERE status IN ('pending', 'running')
            )
            SELECT id FROM ranked WHERE rn > 1
         )",
        [],
    )?;
    let deleted_exact = conn.execute(
        "DELETE FROM memory_refresh_jobs
         WHERE status NOT IN ('pending', 'running')
           AND id NOT IN (
                SELECT MIN(id)
                FROM memory_refresh_jobs
                WHERE status NOT IN ('pending', 'running')
                GROUP BY claim_id, COALESCE(source_id, ''), COALESCE(entity_id, ''), refresh_query, status
           )",
        [],
    )?;
    Ok(deleted_active + deleted_exact)
}
