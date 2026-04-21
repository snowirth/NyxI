use anyhow::Result;
use chrono::{NaiveDateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use std::collections::HashSet;

use super::shared::{map_memory_claim_row, map_memory_episode_row, map_memory_procedure_row};
use super::{Db, MemoryClaimRecord, MemoryEpisodeRecord, MemoryProcedureRecord};

const MEMORY_QUERY_STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "can", "could", "did", "do", "does", "for", "from", "had", "has",
    "have", "how", "i", "is", "me", "my", "of", "or", "please", "real", "remember", "should",
    "tell", "the", "their", "them", "then", "there", "these", "they", "this", "those", "to", "us",
    "was", "we", "were", "what", "when", "where", "which", "who", "why", "with", "would", "you",
    "your",
];

#[derive(Debug, Clone)]
pub(super) struct DerivedClaim {
    pub(super) kind: String,
    pub(super) scope: String,
    pub(super) subject: String,
    pub(super) predicate: String,
    pub(super) object: String,
}

impl DerivedClaim {
    pub(super) fn fallback(kind: &str, scope: &str, statement: &str) -> Self {
        Self {
            kind: if kind.trim().is_empty() {
                "fact".to_string()
            } else {
                kind.trim().to_string()
            },
            scope: if scope.trim().is_empty() {
                "global".to_string()
            } else {
                scope.trim().to_string()
            },
            subject: "general".to_string(),
            predicate: "states".to_string(),
            object: statement.trim().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct MemoryEntitySeed {
    entity_key: String,
    entity_kind: String,
    canonical_name: String,
    aliases: Vec<String>,
}

impl Db {
    pub(super) fn ensure_memory_entity_static(
        conn: &Connection,
        seed: &MemoryEntitySeed,
    ) -> Result<String> {
        if let Some(existing) = conn
            .query_row(
                "SELECT id, aliases_json FROM memory_entities WHERE entity_key = ?1 LIMIT 1",
                rusqlite::params![seed.entity_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let (entity_id, aliases_json) = existing;
            let mut aliases = parse_entity_aliases(&aliases_json);
            let canonical_lower = seed.canonical_name.to_lowercase();
            if !aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(&seed.canonical_name))
            {
                aliases.push(seed.canonical_name.clone());
            }
            for alias in &seed.aliases {
                if alias.trim().is_empty() {
                    continue;
                }
                if !aliases
                    .iter()
                    .any(|existing_alias| existing_alias.eq_ignore_ascii_case(alias))
                {
                    aliases.push(alias.clone());
                }
            }
            aliases.sort_by_key(|alias| alias.to_lowercase());
            aliases.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
            conn.execute(
                "UPDATE memory_entities
                 SET entity_kind = COALESCE(NULLIF(?2, ''), entity_kind),
                     canonical_name = CASE
                        WHEN canonical_name = '' OR LOWER(canonical_name) = entity_key OR LOWER(canonical_name) = ?4
                            THEN ?3
                        ELSE canonical_name
                     END,
                     aliases_json = ?5,
                     updated_at = datetime('now')
                 WHERE id = ?1",
                rusqlite::params![
                    entity_id,
                    seed.entity_kind,
                    seed.canonical_name,
                    canonical_lower,
                    serde_json::to_string(&aliases)?,
                ],
            )?;
            return Ok(entity_id);
        }

        if let Some((entity_id, aliases_json, canonical_name, entity_kind)) =
            find_matching_memory_entity_static(conn, seed)?
        {
            let mut aliases = parse_entity_aliases(&aliases_json);
            if !canonical_name.trim().is_empty()
                && !aliases
                    .iter()
                    .any(|alias| alias.eq_ignore_ascii_case(&canonical_name))
            {
                aliases.push(canonical_name);
            }
            if !aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(&seed.canonical_name))
            {
                aliases.push(seed.canonical_name.clone());
            }
            for alias in &seed.aliases {
                if alias.trim().is_empty() {
                    continue;
                }
                if !aliases
                    .iter()
                    .any(|existing_alias| existing_alias.eq_ignore_ascii_case(alias))
                {
                    aliases.push(alias.clone());
                }
            }
            aliases.sort_by_key(|alias| alias.to_lowercase());
            aliases.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
            conn.execute(
                "UPDATE memory_entities
                 SET entity_kind = ?2,
                     aliases_json = ?3,
                     updated_at = datetime('now')
                 WHERE id = ?1",
                rusqlite::params![
                    entity_id,
                    preferred_entity_kind(&entity_kind, &seed.entity_kind),
                    serde_json::to_string(&aliases)?,
                ],
            )?;
            return Ok(entity_id);
        }

        let entity_id = uuid::Uuid::new_v4().to_string();
        let mut aliases = seed.aliases.clone();
        if !aliases
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(&seed.canonical_name))
        {
            aliases.push(seed.canonical_name.clone());
        }
        aliases.sort_by_key(|alias| alias.to_lowercase());
        aliases.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
        conn.execute(
            "INSERT INTO memory_entities (
                id, entity_key, entity_kind, canonical_name, aliases_json
            ) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                entity_id,
                seed.entity_key,
                seed.entity_kind,
                seed.canonical_name,
                serde_json::to_string(&aliases)?,
            ],
        )?;
        Ok(entity_id)
    }

    pub(super) fn enqueue_memory_refresh_job_static(
        conn: &Connection,
        claim_id: &str,
        source_id: Option<&str>,
        entity_id: Option<&str>,
        refresh_query: &str,
        scheduled_for: Option<&str>,
    ) -> Result<Option<String>> {
        let trimmed_query = refresh_query.trim();
        if trimmed_query.is_empty() {
            return Ok(None);
        }
        if let Some(existing_id) = conn
            .query_row(
                "SELECT id FROM memory_refresh_jobs
                 WHERE claim_id = ?1
                   AND status IN ('pending', 'running')
                 ORDER BY created_at DESC
                 LIMIT 1",
                rusqlite::params![claim_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            return Ok(Some(existing_id));
        }
        let job_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO memory_refresh_jobs (
                id, claim_id, source_id, entity_id, refresh_query, status, scheduled_for
            ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', COALESCE(?6, datetime('now')))",
            rusqlite::params![
                job_id,
                claim_id,
                source_id,
                entity_id,
                trimmed_query,
                scheduled_for,
            ],
        )?;
        Ok(Some(job_id))
    }

    pub(super) fn promote_legacy_memory_static(
        conn: &Connection,
        legacy_memory_id: &str,
        content: &str,
        network: &str,
        importance: f64,
        event_date: Option<&str>,
        superseded_ids: &[String],
    ) {
        let derived = derive_memory_claim(network, content);
        let entity_id = derived
            .as_ref()
            .and_then(|claim| infer_entity_seed_from_claim(content, claim))
            .or_else(|| infer_entity_seed_from_text(content))
            .and_then(|seed| Self::ensure_memory_entity_static(conn, &seed).ok());
        let episode_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT OR IGNORE INTO memory_episodes (
                id, legacy_memory_id, source_id, entity_id, actor, channel, network, summary, content, importance, event_at
            ) VALUES (?1, ?2, NULL, ?3, 'nyx', NULL, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                episode_id,
                legacy_memory_id,
                entity_id,
                network,
                summarize_episode(content, network),
                content.trim(),
                importance,
                event_date,
            ],
        )
        .ok();

        if is_procedure_memory(network, content) {
            let procedure_id = uuid::Uuid::new_v4().to_string();
            conn.execute(
                "INSERT OR IGNORE INTO memory_procedures (
                    id, legacy_memory_id, episode_id, title, content, trigger, confidence, importance, status
                ) VALUES (?1, ?2, ?3, ?4, ?5, 'memory', 0.85, ?6, 'active')",
                rusqlite::params![
                    procedure_id,
                    legacy_memory_id,
                    episode_id,
                    summarize_procedure_title(content),
                    strip_lesson_prefix(content),
                    importance,
                ],
            )
            .ok();
            Self::insert_memory_edge_static(
                conn,
                "procedure",
                &procedure_id,
                "episode",
                &episode_id,
                "derived_from",
            );
            if let Some(entity_id) = entity_id.as_deref() {
                Self::insert_memory_edge_static(
                    conn,
                    "episode",
                    &episode_id,
                    "entity",
                    entity_id,
                    "about",
                );
                Self::insert_memory_edge_static(
                    conn,
                    "procedure",
                    &procedure_id,
                    "entity",
                    entity_id,
                    "about",
                );
            }
            return;
        }

        let Some(derived) = derived else {
            return;
        };
        let claim_id = uuid::Uuid::new_v4().to_string();
        let valid_from = event_date
            .map(str::to_string)
            .unwrap_or_else(db_timestamp_now);
        conn.execute(
            "INSERT OR IGNORE INTO memory_claims (
                id, legacy_memory_id, episode_id, source_id, entity_id, version_root_id, supersedes_claim_id,
                kind, scope, subject, predicate, object, statement, confidence, importance, status,
                valid_from, valid_to, freshness_ttl_secs, superseded_by, visibility, disputed_at, dispute_note
            ) VALUES (?1, ?2, ?3, NULL, ?4, ?1, NULL, ?5, ?6, ?7, ?8, ?9, ?10, 0.75, ?11, 'active', ?12, NULL, NULL, NULL, 'default', NULL, NULL)",
            rusqlite::params![
                claim_id,
                legacy_memory_id,
                episode_id,
                entity_id,
                derived.kind,
                derived.scope,
                derived.subject,
                derived.predicate,
                derived.object,
                content.trim(),
                importance,
                valid_from,
            ],
        )
        .ok();
        Self::insert_memory_edge_static(
            conn,
            "claim",
            &claim_id,
            "episode",
            &episode_id,
            "derived_from",
        );
        if let Some(entity_id) = entity_id.as_deref() {
            Self::insert_memory_edge_static(conn, "claim", &claim_id, "entity", entity_id, "about");
            Self::insert_memory_edge_static(
                conn,
                "episode",
                &episode_id,
                "entity",
                entity_id,
                "about",
            );
        }

        let superseded_at = event_date
            .map(str::to_string)
            .unwrap_or_else(db_timestamp_now);
        for old_legacy_id in superseded_ids {
            if let Some(old_claim_id) = conn
                .query_row(
                    "SELECT id FROM memory_claims WHERE legacy_memory_id = ?1 AND status IN ('active', 'stale') LIMIT 1",
                    rusqlite::params![old_legacy_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .unwrap_or(None)
            {
                let old_root_id: String = conn
                    .query_row(
                        "SELECT COALESCE(version_root_id, id) FROM memory_claims WHERE id = ?1 LIMIT 1",
                        rusqlite::params![old_claim_id],
                        |row| row.get(0),
                    )
                    .unwrap_or_else(|_| old_claim_id.clone());
                conn.execute(
                    "UPDATE memory_claims
                     SET status = 'superseded',
                         valid_to = COALESCE(valid_to, ?2),
                         superseded_by = ?1,
                         visibility = 'history',
                         updated_at = datetime('now')
                     WHERE id = ?3",
                    rusqlite::params![claim_id, superseded_at, old_claim_id],
                )
                .ok();
                conn.execute(
                    "UPDATE memory_claims
                     SET version_root_id = ?2,
                         supersedes_claim_id = ?3,
                         visibility = 'default',
                         updated_at = datetime('now')
                     WHERE id = ?1",
                    rusqlite::params![claim_id, old_root_id, old_claim_id],
                )
                .ok();
                Self::insert_memory_edge_static(conn, "claim", &claim_id, "claim", &old_claim_id, "supersedes");
            }
        }
    }

    pub(super) fn insert_memory_edge_static(
        conn: &Connection,
        source_kind: &str,
        source_id: &str,
        target_kind: &str,
        target_id: &str,
        edge_type: &str,
    ) {
        conn.execute(
            "INSERT INTO memory_edges (source_kind, source_id, target_kind, target_id, edge_type)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![source_kind, source_id, target_kind, target_id, edge_type],
        )
        .ok();
    }
}

impl Db {
    pub fn remember_source(
        &self,
        source_kind: &str,
        url_or_ref: &str,
        title: Option<&str>,
        publisher: Option<&str>,
        trust_tier: f64,
        checksum: Option<&str>,
        observed_at: Option<&str>,
        refresh_query: Option<&str>,
    ) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        if let Some(existing_id) = conn
            .query_row(
                "SELECT id FROM memory_sources WHERE source_kind = ?1 AND url_or_ref = ?2 AND COALESCE(checksum, '') = COALESCE(?3, '') LIMIT 1",
                rusqlite::params![source_kind, url_or_ref, checksum],
                |row| row.get(0),
            )
            .optional()?
        {
            conn.execute(
                "UPDATE memory_sources
                 SET title = COALESCE(?2, title),
                     publisher = COALESCE(?3, publisher),
                     trust_tier = MAX(trust_tier, ?4),
                     refresh_query = COALESCE(NULLIF(?5, ''), refresh_query),
                     observed_at = COALESCE(?6, observed_at),
                     last_checked_at = COALESCE(?6, datetime('now'))
                 WHERE id = ?1",
                rusqlite::params![
                    existing_id,
                    title,
                    publisher,
                    trust_tier,
                    refresh_query,
                    observed_at,
                ],
            )?;
            return Ok(existing_id);
        }

        let source_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO memory_sources (
                id, source_kind, url_or_ref, title, publisher, trust_tier, checksum,
                refresh_query, observed_at, last_checked_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, COALESCE(?9, datetime('now')), COALESCE(?9, datetime('now')))",
            rusqlite::params![
                source_id,
                source_kind,
                url_or_ref,
                title,
                publisher,
                trust_tier,
                checksum,
                refresh_query,
                observed_at,
            ],
        )?;
        Ok(source_id)
    }

    pub fn remember_sourced_claim(
        &self,
        statement: &str,
        kind: &str,
        scope: &str,
        source_id: &str,
        confidence: f64,
        importance: f64,
        freshness_ttl_secs: Option<i64>,
        observed_at: Option<&str>,
    ) -> Result<Option<String>> {
        let trimmed = statement.trim();
        let lower = trimmed.to_lowercase();
        if trimmed.len() < 10 || lower.contains("no useful results") || lower.contains("error:") {
            return Ok(None);
        }

        let conn = self.conn.lock().unwrap();
        let existing: bool = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM memory_claims
                WHERE statement = ?1 AND status IN ('active', 'stale') AND COALESCE(source_id, '') = ?2
            )",
            rusqlite::params![trimmed, source_id],
            |row| row.get(0),
        )?;
        if existing {
            return Ok(None);
        }

        let observed = observed_at
            .map(str::to_string)
            .unwrap_or_else(db_timestamp_now);
        let derived = derive_memory_claim("knowledge", trimmed)
            .unwrap_or_else(|| DerivedClaim::fallback(kind, scope, trimmed));
        let entity_seed = infer_entity_seed_from_claim(trimmed, &derived);
        let entity_id = entity_seed
            .as_ref()
            .and_then(|seed| Self::ensure_memory_entity_static(&conn, seed).ok());
        let episode_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO memory_episodes (
                id, source_id, entity_id, actor, channel, network, summary, content, importance, event_at
            ) VALUES (?1, ?2, ?3, 'web', 'web', 'knowledge', ?4, ?5, ?6, ?7)",
            rusqlite::params![
                episode_id,
                source_id,
                entity_id,
                summarize_episode(trimmed, "knowledge"),
                trimmed,
                importance,
                observed,
            ],
        )?;

        let claim_id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO memory_claims (
                id, legacy_memory_id, episode_id, source_id, entity_id, version_root_id, supersedes_claim_id,
                kind, scope, subject, predicate, object, statement, confidence, importance, status,
                valid_from, valid_to, freshness_ttl_secs, superseded_by, visibility, disputed_at, dispute_note
            ) VALUES (?1, NULL, ?2, ?3, ?4, ?1, NULL, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 'active', ?13, NULL, ?14, NULL, 'default', NULL, NULL)",
            rusqlite::params![
                claim_id,
                episode_id,
                source_id,
                entity_id,
                if kind.is_empty() { derived.kind } else { kind.to_string() },
                if scope.is_empty() { derived.scope } else { scope.to_string() },
                derived.subject,
                derived.predicate,
                derived.object,
                trimmed,
                confidence,
                importance,
                observed,
                freshness_ttl_secs,
            ],
        )?;
        Self::insert_memory_edge_static(
            &conn,
            "claim",
            &claim_id,
            "episode",
            &episode_id,
            "derived_from",
        );
        Self::insert_memory_edge_static(&conn, "claim", &claim_id, "source", source_id, "supports");
        if let Some(entity_id) = entity_id.as_deref() {
            Self::insert_memory_edge_static(
                &conn, "claim", &claim_id, "entity", entity_id, "about",
            );
            Self::insert_memory_edge_static(
                &conn,
                "episode",
                &episode_id,
                "entity",
                entity_id,
                "about",
            );
            Self::insert_memory_edge_static(
                &conn, "source", source_id, "entity", entity_id, "about",
            );
        }
        if let Some(ttl_secs) = freshness_ttl_secs.filter(|ttl| *ttl > 0) {
            let refresh_query: Option<String> = conn
                .query_row(
                    "SELECT refresh_query FROM memory_sources WHERE id = ?1",
                    rusqlite::params![source_id],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();
            if let Some(query) = refresh_query
                .filter(|query| !query.trim().is_empty())
                .or_else(|| Some(trimmed.to_string()))
            {
                let scheduled_for = offset_db_timestamp(&observed, ttl_secs);
                Self::enqueue_memory_refresh_job_static(
                    &conn,
                    &claim_id,
                    Some(source_id),
                    entity_id.as_deref(),
                    &query,
                    scheduled_for.as_deref(),
                )?;
            }
        }
        Ok(Some(claim_id))
    }
}

pub(super) fn summarize_episode(content: &str, network: &str) -> String {
    let trimmed = content.trim();
    match network {
        "lesson" => format!(
            "learned procedure: {}",
            crate::trunc(strip_lesson_prefix(trimmed), 96)
        ),
        "knowledge" => format!("knowledge observed: {}", crate::trunc(trimmed, 96)),
        _ => format!("experience observed: {}", crate::trunc(trimmed, 96)),
    }
}

fn summarize_procedure_title(content: &str) -> String {
    let trimmed = strip_lesson_prefix(content).trim();
    let title = crate::trunc(trimmed, 72).trim();
    if title.is_empty() {
        "learned procedure".to_string()
    } else {
        title.to_string()
    }
}

fn strip_lesson_prefix(content: &str) -> &str {
    let trimmed = content.trim();
    if let Some(rest) = trimmed.strip_prefix("lesson:") {
        rest.trim()
    } else if let Some(rest) = trimmed.strip_prefix("growth lesson:") {
        rest.trim()
    } else {
        trimmed
    }
}

fn is_procedure_memory(network: &str, content: &str) -> bool {
    if network == "lesson" {
        return true;
    }
    let lower = content.trim().to_lowercase();
    lower.starts_with("lesson:") || lower.starts_with("growth lesson:")
}

pub(super) fn derive_memory_claim(network: &str, content: &str) -> Option<DerivedClaim> {
    let statement = content.trim();
    if statement.is_empty() || is_procedure_memory(network, statement) {
        return None;
    }

    let scope = if statement.to_lowercase().starts_with("vd ")
        || statement.to_lowercase().starts_with("vd's ")
        || statement.to_lowercase().starts_with("user ")
        || statement.to_lowercase().starts_with("user's ")
        || network == "experience"
    {
        "personal".to_string()
    } else if statement.to_lowercase().contains("nyx")
        || statement.to_lowercase().contains("project")
    {
        "project".to_string()
    } else {
        "global".to_string()
    };

    let kind = if statement.to_lowercase().contains("prefer")
        || statement.to_lowercase().contains("favorite")
    {
        "preference".to_string()
    } else {
        "fact".to_string()
    };

    let (subject, predicate, object) = split_statement_triplet(statement);
    Some(DerivedClaim {
        kind,
        scope,
        subject,
        predicate,
        object,
    })
}

fn split_statement_triplet(statement: &str) -> (String, String, String) {
    let lower = statement.to_lowercase();
    for verb in [
        " prefers ",
        " like ",
        " likes ",
        " lives ",
        " works ",
        " uses ",
        " needs ",
        " has ",
        " is ",
        " are ",
    ] {
        if let Some(index) = lower.find(verb) {
            let subject = normalize_subject(statement[..index].trim());
            let predicate = verb.trim().to_string();
            let object = statement[index + verb.len()..].trim().to_string();
            return (subject, predicate, object);
        }
    }
    (
        "general".to_string(),
        "states".to_string(),
        statement.trim().to_string(),
    )
}

fn normalize_subject(subject: &str) -> String {
    let normalized = subject
        .trim()
        .trim_matches(|c: char| !c.is_alphanumeric() && c != ' ' && c != '_')
        .to_lowercase();
    if normalized.starts_with("vd") || normalized.starts_with("user") {
        "user".to_string()
    } else if normalized.contains("nyx") || normalized.contains("project") {
        "project".to_string()
    } else if normalized.is_empty() {
        "general".to_string()
    } else {
        normalized
    }
}

pub(super) fn infer_entity_seed_from_claim(
    statement: &str,
    claim: &DerivedClaim,
) -> Option<MemoryEntitySeed> {
    if claim.subject == "user" {
        return Some(user_entity_seed());
    }

    if claim.subject == "project" {
        return Some(project_entity_seed());
    }

    if let Some(seed) = infer_entity_seed_from_text(statement) {
        return Some(seed);
    }

    let subject = claim.subject.trim();
    if subject.is_empty() || subject == "general" {
        return None;
    }
    let canonical_name = humanize_entity_name(subject);
    let entity_kind = if claim.scope == "project" {
        "project"
    } else if claim.scope == "personal" {
        "person"
    } else if subject.contains("api")
        || subject.contains("service")
        || subject.contains("company")
        || subject.contains("corp")
    {
        "organization"
    } else {
        "entity"
    };
    Some(MemoryEntitySeed {
        entity_key: format!("{}:{}", entity_kind, slugify_entity(subject)),
        entity_kind: entity_kind.to_string(),
        canonical_name,
        aliases: vec![subject.to_string()],
    })
}

pub(super) fn infer_entity_seed_from_text(text: &str) -> Option<MemoryEntitySeed> {
    let lower = text.to_lowercase();
    if lower.starts_with("vd ")
        || lower.starts_with("vd's ")
        || lower.starts_with("user ")
        || lower.starts_with("user's ")
        || lower.contains(" the user ")
        || lower.contains(" owner ")
    {
        return Some(user_entity_seed());
    }

    if lower.contains("nyx")
        || lower.starts_with("project ")
        || lower.contains(" project ")
        || lower.contains(" the project ")
    {
        return Some(project_entity_seed());
    }

    None
}

pub(super) fn clean_memory_query(query: &str) -> String {
    query
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .trim()
        .to_string()
}

pub(super) fn make_like_patterns(clean_query: &str) -> Vec<String> {
    if clean_query.is_empty() {
        return Vec::new();
    }
    let mut patterns = Vec::new();
    for word in clean_query.split_whitespace() {
        let normalized = word.trim().to_lowercase();
        if normalized.len() <= 2
            || MEMORY_QUERY_STOP_WORDS
                .iter()
                .any(|stop_word| *stop_word == normalized.as_str())
        {
            continue;
        }

        let pattern = format!("%{}%", normalized.replace('\'', "''"));
        if !patterns.contains(&pattern) {
            patterns.push(pattern);
        }
        if patterns.len() >= 4 {
            break;
        }
    }
    patterns
}

pub(super) fn query_entity_match_ids(
    conn: &Connection,
    query: &str,
    like_patterns: &[String],
    limit: usize,
) -> Vec<String> {
    let mut ids = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let clean_query = clean_memory_query(query).to_lowercase();
    let tokens: Vec<&str> = clean_query.split_whitespace().collect();

    let mut exact_keys = Vec::new();
    if tokens
        .iter()
        .any(|token| matches!(*token, "vd" | "user" | "owner" | "me" | "my" | "i"))
    {
        exact_keys.push("person:user");
    }
    if tokens
        .iter()
        .any(|token| matches!(*token, "nyx" | "project"))
    {
        exact_keys.push("project:nyx");
    }

    for key in exact_keys {
        if let Some(entity_id) = conn
            .query_row(
                "SELECT id FROM memory_entities WHERE entity_key = ?1 LIMIT 1",
                rusqlite::params![key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .unwrap_or(None)
        {
            if seen.insert(entity_id.clone()) {
                ids.push(entity_id);
            }
        }
    }

    if like_patterns.is_empty() {
        return ids;
    }

    let patterns: Vec<&str> = like_patterns.iter().map(String::as_str).collect();
    let mut stmt = conn
        .prepare(
            "SELECT id FROM memory_entities
             WHERE canonical_name LIKE ?1
                OR aliases_json LIKE ?1
                OR entity_key LIKE ?1
                OR canonical_name LIKE ?2
                OR aliases_json LIKE ?2
                OR entity_key LIKE ?2
                OR canonical_name LIKE ?3
                OR aliases_json LIKE ?3
                OR entity_key LIKE ?3
                OR canonical_name LIKE ?4
                OR aliases_json LIKE ?4
                OR entity_key LIKE ?4
             ORDER BY updated_at DESC
             LIMIT ?5",
        )
        .unwrap();
    let matched_ids: Vec<String> = stmt
        .query_map(
            rusqlite::params![
                patterns.first().copied().unwrap_or("__no_match__"),
                patterns.get(1).copied().unwrap_or("__no_match__"),
                patterns.get(2).copied().unwrap_or("__no_match__"),
                patterns.get(3).copied().unwrap_or("__no_match__"),
                limit as i64,
            ],
            |row| row.get(0),
        )
        .unwrap()
        .filter_map(|row| row.ok())
        .collect();
    for entity_id in matched_ids {
        if seen.insert(entity_id.clone()) {
            ids.push(entity_id);
        }
    }
    ids.truncate(limit);
    ids
}

fn humanize_entity_name(raw: &str) -> String {
    let cleaned = raw
        .trim()
        .trim_matches(|c: char| !c.is_alphanumeric() && c != ' ' && c != '_');
    if cleaned.is_empty() {
        return "Entity".to_string();
    }
    cleaned
        .split_whitespace()
        .map(|part| {
            if part.len() <= 3 && part.chars().all(|c| c.is_ascii_uppercase()) {
                part.to_string()
            } else if part.eq_ignore_ascii_case("api") {
                "API".to_string()
            } else {
                let mut chars = part.chars();
                match chars.next() {
                    Some(first) => {
                        format!("{}{}", first.to_uppercase(), chars.as_str().to_lowercase())
                    }
                    None => String::new(),
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn find_matching_memory_entity_static(
    conn: &Connection,
    seed: &MemoryEntitySeed,
) -> Result<Option<(String, String, String, String)>> {
    let seed_signatures = entity_identity_signatures(
        &seed.entity_key,
        &seed.entity_kind,
        &seed.canonical_name,
        &seed.aliases,
    );
    if seed_signatures.is_empty() {
        return Ok(None);
    }

    let seed_signature_set = seed_signatures.iter().cloned().collect::<HashSet<_>>();
    let mut stmt = conn.prepare(
        "SELECT id, entity_key, entity_kind, canonical_name, aliases_json
         FROM memory_entities
         ORDER BY updated_at DESC, created_at DESC",
    )?;
    let candidates: Vec<(String, String, String, String, String)> = stmt
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?
        .filter_map(|row| row.ok())
        .collect();

    let mut best: Option<(usize, String, String, String, String)> = None;
    for (entity_id, entity_key, entity_kind, canonical_name, aliases_json) in candidates {
        if !entity_kinds_compatible(&seed.entity_kind, &entity_kind) {
            continue;
        }
        let aliases = parse_entity_aliases(&aliases_json);
        let existing_signatures =
            entity_identity_signatures(&entity_key, &entity_kind, &canonical_name, &aliases);
        let overlap = existing_signatures
            .iter()
            .filter(|signature| seed_signature_set.contains(*signature))
            .count();
        if overlap == 0 {
            continue;
        }
        let score = overlap * 10
            + usize::from(entity_key == seed.entity_key) * 100
            + usize::from(canonical_name.eq_ignore_ascii_case(&seed.canonical_name)) * 20
            + usize::from(entity_kind == seed.entity_kind) * 5;
        if best
            .as_ref()
            .map(|(best_score, ..)| score > *best_score)
            .unwrap_or(true)
        {
            best = Some((score, entity_id, aliases_json, canonical_name, entity_kind));
        }
    }

    Ok(best.map(
        |(_, entity_id, aliases_json, canonical_name, entity_kind)| {
            (entity_id, aliases_json, canonical_name, entity_kind)
        },
    ))
}

fn slugify_entity(raw: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in raw.chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            last_dash = false;
        } else if !last_dash {
            slug.push('-');
            last_dash = true;
        }
    }
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        "general".to_string()
    } else {
        slug
    }
}

fn alias_signatures_for_value(value: &str, entity_kind: &str) -> Vec<String> {
    let normalized = normalize_entity_alias(value);
    if normalized.is_empty() {
        return Vec::new();
    }

    let mut signatures = vec![normalized.clone()];
    let simplified = simplify_entity_alias(&normalized, entity_kind);
    if !simplified.is_empty() && simplified != normalized {
        signatures.push(simplified);
    }
    signatures.sort();
    signatures.dedup();
    signatures
}

fn should_keep_entity_signature(signature: &str) -> bool {
    if signature.len() < 2 {
        return false;
    }
    !matches!(
        signature,
        "entity"
            | "organization"
            | "person"
            | "project"
            | "general"
            | "system"
            | "app"
            | "platform"
            | "user"
            | "owner"
            | "company"
            | "corp"
    )
}

fn normalize_entity_alias(raw: &str) -> String {
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

fn simplify_entity_alias(normalized: &str, entity_kind: &str) -> String {
    let mut tokens = normalized
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return String::new();
    }

    let suffixes: &[&str] = match entity_kind.trim().to_ascii_lowercase().as_str() {
        "project" => &["project", "app", "system"],
        "organization" | "entity" => &[
            "corp",
            "corporation",
            "company",
            "co",
            "inc",
            "incorporated",
            "llc",
            "ltd",
            "limited",
            "plc",
        ],
        _ => &[],
    };
    while tokens
        .last()
        .map(|token| suffixes.iter().any(|suffix| token == suffix))
        .unwrap_or(false)
    {
        tokens.pop();
    }
    tokens.join(" ")
}

fn user_entity_seed() -> MemoryEntitySeed {
    MemoryEntitySeed {
        entity_key: "person:user".to_string(),
        entity_kind: "person".to_string(),
        canonical_name: "User".to_string(),
        aliases: vec!["VD".to_string(), "user".to_string(), "owner".to_string()],
    }
}

fn project_entity_seed() -> MemoryEntitySeed {
    MemoryEntitySeed {
        entity_key: "project:nyx".to_string(),
        entity_kind: "project".to_string(),
        canonical_name: "Nyx".to_string(),
        aliases: vec!["nyx".to_string(), "project".to_string()],
    }
}

pub(super) fn parse_entity_aliases(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

pub(super) fn entity_identity_signatures(
    entity_key: &str,
    entity_kind: &str,
    canonical_name: &str,
    aliases: &[String],
) -> Vec<String> {
    let mut signatures = HashSet::new();
    let mut values = aliases.to_vec();
    values.push(canonical_name.to_string());
    if let Some((_, suffix)) = entity_key.split_once(':') {
        values.push(suffix.replace('-', " "));
    }

    for value in values {
        for signature in alias_signatures_for_value(&value, entity_kind) {
            if should_keep_entity_signature(&signature) {
                signatures.insert(signature);
            }
        }
    }

    let mut collected = signatures.into_iter().collect::<Vec<_>>();
    collected.sort();
    collected
}

pub(super) fn entity_kinds_compatible(left: &str, right: &str) -> bool {
    let left = left.trim().to_ascii_lowercase();
    let right = right.trim().to_ascii_lowercase();
    if left == right {
        return true;
    }
    matches!(
        (left.as_str(), right.as_str()),
        ("entity", _) | (_, "entity") | ("organization", "project") | ("project", "organization")
    )
}

pub(super) fn preferred_entity_kind(current: &str, incoming: &str) -> String {
    fn rank(kind: &str) -> usize {
        match kind.trim().to_ascii_lowercase().as_str() {
            "project" => 5,
            "person" => 4,
            "organization" => 3,
            "entity" => 2,
            "" => 0,
            _ => 1,
        }
    }

    if rank(incoming) > rank(current) {
        incoming.trim().to_string()
    } else {
        current.trim().to_string()
    }
}

pub(super) fn query_claim_matches(
    conn: &Connection,
    like_patterns: &[String],
    limit: usize,
) -> Vec<MemoryClaimRecord> {
    if like_patterns.is_empty() {
        let mut stmt = conn
            .prepare(
                "SELECT * FROM memory_claims
                 WHERE status IN ('active', 'stale')
                 ORDER BY importance DESC, confidence DESC, updated_at DESC
                 LIMIT ?1",
            )
            .unwrap();
        return stmt
            .query_map(rusqlite::params![limit as i64], map_memory_claim_row)
            .unwrap()
            .filter_map(|row| row.ok())
            .collect();
    }

    let patterns: Vec<&str> = like_patterns.iter().map(String::as_str).collect();
    let mut stmt = conn
        .prepare(
            "SELECT * FROM memory_claims
             WHERE status IN ('active', 'stale')
               AND (
                    statement LIKE ?1 OR subject LIKE ?1 OR object LIKE ?1
                 OR statement LIKE ?2 OR subject LIKE ?2 OR object LIKE ?2
                 OR statement LIKE ?3 OR subject LIKE ?3 OR object LIKE ?3
                 OR statement LIKE ?4 OR subject LIKE ?4 OR object LIKE ?4
               )
             ORDER BY importance DESC, confidence DESC, updated_at DESC
             LIMIT ?5",
        )
        .unwrap();
    stmt.query_map(
        rusqlite::params![
            patterns.first().copied().unwrap_or("__no_match__"),
            patterns.get(1).copied().unwrap_or("__no_match__"),
            patterns.get(2).copied().unwrap_or("__no_match__"),
            patterns.get(3).copied().unwrap_or("__no_match__"),
            limit as i64,
        ],
        map_memory_claim_row,
    )
    .unwrap()
    .filter_map(|row| row.ok())
    .collect()
}

pub(super) fn query_procedure_matches(
    conn: &Connection,
    like_patterns: &[String],
    limit: usize,
) -> Vec<MemoryProcedureRecord> {
    if like_patterns.is_empty() {
        let mut stmt = conn
            .prepare(
                "SELECT * FROM memory_procedures
                 WHERE status = 'active'
                 ORDER BY importance DESC, updated_at DESC
                 LIMIT ?1",
            )
            .unwrap();
        return stmt
            .query_map(rusqlite::params![limit as i64], map_memory_procedure_row)
            .unwrap()
            .filter_map(|row| row.ok())
            .collect();
    }

    let patterns: Vec<&str> = like_patterns.iter().map(String::as_str).collect();
    let mut stmt = conn
        .prepare(
            "SELECT * FROM memory_procedures
             WHERE status = 'active'
               AND (
                    title LIKE ?1 OR content LIKE ?1
                 OR title LIKE ?2 OR content LIKE ?2
                 OR title LIKE ?3 OR content LIKE ?3
                 OR title LIKE ?4 OR content LIKE ?4
               )
             ORDER BY importance DESC, updated_at DESC
             LIMIT ?5",
        )
        .unwrap();
    stmt.query_map(
        rusqlite::params![
            patterns.first().copied().unwrap_or("__no_match__"),
            patterns.get(1).copied().unwrap_or("__no_match__"),
            patterns.get(2).copied().unwrap_or("__no_match__"),
            patterns.get(3).copied().unwrap_or("__no_match__"),
            limit as i64,
        ],
        map_memory_procedure_row,
    )
    .unwrap()
    .filter_map(|row| row.ok())
    .collect()
}

pub(super) fn query_episode_matches(
    conn: &Connection,
    like_patterns: &[String],
    limit: usize,
) -> Vec<MemoryEpisodeRecord> {
    if like_patterns.is_empty() {
        let mut stmt = conn
            .prepare(
                "SELECT * FROM memory_episodes
                 ORDER BY created_at DESC, importance DESC
                 LIMIT ?1",
            )
            .unwrap();
        return stmt
            .query_map(rusqlite::params![limit as i64], map_memory_episode_row)
            .unwrap()
            .filter_map(|row| row.ok())
            .collect();
    }

    let patterns: Vec<&str> = like_patterns.iter().map(String::as_str).collect();
    let mut stmt = conn
        .prepare(
            "SELECT * FROM memory_episodes
             WHERE (
                    summary LIKE ?1 OR content LIKE ?1
                 OR summary LIKE ?2 OR content LIKE ?2
                 OR summary LIKE ?3 OR content LIKE ?3
                 OR summary LIKE ?4 OR content LIKE ?4
               )
             ORDER BY created_at DESC, importance DESC
             LIMIT ?5",
        )
        .unwrap();
    stmt.query_map(
        rusqlite::params![
            patterns.first().copied().unwrap_or("__no_match__"),
            patterns.get(1).copied().unwrap_or("__no_match__"),
            patterns.get(2).copied().unwrap_or("__no_match__"),
            patterns.get(3).copied().unwrap_or("__no_match__"),
            limit as i64,
        ],
        map_memory_episode_row,
    )
    .unwrap()
    .filter_map(|row| row.ok())
    .collect()
}

pub(super) fn claim_record_is_stale(claim: &MemoryClaimRecord) -> bool {
    let Some(ttl_secs) = claim.freshness_ttl_secs else {
        return false;
    };
    if ttl_secs <= 0 {
        return true;
    }
    let anchor = claim
        .valid_from
        .as_deref()
        .and_then(parse_db_timestamp)
        .or_else(|| parse_db_timestamp(&claim.created_at));
    let Some(anchor) = anchor else {
        return false;
    };
    let age = Utc::now().naive_utc() - anchor;
    age.num_seconds() >= ttl_secs
}

pub(super) fn db_timestamp_now() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

pub(super) fn offset_db_timestamp(raw: &str, offset_secs: i64) -> Option<String> {
    let base = parse_db_timestamp(raw)?;
    Some(
        (base + chrono::Duration::seconds(offset_secs))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
    )
}

fn parse_db_timestamp(raw: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S").ok()
}
