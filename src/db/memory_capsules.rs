use std::collections::{HashMap, HashSet};

use anyhow::Result;
use rusqlite::Connection;

use super::{Db, MemoryCapsuleAnchorRecord, MemorySessionCapsuleRecord};

const CAPSULE_WINDOW_MESSAGES: usize = 6;
const CAPSULE_MARKER_TERMS: &[&str] = &[
    "prefer", "favorite", "usually", "use", "using", "switched", "moved", "failed", "fix", "fixed",
    "repair", "build", "project", "plan", "decided", "because", "memory", "refresh",
];

const CAPSULE_STOP_WORDS: &[&str] = &[
    "what",
    "when",
    "where",
    "which",
    "who",
    "whom",
    "whose",
    "with",
    "from",
    "that",
    "this",
    "then",
    "than",
    "have",
    "has",
    "had",
    "were",
    "was",
    "into",
    "about",
    "after",
    "before",
    "your",
    "their",
    "there",
    "would",
    "could",
    "should",
    "just",
    "really",
    "been",
    "being",
    "them",
    "they",
    "you",
    "our",
    "out",
    "for",
    "and",
    "the",
    "are",
    "did",
    "does",
    "how",
    "why",
    "his",
    "her",
    "she",
    "him",
    "its",
    "too",
    "can",
    "get",
    "got",
    "use",
    "using",
    "used",
    "want",
    "like",
    "need",
    "help",
    "some",
    "more",
    "than",
    "also",
    "over",
    "under",
    "month",
    "week",
    "year",
    "years",
    "days",
    "last",
    "next",
    "first",
    "second",
    "user",
    "assistant",
    "nyx",
];

#[derive(Debug, Clone)]
struct MessageCapsuleDraft {
    id: String,
    source_message_id: i64,
    session_key: String,
    channel: String,
    summary: String,
    keyphrases: Vec<String>,
    entity_markers: Vec<String>,
    marker_terms: Vec<String>,
    message_count: i64,
    last_message_at: String,
    anchors: Vec<(i64, String, String, String)>,
    search_text: String,
}

impl Db {
    pub(super) fn capture_message_capsule_static(
        conn: &Connection,
        channel: &str,
        source_message_id: i64,
    ) -> Result<Option<String>> {
        let mut stmt = conn.prepare(
            "SELECT id, role, content, timestamp
             FROM messages
             WHERE channel = ?1 AND id <= ?2
             ORDER BY id DESC
             LIMIT ?3",
        )?;
        let mut rows: Vec<(i64, String, String, String)> = stmt
            .query_map(
                rusqlite::params![channel, source_message_id, CAPSULE_WINDOW_MESSAGES as i64],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?
            .filter_map(|row| row.ok())
            .collect();
        rows.reverse();

        let Some(draft) = build_message_capsule(channel, source_message_id, rows) else {
            return Ok(None);
        };

        conn.execute(
            "INSERT OR REPLACE INTO memory_session_capsules (
                id, source_message_id, session_key, channel, summary,
                keyphrases_json, entity_markers_json, marker_terms_json,
                message_count, last_message_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, datetime('now'))",
            rusqlite::params![
                draft.id,
                draft.source_message_id,
                draft.session_key,
                draft.channel,
                draft.summary,
                serde_json::to_string(&draft.keyphrases)?,
                serde_json::to_string(&draft.entity_markers)?,
                serde_json::to_string(&draft.marker_terms)?,
                draft.message_count,
                draft.last_message_at,
            ],
        )?;
        conn.execute(
            "DELETE FROM memory_capsule_anchors WHERE capsule_id = ?1",
            rusqlite::params![draft.id],
        )?;
        for (anchor_index, role, content, created_at) in &draft.anchors {
            conn.execute(
                "INSERT INTO memory_capsule_anchors (
                    capsule_id, anchor_index, role, content, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![draft.id, anchor_index, role, content, created_at],
            )?;
        }
        conn.execute(
            "DELETE FROM memory_session_capsules_fts WHERE capsule_id = ?1",
            rusqlite::params![draft.id],
        )
        .ok();
        conn.execute(
            "INSERT INTO memory_session_capsules_fts (search_text, capsule_id) VALUES (?1, ?2)",
            rusqlite::params![draft.search_text, draft.id],
        )?;

        Ok(Some(draft.id))
    }

    pub fn list_recent_memory_capsules(
        &self,
        limit: usize,
    ) -> Result<Vec<MemorySessionCapsuleRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM memory_session_capsules
             ORDER BY last_message_at DESC, updated_at DESC
             LIMIT ?1",
        )?;
        let capsules: Vec<MemorySessionCapsuleRecord> = stmt
            .query_map(rusqlite::params![limit as i64], map_memory_capsule_row)?
            .filter_map(|row| row.ok())
            .collect();
        hydrate_capsules(&conn, capsules)
    }

    pub fn recall_memory_capsules(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemorySessionCapsuleRecord>> {
        let fts_query = capsule_fts_query(query);
        if fts_query.is_empty() {
            return self.list_recent_memory_capsules(limit);
        }
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT c.*
             FROM memory_session_capsules_fts f
             JOIN memory_session_capsules c ON c.id = f.capsule_id
             WHERE memory_session_capsules_fts MATCH ?1
             ORDER BY bm25(memory_session_capsules_fts), c.last_message_at DESC
             LIMIT ?2",
        )?;
        let mut capsules: Vec<MemorySessionCapsuleRecord> = stmt
            .query_map(
                rusqlite::params![fts_query, limit as i64],
                map_memory_capsule_row,
            )?
            .filter_map(|row| row.ok())
            .collect();
        if capsules.is_empty() {
            capsules = fallback_capsule_matches(&conn, query, limit)?;
        }
        hydrate_capsules(&conn, capsules)
    }
}

fn build_message_capsule(
    channel: &str,
    source_message_id: i64,
    rows: Vec<(i64, String, String, String)>,
) -> Option<MessageCapsuleDraft> {
    if rows.is_empty() {
        return None;
    }

    let anchors: Vec<(i64, String, String, String)> = rows
        .iter()
        .enumerate()
        .map(|(idx, (_id, role, content, timestamp))| {
            (
                idx as i64,
                role.clone(),
                content.trim().to_string(),
                timestamp.clone(),
            )
        })
        .collect();
    let latest_user = rows
        .iter()
        .rev()
        .find(|(_, role, _, _)| role == "user")
        .map(|(_, _, content, _)| content.trim().to_string());
    let latest_assistant = rows
        .iter()
        .rev()
        .find(|(_, role, _, _)| role == "assistant")
        .map(|(_, _, content, _)| content.trim().to_string());

    let mut summary_parts = Vec::new();
    if let Some(user) = latest_user.as_deref() {
        summary_parts.push(format!("user asked: {}", crate::trunc(user, 140).trim()));
    }
    if let Some(reply) = latest_assistant.as_deref() {
        summary_parts.push(format!("nyx replied: {}", crate::trunc(reply, 140).trim()));
    }
    if summary_parts.is_empty() {
        let fallback = rows
            .last()
            .map(|(_, _, content, _)| content.as_str())
            .unwrap_or("");
        summary_parts.push(crate::trunc(fallback, 140).trim().to_string());
    }
    let summary = summary_parts.join(" | ");

    let anchor_text = anchors
        .iter()
        .map(|(_, role, content, _)| format!("{} {}", role, content))
        .collect::<Vec<_>>()
        .join("\n");
    let keyphrases = extract_keyphrases(&anchor_text);
    let entity_markers = extract_entity_markers(&anchor_text);
    let marker_terms = extract_marker_terms(&anchor_text);
    let search_text = [
        summary.clone(),
        anchor_text,
        keyphrases.join(" "),
        entity_markers.join(" "),
        marker_terms.join(" "),
    ]
    .join("\n");
    let last_message_at = rows
        .last()
        .map(|(_, _, _, timestamp)| timestamp.clone())
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string());

    Some(MessageCapsuleDraft {
        id: uuid::Uuid::new_v4().to_string(),
        source_message_id,
        session_key: format!("{}:{}", channel, source_message_id),
        channel: channel.to_string(),
        summary,
        keyphrases,
        entity_markers,
        marker_terms,
        message_count: rows.len() as i64,
        last_message_at,
        anchors,
        search_text,
    })
}

fn hydrate_capsules(
    conn: &Connection,
    mut capsules: Vec<MemorySessionCapsuleRecord>,
) -> Result<Vec<MemorySessionCapsuleRecord>> {
    for capsule in &mut capsules {
        capsule.anchors = fetch_capsule_anchors(conn, &capsule.id)?;
    }
    Ok(capsules)
}

fn fetch_capsule_anchors(
    conn: &Connection,
    capsule_id: &str,
) -> Result<Vec<MemoryCapsuleAnchorRecord>> {
    let mut stmt = conn.prepare(
        "SELECT anchor_index, role, content, created_at
         FROM memory_capsule_anchors
         WHERE capsule_id = ?1
         ORDER BY anchor_index ASC",
    )?;
    let anchors = stmt
        .query_map(rusqlite::params![capsule_id], |row| {
            Ok(MemoryCapsuleAnchorRecord {
                anchor_index: row.get(0)?,
                role: row.get(1)?,
                content: row.get(2)?,
                created_at: row.get(3)?,
            })
        })?
        .filter_map(|row| row.ok())
        .collect();
    Ok(anchors)
}

fn map_memory_capsule_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemorySessionCapsuleRecord> {
    let keyphrases_json: String = row.get("keyphrases_json")?;
    let entity_markers_json: String = row.get("entity_markers_json")?;
    let marker_terms_json: String = row.get("marker_terms_json")?;
    Ok(MemorySessionCapsuleRecord {
        id: row.get("id")?,
        source_message_id: row.get("source_message_id")?,
        session_key: row.get("session_key")?,
        channel: row.get("channel")?,
        summary: row.get("summary")?,
        keyphrases: parse_string_vec(&keyphrases_json),
        entity_markers: parse_string_vec(&entity_markers_json),
        marker_terms: parse_string_vec(&marker_terms_json),
        message_count: row.get("message_count")?,
        last_message_at: row.get("last_message_at")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        anchors: Vec::new(),
    })
}

fn fallback_capsule_matches(
    conn: &Connection,
    query: &str,
    limit: usize,
) -> Result<Vec<MemorySessionCapsuleRecord>> {
    let clean = clean_capsule_query(query);
    let like = format!("%{}%", clean.replace('"', "").replace('\'', "''"));
    let mut stmt = conn.prepare(
        "SELECT * FROM memory_session_capsules
         WHERE summary LIKE ?1
            OR keyphrases_json LIKE ?1
            OR entity_markers_json LIKE ?1
            OR marker_terms_json LIKE ?1
         ORDER BY last_message_at DESC
         LIMIT ?2",
    )?;
    let capsules = stmt
        .query_map(
            rusqlite::params![like, limit as i64],
            map_memory_capsule_row,
        )?
        .filter_map(|row| row.ok())
        .collect();
    Ok(capsules)
}

fn clean_capsule_query(query: &str) -> String {
    query
        .chars()
        .filter(|ch| ch.is_alphanumeric() || ch.is_whitespace())
        .collect::<String>()
        .trim()
        .to_lowercase()
}

fn capsule_fts_query(query: &str) -> String {
    let terms: Vec<String> = clean_capsule_query(query)
        .split_whitespace()
        .filter(|term| term.len() > 2)
        .take(6)
        .map(str::to_string)
        .collect();
    if terms.is_empty() {
        String::new()
    } else {
        terms.join(" OR ")
    }
}

fn extract_keyphrases(input: &str) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for token in tokenize(input) {
        *counts.entry(token).or_insert(0) += 1;
    }
    let mut ranked: Vec<(String, usize)> = counts.into_iter().collect();
    ranked.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.0.len().cmp(&a.0.len()))
            .then_with(|| a.0.cmp(&b.0))
    });
    ranked
        .into_iter()
        .take(12)
        .map(|(token, _)| token)
        .collect()
}

fn extract_entity_markers(input: &str) -> Vec<String> {
    let mut markers = HashSet::new();
    for token in input.split_whitespace() {
        let trimmed = token
            .trim_matches(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-' && ch != '.');
        if trimmed.len() < 3 {
            continue;
        }
        let lower = trimmed.to_lowercase();
        if CAPSULE_STOP_WORDS
            .iter()
            .any(|stop_word| *stop_word == lower.as_str())
        {
            continue;
        }
        let has_entity_signal = trimmed.chars().any(|ch| ch.is_uppercase())
            || trimmed.contains('_')
            || trimmed.contains('-')
            || trimmed.contains('.')
            || matches!(
                lower.as_str(),
                "nyx" | "sqlite" | "tokio" | "rust" | "forge" | "mcp"
            );
        if has_entity_signal {
            markers.insert(lower);
        }
    }
    let mut out: Vec<String> = markers.into_iter().collect();
    out.sort();
    out.truncate(10);
    out
}

fn extract_marker_terms(input: &str) -> Vec<String> {
    let lower = input.to_lowercase();
    CAPSULE_MARKER_TERMS
        .iter()
        .filter(|marker| lower.contains(**marker))
        .map(|marker| (*marker).to_string())
        .collect()
}

fn tokenize(input: &str) -> Vec<String> {
    input
        .split(|ch: char| !ch.is_alphanumeric())
        .filter_map(|token| {
            let trimmed = token.trim().to_lowercase();
            if trimmed.len() < 3
                || CAPSULE_STOP_WORDS
                    .iter()
                    .any(|stop_word| *stop_word == trimmed.as_str())
            {
                None
            } else {
                Some(trimmed)
            }
        })
        .collect()
}

fn parse_string_vec(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}
