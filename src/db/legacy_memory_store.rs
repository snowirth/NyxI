use anyhow::Result;
use rusqlite::Connection;

use super::Db;

impl Db {
    /// Store a memory. Returns the memory ID if stored, None if rejected by quality gate/dedup.
    /// Detects contradictions: if a new fact supersedes an old one in the same network,
    /// the old memory is marked as superseded rather than keeping both.
    pub fn remember(
        &self,
        content: &str,
        network: &str,
        importance: f64,
    ) -> Result<Option<String>> {
        self.remember_with_date(content, network, importance, None)
    }

    /// Like remember() but with an optional event_date for temporal grounding.
    /// event_date represents when the event *happened*, not when it was stored.
    /// Example: "going to Japan next month" stored today -> event_date = next month.
    pub fn remember_with_date(
        &self,
        content: &str,
        network: &str,
        importance: f64,
        event_date: Option<&str>,
    ) -> Result<Option<String>> {
        let lower = content.to_lowercase();
        if content.len() < 10 || lower.contains("no useful results") || lower.contains("error:") {
            return Ok(None);
        }

        let conn = self.conn.lock().unwrap();

        let existing: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM memories WHERE content = ?1 AND superseded_by IS NULL)",
                rusqlite::params![content],
                |r| r.get(0),
            )
            .unwrap_or(false);
        if existing {
            return Ok(None);
        }

        let new_id = uuid::Uuid::new_v4().to_string();
        let words: Vec<&str> = lower
            .split_whitespace()
            .filter(|w| w.len() > 3)
            .take(5)
            .collect();

        let mut superseded_ids: Vec<String> = Vec::new();

        if words.len() >= 2 {
            let like_clauses: Vec<String> = words
                .iter()
                .map(|w| format!("LOWER(content) LIKE '%{}%'", w.replace('\'', "''")))
                .collect();
            let sql = format!(
                "SELECT id, content FROM memories \
                 WHERE network = ?1 AND superseded_by IS NULL AND ({}) \
                 ORDER BY created_at DESC LIMIT 10",
                like_clauses.join(" OR ")
            );
            if let Ok(mut stmt) = conn.prepare(&sql) {
                let candidates: Vec<(String, String)> = stmt
                    .query_map(rusqlite::params![network], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .unwrap_or_else(|_| panic!())
                    .filter_map(|r| r.ok())
                    .collect();

                for (old_id, old_content) in &candidates {
                    if is_contradiction(old_content, content) {
                        conn.execute(
                            "UPDATE memories SET superseded_by = ?1 WHERE id = ?2",
                            rusqlite::params![new_id, old_id],
                        )
                        .ok();
                        superseded_ids.push(old_id.clone());
                        tracing::info!(
                            "memory superseded: \"{}\" -> \"{}\"",
                            crate::trunc(old_content, 40),
                            crate::trunc(content, 40)
                        );
                    }
                }
            }
        }

        conn.execute(
            "INSERT INTO memories (id, content, network, importance, event_date) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![new_id, content, network, importance, event_date],
        )?;
        for old_id in &superseded_ids {
            conn.execute(
                "INSERT INTO memory_links (source_id, target_id, link_type) VALUES (?1, ?2, 'updates')",
                rusqlite::params![new_id, old_id],
            )
            .ok();
        }

        Self::promote_legacy_memory_static(
            &conn,
            &new_id,
            content,
            network,
            importance,
            event_date,
            &superseded_ids,
        );

        Ok(Some(new_id))
    }

    /// Full-text search memories using FTS5. Falls back to LIKE if FTS fails.
    /// Filters out superseded memories. Bumps access stats on returned results.
    pub fn recall(&self, query: &str, limit: usize) -> Vec<String> {
        let conn = self.conn.lock().unwrap();

        let clean_query: String = query
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace())
            .collect();
        let clean_query = clean_query.trim();

        if clean_query.is_empty() {
            let mut stmt = conn
                .prepare(
                    "SELECT id, content FROM memories WHERE superseded_by IS NULL \
                     ORDER BY importance DESC LIMIT ?1",
                )
                .unwrap();
            let results: Vec<(String, String)> = stmt
                .query_map(rusqlite::params![limit as i64], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
            bump_access_static(&conn, &results);
            return results.into_iter().map(|(_, c)| c).collect();
        }

        let fts_result: Result<Vec<(String, String)>, rusqlite::Error> = (|| {
            let mut stmt = conn.prepare(
                "SELECT m.id, m.content FROM memories_fts \
                 JOIN memories m ON m.rowid = memories_fts.rowid \
                 WHERE memories_fts MATCH ?1 AND m.superseded_by IS NULL \
                 ORDER BY (bm25(memories_fts) * (1.0 + m.importance)) \
                 LIMIT ?2",
            )?;
            let results: Vec<(String, String)> = stmt
                .query_map(rusqlite::params![clean_query, limit as i64], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(results)
        })();

        match fts_result {
            Ok(results) if !results.is_empty() => {
                bump_access_static(&conn, &results);
                results.into_iter().map(|(_, c)| c).collect()
            }
            _ => {
                let words: Vec<&str> = clean_query
                    .split_whitespace()
                    .filter(|w| w.len() > 2)
                    .take(3)
                    .collect();
                if words.is_empty() {
                    let mut stmt = conn
                        .prepare(
                            "SELECT id, content FROM memories WHERE superseded_by IS NULL \
                             ORDER BY importance DESC LIMIT ?1",
                        )
                        .unwrap();
                    let results: Vec<(String, String)> = stmt
                        .query_map(rusqlite::params![limit as i64], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })
                        .unwrap()
                        .filter_map(|r| r.ok())
                        .collect();
                    bump_access_static(&conn, &results);
                    return results.into_iter().map(|(_, c)| c).collect();
                }
                let like_clauses: Vec<String> = words
                    .iter()
                    .map(|w| format!("content LIKE '%{}%'", w.replace('\'', "''")))
                    .collect();
                let sql = format!(
                    "SELECT id, content FROM memories WHERE superseded_by IS NULL AND ({}) \
                     ORDER BY importance DESC LIMIT ?1",
                    like_clauses.join(" OR ")
                );
                let mut stmt = conn.prepare(&sql).unwrap();
                let results: Vec<(String, String)> = stmt
                    .query_map(rusqlite::params![limit as i64], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                bump_access_static(&conn, &results);
                results.into_iter().map(|(_, c)| c).collect()
            }
        }
    }

    pub fn get_memory_content(&self, memory_id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT content FROM memories WHERE id = ?1",
            rusqlite::params![memory_id],
            |row| row.get(0),
        );
        match result {
            Ok(content) => Ok(Some(content)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Three-tier memory recall with actual content search
    /// Three-tier memory recall with content search. Excludes superseded memories.
    pub fn recall_smart(&self, query: &str, tier: &str, limit: usize) -> Vec<String> {
        let conn = self.conn.lock().unwrap();
        let network = match tier {
            "personal" => "experience",
            "task" => "knowledge",
            "tool" => "lesson",
            _ => "",
        };

        let clean_query: String = query
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace())
            .collect();
        let clean_query = clean_query.trim();

        if clean_query.is_empty() || network.is_empty() {
            if network.is_empty() {
                let mut stmt = conn
                    .prepare(
                        "SELECT content FROM memories WHERE superseded_by IS NULL ORDER BY importance DESC LIMIT ?1",
                    )
                    .unwrap();
                return stmt
                    .query_map(rusqlite::params![limit as i64], |row| row.get(0))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
            }
            let mut stmt = conn
                .prepare(
                    "SELECT content FROM memories WHERE network = ?1 AND superseded_by IS NULL ORDER BY importance DESC LIMIT ?2",
                )
                .unwrap();
            return stmt
                .query_map(rusqlite::params![network, limit as i64], |row| row.get(0))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
        }

        let fts_result: Result<Vec<String>, rusqlite::Error> = (|| {
            let mut stmt = conn.prepare(
                "SELECT m.content FROM memories_fts \
                 JOIN memories m ON m.rowid = memories_fts.rowid \
                 WHERE memories_fts MATCH ?1 AND m.network = ?2 AND m.superseded_by IS NULL \
                 ORDER BY (bm25(memories_fts) * (1.0 + m.importance)) \
                 LIMIT ?3",
            )?;
            let results: Vec<String> = stmt
                .query_map(
                    rusqlite::params![clean_query, network, limit as i64],
                    |row| row.get(0),
                )?
                .filter_map(|r| r.ok())
                .collect();
            Ok(results)
        })();

        match fts_result {
            Ok(results) if !results.is_empty() => results,
            _ => {
                let mut stmt = conn
                    .prepare(
                        "SELECT content FROM memories WHERE network = ?1 AND superseded_by IS NULL ORDER BY importance DESC LIMIT ?2",
                    )
                    .unwrap();
                stmt.query_map(rusqlite::params![network, limit as i64], |row| row.get(0))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect()
            }
        }
    }

    /// Create a relationship between two memories.
    /// link_type should be one of: 'updates', 'extends', 'derives'.
    pub fn add_memory_link(&self, source_id: &str, target_id: &str, link_type: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO memory_links (source_id, target_id, link_type) VALUES (?1, ?2, ?3)",
            rusqlite::params![source_id, target_id, link_type],
        )
        .ok();
    }

    /// Get all memories related to a given memory (in either direction).
    /// Returns Vec of (content, link_type, linked_memory_content).
    pub fn get_related_memories(&self, memory_id: &str) -> Vec<(String, String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT m_source.content, l.link_type, m_target.content \
                 FROM memory_links l \
                 JOIN memories m_source ON m_source.id = l.source_id \
                 JOIN memories m_target ON m_target.id = l.target_id \
                 WHERE l.source_id = ?1 OR l.target_id = ?1 \
                 ORDER BY l.created_at DESC",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![memory_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
    }
}

fn bump_access_static(conn: &Connection, memories: &[(String, String)]) {
    for (id, _) in memories {
        conn.execute(
            "UPDATE memories SET last_accessed = datetime('now'), access_count = access_count + 1 WHERE id = ?1",
            rusqlite::params![id],
        )
        .ok();
    }
}

fn is_contradiction(old: &str, new: &str) -> bool {
    let old_lower = old.to_lowercase();
    let new_lower = new.to_lowercase();

    let prefixes = [
        "lives in ",
        "moved to ",
        "works at ",
        "works for ",
        "favorite ",
        "prefers ",
        "uses ",
        "is a ",
        "is an ",
        "switched to ",
        "changed to ",
    ];

    for prefix in &prefixes {
        let old_has = old_lower.find(prefix);
        let new_has = new_lower.find(prefix);

        if let (Some(oi), Some(ni)) = (old_has, new_has) {
            let old_val = &old_lower[oi + prefix.len()..];
            let new_val = &new_lower[ni + prefix.len()..];
            let old_val = old_val
                .split(|c: char| c == '.' || c == ',' || c == '\n')
                .next()
                .unwrap_or("")
                .trim();
            let new_val = new_val
                .split(|c: char| c == '.' || c == ',' || c == '\n')
                .next()
                .unwrap_or("")
                .trim();
            if !old_val.is_empty() && !new_val.is_empty() && old_val != new_val {
                return true;
            }
        }
    }

    false
}
