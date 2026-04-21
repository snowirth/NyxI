use anyhow::Result;

use super::Db;

impl Db {
    pub fn store_message(&self, channel: &str, role: &str, content: &str) {
        let conn = self.conn.lock().unwrap();
        if conn
            .execute(
                "INSERT INTO messages (channel, role, content) VALUES (?1, ?2, ?3)",
                rusqlite::params![channel, role, content],
            )
            .is_ok()
        {
            let message_id = conn.last_insert_rowid();
            if role == "assistant" {
                Self::capture_message_capsule_static(&conn, channel, message_id).ok();
            }
        }
    }

    pub fn get_history(&self, channel: &str, limit: usize) -> Vec<(String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT role, content FROM messages WHERE channel = ?1 ORDER BY id DESC LIMIT ?2",
            )
            .unwrap();
        let mut results: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![channel, limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        results.reverse();
        results
    }

    pub fn get_history_by_prefix(&self, prefix: &str, limit: usize) -> Vec<(String, String)> {
        let conn = self.conn.lock().unwrap();
        let pattern = format!("{}%", prefix);
        let mut stmt = conn
            .prepare(
                "SELECT role, content FROM messages WHERE channel LIKE ?1 ORDER BY id DESC LIMIT ?2",
            )
            .unwrap();
        let mut results: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![pattern, limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        results.reverse();
        results
    }

    pub fn get_history_with_time(
        &self,
        channel: &str,
        limit: usize,
    ) -> Vec<(String, String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT role, content, timestamp FROM messages WHERE channel = ?1 ORDER BY id DESC LIMIT ?2",
            )
            .unwrap();
        let mut results: Vec<(String, String, String)> = stmt
            .query_map(rusqlite::params![channel, limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        results.reverse();
        results
    }

    pub fn get_history_with_meta(
        &self,
        channel: &str,
        limit: usize,
    ) -> Vec<(i64, String, String, String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, channel, role, content, timestamp \
                 FROM messages WHERE channel = ?1 ORDER BY id DESC LIMIT ?2",
            )
            .unwrap();
        let mut results: Vec<(i64, String, String, String, String)> = stmt
            .query_map(rusqlite::params![channel, limit as i64], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        results.reverse();
        results
    }

    pub fn get_history_with_meta_by_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Vec<(i64, String, String, String, String)> {
        let conn = self.conn.lock().unwrap();
        let pattern = format!("{}%", prefix);
        let mut stmt = conn
            .prepare(
                "SELECT id, channel, role, content, timestamp \
                 FROM messages WHERE channel LIKE ?1 ORDER BY id DESC LIMIT ?2",
            )
            .unwrap();
        let mut results: Vec<(i64, String, String, String, String)> = stmt
            .query_map(rusqlite::params![pattern, limit as i64], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        results.reverse();
        results
    }

    pub fn add_reminder(&self, content: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO reminders (content) VALUES (?1)",
            rusqlite::params![content],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get_reminders(&self, include_done: bool) -> Vec<(i64, String, bool)> {
        let conn = self.conn.lock().unwrap();
        let sql = if include_done {
            "SELECT id, content, done FROM reminders ORDER BY created_at DESC LIMIT 20"
        } else {
            "SELECT id, content, done FROM reminders WHERE done = 0 ORDER BY created_at DESC LIMIT 20"
        };
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    pub fn complete_reminder(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE reminders SET done = 1 WHERE id = ?1",
            rusqlite::params![id],
        )?;
        Ok(())
    }

    pub fn store_interaction(&self, i: &crate::interaction::Interaction) {
        let conn = self.conn.lock().unwrap();
        let outcome_str = match i.outcome {
            crate::interaction::Outcome::Engaged => "engaged",
            crate::interaction::Outcome::Expanded => "expanded",
            crate::interaction::Outcome::Corrected => "corrected",
            crate::interaction::Outcome::Ignored => "ignored",
            crate::interaction::Outcome::Acknowledged => "acknowledged",
        };
        conn.execute(
            "INSERT INTO interactions (timestamp, channel, user_msg_len, response_len, \
             response_time_ms, outcome, warmth, verbosity, assertiveness, hour) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                i.timestamp,
                i.channel,
                i.user_msg_len as i64,
                i.response_len as i64,
                i.response_time_ms as i64,
                outcome_str,
                i.warmth,
                i.verbosity,
                i.assertiveness,
                i.hour
            ],
        )
        .ok();
    }

    pub fn get_interactions(&self, limit: usize) -> Vec<crate::interaction::Interaction> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT timestamp, channel, user_msg_len, response_len, response_time_ms, \
                 outcome, warmth, verbosity, assertiveness, hour \
                 FROM interactions ORDER BY id DESC LIMIT ?1",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![limit as i64], |row| {
            let outcome_str: String = row.get(5)?;
            let outcome = match outcome_str.as_str() {
                "expanded" => crate::interaction::Outcome::Expanded,
                "corrected" => crate::interaction::Outcome::Corrected,
                "ignored" => crate::interaction::Outcome::Ignored,
                "acknowledged" => crate::interaction::Outcome::Acknowledged,
                _ => crate::interaction::Outcome::Engaged,
            };
            Ok(crate::interaction::Interaction {
                timestamp: row.get(0)?,
                channel: row.get(1)?,
                user_msg_len: row.get::<_, i64>(2)? as usize,
                response_len: row.get::<_, i64>(3)? as usize,
                response_time_ms: row.get::<_, i64>(4)? as u64,
                outcome,
                warmth: row.get(6)?,
                verbosity: row.get(7)?,
                assertiveness: row.get(8)?,
                hour: row.get(9)?,
            })
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
    }

    pub fn get_topic_counts(&self, msg_limit: usize) -> Vec<(String, u32)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT content FROM messages WHERE role = 'user' ORDER BY id DESC LIMIT ?1")
            .unwrap();
        let messages: Vec<String> = stmt
            .query_map(rusqlite::params![msg_limit as i64], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        let topics: &[(&str, &[&str])] = &[
            (
                "Rust",
                &[
                    "rust", "cargo", "tokio", "async fn", "impl ", "struct ", "enum ",
                ],
            ),
            (
                "Python",
                &["python", "pip", "django", "flask", "numpy", "pandas"],
            ),
            (
                "JavaScript",
                &["javascript", "typescript", "node", "react", "npm"],
            ),
            (
                "AI/ML",
                &[
                    "llm",
                    "gpt",
                    "claude",
                    "embedding",
                    "model",
                    "neural",
                    "transformer",
                ],
            ),
            (
                "DevOps",
                &["docker", "kubernetes", "deploy", "ci/cd", "nginx", "server"],
            ),
            (
                "Git",
                &[
                    "git ",
                    "commit",
                    "branch",
                    "merge",
                    "rebase",
                    "pull request",
                ],
            ),
            (
                "Database",
                &["sql", "sqlite", "postgres", "database", "query", "schema"],
            ),
            (
                "Linux",
                &["linux", "bash", "terminal", "sudo", "ssh", "systemd"],
            ),
        ];

        let mut counts: Vec<(String, u32)> = topics
            .iter()
            .map(|(name, keywords)| {
                let count = messages
                    .iter()
                    .filter(|m| {
                        let lower = m.to_lowercase();
                        keywords.iter().any(|k| lower.contains(k))
                    })
                    .count() as u32;
                (name.to_string(), count)
            })
            .filter(|(_, c)| *c > 0)
            .collect();

        counts.sort_by(|a, b| b.1.cmp(&a.1));
        counts
    }

    pub fn add_scheduled_task(
        &self,
        description: &str,
        tool: &str,
        tool_args: &str,
        interval_secs: i64,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO scheduled_tasks (description, tool, tool_args, interval_secs) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![description, tool, tool_args, interval_secs],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get_due_tasks(&self) -> Vec<(i64, String, String, String)> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        let mut stmt = conn
            .prepare(
                "SELECT id, description, tool, tool_args FROM scheduled_tasks \
                 WHERE enabled = 1 AND (?1 - last_run) >= interval_secs",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![now], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
    }

    pub fn mark_task_run(&self, id: i64) {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE scheduled_tasks SET last_run = ?1 WHERE id = ?2",
            rusqlite::params![now, id],
        )
        .ok();
    }

    pub fn list_scheduled_tasks(&self) -> Vec<(i64, String, String, i64, bool)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, description, tool, interval_secs, enabled FROM scheduled_tasks ORDER BY id",
            )
            .unwrap();
        stmt.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
    }

    pub fn disable_scheduled_task(&self, id: i64) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE scheduled_tasks SET enabled = 0 WHERE id = ?1",
            rusqlite::params![id],
        )
        .ok();
    }

    pub fn track_skill_usage(&self, skill: &str, success: bool) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO state (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)",
            rusqlite::params![
                format!("skill_{}_{}", skill, if success { "ok" } else { "fail" }),
                "1"
            ],
        )
        .ok();
    }

    pub fn message_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
            .unwrap_or(0)
    }
}
