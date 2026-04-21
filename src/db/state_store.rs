use anyhow::Result;

use super::Db;
use super::shared::{current_timestamp_string, parse_json_value};

const ACTIVE_RUNTIME_SESSION_KEY: &str = "runtime.active_session";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UserProfile {
    pub name: String,
    pub location: String,
    pub preferences: Vec<String>,
    pub habits: Vec<String>,
    pub facts: Vec<String>,
}

impl Default for UserProfile {
    fn default() -> Self {
        Self {
            name: String::new(),
            location: String::new(),
            preferences: Vec::new(),
            habits: Vec::new(),
            facts: Vec::new(),
        }
    }
}

impl Db {
    pub fn register_runtime_start(&self, source: &str) -> Result<()> {
        let now = current_timestamp_string();
        if let Some(previous_raw) = self.get_state(ACTIVE_RUNTIME_SESSION_KEY) {
            if !previous_raw.trim().is_empty() {
                let previous_session = parse_json_value(previous_raw);
                let summary = format!(
                    "detected unclean shutdown of session {}",
                    previous_session
                        .get("session_id")
                        .and_then(|value| value.as_str())
                        .unwrap_or("unknown")
                );
                let details = serde_json::json!({
                    "detected_at": now,
                    "previous_session": previous_session,
                });
                self.record_system_incident(
                    "unclean_shutdown",
                    source,
                    "warn",
                    &summary,
                    &details,
                )?;
            }
        }

        let session = serde_json::json!({
            "session_id": format!("runtime-{}-{}", std::process::id(), chrono::Utc::now().timestamp_micros()),
            "pid": std::process::id(),
            "source": source,
            "started_at": now,
        });
        self.set_state(ACTIVE_RUNTIME_SESSION_KEY, &session.to_string());
        self.record_system_incident(
            "runtime_start",
            source,
            "info",
            "runtime session started",
            &session,
        )?;
        Ok(())
    }

    pub fn register_runtime_shutdown(&self, source: &str, reason: &str) -> Result<()> {
        let Some(raw) = self.get_state(ACTIVE_RUNTIME_SESSION_KEY) else {
            return Ok(());
        };
        if raw.trim().is_empty() {
            return Ok(());
        }

        let session = parse_json_value(raw);
        let active_pid = session
            .get("pid")
            .and_then(|value| value.as_u64())
            .unwrap_or_default();
        if active_pid != std::process::id() as u64 {
            return Ok(());
        }

        let details = serde_json::json!({
            "ended_at": current_timestamp_string(),
            "reason": reason,
            "session": session,
        });
        self.record_system_incident(
            "runtime_shutdown",
            source,
            "info",
            &format!("runtime session stopped: {}", reason),
            &details,
        )?;

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM state WHERE key = ?1",
            rusqlite::params![ACTIVE_RUNTIME_SESSION_KEY],
        )?;
        Ok(())
    }

    pub fn get_state(&self, key: &str) -> Option<String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM state WHERE key = ?1",
            rusqlite::params![key],
            |r| r.get(0),
        )
        .ok()
    }

    pub fn set_state(&self, key: &str, value: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO state (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = ?2",
            rusqlite::params![key, value],
        )
        .ok();
    }
}

impl UserProfile {
    /// Load profile for a specific user. Falls back to default "owner" profile.
    pub fn load(db: &Db) -> Self {
        Self::load_for(db, "owner")
    }

    /// Load profile for a named user.
    pub fn load_for(db: &Db, user: &str) -> Self {
        let key = format!("profile:{}", user);
        db.get_state(&key)
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| {
                if user == "owner" {
                    db.get_state("user_profile")
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or_default()
                } else {
                    let mut p = UserProfile::default();
                    p.name = user.to_string();
                    p
                }
            })
    }

    /// Save profile for the default owner.
    pub fn save(&self, db: &Db) {
        self.save_for(db, "owner");
    }

    /// Save profile for a named user.
    pub fn save_for(&self, db: &Db, user: &str) {
        if let Ok(json) = serde_json::to_string(self) {
            db.set_state(&format!("profile:{}", user), &json);
            if user == "owner" {
                db.set_state("user_profile", &json);
            }
        }
    }

    pub fn add_fact(&mut self, fact: &str) {
        let lower = fact.to_lowercase();
        if self.facts.iter().any(|f| {
            let fl = f.to_lowercase();
            fl == lower || fl.contains(&lower) || lower.contains(&fl)
        }) {
            return;
        }
        self.facts.push(fact.to_string());
        if self.facts.len() > 200 {
            self.facts.remove(0);
        }
    }

    pub fn to_prompt(&self) -> String {
        let mut parts = vec![format!("User: {} ({})", self.name, self.location)];
        if !self.preferences.is_empty() {
            parts.push(format!("Preferences: {}", self.preferences.join(", ")));
        }
        if !self.habits.is_empty() {
            parts.push(format!("Habits: {}", self.habits.join(", ")));
        }
        if !self.facts.is_empty() {
            let facts: Vec<&str> = self
                .facts
                .iter()
                .rev()
                .take(15)
                .map(|s| s.as_str())
                .collect();
            parts.push(format!("Facts: {}", facts.join("; ")));
        }
        parts.join("\n")
    }
}
