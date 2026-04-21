//! Database — SQLite for memories, profile, conversations.
//! Uses FTS5 for full-text memory search (the single most important fix).

use anyhow::Result;
use rusqlite::Connection;
use std::sync::Mutex;

mod autonomy_store;
mod legacy_memory_store;
mod memory_capsules;
mod memory_maintenance;
mod memory_model;
mod memory_queries;
mod memory_recall;
mod policy_learning;
mod records;
mod schema;
mod shared;
mod state_store;
mod support_store;
mod telemetry_store;
mod trace_store;

pub use records::*;
pub use state_store::UserProfile;

const REPLAY_FAILURE_CLUSTER_CURSOR_KEY: &str = "replay.failure_clusters.last_action_run_id";

pub struct Db {
    conn: Mutex<Connection>,
}

impl Db {
    pub fn open(path: &str) -> Result<Self> {
        std::fs::create_dir_all(
            std::path::Path::new(path)
                .parent()
                .unwrap_or(std::path::Path::new(".")),
        )?;
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        schema::migrate(&conn)
    }
}
