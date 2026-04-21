use anyhow::Result;

use super::shared::{
    map_memory_claim_row, map_memory_procedure_row, map_memory_refresh_job_row,
    map_memory_source_row,
};
use super::{
    Db, MemoryClaimRecord, MemoryProcedureRecord, MemoryRefreshJobRecord, MemorySourceRecord,
};

impl Db {
    pub fn memory_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM memories", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn active_memory_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM memories WHERE superseded_by IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0)
    }

    pub fn memory_episode_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM memory_episodes", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn memory_claim_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM memory_claims", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn memory_procedure_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM memory_procedures", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn memory_source_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM memory_sources", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn memory_entity_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM memory_entities", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn memory_session_capsule_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM memory_session_capsules", [], |r| {
            r.get(0)
        })
        .unwrap_or(0)
    }

    pub fn memory_refresh_job_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM memory_refresh_jobs", [], |r| r.get(0))
            .unwrap_or(0)
    }

    pub fn stale_memory_claim_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM memory_claims WHERE status = 'stale'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0)
    }

    pub fn list_recent_memory_claims(&self, limit: usize) -> Result<Vec<MemoryClaimRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM memory_claims
             WHERE status IN ('active', 'stale', 'invalidated', 'superseded', 'disputed')
             ORDER BY updated_at DESC, created_at DESC
             LIMIT ?1",
        )?;
        let claims = stmt
            .query_map(rusqlite::params![limit as i64], map_memory_claim_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(claims)
    }

    pub fn list_recent_memory_procedures(
        &self,
        limit: usize,
    ) -> Result<Vec<MemoryProcedureRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM memory_procedures
             ORDER BY updated_at DESC, created_at DESC
             LIMIT ?1",
        )?;
        let procedures = stmt
            .query_map(rusqlite::params![limit as i64], map_memory_procedure_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(procedures)
    }

    pub fn list_recent_memory_sources(&self, limit: usize) -> Result<Vec<MemorySourceRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM memory_sources
             ORDER BY COALESCE(last_checked_at, observed_at) DESC, created_at DESC
             LIMIT ?1",
        )?;
        let sources = stmt
            .query_map(rusqlite::params![limit as i64], map_memory_source_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(sources)
    }

    pub fn list_recent_memory_refresh_jobs(
        &self,
        limit: usize,
    ) -> Result<Vec<MemoryRefreshJobRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM memory_refresh_jobs
             ORDER BY updated_at DESC, created_at DESC
             LIMIT ?1",
        )?;
        let jobs = stmt
            .query_map(rusqlite::params![limit as i64], map_memory_refresh_job_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(jobs)
    }

    pub fn count_due_memory_refresh_jobs(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM memory_refresh_jobs
             WHERE status = 'pending'
               AND scheduled_for <= datetime('now')",
            [],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }
}
