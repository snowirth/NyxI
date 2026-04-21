use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde::de::DeserializeOwned;

use super::shared::{
    current_timestamp_string, insert_execution_ledger_entry, last_execution_llm_call,
    parse_json_value,
};
use super::{
    Db, ExecutionLedgerWrite, REPLAY_FAILURE_CLUSTER_CURSOR_KEY, ReplayFailureClusterRecord,
};

impl Db {
    pub fn upsert_autonomy_goal(
        &self,
        title: &str,
        source: &str,
        details: Option<&str>,
        priority: f64,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();

        let existing_id = conn.query_row(
            "SELECT id FROM autonomy_goals WHERE title = ?1 AND status = 'active' ORDER BY id DESC LIMIT 1",
            rusqlite::params![title],
            |row| row.get::<_, i64>(0),
        );

        match existing_id {
            Ok(id) => {
                conn.execute(
                    "UPDATE autonomy_goals \
                     SET priority = MAX(priority, ?2), \
                         source = ?3, \
                         details = COALESCE(?4, details), \
                         updated_at = datetime('now'), \
                         last_reviewed_at = datetime('now') \
                     WHERE id = ?1",
                    rusqlite::params![id, normalize_priority(priority), source, details],
                )?;
                Ok(id)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                conn.execute(
                    "INSERT INTO autonomy_goals (title, status, priority, source, details, last_reviewed_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))",
                    rusqlite::params![
                        title,
                        crate::autonomy::GoalStatus::Active.as_str(),
                        normalize_priority(priority),
                        source,
                        details
                    ],
                )?;
                Ok(conn.last_insert_rowid())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_active_autonomy_goals(&self) -> Result<Vec<crate::autonomy::Goal>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, title, status, priority, source, details, created_at, updated_at, last_reviewed_at \
             FROM autonomy_goals WHERE status = 'active' ORDER BY priority DESC, updated_at DESC",
        )?;
        let results = stmt
            .query_map([], map_autonomy_goal_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn list_autonomy_tasks_with_status(
        &self,
        status: crate::autonomy::TaskStatus,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::Task>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, goal_id, kind, title, status, tool, args_json, notes, priority, \
                    scheduled_for, created_at, updated_at, last_run_at \
             FROM autonomy_tasks \
             WHERE status = ?1 \
             ORDER BY updated_at DESC, id DESC LIMIT ?2",
        )?;
        let results = stmt
            .query_map(
                rusqlite::params![status.as_str(), limit as i64],
                map_autonomy_task_row,
            )?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn list_stale_running_autonomy_tasks(
        &self,
        stale_before: &str,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::Task>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, goal_id, kind, title, status, tool, args_json, notes, priority, \
                    scheduled_for, created_at, updated_at, last_run_at \
             FROM autonomy_tasks \
             WHERE status = 'running'
               AND last_run_at IS NOT NULL
               AND last_run_at <= ?1 \
             ORDER BY last_run_at ASC, id ASC LIMIT ?2",
        )?;
        let results = stmt
            .query_map(
                rusqlite::params![stale_before, limit as i64],
                map_autonomy_task_row,
            )?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn count_stale_running_autonomy_tasks(&self, stale_before: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM autonomy_tasks \
             WHERE status = 'running'
               AND last_run_at IS NOT NULL
               AND last_run_at <= ?1",
            rusqlite::params![stale_before],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }

    pub fn count_autonomy_tasks_with_status(
        &self,
        status: crate::autonomy::TaskStatus,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM autonomy_tasks WHERE status = ?1",
            rusqlite::params![status.as_str()],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }

    pub fn get_autonomy_task(&self, id: i64) -> Result<Option<crate::autonomy::Task>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, goal_id, kind, title, status, tool, args_json, notes, priority, \
                    scheduled_for, created_at, updated_at, last_run_at \
             FROM autonomy_tasks WHERE id = ?1",
        )?;
        let result = stmt.query_row(rusqlite::params![id], map_autonomy_task_row);
        match result {
            Ok(task) => Ok(Some(task)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn create_autonomy_task(
        &self,
        goal_id: Option<i64>,
        kind: &str,
        title: &str,
        tool: Option<&str>,
        args: &serde_json::Value,
        notes: Option<&str>,
        priority: f64,
        scheduled_for: Option<&str>,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let args_json = serde_json::to_string(args)?;
        conn.execute(
            "INSERT INTO autonomy_tasks \
             (goal_id, kind, title, status, tool, args_json, notes, priority, scheduled_for) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                goal_id,
                kind,
                title,
                crate::autonomy::TaskStatus::Pending.as_str(),
                tool,
                args_json,
                notes,
                normalize_priority(priority),
                scheduled_for
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn has_recent_autonomy_task_with_dedupe_key(
        &self,
        kind: &str,
        dedupe_key: &str,
        cooldown_secs: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let window = format!("-{} seconds", cooldown_secs.max(0));
        let mut stmt = conn.prepare(
            "SELECT id, goal_id, kind, title, status, tool, args_json, notes, priority, \
                    scheduled_for, created_at, updated_at, last_run_at \
             FROM autonomy_tasks \
             WHERE kind = ?1
               AND (
                    status IN ('pending', 'running')
                    OR updated_at >= datetime('now', ?2)
               )
             ORDER BY id DESC
             LIMIT 64",
        )?;
        let tasks = stmt
            .query_map(rusqlite::params![kind, window], map_autonomy_task_row)?
            .filter_map(|row| row.ok());

        Ok(tasks.into_iter().any(|task| {
            task.args.get("dedupe_key").and_then(|value| value.as_str()) == Some(dedupe_key)
        }))
    }

    pub fn list_ready_autonomy_tasks(&self, limit: usize) -> Vec<crate::autonomy::Task> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, goal_id, kind, title, status, tool, args_json, notes, priority, \
                    scheduled_for, created_at, updated_at, last_run_at \
             FROM autonomy_tasks \
             WHERE status = 'pending' AND (scheduled_for IS NULL OR scheduled_for <= datetime('now')) \
             ORDER BY priority DESC, created_at ASC LIMIT ?1",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![limit as i64], map_autonomy_task_row)
            .unwrap()
            .filter_map(|row| row.ok())
            .collect()
    }

    pub fn try_list_ready_autonomy_tasks(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::Task>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, goal_id, kind, title, status, tool, args_json, notes, priority, \
                    scheduled_for, created_at, updated_at, last_run_at \
             FROM autonomy_tasks \
             WHERE status = 'pending' AND (scheduled_for IS NULL OR scheduled_for <= datetime('now')) \
             ORDER BY priority DESC, created_at ASC LIMIT ?1",
        )?;
        let results = stmt
            .query_map(rusqlite::params![limit as i64], map_autonomy_task_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn count_ready_autonomy_tasks(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM autonomy_tasks \
             WHERE status = 'pending' AND (scheduled_for IS NULL OR scheduled_for <= datetime('now'))",
            [],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }

    pub fn claim_autonomy_task(&self, id: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE autonomy_tasks \
             SET status = ?2, updated_at = datetime('now'), last_run_at = datetime('now') \
             WHERE id = ?1 AND status = 'pending'",
            rusqlite::params![id, crate::autonomy::TaskStatus::Running.as_str()],
        )?;
        Ok(changed > 0)
    }

    pub fn set_autonomy_task_last_run_at(&self, id: i64, last_run_at: Option<&str>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE autonomy_tasks \
             SET last_run_at = ?2, updated_at = datetime('now') \
             WHERE id = ?1",
            rusqlite::params![id, last_run_at],
        )?;
        Ok(())
    }

    pub fn complete_autonomy_task(&self, id: i64, notes: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE autonomy_tasks \
             SET status = ?2, notes = CASE \
                    WHEN COALESCE(notes, '') = '' THEN ?3 \
                    ELSE notes || char(10) || ?3 \
                 END, updated_at = datetime('now') \
             WHERE id = ?1",
            rusqlite::params![id, crate::autonomy::TaskStatus::Completed.as_str(), notes],
        )?;
        Ok(())
    }

    pub fn cancel_autonomy_task(&self, id: i64, notes: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE autonomy_tasks \
             SET status = ?2, notes = CASE \
                    WHEN COALESCE(notes, '') = '' THEN ?3 \
                    ELSE notes || char(10) || ?3 \
                 END, updated_at = datetime('now') \
             WHERE id = ?1 AND status IN ('pending', 'running')",
            rusqlite::params![id, crate::autonomy::TaskStatus::Cancelled.as_str(), notes],
        )?;
        Ok(())
    }

    pub fn fail_autonomy_task(&self, id: i64, notes: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE autonomy_tasks \
             SET status = ?2, notes = CASE \
                    WHEN COALESCE(notes, '') = '' THEN ?3 \
                    ELSE notes || char(10) || ?3 \
                 END, updated_at = datetime('now') \
             WHERE id = ?1",
            rusqlite::params![id, crate::autonomy::TaskStatus::Failed.as_str(), notes],
        )?;
        Ok(())
    }

    pub fn reschedule_autonomy_task(
        &self,
        id: i64,
        scheduled_for: Option<&str>,
        notes: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE autonomy_tasks \
             SET status = ?2, scheduled_for = ?3, notes = CASE \
                    WHEN COALESCE(notes, '') = '' THEN ?4 \
                    ELSE notes || char(10) || ?4 \
                 END, updated_at = datetime('now') \
             WHERE id = ?1",
            rusqlite::params![
                id,
                crate::autonomy::TaskStatus::Pending.as_str(),
                scheduled_for,
                notes
            ],
        )?;
        Ok(())
    }

    pub fn recover_stale_autonomy_task(
        &self,
        id: i64,
        scheduled_for: Option<&str>,
        notes: &str,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE autonomy_tasks \
             SET status = ?2, scheduled_for = ?3, notes = CASE \
                    WHEN COALESCE(notes, '') = '' THEN ?4 \
                    ELSE notes || char(10) || ?4 \
                 END, updated_at = datetime('now') \
             WHERE id = ?1 AND status = 'running'",
            rusqlite::params![
                id,
                crate::autonomy::TaskStatus::Pending.as_str(),
                scheduled_for,
                notes
            ],
        )?;
        Ok(changed > 0)
    }

    pub fn record_autonomy_action_run(
        &self,
        task_id: i64,
        outcome: &str,
        summary: &str,
        output: Option<&serde_json::Value>,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let output_json = match output {
            Some(value) => Some(serde_json::to_string(value)?),
            None => None,
        };
        let executed = output
            .and_then(|value| value.get("executed"))
            .and_then(|value| value.as_bool());
        let verified = output
            .and_then(|value| value.get("verified"))
            .and_then(|value| value.as_bool());
        let expected_effect_json = serialize_output_field(output, "expected_effect")?;
        let verifier_verdict_json = serialize_output_field(output, "verifier_verdict")?;
        let rollback_reason_json = serialize_output_field(output, "rollback_reason")?;
        conn.execute(
            "INSERT INTO autonomy_action_runs \
             (task_id, outcome, summary, output_json, executed, verified, expected_effect_json, \
              verifier_verdict_json, rollback_reason_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                task_id,
                outcome,
                summary,
                output_json,
                executed,
                verified,
                expected_effect_json,
                verifier_verdict_json,
                rollback_reason_json
            ],
        )?;
        let action_run_id = conn.last_insert_rowid();
        if let Err(error) = append_autonomy_action_run_ledger(
            &conn,
            action_run_id,
            task_id,
            outcome,
            summary,
            output,
        ) {
            tracing::warn!(
                "execution ledger: failed to mirror autonomy action run {}: {}",
                action_run_id,
                error
            );
        }
        Ok(action_run_id)
    }

    pub fn list_autonomy_action_runs(
        &self,
        task_id: i64,
    ) -> Result<Vec<crate::autonomy::ActionRun>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, task_id, outcome, summary, output_json, executed, verified, expected_effect_json, \
                    verifier_verdict_json, rollback_reason_json, created_at \
             FROM autonomy_action_runs WHERE task_id = ?1 ORDER BY id ASC",
        )?;
        let results = stmt
            .query_map(rusqlite::params![task_id], map_autonomy_action_run_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn get_autonomy_action_run_record(
        &self,
        id: i64,
    ) -> Result<Option<crate::autonomy::ActionRunRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT r.id, r.task_id, t.kind, t.title, t.status, t.tool, t.goal_id, r.outcome, \
                    r.summary, r.executed, r.verified, r.expected_effect_json, \
                    r.verifier_verdict_json, r.rollback_reason_json, r.output_json, r.created_at \
             FROM autonomy_action_runs r \
             JOIN autonomy_tasks t ON t.id = r.task_id \
             WHERE r.id = ?1",
        )?;
        let result = stmt.query_row(rusqlite::params![id], map_autonomy_action_run_record_row);
        match result {
            Ok(action_run) => Ok(Some(action_run)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn list_recent_autonomy_action_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::ActionRunRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT r.id, r.task_id, t.kind, t.title, t.status, t.tool, t.goal_id, r.outcome, \
                    r.summary, r.executed, r.verified, r.expected_effect_json, \
                    r.verifier_verdict_json, r.rollback_reason_json, r.output_json, r.created_at \
             FROM autonomy_action_runs r \
             JOIN autonomy_tasks t ON t.id = r.task_id \
             ORDER BY r.id DESC LIMIT ?1",
        )?;
        let results = stmt
            .query_map(
                rusqlite::params![limit as i64],
                map_autonomy_action_run_record_row,
            )?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn count_autonomy_action_runs(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM autonomy_action_runs", [], |row| {
                row.get(0)
            })?;
        Ok(count.max(0) as usize)
    }

    pub fn list_recent_autonomy_action_runs_for_task_kind(
        &self,
        task_kind: &str,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::ActionRunRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT r.id, r.task_id, t.kind, t.title, t.status, t.tool, t.goal_id, r.outcome, \
                    r.summary, r.executed, r.verified, r.expected_effect_json, \
                    r.verifier_verdict_json, r.rollback_reason_json, r.output_json, r.created_at \
             FROM autonomy_action_runs r \
             JOIN autonomy_tasks t ON t.id = r.task_id \
             WHERE t.kind = ?1 \
             ORDER BY r.id DESC LIMIT ?2",
        )?;
        let results = stmt
            .query_map(
                rusqlite::params![task_kind, limit as i64],
                map_autonomy_action_run_record_row,
            )?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn list_recent_autonomy_action_runs_with_outcome(
        &self,
        outcome: &str,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::ActionRunRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT r.id, r.task_id, t.kind, t.title, t.status, t.tool, t.goal_id, r.outcome, \
                    r.summary, r.executed, r.verified, r.expected_effect_json, \
                    r.verifier_verdict_json, r.rollback_reason_json, r.output_json, r.created_at \
             FROM autonomy_action_runs r \
             JOIN autonomy_tasks t ON t.id = r.task_id \
             WHERE r.outcome = ?1 \
             ORDER BY r.id DESC LIMIT ?2",
        )?;
        let results = stmt
            .query_map(
                rusqlite::params![outcome, limit as i64],
                map_autonomy_action_run_record_row,
            )?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn list_recent_autonomy_action_runs_filtered(
        &self,
        task_kind: Option<&str>,
        outcome: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::ActionRunRecord>> {
        match (task_kind, outcome) {
            (Some(task_kind), Some(outcome)) => {
                let conn = self.conn.lock().unwrap();
                let mut stmt = conn.prepare(
                    "SELECT r.id, r.task_id, t.kind, t.title, t.status, t.tool, t.goal_id, r.outcome, \
                            r.summary, r.executed, r.verified, r.expected_effect_json, \
                            r.verifier_verdict_json, r.rollback_reason_json, r.output_json, r.created_at \
                     FROM autonomy_action_runs r \
                     JOIN autonomy_tasks t ON t.id = r.task_id \
                     WHERE t.kind = ?1 AND r.outcome = ?2 \
                     ORDER BY r.id DESC LIMIT ?3",
                )?;
                let results = stmt
                    .query_map(
                        rusqlite::params![task_kind, outcome, limit as i64],
                        map_autonomy_action_run_record_row,
                    )?
                    .filter_map(|row| row.ok())
                    .collect();
                Ok(results)
            }
            (Some(task_kind), None) => {
                self.list_recent_autonomy_action_runs_for_task_kind(task_kind, limit)
            }
            (None, Some(outcome)) => {
                self.list_recent_autonomy_action_runs_with_outcome(outcome, limit)
            }
            (None, None) => self.list_recent_autonomy_action_runs(limit),
        }
    }

    pub fn count_autonomy_action_runs_for_task_kind(&self, task_kind: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) \
             FROM autonomy_action_runs r \
             JOIN autonomy_tasks t ON t.id = r.task_id \
             WHERE t.kind = ?1",
            rusqlite::params![task_kind],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }

    pub fn count_autonomy_action_runs_with_outcome(&self, outcome: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM autonomy_action_runs WHERE outcome = ?1",
            rusqlite::params![outcome],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as usize)
    }

    pub fn count_autonomy_action_runs_filtered(
        &self,
        task_kind: Option<&str>,
        outcome: Option<&str>,
    ) -> Result<usize> {
        match (task_kind, outcome) {
            (Some(task_kind), Some(outcome)) => {
                let conn = self.conn.lock().unwrap();
                let count: i64 = conn.query_row(
                    "SELECT COUNT(*) \
                     FROM autonomy_action_runs r \
                     JOIN autonomy_tasks t ON t.id = r.task_id \
                     WHERE t.kind = ?1 AND r.outcome = ?2",
                    rusqlite::params![task_kind, outcome],
                    |row| row.get(0),
                )?;
                Ok(count.max(0) as usize)
            }
            (Some(task_kind), None) => self.count_autonomy_action_runs_for_task_kind(task_kind),
            (None, Some(outcome)) => self.count_autonomy_action_runs_with_outcome(outcome),
            (None, None) => self.count_autonomy_action_runs(),
        }
    }

    pub fn list_recent_replay_failure_clusters_filtered(
        &self,
        task_kind: Option<&str>,
        failure_class: Option<&str>,
        tool: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ReplayFailureClusterRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(task_kind) = task_kind {
            clauses.push("task_kind = ?");
            params.push(rusqlite::types::Value::from(task_kind.to_string()));
        }
        if let Some(failure_class) = failure_class {
            clauses.push("failure_class = ?");
            params.push(rusqlite::types::Value::from(failure_class.to_string()));
        }
        if let Some(tool) = tool {
            clauses.push("tool = ?");
            params.push(rusqlite::types::Value::from(tool.to_string()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT id, fingerprint, task_kind, tool, failure_class, failure_stage, latest_outcome, \
                    issue_signature, exemplar_summary, exemplar_error, target, provider, model, route, \
                    occurrence_count, first_seen_at, last_seen_at, sample_action_run_ids_json, \
                    latest_action_run_id, latest_task_id, created_at, updated_at \
             FROM replay_failure_clusters{} \
             ORDER BY occurrence_count DESC, last_seen_at DESC, id DESC \
             LIMIT ?",
            where_clause
        );
        params.push(rusqlite::types::Value::from(limit as i64));

        let mut stmt = conn.prepare(&sql)?;
        let clusters = stmt
            .query_map(
                rusqlite::params_from_iter(params),
                map_replay_failure_cluster_row,
            )?
            .filter_map(|row| row.ok())
            .collect();
        Ok(clusters)
    }

    pub fn count_replay_failure_clusters_filtered(
        &self,
        task_kind: Option<&str>,
        failure_class: Option<&str>,
        tool: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(task_kind) = task_kind {
            clauses.push("task_kind = ?");
            params.push(rusqlite::types::Value::from(task_kind.to_string()));
        }
        if let Some(failure_class) = failure_class {
            clauses.push("failure_class = ?");
            params.push(rusqlite::types::Value::from(failure_class.to_string()));
        }
        if let Some(tool) = tool {
            clauses.push("tool = ?");
            params.push(rusqlite::types::Value::from(tool.to_string()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT COUNT(*) FROM replay_failure_clusters{}",
            where_clause
        );
        let count: i64 =
            conn.query_row(&sql, rusqlite::params_from_iter(params), |row| row.get(0))?;
        Ok(count.max(0) as usize)
    }

    pub fn ingest_replay_failure_clusters(&self, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap();
        let last_ingested_id = conn
            .query_row(
                "SELECT value FROM state WHERE key = ?1",
                rusqlite::params![REPLAY_FAILURE_CLUSTER_CURSOR_KEY],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|raw| raw.parse::<i64>().ok())
            .unwrap_or(0);
        let mut stmt = conn.prepare(
            "SELECT r.id, r.task_id, t.kind, t.title, t.status, t.tool, t.goal_id, r.outcome, \
                    r.summary, r.executed, r.verified, r.expected_effect_json, \
                    r.verifier_verdict_json, r.rollback_reason_json, r.output_json, r.created_at \
             FROM autonomy_action_runs r \
             JOIN autonomy_tasks t ON t.id = r.task_id \
             WHERE r.id > ?1 \
             ORDER BY r.id ASC \
             LIMIT ?2",
        )?;
        let action_runs = stmt
            .query_map(
                rusqlite::params![last_ingested_id, limit as i64],
                map_autonomy_action_run_record_row,
            )?
            .filter_map(|row| row.ok())
            .collect::<Vec<_>>();

        let mut max_ingested_id = last_ingested_id;
        let mut clustered = 0usize;
        for action_run in action_runs {
            max_ingested_id = max_ingested_id.max(action_run.id);
            let Some(seed) = crate::improvement::replay::failure_cluster_seed(&action_run) else {
                continue;
            };
            upsert_replay_failure_cluster_static(&conn, &seed)?;
            clustered += 1;
        }

        if max_ingested_id > last_ingested_id {
            conn.execute(
                "INSERT INTO state (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                rusqlite::params![
                    REPLAY_FAILURE_CLUSTER_CURSOR_KEY,
                    max_ingested_id.to_string(),
                ],
            )?;
        }

        Ok(clustered)
    }

    pub fn get_autonomy_retry_state(
        &self,
        key: &str,
    ) -> Result<Option<crate::autonomy::RetryState>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT key, task_kind, target, failure_class, attempt_count, first_failed_at, \
                    last_failed_at, next_retry_at, quarantined_until, last_error, last_task_id, \
                    last_growth_event_id, last_snapshot_id, updated_at \
             FROM autonomy_retry_state WHERE key = ?1",
        )?;
        let result = stmt.query_row(rusqlite::params![key], map_autonomy_retry_state_row);
        match result {
            Ok(state) => Ok(Some(state)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn upsert_autonomy_retry_state(&self, state: &crate::autonomy::RetryState) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let updated_at = if state.updated_at.trim().is_empty() {
            current_timestamp_string()
        } else {
            state.updated_at.clone()
        };
        conn.execute(
            "INSERT INTO autonomy_retry_state \
             (key, task_kind, target, failure_class, attempt_count, first_failed_at, last_failed_at, \
              next_retry_at, quarantined_until, last_error, last_task_id, last_growth_event_id, \
              last_snapshot_id, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
             ON CONFLICT(key) DO UPDATE SET \
                task_kind = excluded.task_kind, \
                target = excluded.target, \
                failure_class = excluded.failure_class, \
                attempt_count = excluded.attempt_count, \
                first_failed_at = excluded.first_failed_at, \
                last_failed_at = excluded.last_failed_at, \
                next_retry_at = excluded.next_retry_at, \
                quarantined_until = excluded.quarantined_until, \
                last_error = excluded.last_error, \
                last_task_id = excluded.last_task_id, \
                last_growth_event_id = excluded.last_growth_event_id, \
                last_snapshot_id = excluded.last_snapshot_id, \
                updated_at = excluded.updated_at",
            rusqlite::params![
                state.key,
                state.task_kind,
                state.target,
                state.failure_class.as_str(),
                state.attempt_count,
                state.first_failed_at,
                state.last_failed_at,
                state.next_retry_at,
                state.quarantined_until,
                state.last_error,
                state.last_task_id,
                state.last_growth_event_id,
                state.last_snapshot_id,
                updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn clear_autonomy_retry_state(&self, key: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM autonomy_retry_state WHERE key = ?1",
            rusqlite::params![key],
        )?;
        Ok(())
    }

    pub fn prune_inactive_autonomy_retry_state(
        &self,
        now: &str,
        retry_window_cutoff: &str,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM autonomy_retry_state
             WHERE (quarantined_until IS NOT NULL AND quarantined_until <= ?1)
                OR (
                    (quarantined_until IS NULL OR quarantined_until <= ?1)
                    AND (next_retry_at IS NULL OR next_retry_at <= ?1)
                    AND (first_failed_at IS NULL OR first_failed_at <= ?2)
                )",
            rusqlite::params![now, retry_window_cutoff],
        )?;
        Ok(deleted)
    }
}

fn append_autonomy_action_run_ledger(
    conn: &Connection,
    action_run_id: i64,
    task_id: i64,
    outcome: &str,
    summary: &str,
    output: Option<&serde_json::Value>,
) -> Result<i64> {
    let payload = output.cloned().unwrap_or_else(|| serde_json::json!({}));
    let execution = payload.get("execution");
    let llm_call = last_execution_llm_call(&payload);
    let route = llm_call
        .as_ref()
        .and_then(|call| call.get("route"))
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .or_else(|| {
            execution
                .and_then(|value| value.pointer("/trace/tool"))
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            payload
                .pointer("/task/tool")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        });
    let source = execution
        .and_then(|value| value.pointer("/trace/source"))
        .and_then(|value| value.as_str())
        .unwrap_or("autonomy")
        .to_string();
    let target = execution
        .and_then(|value| value.pointer("/trace/target"))
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .or_else(|| {
            payload
                .pointer("/trace/target")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            execution
                .and_then(|value| value.pointer("/trace/tool"))
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .or_else(|| route.clone());
    let entry = ExecutionLedgerWrite {
        surface: execution
            .and_then(|value| value.get("surface"))
            .and_then(|value| value.as_str())
            .unwrap_or("autonomy")
            .to_string(),
        kind: execution
            .and_then(|value| value.get("kind"))
            .and_then(|value| value.as_str())
            .or_else(|| payload.get("kind").and_then(|value| value.as_str()))
            .unwrap_or("autonomy_action")
            .to_string(),
        source,
        target,
        summary: execution
            .and_then(|value| value.get("summary"))
            .and_then(|value| value.as_str())
            .unwrap_or(summary)
            .to_string(),
        outcome: execution
            .and_then(|value| value.get("outcome"))
            .and_then(|value| value.as_str())
            .unwrap_or(outcome)
            .to_string(),
        success: execution
            .and_then(|value| value.get("success"))
            .and_then(|value| value.as_bool())
            .unwrap_or(!matches!(outcome, "failed" | "quarantined")),
        correlation_id: Some(format!("autonomy_task:{}", task_id)),
        reference_kind: Some("autonomy_action_run".to_string()),
        reference_id: Some(action_run_id),
        channel: None,
        provider: llm_call
            .as_ref()
            .and_then(|call| call.get("provider"))
            .and_then(|value| value.as_str())
            .map(str::to_string),
        model: llm_call
            .as_ref()
            .and_then(|call| call.get("model"))
            .and_then(|value| value.as_str())
            .map(str::to_string),
        route,
        latency_ms: llm_call
            .as_ref()
            .and_then(|call| call.get("latency_ms"))
            .and_then(|value| value.as_i64()),
        payload,
    };
    insert_execution_ledger_entry(conn, &entry)
}

fn upsert_replay_failure_cluster_static(
    conn: &Connection,
    seed: &crate::improvement::replay::FailureClusterSeed,
) -> Result<()> {
    let existing = conn
        .query_row(
            "SELECT id, occurrence_count, first_seen_at, sample_action_run_ids_json
             FROM replay_failure_clusters
             WHERE fingerprint = ?1
             LIMIT 1",
            rusqlite::params![seed.fingerprint.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;

    if let Some((cluster_id, occurrence_count, first_seen_at, existing_samples_json)) = existing {
        let sample_action_run_ids = merge_sample_action_run_ids(
            parse_action_run_ids(&existing_samples_json),
            &seed.sample_action_run_ids,
        );
        let merged_first_seen_at = if seed.first_seen_at < first_seen_at {
            seed.first_seen_at.as_str()
        } else {
            first_seen_at.as_str()
        };
        conn.execute(
            "UPDATE replay_failure_clusters
             SET failure_stage = ?2,
                 latest_outcome = ?3,
                 exemplar_summary = ?4,
                 exemplar_error = COALESCE(?5, exemplar_error),
                 target = COALESCE(?6, target),
                 provider = COALESCE(?7, provider),
                 model = COALESCE(?8, model),
                 route = COALESCE(?9, route),
                 occurrence_count = ?10,
                 first_seen_at = ?11,
                 last_seen_at = ?12,
                 sample_action_run_ids_json = ?13,
                 latest_action_run_id = ?14,
                 latest_task_id = ?15,
                 updated_at = datetime('now')
             WHERE id = ?1",
            rusqlite::params![
                cluster_id,
                seed.failure_stage.as_str(),
                seed.latest_outcome.as_str(),
                seed.exemplar_summary.as_str(),
                seed.exemplar_error.as_deref(),
                seed.target.as_deref(),
                seed.provider.as_deref(),
                seed.model.as_deref(),
                seed.route.as_deref(),
                occurrence_count + 1,
                merged_first_seen_at,
                seed.last_seen_at.as_str(),
                serde_json::to_string(&sample_action_run_ids)?,
                seed.latest_action_run_id,
                seed.latest_task_id,
            ],
        )?;
    } else {
        conn.execute(
            "INSERT INTO replay_failure_clusters (
                fingerprint, task_kind, tool, failure_class, failure_stage, latest_outcome,
                issue_signature, exemplar_summary, exemplar_error, target, provider, model, route,
                occurrence_count, first_seen_at, last_seen_at, sample_action_run_ids_json,
                latest_action_run_id, latest_task_id
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 1, ?14, ?15, ?16, ?17, ?18)",
            rusqlite::params![
                seed.fingerprint.as_str(),
                seed.task_kind.as_str(),
                seed.tool.as_deref(),
                seed.failure_class.as_str(),
                seed.failure_stage.as_str(),
                seed.latest_outcome.as_str(),
                seed.issue_signature.as_str(),
                seed.exemplar_summary.as_str(),
                seed.exemplar_error.as_deref(),
                seed.target.as_deref(),
                seed.provider.as_deref(),
                seed.model.as_deref(),
                seed.route.as_deref(),
                seed.first_seen_at.as_str(),
                seed.last_seen_at.as_str(),
                serde_json::to_string(&seed.sample_action_run_ids)?,
                seed.latest_action_run_id,
                seed.latest_task_id,
            ],
        )?;
    }

    Ok(())
}

fn merge_sample_action_run_ids(mut existing: Vec<i64>, incoming: &[i64]) -> Vec<i64> {
    for action_run_id in incoming {
        if !existing.contains(action_run_id) {
            existing.push(*action_run_id);
        }
    }
    if existing.len() > 8 {
        existing = existing.split_off(existing.len() - 8);
    }
    existing
}

fn normalize_priority(priority: f64) -> f64 {
    priority.clamp(0.0, 1.0)
}

pub(super) fn parse_action_run_ids(raw: &str) -> Vec<i64> {
    serde_json::from_str::<Vec<i64>>(raw).unwrap_or_default()
}

fn parse_optional_json_value(raw: Option<String>) -> Option<serde_json::Value> {
    raw.map(parse_json_value)
}

fn parse_optional_json_typed<T: DeserializeOwned>(raw: Option<String>) -> Option<T> {
    raw.and_then(|raw| serde_json::from_str(&raw).ok())
}

fn output_json_bool(output: Option<&serde_json::Value>, key: &str) -> Option<bool> {
    output
        .and_then(|value| value.get(key))
        .and_then(|value| value.as_bool())
}

fn output_json_typed<T: DeserializeOwned>(
    output: Option<&serde_json::Value>,
    key: &str,
) -> Option<T> {
    output
        .and_then(|value| value.get(key))
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn serialize_output_field(output: Option<&serde_json::Value>, key: &str) -> Result<Option<String>> {
    output
        .and_then(|value| value.get(key))
        .cloned()
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .map_err(Into::into)
}

fn map_autonomy_goal_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<crate::autonomy::Goal> {
    let status: String = row.get(2)?;
    Ok(crate::autonomy::Goal {
        id: row.get(0)?,
        title: row.get(1)?,
        status: crate::autonomy::GoalStatus::from_db(&status),
        priority: row.get(3)?,
        source: row.get(4)?,
        details: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
        last_reviewed_at: row.get(8)?,
    })
}

fn map_autonomy_task_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<crate::autonomy::Task> {
    let status: String = row.get(4)?;
    let args_json: String = row.get(6)?;
    Ok(crate::autonomy::Task {
        id: row.get(0)?,
        goal_id: row.get(1)?,
        kind: row.get(2)?,
        title: row.get(3)?,
        status: crate::autonomy::TaskStatus::from_db(&status),
        tool: row.get(5)?,
        args: parse_json_value(args_json),
        notes: row.get(7)?,
        priority: row.get(8)?,
        scheduled_for: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
        last_run_at: row.get(12)?,
    })
}

fn map_autonomy_action_run_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<crate::autonomy::ActionRun> {
    let output_json: Option<String> = row.get(4)?;
    let output = parse_optional_json_value(output_json);
    let executed: Option<bool> = row.get(5)?;
    let verified: Option<bool> = row.get(6)?;
    let expected_effect_json: Option<String> = row.get(7)?;
    let verifier_verdict_json: Option<String> = row.get(8)?;
    let rollback_reason_json: Option<String> = row.get(9)?;
    Ok(crate::autonomy::ActionRun {
        id: row.get(0)?,
        task_id: row.get(1)?,
        outcome: row.get(2)?,
        summary: row.get(3)?,
        executed: executed
            .or_else(|| output_json_bool(output.as_ref(), "executed"))
            .unwrap_or(false),
        verified: verified.or_else(|| output_json_bool(output.as_ref(), "verified")),
        expected_effect: parse_optional_json_typed(expected_effect_json)
            .or_else(|| output_json_typed(output.as_ref(), "expected_effect")),
        verifier_verdict: parse_optional_json_typed(verifier_verdict_json)
            .or_else(|| output_json_typed(output.as_ref(), "verifier_verdict")),
        rollback_reason: parse_optional_json_typed(rollback_reason_json)
            .or_else(|| output_json_typed(output.as_ref(), "rollback_reason")),
        output,
        created_at: row.get(10)?,
    })
}

fn map_autonomy_action_run_record_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<crate::autonomy::ActionRunRecord> {
    let task_status: String = row.get(4)?;
    let executed: Option<bool> = row.get(9)?;
    let verified: Option<bool> = row.get(10)?;
    let expected_effect_json: Option<String> = row.get(11)?;
    let verifier_verdict_json: Option<String> = row.get(12)?;
    let rollback_reason_json: Option<String> = row.get(13)?;
    let output_json: Option<String> = row.get(14)?;
    let output = parse_optional_json_value(output_json);
    Ok(crate::autonomy::ActionRunRecord {
        id: row.get(0)?,
        task_id: row.get(1)?,
        task_kind: row.get(2)?,
        task_title: row.get(3)?,
        task_status: crate::autonomy::TaskStatus::from_db(&task_status),
        tool: row.get(5)?,
        goal_id: row.get(6)?,
        outcome: row.get(7)?,
        summary: row.get(8)?,
        executed: executed
            .or_else(|| output_json_bool(output.as_ref(), "executed"))
            .unwrap_or(false),
        verified: verified.or_else(|| output_json_bool(output.as_ref(), "verified")),
        expected_effect: parse_optional_json_typed(expected_effect_json)
            .or_else(|| output_json_typed(output.as_ref(), "expected_effect")),
        verifier_verdict: parse_optional_json_typed(verifier_verdict_json)
            .or_else(|| output_json_typed(output.as_ref(), "verifier_verdict")),
        rollback_reason: parse_optional_json_typed(rollback_reason_json)
            .or_else(|| output_json_typed(output.as_ref(), "rollback_reason")),
        output,
        created_at: row.get(15)?,
    })
}

fn map_autonomy_retry_state_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<crate::autonomy::RetryState> {
    let failure_class: String = row.get(3)?;
    Ok(crate::autonomy::RetryState {
        key: row.get(0)?,
        task_kind: row.get(1)?,
        target: row.get(2)?,
        failure_class: crate::autonomy::FailureClass::from_db(&failure_class),
        attempt_count: row.get(4)?,
        first_failed_at: row.get(5)?,
        last_failed_at: row.get(6)?,
        next_retry_at: row.get(7)?,
        quarantined_until: row.get(8)?,
        last_error: row.get(9)?,
        last_task_id: row.get(10)?,
        last_growth_event_id: row.get(11)?,
        last_snapshot_id: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

pub(super) fn map_replay_failure_cluster_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ReplayFailureClusterRecord> {
    let sample_action_run_ids_json: String = row.get(17)?;
    Ok(ReplayFailureClusterRecord {
        id: row.get(0)?,
        fingerprint: row.get(1)?,
        task_kind: row.get(2)?,
        tool: row.get(3)?,
        failure_class: row.get(4)?,
        failure_stage: row.get(5)?,
        latest_outcome: row.get(6)?,
        issue_signature: row.get(7)?,
        exemplar_summary: row.get(8)?,
        exemplar_error: row.get(9)?,
        target: row.get(10)?,
        provider: row.get(11)?,
        model: row.get(12)?,
        route: row.get(13)?,
        occurrence_count: row.get(14)?,
        first_seen_at: row.get(15)?,
        last_seen_at: row.get(16)?,
        sample_action_run_ids: parse_action_run_ids(&sample_action_run_ids_json),
        latest_action_run_id: row.get(18)?,
        latest_task_id: row.get(19)?,
        created_at: row.get(20)?,
        updated_at: row.get(21)?,
    })
}
