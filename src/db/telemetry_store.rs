use anyhow::Result;
use rusqlite::Connection;

use super::shared::{
    current_timestamp_string, insert_execution_ledger_entry, last_execution_llm_call,
    parse_json_value,
};
use super::{Db, ExecutionLedgerWrite, SystemIncidentRecord};

impl Db {
    pub fn record_system_incident(
        &self,
        kind: &str,
        source: &str,
        severity: &str,
        summary: &str,
        details: &serde_json::Value,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let details_json = serde_json::to_string(details)?;
        conn.execute(
            "INSERT INTO system_incidents (kind, source, severity, summary, details_json) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![kind, source, severity, summary, details_json],
        )?;
        let incident_id = conn.last_insert_rowid();
        if let Err(error) = append_system_incident_ledger(
            &conn,
            incident_id,
            kind,
            source,
            severity,
            summary,
            details,
        ) {
            tracing::warn!(
                "execution ledger: failed to mirror system incident {}: {}",
                incident_id,
                error
            );
        }
        Ok(incident_id)
    }

    pub fn list_recent_system_incidents_filtered(
        &self,
        kind: Option<&str>,
        source: Option<&str>,
        severity: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SystemIncidentRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(kind) = kind {
            clauses.push("kind = ?");
            params.push(rusqlite::types::Value::from(kind.to_string()));
        }
        if let Some(source) = source {
            clauses.push("source = ?");
            params.push(rusqlite::types::Value::from(source.to_string()));
        }
        if let Some(severity) = severity {
            clauses.push("severity = ?");
            params.push(rusqlite::types::Value::from(severity.to_string()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT id, kind, source, severity, summary, details_json, created_at \
             FROM system_incidents{} ORDER BY id DESC LIMIT ?",
            where_clause
        );
        params.push(rusqlite::types::Value::from(limit as i64));

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(params.iter()),
            map_system_incident_row,
        )?;
        Ok(rows.filter_map(|row| row.ok()).collect())
    }

    pub fn count_system_incidents_filtered(
        &self,
        kind: Option<&str>,
        source: Option<&str>,
        severity: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(kind) = kind {
            clauses.push("kind = ?");
            params.push(rusqlite::types::Value::from(kind.to_string()));
        }
        if let Some(source) = source {
            clauses.push("source = ?");
            params.push(rusqlite::types::Value::from(source.to_string()));
        }
        if let Some(severity) = severity {
            clauses.push("severity = ?");
            params.push(rusqlite::types::Value::from(severity.to_string()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!("SELECT COUNT(*) FROM system_incidents{}", where_clause);
        let count: i64 =
            conn.query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                row.get(0)
            })?;
        Ok(count.max(0) as usize)
    }

    pub fn add_autonomy_observation(
        &self,
        kind: &str,
        source: &str,
        content: &str,
        context: &serde_json::Value,
        priority: f64,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let context_json = serde_json::to_string(context)?;
        conn.execute(
            "INSERT INTO autonomy_observations (kind, source, content, context_json, priority) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                kind,
                source,
                content,
                context_json,
                priority.clamp(0.0, 1.0)
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get_autonomy_observation(
        &self,
        id: i64,
    ) -> Result<Option<crate::autonomy::Observation>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, kind, source, content, context_json, priority, created_at, consumed \
             FROM autonomy_observations WHERE id = ?1",
        )?;
        let result = stmt.query_row(rusqlite::params![id], map_autonomy_observation_row);
        match result {
            Ok(observation) => Ok(Some(observation)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_unconsumed_autonomy_observations(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::Observation>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, kind, source, content, context_json, priority, created_at, consumed \
             FROM autonomy_observations WHERE consumed = 0 ORDER BY priority DESC, id ASC LIMIT ?1",
        )?;
        let results = stmt
            .query_map(
                rusqlite::params![limit as i64],
                map_autonomy_observation_row,
            )?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn mark_autonomy_observation_consumed(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE autonomy_observations SET consumed = 1 WHERE id = ?1",
            rusqlite::params![id],
        )?;
        Ok(())
    }

    pub fn record_growth_event(
        &self,
        kind: &str,
        source: &str,
        target: Option<&str>,
        summary: &str,
        success: bool,
        details: &serde_json::Value,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let details_json = serde_json::to_string(details)?;
        conn.execute(
            "INSERT INTO growth_events (kind, source, target, summary, success, details_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![kind, source, target, summary, success, details_json],
        )?;
        let growth_event_id = conn.last_insert_rowid();
        if let Err(error) = append_growth_event_ledger(
            &conn,
            growth_event_id,
            kind,
            source,
            target,
            summary,
            success,
            details,
        ) {
            tracing::warn!(
                "execution ledger: failed to mirror growth event {}: {}",
                growth_event_id,
                error
            );
        }
        Ok(growth_event_id)
    }

    pub fn get_growth_event(&self, id: i64) -> Result<Option<crate::autonomy::GrowthEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, kind, source, target, summary, success, details_json, created_at \
             FROM growth_events WHERE id = ?1",
        )?;
        let result = stmt.query_row(rusqlite::params![id], map_growth_event_row);
        match result {
            Ok(event) => Ok(Some(event)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_recent_growth_events(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::GrowthEvent>> {
        self.list_recent_growth_events_filtered(None, None, None, None, limit)
    }

    pub fn list_recent_growth_events_filtered(
        &self,
        kind: Option<&str>,
        source: Option<&str>,
        target: Option<&str>,
        success: Option<bool>,
        limit: usize,
    ) -> Result<Vec<crate::autonomy::GrowthEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(kind) = kind {
            clauses.push("kind = ?");
            params.push(rusqlite::types::Value::from(kind.to_string()));
        }
        if let Some(source) = source {
            clauses.push("source = ?");
            params.push(rusqlite::types::Value::from(source.to_string()));
        }
        if let Some(target) = target {
            clauses.push("target = ?");
            params.push(rusqlite::types::Value::from(target.to_string()));
        }
        if let Some(success) = success {
            clauses.push("success = ?");
            params.push(rusqlite::types::Value::from(success as i64));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT id, kind, source, target, summary, success, details_json, created_at \
             FROM growth_events{} ORDER BY id DESC LIMIT ?",
            where_clause
        );
        params.push(rusqlite::types::Value::from(limit as i64));

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(params.iter()),
            map_growth_event_row,
        )?;
        Ok(rows.filter_map(|row| row.ok()).collect())
    }

    pub fn count_growth_events_filtered(
        &self,
        kind: Option<&str>,
        source: Option<&str>,
        target: Option<&str>,
        success: Option<bool>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(kind) = kind {
            clauses.push("kind = ?");
            params.push(rusqlite::types::Value::from(kind.to_string()));
        }
        if let Some(source) = source {
            clauses.push("source = ?");
            params.push(rusqlite::types::Value::from(source.to_string()));
        }
        if let Some(target) = target {
            clauses.push("target = ?");
            params.push(rusqlite::types::Value::from(target.to_string()));
        }
        if let Some(success) = success {
            clauses.push("success = ?");
            params.push(rusqlite::types::Value::from(success as i64));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!("SELECT COUNT(*) FROM growth_events{}", where_clause);
        let mut stmt = conn.prepare(&sql)?;
        let count: i64 =
            stmt.query_row(rusqlite::params_from_iter(params.iter()), |row| row.get(0))?;
        Ok(count.max(0) as usize)
    }

    pub fn record_self_model_snapshot(
        &self,
        source: &str,
        trigger_kind: &str,
        trigger_target: Option<&str>,
        summary: &str,
        snapshot: &serde_json::Value,
        capability_count: usize,
        constraint_count: usize,
        active_goal_count: usize,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let snapshot_json = serde_json::to_string(snapshot)?;
        conn.execute(
            "INSERT INTO self_model_snapshots \
             (source, trigger_kind, trigger_target, summary, snapshot_json, capability_count, constraint_count, active_goal_count) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                source,
                trigger_kind,
                trigger_target,
                summary,
                snapshot_json,
                capability_count as i64,
                constraint_count as i64,
                active_goal_count as i64,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn list_recent_self_model_snapshots(&self, limit: usize) -> Result<Vec<serde_json::Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source, trigger_kind, trigger_target, summary, snapshot_json, \
                    capability_count, constraint_count, active_goal_count, created_at \
             FROM self_model_snapshots ORDER BY id DESC LIMIT ?1",
        )?;
        let results = stmt
            .query_map(rusqlite::params![limit as i64], map_self_model_snapshot_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn get_self_model_snapshot(&self, id: i64) -> Result<Option<serde_json::Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source, trigger_kind, trigger_target, summary, snapshot_json, \
                    capability_count, constraint_count, active_goal_count, created_at \
             FROM self_model_snapshots WHERE id = ?1",
        )?;
        let result = stmt.query_row(rusqlite::params![id], map_self_model_snapshot_row);
        match result {
            Ok(snapshot) => Ok(Some(snapshot)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn get_built_tool_health(
        &self,
        tool_name: &str,
    ) -> Result<Option<crate::forge::BuiltToolHealthState>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT tool_name, filename, failure_count, first_failed_at, last_failed_at, \
                    quarantined_until, last_error, updated_at \
             FROM built_tool_health WHERE tool_name = ?1",
        )?;
        let result = stmt.query_row(rusqlite::params![tool_name], map_built_tool_health_row);
        match result {
            Ok(state) => Ok(Some(state)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn upsert_built_tool_health(
        &self,
        state: &crate::forge::BuiltToolHealthState,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let updated_at = if state.updated_at.trim().is_empty() {
            current_timestamp_string()
        } else {
            state.updated_at.clone()
        };
        conn.execute(
            "INSERT INTO built_tool_health \
             (tool_name, filename, failure_count, first_failed_at, last_failed_at, quarantined_until, \
              last_error, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(tool_name) DO UPDATE SET \
                filename = excluded.filename, \
                failure_count = excluded.failure_count, \
                first_failed_at = excluded.first_failed_at, \
                last_failed_at = excluded.last_failed_at, \
                quarantined_until = excluded.quarantined_until, \
                last_error = excluded.last_error, \
                updated_at = excluded.updated_at",
            rusqlite::params![
                state.tool_name,
                state.filename,
                state.failure_count as i64,
                state.first_failed_at,
                state.last_failed_at,
                state.quarantined_until,
                state.last_error,
                updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn clear_built_tool_health(&self, tool_name: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM built_tool_health WHERE tool_name = ?1",
            rusqlite::params![tool_name],
        )?;
        Ok(())
    }

    pub fn list_unhealthy_built_tools(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::forge::BuiltToolHealthState>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT tool_name, filename, failure_count, first_failed_at, last_failed_at, \
                    quarantined_until, last_error, updated_at \
             FROM built_tool_health ORDER BY updated_at DESC, tool_name ASC LIMIT ?1",
        )?;
        let results = stmt
            .query_map(rusqlite::params![limit as i64], map_built_tool_health_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(results)
    }

    pub fn count_unhealthy_built_tools(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM built_tool_health", [], |row| {
            row.get(0)
        })?;
        Ok(count.max(0) as usize)
    }
}

fn append_growth_event_ledger(
    conn: &Connection,
    growth_event_id: i64,
    kind: &str,
    source: &str,
    target: Option<&str>,
    summary: &str,
    success: bool,
    details: &serde_json::Value,
) -> Result<i64> {
    let llm_call = last_execution_llm_call(details);
    let target_string = target.map(str::to_string).or_else(|| {
        details
            .pointer("/execution/trace/target")
            .and_then(|value| value.as_str())
            .map(str::to_string)
    });
    let entry = ExecutionLedgerWrite {
        surface: details
            .pointer("/execution/surface")
            .and_then(|value| value.as_str())
            .unwrap_or("growth_event")
            .to_string(),
        kind: details
            .pointer("/execution/kind")
            .and_then(|value| value.as_str())
            .unwrap_or(kind)
            .to_string(),
        source: source.to_string(),
        target: target_string,
        summary: summary.to_string(),
        outcome: details
            .pointer("/execution/outcome")
            .and_then(|value| value.as_str())
            .unwrap_or(if success { "completed" } else { "failed" })
            .to_string(),
        success,
        correlation_id: Some(format!("growth_event:{}", growth_event_id)),
        reference_kind: Some("growth_event".to_string()),
        reference_id: Some(growth_event_id),
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
        route: llm_call
            .as_ref()
            .and_then(|call| call.get("route"))
            .and_then(|value| value.as_str())
            .map(str::to_string),
        latency_ms: llm_call
            .as_ref()
            .and_then(|call| call.get("latency_ms"))
            .and_then(|value| value.as_i64()),
        payload: details.clone(),
    };
    insert_execution_ledger_entry(conn, &entry)
}

fn append_system_incident_ledger(
    conn: &Connection,
    incident_id: i64,
    kind: &str,
    source: &str,
    severity: &str,
    summary: &str,
    details: &serde_json::Value,
) -> Result<i64> {
    let session_id = details
        .get("session_id")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .or_else(|| {
            details
                .pointer("/session/session_id")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            details
                .pointer("/previous_session/session_id")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        });
    let success = matches!(severity, "info" | "notice");
    let entry = ExecutionLedgerWrite {
        surface: "system".to_string(),
        kind: kind.to_string(),
        source: source.to_string(),
        target: session_id.clone(),
        summary: summary.to_string(),
        outcome: "recorded".to_string(),
        success,
        correlation_id: session_id.map(|value| format!("runtime_session:{}", value)),
        reference_kind: Some("system_incident".to_string()),
        reference_id: Some(incident_id),
        channel: None,
        provider: None,
        model: None,
        route: Some(severity.to_string()),
        latency_ms: None,
        payload: serde_json::json!({
            "severity": severity,
            "details": details,
        }),
    };
    insert_execution_ledger_entry(conn, &entry)
}

fn map_autonomy_observation_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<crate::autonomy::Observation> {
    let context_json: String = row.get(4)?;
    Ok(crate::autonomy::Observation {
        id: row.get(0)?,
        kind: row.get(1)?,
        source: row.get(2)?,
        content: row.get(3)?,
        context: parse_json_value(context_json),
        priority: row.get(5)?,
        created_at: row.get(6)?,
        consumed: row.get(7)?,
    })
}

fn map_built_tool_health_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<crate::forge::BuiltToolHealthState> {
    Ok(crate::forge::BuiltToolHealthState {
        tool_name: row.get(0)?,
        filename: row.get(1)?,
        failure_count: row.get::<_, i64>(2)?.max(0) as usize,
        first_failed_at: row.get(3)?,
        last_failed_at: row.get(4)?,
        quarantined_until: row.get(5)?,
        last_error: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn map_growth_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<crate::autonomy::GrowthEvent> {
    let details_json: String = row.get(6)?;
    Ok(crate::autonomy::GrowthEvent {
        id: row.get(0)?,
        kind: row.get(1)?,
        source: row.get(2)?,
        target: row.get(3)?,
        summary: row.get(4)?,
        success: row.get(5)?,
        details: serde_json::from_str(&details_json).unwrap_or_default(),
        created_at: row.get(7)?,
    })
}

fn map_system_incident_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SystemIncidentRecord> {
    let details_json: String = row.get(5)?;
    Ok(SystemIncidentRecord {
        id: row.get(0)?,
        kind: row.get(1)?,
        source: row.get(2)?,
        severity: row.get(3)?,
        summary: row.get(4)?,
        details: parse_json_value(details_json),
        created_at: row.get(6)?,
    })
}

fn map_self_model_snapshot_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<serde_json::Value> {
    let snapshot_json: String = row.get(5)?;
    Ok(serde_json::json!({
        "id": row.get::<_, i64>(0)?,
        "source": row.get::<_, String>(1)?,
        "trigger_kind": row.get::<_, String>(2)?,
        "trigger_target": row.get::<_, Option<String>>(3)?,
        "summary": row.get::<_, String>(4)?,
        "snapshot": parse_json_value(snapshot_json),
        "capability_count": row.get::<_, i64>(6)?,
        "constraint_count": row.get::<_, i64>(7)?,
        "active_goal_count": row.get::<_, i64>(8)?,
        "created_at": row.get::<_, String>(9)?,
    }))
}
