use anyhow::Result;
use rusqlite::Connection;

use super::shared::{insert_execution_ledger_entry, map_execution_ledger_row, parse_json_value};
use super::{ChatTraceRecord, Db, ExecutionLedgerRecord, ExecutionLedgerWrite};

impl Db {
    pub fn record_chat_trace(
        &self,
        channel: &str,
        sender: &str,
        intent: &str,
        route: &str,
        outcome: &str,
        cache_hit: bool,
        depth: u8,
        trace: &serde_json::Value,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let trace_json = serde_json::to_string(trace)?;
        conn.execute(
            "INSERT INTO chat_traces \
             (channel, sender, intent, route, outcome, cache_hit, depth, trace_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                channel,
                sender,
                intent,
                route,
                outcome,
                cache_hit,
                depth as i64,
                trace_json
            ],
        )?;
        let chat_trace_id = conn.last_insert_rowid();
        if let Err(error) = append_chat_trace_ledger(
            &conn,
            chat_trace_id,
            channel,
            sender,
            route,
            outcome,
            cache_hit,
            trace,
        ) {
            tracing::warn!(
                "execution ledger: failed to mirror chat trace {}: {}",
                chat_trace_id,
                error
            );
        }
        Ok(chat_trace_id)
    }

    pub fn list_recent_chat_traces(
        &self,
        channel: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ChatTraceRecord>> {
        self.list_recent_chat_traces_filtered(channel, None, None, limit)
    }

    pub fn count_chat_traces(&self, channel: Option<&str>) -> Result<usize> {
        self.count_chat_traces_filtered(channel, None, None)
    }

    pub fn list_recent_chat_traces_filtered(
        &self,
        channel: Option<&str>,
        route: Option<&str>,
        outcome: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ChatTraceRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(channel) = channel {
            clauses.push("channel = ?");
            params.push(rusqlite::types::Value::from(channel.to_string()));
        }
        if let Some(route) = route {
            clauses.push("route = ?");
            params.push(rusqlite::types::Value::from(route.to_string()));
        }
        if let Some(outcome) = outcome {
            clauses.push("outcome = ?");
            params.push(rusqlite::types::Value::from(outcome.to_string()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT id, channel, sender, intent, route, outcome, cache_hit, depth, trace_json, created_at \
             FROM chat_traces{} \
             ORDER BY id DESC LIMIT ?",
            where_clause
        );
        params.push(rusqlite::types::Value::from(limit as i64));

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(params.iter()),
            map_chat_trace_row,
        )?;
        Ok(rows.filter_map(|row| row.ok()).collect())
    }

    pub fn count_chat_traces_filtered(
        &self,
        channel: Option<&str>,
        route: Option<&str>,
        outcome: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(channel) = channel {
            clauses.push("channel = ?");
            params.push(rusqlite::types::Value::from(channel.to_string()));
        }
        if let Some(route) = route {
            clauses.push("route = ?");
            params.push(rusqlite::types::Value::from(route.to_string()));
        }
        if let Some(outcome) = outcome {
            clauses.push("outcome = ?");
            params.push(rusqlite::types::Value::from(outcome.to_string()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!("SELECT COUNT(*) FROM chat_traces{}", where_clause);
        let count: i64 =
            conn.query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                row.get(0)
            })?;
        Ok(count.max(0) as usize)
    }

    pub fn record_execution_ledger_entry(&self, entry: &ExecutionLedgerWrite) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        insert_execution_ledger_entry(&conn, entry)
    }

    pub fn list_recent_execution_ledger_filtered(
        &self,
        surface: Option<&str>,
        kind: Option<&str>,
        outcome: Option<&str>,
        source: Option<&str>,
        reference_kind: Option<&str>,
        reference_id: Option<i64>,
        correlation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ExecutionLedgerRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(surface) = surface {
            clauses.push("surface = ?");
            params.push(rusqlite::types::Value::from(surface.to_string()));
        }
        if let Some(kind) = kind {
            clauses.push("kind = ?");
            params.push(rusqlite::types::Value::from(kind.to_string()));
        }
        if let Some(outcome) = outcome {
            clauses.push("outcome = ?");
            params.push(rusqlite::types::Value::from(outcome.to_string()));
        }
        if let Some(source) = source {
            clauses.push("source = ?");
            params.push(rusqlite::types::Value::from(source.to_string()));
        }
        if let Some(reference_kind) = reference_kind {
            clauses.push("reference_kind = ?");
            params.push(rusqlite::types::Value::from(reference_kind.to_string()));
        }
        if let Some(reference_id) = reference_id {
            clauses.push("reference_id = ?");
            params.push(rusqlite::types::Value::from(reference_id));
        }
        if let Some(correlation_id) = correlation_id {
            clauses.push("correlation_id = ?");
            params.push(rusqlite::types::Value::from(correlation_id.to_string()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT id, surface, kind, source, target, summary, outcome, success, correlation_id, \
                    reference_kind, reference_id, channel, provider, model, route, latency_ms, \
                    payload_json, created_at \
             FROM execution_ledger{} ORDER BY id DESC LIMIT ?",
            where_clause
        );
        params.push(rusqlite::types::Value::from(limit as i64));

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(params.iter()),
            map_execution_ledger_row,
        )?;
        Ok(rows.filter_map(|row| row.ok()).collect())
    }

    pub fn count_execution_ledger_filtered(
        &self,
        surface: Option<&str>,
        kind: Option<&str>,
        outcome: Option<&str>,
        source: Option<&str>,
        reference_kind: Option<&str>,
        reference_id: Option<i64>,
        correlation_id: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut clauses = Vec::new();
        let mut params = Vec::new();

        if let Some(surface) = surface {
            clauses.push("surface = ?");
            params.push(rusqlite::types::Value::from(surface.to_string()));
        }
        if let Some(kind) = kind {
            clauses.push("kind = ?");
            params.push(rusqlite::types::Value::from(kind.to_string()));
        }
        if let Some(outcome) = outcome {
            clauses.push("outcome = ?");
            params.push(rusqlite::types::Value::from(outcome.to_string()));
        }
        if let Some(source) = source {
            clauses.push("source = ?");
            params.push(rusqlite::types::Value::from(source.to_string()));
        }
        if let Some(reference_kind) = reference_kind {
            clauses.push("reference_kind = ?");
            params.push(rusqlite::types::Value::from(reference_kind.to_string()));
        }
        if let Some(reference_id) = reference_id {
            clauses.push("reference_id = ?");
            params.push(rusqlite::types::Value::from(reference_id));
        }
        if let Some(correlation_id) = correlation_id {
            clauses.push("correlation_id = ?");
            params.push(rusqlite::types::Value::from(correlation_id.to_string()));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!("SELECT COUNT(*) FROM execution_ledger{}", where_clause);
        let count: i64 =
            conn.query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                row.get(0)
            })?;
        Ok(count.max(0) as usize)
    }
}

fn append_chat_trace_ledger(
    conn: &Connection,
    chat_trace_id: i64,
    channel: &str,
    sender: &str,
    route: &str,
    outcome: &str,
    _cache_hit: bool,
    trace: &serde_json::Value,
) -> Result<i64> {
    let llm = trace.pointer("/details/llm");
    let entry = ExecutionLedgerWrite {
        surface: trace
            .get("surface")
            .and_then(|value| value.as_str())
            .unwrap_or("chat")
            .to_string(),
        kind: trace
            .get("kind")
            .and_then(|value| value.as_str())
            .unwrap_or("chat_turn")
            .to_string(),
        source: sender.to_string(),
        target: None,
        summary: trace
            .get("summary")
            .and_then(|value| value.as_str())
            .unwrap_or("chat turn")
            .to_string(),
        outcome: outcome.to_string(),
        success: trace
            .get("success")
            .and_then(|value| value.as_bool())
            .unwrap_or_else(|| outcome != "failed"),
        correlation_id: Some(format!(
            "chat:{}:{}",
            channel,
            trace
                .pointer("/trace/cache_hash")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown")
        )),
        reference_kind: Some("chat_trace".to_string()),
        reference_id: Some(chat_trace_id),
        channel: Some(channel.to_string()),
        provider: llm
            .and_then(|value| value.get("provider"))
            .and_then(|value| value.as_str())
            .map(str::to_string),
        model: llm
            .and_then(|value| value.get("model"))
            .and_then(|value| value.as_str())
            .map(str::to_string),
        route: llm
            .and_then(|value| value.get("route"))
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .or_else(|| Some(route.to_string())),
        latency_ms: llm
            .and_then(|value| value.get("latency_ms"))
            .and_then(|value| value.as_i64()),
        payload: trace.clone(),
    };
    insert_execution_ledger_entry(conn, &entry)
}

fn map_chat_trace_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChatTraceRecord> {
    let trace_json: String = row.get(8)?;
    Ok(ChatTraceRecord {
        id: row.get(0)?,
        channel: row.get(1)?,
        sender: row.get(2)?,
        intent: row.get(3)?,
        route: row.get(4)?,
        outcome: row.get(5)?,
        cache_hit: row.get(6)?,
        depth: row.get(7)?,
        trace: parse_json_value(trace_json),
        created_at: row.get(9)?,
    })
}
