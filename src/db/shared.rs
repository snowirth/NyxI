use anyhow::Result;
use rusqlite::Connection;

use super::memory_model::parse_entity_aliases;
use super::{
    ExecutionLedgerRecord, ExecutionLedgerWrite, MemoryClaimRecord, MemoryEntityRecord,
    MemoryEpisodeRecord, MemoryProcedureRecord, MemoryRefreshJobRecord, MemorySourceRecord,
};

pub(super) fn insert_execution_ledger_entry(
    conn: &Connection,
    entry: &ExecutionLedgerWrite,
) -> Result<i64> {
    let payload_json = serde_json::to_string(&entry.payload)?;
    conn.execute(
        "INSERT INTO execution_ledger \
         (surface, kind, source, target, summary, outcome, success, correlation_id, reference_kind, \
          reference_id, channel, provider, model, route, latency_ms, payload_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        rusqlite::params![
            &entry.surface,
            &entry.kind,
            &entry.source,
            &entry.target,
            &entry.summary,
            &entry.outcome,
            entry.success,
            &entry.correlation_id,
            &entry.reference_kind,
            entry.reference_id,
            &entry.channel,
            &entry.provider,
            &entry.model,
            &entry.route,
            entry.latency_ms,
            payload_json,
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

pub(super) fn last_execution_llm_call(details: &serde_json::Value) -> Option<serde_json::Value> {
    details
        .pointer("/execution/llm/actual_calls")
        .and_then(|value| value.as_array())
        .and_then(|calls| calls.last().cloned())
}

pub(super) fn current_timestamp_string() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

pub(super) fn parse_json_value(raw: String) -> serde_json::Value {
    serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::json!({}))
}

pub(super) fn map_memory_source_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MemorySourceRecord> {
    Ok(MemorySourceRecord {
        id: row.get("id")?,
        source_kind: row.get("source_kind")?,
        url_or_ref: row.get("url_or_ref")?,
        title: row.get("title")?,
        publisher: row.get("publisher")?,
        trust_tier: row.get("trust_tier")?,
        checksum: row.get("checksum")?,
        refresh_query: row.get("refresh_query")?,
        observed_at: row.get("observed_at")?,
        last_checked_at: row.get("last_checked_at")?,
        created_at: row.get("created_at")?,
    })
}

pub(super) fn map_memory_episode_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MemoryEpisodeRecord> {
    Ok(MemoryEpisodeRecord {
        id: row.get("id")?,
        legacy_memory_id: row.get("legacy_memory_id")?,
        source_id: row.get("source_id")?,
        entity_id: row.get("entity_id")?,
        actor: row.get("actor")?,
        channel: row.get("channel")?,
        network: row.get("network")?,
        summary: row.get("summary")?,
        content: row.get("content")?,
        importance: row.get("importance")?,
        event_at: row.get("event_at")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

pub(super) fn map_memory_claim_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryClaimRecord> {
    Ok(MemoryClaimRecord {
        id: row.get("id")?,
        legacy_memory_id: row.get("legacy_memory_id")?,
        episode_id: row.get("episode_id")?,
        source_id: row.get("source_id")?,
        entity_id: row.get("entity_id")?,
        version_root_id: row.get("version_root_id")?,
        supersedes_claim_id: row.get("supersedes_claim_id")?,
        kind: row.get("kind")?,
        scope: row.get("scope")?,
        subject: row.get("subject")?,
        predicate: row.get("predicate")?,
        object: row.get("object")?,
        statement: row.get("statement")?,
        confidence: row.get("confidence")?,
        importance: row.get("importance")?,
        status: row.get("status")?,
        valid_from: row.get("valid_from")?,
        valid_to: row.get("valid_to")?,
        freshness_ttl_secs: row.get("freshness_ttl_secs")?,
        superseded_by: row.get("superseded_by")?,
        visibility: row.get("visibility")?,
        disputed_at: row.get("disputed_at")?,
        dispute_note: row.get("dispute_note")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        is_stale: false,
    })
}

pub(super) fn map_memory_entity_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MemoryEntityRecord> {
    let aliases_json: String = row.get("aliases_json")?;
    Ok(MemoryEntityRecord {
        id: row.get("id")?,
        entity_key: row.get("entity_key")?,
        entity_kind: row.get("entity_kind")?,
        canonical_name: row.get("canonical_name")?,
        aliases: parse_entity_aliases(&aliases_json),
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

pub(super) fn map_memory_refresh_job_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MemoryRefreshJobRecord> {
    Ok(MemoryRefreshJobRecord {
        id: row.get("id")?,
        claim_id: row.get("claim_id")?,
        source_id: row.get("source_id")?,
        entity_id: row.get("entity_id")?,
        refresh_query: row.get("refresh_query")?,
        status: row.get("status")?,
        attempt_count: row.get("attempt_count")?,
        scheduled_for: row.get("scheduled_for")?,
        started_at: row.get("started_at")?,
        completed_at: row.get("completed_at")?,
        last_error: row.get("last_error")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

pub(super) fn map_memory_procedure_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<MemoryProcedureRecord> {
    Ok(MemoryProcedureRecord {
        id: row.get("id")?,
        legacy_memory_id: row.get("legacy_memory_id")?,
        episode_id: row.get("episode_id")?,
        title: row.get("title")?,
        content: row.get("content")?,
        trigger: row.get("trigger")?,
        confidence: row.get("confidence")?,
        importance: row.get("importance")?,
        status: row.get("status")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

pub(super) fn map_execution_ledger_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionLedgerRecord> {
    let payload_json: String = row.get(16)?;
    Ok(ExecutionLedgerRecord {
        id: row.get(0)?,
        surface: row.get(1)?,
        kind: row.get(2)?,
        source: row.get(3)?,
        target: row.get(4)?,
        summary: row.get(5)?,
        outcome: row.get(6)?,
        success: row.get(7)?,
        correlation_id: row.get(8)?,
        reference_kind: row.get(9)?,
        reference_id: row.get(10)?,
        channel: row.get(11)?,
        provider: row.get(12)?,
        model: row.get(13)?,
        route: row.get(14)?,
        latency_ms: row.get(15)?,
        payload: parse_json_value(payload_json),
        created_at: row.get(17)?,
    })
}
