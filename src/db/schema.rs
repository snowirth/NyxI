use anyhow::{Context, Result};
use rusqlite::Connection;

const BASE_SCHEMA_SQL: &str = "
    CREATE TABLE IF NOT EXISTS memories (
        id TEXT PRIMARY KEY,
        content TEXT NOT NULL,
        network TEXT NOT NULL DEFAULT 'experience',
        importance REAL NOT NULL DEFAULT 0.5,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
        access_count INTEGER NOT NULL DEFAULT 0,
        superseded_by TEXT DEFAULT NULL,
        event_date TEXT DEFAULT NULL
    );

    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel TEXT NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_msg_channel ON messages(channel);

    CREATE TABLE IF NOT EXISTS memory_session_capsules (
        id TEXT PRIMARY KEY,
        source_message_id INTEGER NOT NULL UNIQUE,
        session_key TEXT NOT NULL UNIQUE,
        channel TEXT NOT NULL,
        summary TEXT NOT NULL,
        keyphrases_json TEXT NOT NULL DEFAULT '[]',
        entity_markers_json TEXT NOT NULL DEFAULT '[]',
        marker_terms_json TEXT NOT NULL DEFAULT '[]',
        message_count INTEGER NOT NULL DEFAULT 0,
        last_message_at TEXT NOT NULL DEFAULT (datetime('now')),
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (source_message_id) REFERENCES messages(id)
    );
    CREATE INDEX IF NOT EXISTS idx_memory_session_capsules_channel_time
        ON memory_session_capsules(channel, last_message_at DESC);
    CREATE INDEX IF NOT EXISTS idx_memory_session_capsules_updated
        ON memory_session_capsules(updated_at DESC);

    CREATE TABLE IF NOT EXISTS memory_capsule_anchors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        capsule_id TEXT NOT NULL,
        anchor_index INTEGER NOT NULL,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (capsule_id) REFERENCES memory_session_capsules(id)
    );
    CREATE INDEX IF NOT EXISTS idx_memory_capsule_anchors_capsule
        ON memory_capsule_anchors(capsule_id, anchor_index ASC);

    CREATE VIRTUAL TABLE IF NOT EXISTS memory_session_capsules_fts USING fts5(
        search_text,
        capsule_id UNINDEXED
    );

    CREATE TABLE IF NOT EXISTS chat_traces (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel TEXT NOT NULL,
        sender TEXT NOT NULL,
        intent TEXT NOT NULL,
        route TEXT NOT NULL,
        outcome TEXT NOT NULL,
        cache_hit BOOLEAN NOT NULL DEFAULT 0,
        depth INTEGER NOT NULL DEFAULT 0,
        trace_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_chat_traces_channel_created
        ON chat_traces(channel, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_chat_traces_route_created
        ON chat_traces(route, created_at DESC);

    CREATE TABLE IF NOT EXISTS execution_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        surface TEXT NOT NULL,
        kind TEXT NOT NULL,
        source TEXT NOT NULL,
        target TEXT,
        summary TEXT NOT NULL,
        outcome TEXT NOT NULL,
        success BOOLEAN NOT NULL DEFAULT 0,
        correlation_id TEXT,
        reference_kind TEXT,
        reference_id INTEGER,
        channel TEXT,
        provider TEXT,
        model TEXT,
        route TEXT,
        latency_ms INTEGER,
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_execution_ledger_surface_created
        ON execution_ledger(surface, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_execution_ledger_kind_created
        ON execution_ledger(kind, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_execution_ledger_reference
        ON execution_ledger(reference_kind, reference_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_execution_ledger_correlation
        ON execution_ledger(correlation_id, created_at DESC);

    CREATE TABLE IF NOT EXISTS system_incidents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        source TEXT NOT NULL,
        severity TEXT NOT NULL DEFAULT 'info',
        summary TEXT NOT NULL,
        details_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_system_incidents_kind_created
        ON system_incidents(kind, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_system_incidents_severity_created
        ON system_incidents(severity, created_at DESC);

    CREATE TABLE IF NOT EXISTS replay_failure_clusters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fingerprint TEXT NOT NULL UNIQUE,
        task_kind TEXT NOT NULL,
        tool TEXT,
        failure_class TEXT NOT NULL,
        failure_stage TEXT NOT NULL,
        latest_outcome TEXT NOT NULL,
        issue_signature TEXT NOT NULL,
        exemplar_summary TEXT NOT NULL,
        exemplar_error TEXT,
        target TEXT,
        provider TEXT,
        model TEXT,
        route TEXT,
        occurrence_count INTEGER NOT NULL DEFAULT 0,
        first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        sample_action_run_ids_json TEXT NOT NULL DEFAULT '[]',
        latest_action_run_id INTEGER NOT NULL,
        latest_task_id INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_replay_failure_clusters_last_seen
        ON replay_failure_clusters(last_seen_at DESC, occurrence_count DESC);
    CREATE INDEX IF NOT EXISTS idx_replay_failure_clusters_kind_class
        ON replay_failure_clusters(task_kind, failure_class, last_seen_at DESC);
    CREATE INDEX IF NOT EXISTS idx_replay_failure_clusters_tool
        ON replay_failure_clusters(tool, last_seen_at DESC);

    CREATE TABLE IF NOT EXISTS policy_candidates (
        id TEXT PRIMARY KEY,
        source_kind TEXT NOT NULL,
        source_ref TEXT NOT NULL,
        kind TEXT NOT NULL,
        scope TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        rationale TEXT NOT NULL,
        trigger TEXT NOT NULL UNIQUE,
        proposed_change_json TEXT NOT NULL DEFAULT '{}',
        evidence_json TEXT NOT NULL DEFAULT '{}',
        confidence REAL NOT NULL DEFAULT 0.5,
        importance REAL NOT NULL DEFAULT 0.5,
        status TEXT NOT NULL DEFAULT 'candidate',
        last_score REAL,
        last_verdict TEXT,
        approved_at TEXT,
        rolled_back_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_policy_candidates_status_updated
        ON policy_candidates(status, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_policy_candidates_kind_scope
        ON policy_candidates(kind, scope, updated_at DESC);

    CREATE TABLE IF NOT EXISTS policy_evaluations (
        id TEXT PRIMARY KEY,
        candidate_id TEXT NOT NULL,
        evaluation_kind TEXT NOT NULL,
        summary TEXT NOT NULL,
        score REAL NOT NULL,
        verdict TEXT NOT NULL,
        metrics_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (candidate_id) REFERENCES policy_candidates(id)
    );
    CREATE INDEX IF NOT EXISTS idx_policy_evaluations_candidate_created
        ON policy_evaluations(candidate_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_policy_evaluations_verdict_created
        ON policy_evaluations(verdict, created_at DESC);

    CREATE TABLE IF NOT EXISTS policy_change_events (
        id TEXT PRIMARY KEY,
        candidate_id TEXT NOT NULL,
        event_kind TEXT NOT NULL,
        summary TEXT NOT NULL,
        details_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (candidate_id) REFERENCES policy_candidates(id)
    );
    CREATE INDEX IF NOT EXISTS idx_policy_change_events_candidate_created
        ON policy_change_events(candidate_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_policy_change_events_kind_created
        ON policy_change_events(event_kind, created_at DESC);

    CREATE TABLE IF NOT EXISTS state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS reminders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        content TEXT NOT NULL,
        due_at TEXT,
        done BOOLEAN NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS scheduled_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT NOT NULL,
        tool TEXT NOT NULL,
        tool_args TEXT NOT NULL DEFAULT '{}',
        interval_secs INTEGER NOT NULL,
        last_run INTEGER NOT NULL DEFAULT 0,
        enabled BOOLEAN NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS autonomy_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        source TEXT NOT NULL,
        content TEXT NOT NULL,
        context_json TEXT NOT NULL DEFAULT '{}',
        priority REAL NOT NULL DEFAULT 0.5,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        consumed BOOLEAN NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_autonomy_observations_consumed
        ON autonomy_observations(consumed, created_at);

    CREATE TABLE IF NOT EXISTS autonomy_goals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        priority REAL NOT NULL DEFAULT 0.5,
        source TEXT NOT NULL DEFAULT 'system',
        details TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        last_reviewed_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_autonomy_goals_status
        ON autonomy_goals(status, priority DESC, updated_at DESC);

    CREATE TABLE IF NOT EXISTS autonomy_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        goal_id INTEGER,
        kind TEXT NOT NULL,
        title TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        tool TEXT,
        args_json TEXT NOT NULL DEFAULT '{}',
        notes TEXT,
        priority REAL NOT NULL DEFAULT 0.5,
        scheduled_for TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        last_run_at TEXT,
        FOREIGN KEY (goal_id) REFERENCES autonomy_goals(id)
    );
    CREATE INDEX IF NOT EXISTS idx_autonomy_tasks_ready
        ON autonomy_tasks(status, scheduled_for, priority DESC, created_at);

    CREATE TABLE IF NOT EXISTS autonomy_action_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        outcome TEXT NOT NULL,
        summary TEXT NOT NULL,
        output_json TEXT,
        executed BOOLEAN,
        verified BOOLEAN,
        expected_effect_json TEXT,
        verifier_verdict_json TEXT,
        rollback_reason_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (task_id) REFERENCES autonomy_tasks(id)
    );
    CREATE INDEX IF NOT EXISTS idx_autonomy_action_runs_task
        ON autonomy_action_runs(task_id, created_at DESC);

    CREATE TABLE IF NOT EXISTS growth_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        source TEXT NOT NULL,
        target TEXT,
        summary TEXT NOT NULL,
        success BOOLEAN NOT NULL DEFAULT 0,
        details_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_growth_events_kind_created
        ON growth_events(kind, created_at DESC);

    CREATE TABLE IF NOT EXISTS self_model_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        trigger_kind TEXT NOT NULL,
        trigger_target TEXT,
        summary TEXT NOT NULL,
        snapshot_json TEXT NOT NULL,
        capability_count INTEGER NOT NULL DEFAULT 0,
        constraint_count INTEGER NOT NULL DEFAULT 0,
        active_goal_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_self_model_snapshots_created
        ON self_model_snapshots(created_at DESC, id DESC);

    CREATE TABLE IF NOT EXISTS autonomy_retry_state (
        key TEXT PRIMARY KEY,
        task_kind TEXT NOT NULL,
        target TEXT,
        failure_class TEXT NOT NULL DEFAULT 'transient',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        first_failed_at TEXT,
        last_failed_at TEXT,
        next_retry_at TEXT,
        quarantined_until TEXT,
        last_error TEXT,
        last_task_id INTEGER,
        last_growth_event_id INTEGER,
        last_snapshot_id INTEGER,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_autonomy_retry_state_kind_updated
        ON autonomy_retry_state(task_kind, updated_at DESC);

    CREATE TABLE IF NOT EXISTS built_tool_health (
        tool_name TEXT PRIMARY KEY,
        filename TEXT NOT NULL,
        failure_count INTEGER NOT NULL DEFAULT 0,
        first_failed_at TEXT,
        last_failed_at TEXT,
        quarantined_until TEXT,
        last_error TEXT,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_built_tool_health_updated
        ON built_tool_health(updated_at DESC);

    CREATE TABLE IF NOT EXISTS memory_inbox (
        id TEXT PRIMARY KEY,
        event_kind TEXT NOT NULL,
        actor TEXT,
        channel TEXT,
        content TEXT NOT NULL,
        source_kind TEXT,
        source_ref TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_memory_inbox_created
        ON memory_inbox(created_at DESC);

    CREATE TABLE IF NOT EXISTS memory_sources (
        id TEXT PRIMARY KEY,
        source_kind TEXT NOT NULL,
        url_or_ref TEXT NOT NULL,
        title TEXT,
        publisher TEXT,
        trust_tier REAL NOT NULL DEFAULT 0.5,
        checksum TEXT,
        refresh_query TEXT,
        observed_at TEXT NOT NULL DEFAULT (datetime('now')),
        last_checked_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_memory_sources_kind_observed
        ON memory_sources(source_kind, observed_at DESC);

    CREATE TABLE IF NOT EXISTS memory_entities (
        id TEXT PRIMARY KEY,
        entity_key TEXT NOT NULL UNIQUE,
        entity_kind TEXT NOT NULL DEFAULT 'entity',
        canonical_name TEXT NOT NULL,
        aliases_json TEXT NOT NULL DEFAULT '[]',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_memory_entities_kind_updated
        ON memory_entities(entity_kind, updated_at DESC);

    CREATE TABLE IF NOT EXISTS memory_episodes (
        id TEXT PRIMARY KEY,
        legacy_memory_id TEXT UNIQUE,
        source_id TEXT,
        entity_id TEXT,
        actor TEXT NOT NULL DEFAULT 'nyx',
        channel TEXT,
        network TEXT NOT NULL DEFAULT 'experience',
        summary TEXT NOT NULL,
        content TEXT NOT NULL,
        importance REAL NOT NULL DEFAULT 0.5,
        event_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (legacy_memory_id) REFERENCES memories(id),
        FOREIGN KEY (source_id) REFERENCES memory_sources(id),
        FOREIGN KEY (entity_id) REFERENCES memory_entities(id)
    );
    CREATE INDEX IF NOT EXISTS idx_memory_episodes_legacy
        ON memory_episodes(legacy_memory_id);
    CREATE INDEX IF NOT EXISTS idx_memory_episodes_created
        ON memory_episodes(created_at DESC);

    CREATE TABLE IF NOT EXISTS memory_claims (
        id TEXT PRIMARY KEY,
        legacy_memory_id TEXT,
        episode_id TEXT,
        source_id TEXT,
        entity_id TEXT,
        version_root_id TEXT,
        supersedes_claim_id TEXT,
        kind TEXT NOT NULL DEFAULT 'fact',
        scope TEXT NOT NULL DEFAULT 'global',
        subject TEXT NOT NULL DEFAULT 'general',
        predicate TEXT NOT NULL DEFAULT 'states',
        object TEXT NOT NULL,
        statement TEXT NOT NULL,
        confidence REAL NOT NULL DEFAULT 0.5,
        importance REAL NOT NULL DEFAULT 0.5,
        status TEXT NOT NULL DEFAULT 'active',
        valid_from TEXT,
        valid_to TEXT,
        freshness_ttl_secs INTEGER,
        superseded_by TEXT,
        visibility TEXT NOT NULL DEFAULT 'default',
        disputed_at TEXT,
        dispute_note TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (legacy_memory_id) REFERENCES memories(id),
        FOREIGN KEY (episode_id) REFERENCES memory_episodes(id),
        FOREIGN KEY (source_id) REFERENCES memory_sources(id),
        FOREIGN KEY (entity_id) REFERENCES memory_entities(id)
    );
    CREATE INDEX IF NOT EXISTS idx_memory_claims_status_scope
        ON memory_claims(status, scope, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_memory_claims_legacy
        ON memory_claims(legacy_memory_id);

    CREATE TABLE IF NOT EXISTS memory_refresh_jobs (
        id TEXT PRIMARY KEY,
        claim_id TEXT NOT NULL,
        source_id TEXT,
        entity_id TEXT,
        refresh_query TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        scheduled_for TEXT NOT NULL DEFAULT (datetime('now')),
        started_at TEXT,
        completed_at TEXT,
        last_error TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (claim_id) REFERENCES memory_claims(id),
        FOREIGN KEY (source_id) REFERENCES memory_sources(id),
        FOREIGN KEY (entity_id) REFERENCES memory_entities(id)
    );
    CREATE INDEX IF NOT EXISTS idx_memory_refresh_jobs_ready
        ON memory_refresh_jobs(status, scheduled_for, updated_at DESC);

    CREATE TABLE IF NOT EXISTS memory_procedures (
        id TEXT PRIMARY KEY,
        legacy_memory_id TEXT UNIQUE,
        episode_id TEXT,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        trigger TEXT NOT NULL DEFAULT 'memory',
        confidence REAL NOT NULL DEFAULT 0.5,
        importance REAL NOT NULL DEFAULT 0.5,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (legacy_memory_id) REFERENCES memories(id),
        FOREIGN KEY (episode_id) REFERENCES memory_episodes(id)
    );
    CREATE INDEX IF NOT EXISTS idx_memory_procedures_status_updated
        ON memory_procedures(status, updated_at DESC);

    CREATE TABLE IF NOT EXISTS memory_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_kind TEXT NOT NULL,
        source_id TEXT NOT NULL,
        target_kind TEXT NOT NULL,
        target_id TEXT NOT NULL,
        edge_type TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_memory_edges_source
        ON memory_edges(source_kind, source_id, created_at DESC);

    CREATE TABLE IF NOT EXISTS interactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER NOT NULL,
        channel TEXT NOT NULL,
        user_msg_len INTEGER NOT NULL,
        response_len INTEGER NOT NULL,
        response_time_ms INTEGER NOT NULL,
        outcome TEXT NOT NULL,
        warmth REAL NOT NULL,
        verbosity REAL NOT NULL,
        assertiveness REAL NOT NULL,
        hour INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS memory_embeddings (
        memory_id TEXT PRIMARY KEY,
        embedding BLOB NOT NULL,
        FOREIGN KEY (memory_id) REFERENCES memories(id)
    );

    CREATE TABLE IF NOT EXISTS memory_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        link_type TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (source_id) REFERENCES memories(id),
        FOREIGN KEY (target_id) REFERENCES memories(id)
    );
";

const ADDITIVE_ALTERS: &[&str] = &[
    "ALTER TABLE memories ADD COLUMN last_accessed TEXT NOT NULL DEFAULT (datetime('now'))",
    "ALTER TABLE memories ADD COLUMN access_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE memories ADD COLUMN superseded_by TEXT DEFAULT NULL",
    "ALTER TABLE memories ADD COLUMN event_date TEXT DEFAULT NULL",
    "ALTER TABLE memory_sources ADD COLUMN refresh_query TEXT",
    "ALTER TABLE memory_sources ADD COLUMN last_checked_at TEXT",
    "ALTER TABLE memory_episodes ADD COLUMN entity_id TEXT",
    "ALTER TABLE memory_claims ADD COLUMN entity_id TEXT",
    "ALTER TABLE memory_claims ADD COLUMN version_root_id TEXT",
    "ALTER TABLE memory_claims ADD COLUMN supersedes_claim_id TEXT",
    "ALTER TABLE memory_claims ADD COLUMN visibility TEXT NOT NULL DEFAULT 'default'",
    "ALTER TABLE memory_claims ADD COLUMN disputed_at TEXT",
    "ALTER TABLE memory_claims ADD COLUMN dispute_note TEXT",
    "ALTER TABLE autonomy_action_runs ADD COLUMN executed BOOLEAN",
    "ALTER TABLE autonomy_action_runs ADD COLUMN verified BOOLEAN",
    "ALTER TABLE autonomy_action_runs ADD COLUMN expected_effect_json TEXT",
    "ALTER TABLE autonomy_action_runs ADD COLUMN verifier_verdict_json TEXT",
    "ALTER TABLE autonomy_action_runs ADD COLUMN rollback_reason_json TEXT",
];

const POST_MIGRATION_SQL: &str = "
    CREATE INDEX IF NOT EXISTS idx_memory_sources_refresh
        ON memory_sources(last_checked_at DESC, observed_at DESC);
    CREATE INDEX IF NOT EXISTS idx_memory_episodes_entity
        ON memory_episodes(entity_id, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_memory_claims_entity
        ON memory_claims(entity_id, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_memory_claims_version_root
        ON memory_claims(version_root_id, created_at ASC, updated_at ASC);
";

const CLAIM_BACKFILL_SQL: &str = "
    UPDATE memory_claims
    SET version_root_id = COALESCE(version_root_id, id),
        visibility = COALESCE(NULLIF(visibility, ''), CASE
           WHEN status IN ('superseded', 'invalidated', 'disputed') THEN 'history'
           ELSE 'default'
        END)
";

pub(super) fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(BASE_SCHEMA_SQL)
        .context("migration failed")?;

    for statement in ADDITIVE_ALTERS {
        conn.execute_batch(statement).ok();
    }

    conn.execute_batch(POST_MIGRATION_SQL).ok();
    conn.execute(CLAIM_BACKFILL_SQL, []).ok();
    ensure_memories_fts_schema(conn)?;

    Ok(())
}

fn ensure_memories_fts_schema(conn: &Connection) -> Result<()> {
    let existing_sql = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name = 'memories_fts'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok();
    let has_external_content = existing_sql
        .as_deref()
        .map(|sql| sql.contains("content=memories"))
        .unwrap_or(false);

    if !has_external_content {
        conn.execute_batch(
            "
            DROP TABLE IF EXISTS memories_fts;
            DROP TRIGGER IF EXISTS memories_ai;
            DROP TRIGGER IF EXISTS memories_ad;
            DROP TRIGGER IF EXISTS memories_au;

            CREATE VIRTUAL TABLE memories_fts USING fts5(
                content,
                content=memories,
                content_rowid=rowid
            );
            ",
        )?;
    }

    conn.execute_batch(
        "
        CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
            INSERT INTO memories_fts(rowid, content)
            VALUES (new.rowid, new.content);
        END;
        CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content)
            VALUES ('delete', old.rowid, old.content);
        END;
        CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content)
            VALUES ('delete', old.rowid, old.content);
            INSERT INTO memories_fts(rowid, content)
            VALUES (new.rowid, new.content);
        END;
        ",
    )?;

    if !has_external_content {
        conn.execute(
            "INSERT INTO memories_fts(memories_fts) VALUES ('rebuild')",
            [],
        )
        .ok();
    }

    Ok(())
}
