use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemorySourceRecord {
    pub id: String,
    pub source_kind: String,
    pub url_or_ref: String,
    pub title: Option<String>,
    pub publisher: Option<String>,
    pub trust_tier: f64,
    pub checksum: Option<String>,
    pub refresh_query: Option<String>,
    pub observed_at: String,
    pub last_checked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryEpisodeRecord {
    pub id: String,
    pub legacy_memory_id: Option<String>,
    pub source_id: Option<String>,
    pub entity_id: Option<String>,
    pub actor: String,
    pub channel: Option<String>,
    pub network: String,
    pub summary: String,
    pub content: String,
    pub importance: f64,
    pub event_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryClaimRecord {
    pub id: String,
    pub legacy_memory_id: Option<String>,
    pub episode_id: Option<String>,
    pub source_id: Option<String>,
    pub entity_id: Option<String>,
    pub version_root_id: Option<String>,
    pub supersedes_claim_id: Option<String>,
    pub kind: String,
    pub scope: String,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub statement: String,
    pub confidence: f64,
    pub importance: f64,
    pub status: String,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    pub freshness_ttl_secs: Option<i64>,
    pub superseded_by: Option<String>,
    pub visibility: String,
    pub disputed_at: Option<String>,
    pub dispute_note: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub is_stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryEntityRecord {
    pub id: String,
    pub entity_key: String,
    pub entity_kind: String,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryRefreshJobRecord {
    pub id: String,
    pub claim_id: String,
    pub source_id: Option<String>,
    pub entity_id: Option<String>,
    pub refresh_query: String,
    pub status: String,
    pub attempt_count: i64,
    pub scheduled_for: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryProcedureRecord {
    pub id: String,
    pub legacy_memory_id: Option<String>,
    pub episode_id: Option<String>,
    pub title: String,
    pub content: String,
    pub trigger: String,
    pub confidence: f64,
    pub importance: f64,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryCapsuleAnchorRecord {
    pub anchor_index: i64,
    pub role: String,
    pub content: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemorySessionCapsuleRecord {
    pub id: String,
    pub source_message_id: i64,
    pub session_key: String,
    pub channel: String,
    pub summary: String,
    pub keyphrases: Vec<String>,
    pub entity_markers: Vec<String>,
    pub marker_terms: Vec<String>,
    pub message_count: i64,
    pub last_message_at: String,
    pub created_at: String,
    pub updated_at: String,
    pub anchors: Vec<MemoryCapsuleAnchorRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatTraceRecord {
    pub id: i64,
    pub channel: String,
    pub sender: String,
    pub intent: String,
    pub route: String,
    pub outcome: String,
    pub cache_hit: bool,
    pub depth: i64,
    pub trace: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionLedgerRecord {
    pub id: i64,
    pub surface: String,
    pub kind: String,
    pub source: String,
    pub target: Option<String>,
    pub summary: String,
    pub outcome: String,
    pub success: bool,
    pub correlation_id: Option<String>,
    pub reference_kind: Option<String>,
    pub reference_id: Option<i64>,
    pub channel: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub route: Option<String>,
    pub latency_ms: Option<i64>,
    pub payload: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionLedgerWrite {
    pub surface: String,
    pub kind: String,
    pub source: String,
    pub target: Option<String>,
    pub summary: String,
    pub outcome: String,
    pub success: bool,
    pub correlation_id: Option<String>,
    pub reference_kind: Option<String>,
    pub reference_id: Option<i64>,
    pub channel: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub route: Option<String>,
    pub latency_ms: Option<i64>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SystemIncidentRecord {
    pub id: i64,
    pub kind: String,
    pub source: String,
    pub severity: String,
    pub summary: String,
    pub details: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayFailureClusterRecord {
    pub id: i64,
    pub fingerprint: String,
    pub task_kind: String,
    pub tool: Option<String>,
    pub failure_class: String,
    pub failure_stage: String,
    pub latest_outcome: String,
    pub issue_signature: String,
    pub exemplar_summary: String,
    pub exemplar_error: Option<String>,
    pub target: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub route: Option<String>,
    pub occurrence_count: i64,
    pub first_seen_at: String,
    pub last_seen_at: String,
    pub sample_action_run_ids: Vec<i64>,
    pub latest_action_run_id: i64,
    pub latest_task_id: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyCandidateRecord {
    pub id: String,
    pub source_kind: String,
    pub source_ref: String,
    pub kind: String,
    pub scope: String,
    pub title: String,
    pub description: String,
    pub rationale: String,
    pub trigger: String,
    pub proposed_change: serde_json::Value,
    pub evidence: serde_json::Value,
    pub confidence: f64,
    pub importance: f64,
    pub status: String,
    pub last_score: Option<f64>,
    pub last_verdict: Option<String>,
    pub approved_at: Option<String>,
    pub rolled_back_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyEvaluationRecord {
    pub id: String,
    pub candidate_id: String,
    pub evaluation_kind: String,
    pub summary: String,
    pub score: f64,
    pub verdict: String,
    pub metrics: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyChangeEventRecord {
    pub id: String,
    pub candidate_id: String,
    pub event_kind: String,
    pub summary: String,
    pub details: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct RecallContext {
    pub profile: Vec<String>,
    pub active_claims: Vec<MemoryClaimRecord>,
    pub recent_episodes: Vec<MemoryEpisodeRecord>,
    pub procedures: Vec<MemoryProcedureRecord>,
    pub supporting_sources: Vec<MemorySourceRecord>,
    pub uncertainties: Vec<String>,
}
