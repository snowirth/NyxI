use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};

use super::autonomy_store::map_replay_failure_cluster_row;
use super::shared::parse_json_value;
use super::{
    Db, PolicyCandidateRecord, PolicyChangeEventRecord, PolicyEvaluationRecord,
    ReplayFailureClusterRecord,
};

impl Db {
    pub fn policy_candidate_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM policy_candidates", [], |row| {
            row.get(0)
        })
        .unwrap_or(0)
    }

    pub fn policy_evaluation_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM policy_evaluations", [], |row| {
            row.get(0)
        })
        .unwrap_or(0)
    }

    pub fn policy_change_event_count(&self) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM policy_change_events", [], |row| {
            row.get(0)
        })
        .unwrap_or(0)
    }

    pub fn list_recent_policy_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<PolicyCandidateRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source_kind, source_ref, kind, scope, title, description, rationale,
                    trigger, proposed_change_json, evidence_json, confidence, importance, status,
                    last_score, last_verdict, approved_at, rolled_back_at, created_at, updated_at
             FROM policy_candidates
             ORDER BY updated_at DESC, created_at DESC
             LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![limit as i64], map_policy_candidate_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(rows)
    }

    pub fn list_approved_policy_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<PolicyCandidateRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source_kind, source_ref, kind, scope, title, description, rationale,
                    trigger, proposed_change_json, evidence_json, confidence, importance, status,
                    last_score, last_verdict, approved_at, rolled_back_at, created_at, updated_at
             FROM policy_candidates
             WHERE status = 'approved'
             ORDER BY importance DESC, confidence DESC, updated_at DESC, created_at DESC
             LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![limit as i64], map_policy_candidate_row)?
            .filter_map(|row| row.ok())
            .collect();
        Ok(rows)
    }

    pub fn list_recent_policy_evaluations(
        &self,
        candidate_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PolicyEvaluationRecord>> {
        let conn = self.conn.lock().unwrap();
        let sql = if candidate_id.is_some() {
            "SELECT id, candidate_id, evaluation_kind, summary, score, verdict, metrics_json, created_at
             FROM policy_evaluations
             WHERE candidate_id = ?1
             ORDER BY created_at DESC, id DESC
             LIMIT ?2"
        } else {
            "SELECT id, candidate_id, evaluation_kind, summary, score, verdict, metrics_json, created_at
             FROM policy_evaluations
             ORDER BY created_at DESC, id DESC
             LIMIT ?1"
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = if let Some(candidate_id) = candidate_id {
            stmt.query_map(
                rusqlite::params![candidate_id, limit as i64],
                map_policy_evaluation_row,
            )?
            .filter_map(|row| row.ok())
            .collect()
        } else {
            stmt.query_map(rusqlite::params![limit as i64], map_policy_evaluation_row)?
                .filter_map(|row| row.ok())
                .collect()
        };
        Ok(rows)
    }

    pub fn list_recent_policy_change_events(
        &self,
        candidate_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PolicyChangeEventRecord>> {
        let conn = self.conn.lock().unwrap();
        let sql = if candidate_id.is_some() {
            "SELECT id, candidate_id, event_kind, summary, details_json, created_at
             FROM policy_change_events
             WHERE candidate_id = ?1
             ORDER BY created_at DESC, id DESC
             LIMIT ?2"
        } else {
            "SELECT id, candidate_id, event_kind, summary, details_json, created_at
             FROM policy_change_events
             ORDER BY created_at DESC, id DESC
             LIMIT ?1"
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = if let Some(candidate_id) = candidate_id {
            stmt.query_map(
                rusqlite::params![candidate_id, limit as i64],
                map_policy_change_event_row,
            )?
            .filter_map(|row| row.ok())
            .collect()
        } else {
            stmt.query_map(rusqlite::params![limit as i64], map_policy_change_event_row)?
                .filter_map(|row| row.ok())
                .collect()
        };
        Ok(rows)
    }

    pub fn promote_replay_failure_clusters_to_policy_candidates(
        &self,
        min_occurrences: usize,
        limit: usize,
    ) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }

        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, fingerprint, task_kind, tool, failure_class, failure_stage, latest_outcome,
                    issue_signature, exemplar_summary, exemplar_error, target, provider, model, route,
                    occurrence_count, first_seen_at, last_seen_at, sample_action_run_ids_json,
                    latest_action_run_id, latest_task_id, created_at, updated_at
             FROM replay_failure_clusters
             WHERE occurrence_count >= ?1
             ORDER BY occurrence_count DESC, last_seen_at DESC
             LIMIT ?2",
        )?;
        let clusters = stmt
            .query_map(
                rusqlite::params![min_occurrences.max(3) as i64, limit as i64],
                map_replay_failure_cluster_row,
            )?
            .filter_map(|row| row.ok())
            .collect::<Vec<_>>();

        let mut promoted = 0usize;
        for cluster in clusters {
            let Some(candidate) =
                crate::improvement::policy::candidate_from_failure_cluster(&cluster)
            else {
                continue;
            };
            let evaluation = crate::improvement::experiments::evaluate_replay_policy_candidate(
                &candidate, &cluster,
            );
            if upsert_replay_failure_policy_candidate_static(
                &conn,
                &cluster,
                &candidate,
                &evaluation,
            )? {
                promoted += 1;
            }
        }

        Ok(promoted)
    }

    pub fn record_policy_runtime_event_by_trigger(
        &self,
        trigger: &str,
        event_kind: &str,
        summary: &str,
        details: &serde_json::Value,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let candidate_id = conn
            .query_row(
                "SELECT id FROM policy_candidates WHERE trigger = ?1 LIMIT 1",
                rusqlite::params![trigger],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(candidate_id) = candidate_id else {
            return Ok(false);
        };

        insert_policy_change_event_static(&conn, &candidate_id, event_kind, summary, details)?;
        Ok(true)
    }
}

fn map_policy_candidate_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PolicyCandidateRecord> {
    Ok(PolicyCandidateRecord {
        id: row.get(0)?,
        source_kind: row.get(1)?,
        source_ref: row.get(2)?,
        kind: row.get(3)?,
        scope: row.get(4)?,
        title: row.get(5)?,
        description: row.get(6)?,
        rationale: row.get(7)?,
        trigger: row.get(8)?,
        proposed_change: parse_json_value(row.get(9)?),
        evidence: parse_json_value(row.get(10)?),
        confidence: row.get(11)?,
        importance: row.get(12)?,
        status: row.get(13)?,
        last_score: row.get(14)?,
        last_verdict: row.get(15)?,
        approved_at: row.get(16)?,
        rolled_back_at: row.get(17)?,
        created_at: row.get(18)?,
        updated_at: row.get(19)?,
    })
}

fn map_policy_evaluation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<PolicyEvaluationRecord> {
    Ok(PolicyEvaluationRecord {
        id: row.get(0)?,
        candidate_id: row.get(1)?,
        evaluation_kind: row.get(2)?,
        summary: row.get(3)?,
        score: row.get(4)?,
        verdict: row.get(5)?,
        metrics: parse_json_value(row.get(6)?),
        created_at: row.get(7)?,
    })
}

fn map_policy_change_event_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<PolicyChangeEventRecord> {
    Ok(PolicyChangeEventRecord {
        id: row.get(0)?,
        candidate_id: row.get(1)?,
        event_kind: row.get(2)?,
        summary: row.get(3)?,
        details: parse_json_value(row.get(4)?),
        created_at: row.get(5)?,
    })
}

fn upsert_replay_failure_policy_candidate_static(
    conn: &Connection,
    cluster: &ReplayFailureClusterRecord,
    candidate: &crate::improvement::policy::DistilledPolicyCandidate,
    evaluation: &crate::improvement::experiments::OfflinePolicyEvaluation,
) -> Result<bool> {
    let proposed_change_json = serde_json::to_string(&candidate.proposed_change)?;
    let evidence_json = serde_json::to_string(&candidate.evidence)?;
    let existing = conn
        .query_row(
            "SELECT id, source_kind, source_ref, kind, scope, title, description, rationale,
                    proposed_change_json, evidence_json, confidence, importance, status,
                    last_score, last_verdict, approved_at, rolled_back_at
             FROM policy_candidates
             WHERE trigger = ?1
             LIMIT 1",
            rusqlite::params![candidate.trigger.as_str()],
            |row| {
                Ok(ExistingPolicyCandidate {
                    id: row.get(0)?,
                    source_kind: row.get(1)?,
                    source_ref: row.get(2)?,
                    kind: row.get(3)?,
                    scope: row.get(4)?,
                    title: row.get(5)?,
                    description: row.get(6)?,
                    rationale: row.get(7)?,
                    proposed_change_json: row.get(8)?,
                    evidence_json: row.get(9)?,
                    confidence: row.get(10)?,
                    importance: row.get(11)?,
                    status: row.get(12)?,
                    last_score: row.get(13)?,
                    last_verdict: row.get(14)?,
                    approved_at: row.get(15)?,
                    rolled_back_at: row.get(16)?,
                })
            },
        )
        .optional()?;

    let desired_status = desired_policy_status(
        existing.as_ref().map(|value| value.status.as_str()),
        &evaluation.verdict,
    );

    if let Some(existing) = existing {
        let candidate_changed = policy_candidate_needs_update(
            &existing,
            cluster,
            candidate,
            evaluation,
            desired_status,
            &proposed_change_json,
            &evidence_json,
        );
        let evaluation_changed = policy_evaluation_needs_insert(conn, &existing.id, evaluation)?;
        if !candidate_changed && !evaluation_changed {
            return Ok(false);
        }

        let approved_at = if desired_status == "approved" {
            existing
                .approved_at
                .clone()
                .or_else(|| Some(current_db_time(conn)))
        } else {
            existing.approved_at.clone()
        };
        let rolled_back_at = if desired_status == "rolled_back" {
            existing
                .rolled_back_at
                .clone()
                .or_else(|| Some(current_db_time(conn)))
        } else {
            existing.rolled_back_at.clone()
        };

        if candidate_changed {
            conn.execute(
                "UPDATE policy_candidates
                 SET source_kind = ?2,
                     source_ref = ?3,
                     kind = ?4,
                     scope = ?5,
                     title = ?6,
                     description = ?7,
                     rationale = ?8,
                     proposed_change_json = ?9,
                     evidence_json = ?10,
                     confidence = ?11,
                     importance = ?12,
                     status = ?13,
                     last_score = ?14,
                     last_verdict = ?15,
                     approved_at = ?16,
                     rolled_back_at = ?17,
                     updated_at = datetime('now')
                 WHERE id = ?1",
                rusqlite::params![
                    existing.id.as_str(),
                    "replay_failure_cluster",
                    cluster.fingerprint.as_str(),
                    candidate.kind.as_str(),
                    candidate.scope.as_str(),
                    candidate.title.as_str(),
                    candidate.description.as_str(),
                    candidate.rationale.as_str(),
                    proposed_change_json.as_str(),
                    evidence_json.as_str(),
                    candidate.confidence,
                    candidate.importance,
                    desired_status,
                    evaluation.score,
                    evaluation.verdict.as_str(),
                    approved_at.as_deref(),
                    rolled_back_at.as_deref(),
                ],
            )?;
        }
        if evaluation_changed {
            insert_policy_evaluation_static(conn, &existing.id, evaluation)?;
        }
        insert_policy_status_event_if_needed(
            conn,
            &existing.id,
            Some(existing.status.as_str()),
            desired_status,
            candidate,
            cluster,
            evaluation,
        )?;
        Ok(true)
    } else {
        let candidate_id = uuid::Uuid::new_v4().to_string();
        let approved_at = if desired_status == "approved" {
            Some(current_db_time(conn))
        } else {
            None
        };
        let rolled_back_at = if desired_status == "rolled_back" {
            Some(current_db_time(conn))
        } else {
            None
        };
        conn.execute(
            "INSERT INTO policy_candidates (
                id, source_kind, source_ref, kind, scope, title, description, rationale,
                trigger, proposed_change_json, evidence_json, confidence, importance, status,
                last_score, last_verdict, approved_at, rolled_back_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            rusqlite::params![
                candidate_id.as_str(),
                "replay_failure_cluster",
                cluster.fingerprint.as_str(),
                candidate.kind.as_str(),
                candidate.scope.as_str(),
                candidate.title.as_str(),
                candidate.description.as_str(),
                candidate.rationale.as_str(),
                candidate.trigger.as_str(),
                proposed_change_json.as_str(),
                evidence_json.as_str(),
                candidate.confidence,
                candidate.importance,
                desired_status,
                evaluation.score,
                evaluation.verdict.as_str(),
                approved_at.as_deref(),
                rolled_back_at.as_deref(),
            ],
        )?;
        insert_policy_change_event_static(
            conn,
            &candidate_id,
            "candidate_created",
            &format!(
                "Created replay-derived policy candidate '{}'.",
                candidate.title
            ),
            &event_details(candidate, cluster, evaluation),
        )?;
        insert_policy_evaluation_static(conn, &candidate_id, evaluation)?;
        insert_policy_status_event_if_needed(
            conn,
            &candidate_id,
            None,
            desired_status,
            candidate,
            cluster,
            evaluation,
        )?;
        Ok(true)
    }
}

#[derive(Debug, Clone)]
struct ExistingPolicyCandidate {
    id: String,
    source_kind: String,
    source_ref: String,
    kind: String,
    scope: String,
    title: String,
    description: String,
    rationale: String,
    proposed_change_json: String,
    evidence_json: String,
    confidence: f64,
    importance: f64,
    status: String,
    last_score: Option<f64>,
    last_verdict: Option<String>,
    approved_at: Option<String>,
    rolled_back_at: Option<String>,
}

fn desired_policy_status(existing_status: Option<&str>, verdict: &str) -> &'static str {
    match verdict {
        "approve" => "approved",
        "reject" if existing_status == Some("approved") => "rolled_back",
        "reject" => "rejected",
        _ => match existing_status {
            Some("approved") => "approved",
            Some("rolled_back") => "rolled_back",
            Some("rejected") => "rejected",
            _ => "candidate",
        },
    }
}

fn policy_candidate_needs_update(
    existing: &ExistingPolicyCandidate,
    cluster: &ReplayFailureClusterRecord,
    candidate: &crate::improvement::policy::DistilledPolicyCandidate,
    evaluation: &crate::improvement::experiments::OfflinePolicyEvaluation,
    desired_status: &str,
    proposed_change_json: &str,
    evidence_json: &str,
) -> bool {
    existing.source_kind != "replay_failure_cluster"
        || existing.source_ref != cluster.fingerprint
        || existing.kind != candidate.kind
        || existing.scope != candidate.scope
        || existing.title != candidate.title
        || existing.description != candidate.description
        || existing.rationale != candidate.rationale
        || existing.proposed_change_json != proposed_change_json
        || existing.evidence_json != evidence_json
        || !same_float(existing.confidence, candidate.confidence)
        || !same_float(existing.importance, candidate.importance)
        || existing.status != desired_status
        || existing
            .last_score
            .map(|value| !same_float(value, evaluation.score))
            .unwrap_or(true)
        || existing.last_verdict.as_deref() != Some(evaluation.verdict.as_str())
        || (desired_status == "approved" && existing.approved_at.is_none())
        || (desired_status == "rolled_back" && existing.rolled_back_at.is_none())
}

fn policy_evaluation_needs_insert(
    conn: &Connection,
    candidate_id: &str,
    evaluation: &crate::improvement::experiments::OfflinePolicyEvaluation,
) -> Result<bool> {
    let existing = conn
        .query_row(
            "SELECT score, verdict, summary, metrics_json
             FROM policy_evaluations
             WHERE candidate_id = ?1 AND evaluation_kind = ?2
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
            rusqlite::params![candidate_id, evaluation.evaluation_kind.as_str()],
            |row| {
                Ok((
                    row.get::<_, f64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    let metrics_json = serde_json::to_string(&evaluation.metrics)?;
    Ok(match existing {
        Some((score, verdict, summary, existing_metrics_json)) => {
            !same_float(score, evaluation.score)
                || verdict != evaluation.verdict
                || summary != evaluation.summary
                || existing_metrics_json != metrics_json
        }
        None => true,
    })
}

fn insert_policy_evaluation_static(
    conn: &Connection,
    candidate_id: &str,
    evaluation: &crate::improvement::experiments::OfflinePolicyEvaluation,
) -> Result<()> {
    conn.execute(
        "INSERT INTO policy_evaluations (
            id, candidate_id, evaluation_kind, summary, score, verdict, metrics_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            uuid::Uuid::new_v4().to_string(),
            candidate_id,
            evaluation.evaluation_kind.as_str(),
            evaluation.summary.as_str(),
            evaluation.score,
            evaluation.verdict.as_str(),
            serde_json::to_string(&evaluation.metrics)?,
        ],
    )?;
    Ok(())
}

fn insert_policy_status_event_if_needed(
    conn: &Connection,
    candidate_id: &str,
    previous_status: Option<&str>,
    next_status: &str,
    candidate: &crate::improvement::policy::DistilledPolicyCandidate,
    cluster: &ReplayFailureClusterRecord,
    evaluation: &crate::improvement::experiments::OfflinePolicyEvaluation,
) -> Result<()> {
    if previous_status == Some(next_status) || next_status == "candidate" {
        return Ok(());
    }

    let (event_kind, summary) = match next_status {
        "approved" => (
            "approved_for_guarded_rollout",
            format!(
                "Approved '{}' for guarded rollout after replay evaluation scored {:.2}.",
                candidate.title, evaluation.score
            ),
        ),
        "rolled_back" => (
            "rolled_back_after_replay_eval",
            format!(
                "Rolled back '{}' after replay evaluation dropped to a '{}' verdict.",
                candidate.title, evaluation.verdict
            ),
        ),
        "rejected" => (
            "rejected_after_replay_eval",
            format!(
                "Rejected '{}' after replay evaluation found insufficient evidence.",
                candidate.title
            ),
        ),
        _ => return Ok(()),
    };

    insert_policy_change_event_static(
        conn,
        candidate_id,
        event_kind,
        &summary,
        &event_details(candidate, cluster, evaluation),
    )
}

fn insert_policy_change_event_static(
    conn: &Connection,
    candidate_id: &str,
    event_kind: &str,
    summary: &str,
    details: &serde_json::Value,
) -> Result<()> {
    conn.execute(
        "INSERT INTO policy_change_events (
            id, candidate_id, event_kind, summary, details_json
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            uuid::Uuid::new_v4().to_string(),
            candidate_id,
            event_kind,
            summary,
            serde_json::to_string(details)?,
        ],
    )?;
    Ok(())
}

fn event_details(
    candidate: &crate::improvement::policy::DistilledPolicyCandidate,
    cluster: &ReplayFailureClusterRecord,
    evaluation: &crate::improvement::experiments::OfflinePolicyEvaluation,
) -> serde_json::Value {
    serde_json::json!({
        "trigger": candidate.trigger,
        "policy_kind": candidate.kind,
        "scope": candidate.scope,
        "cluster_fingerprint": cluster.fingerprint,
        "issue_signature": cluster.issue_signature,
        "failure_class": cluster.failure_class,
        "occurrence_count": cluster.occurrence_count,
        "score": evaluation.score,
        "verdict": evaluation.verdict,
    })
}

fn current_db_time(conn: &Connection) -> String {
    conn.query_row("SELECT datetime('now')", [], |row| row.get(0))
        .unwrap_or_else(|_| chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string())
}

fn same_float(left: f64, right: f64) -> bool {
    (left - right).abs() < 0.000_001
}
