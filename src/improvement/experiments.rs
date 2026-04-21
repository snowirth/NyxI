use crate::db::ReplayFailureClusterRecord;

use super::policy::DistilledPolicyCandidate;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct OfflinePolicyEvaluation {
    pub evaluation_kind: String,
    pub score: f64,
    pub verdict: String,
    pub summary: String,
    pub metrics: serde_json::Value,
}

pub fn evaluate_replay_policy_candidate(
    candidate: &DistilledPolicyCandidate,
    cluster: &ReplayFailureClusterRecord,
) -> OfflinePolicyEvaluation {
    let recurrence_score =
        (0.55 + (cluster.occurrence_count.saturating_sub(3) as f64 * 0.1)).clamp(0.55, 1.0);
    let severity_score = match cluster.failure_class.as_str() {
        "unsafe" => 0.94,
        "permanent" => 0.88,
        "inconsistent_state" => 0.82,
        _ => 0.72,
    };
    let specificity_score = if cluster.issue_signature == "unknown failure" {
        0.45
    } else {
        0.92
    };
    let scope_score = match candidate.kind.as_str() {
        "tool_guard" => 0.9,
        "verification_rule" => 0.86,
        _ => {
            if cluster.provider.is_some() || cluster.route.is_some() {
                0.84
            } else {
                0.72
            }
        }
    };
    let score = (recurrence_score * 0.3
        + severity_score * 0.25
        + specificity_score * 0.2
        + scope_score * 0.15
        + candidate.confidence.clamp(0.0, 1.0) * 0.1)
        .clamp(0.0, 1.0);
    let verdict = if score >= 0.74 && cluster.occurrence_count >= 3 {
        "approve"
    } else if score >= 0.58 {
        "hold"
    } else {
        "reject"
    };
    let summary = match verdict {
        "approve" => format!(
            "Offline replay supports guarded rollout for '{}' because the failure '{}' repeated {} times.",
            candidate.title, cluster.issue_signature, cluster.occurrence_count
        ),
        "hold" => format!(
            "Offline replay keeps '{}' in candidate status until more evidence accumulates.",
            candidate.title
        ),
        _ => format!(
            "Offline replay rejects '{}' because the supporting pattern is still too weak or noisy.",
            candidate.title
        ),
    };

    OfflinePolicyEvaluation {
        evaluation_kind: "offline_replay".to_string(),
        score,
        verdict: verdict.to_string(),
        summary,
        metrics: serde_json::json!({
            "occurrence_count": cluster.occurrence_count,
            "failure_class": cluster.failure_class,
            "failure_stage": cluster.failure_stage,
            "issue_signature": cluster.issue_signature,
            "recurrence_score": recurrence_score,
            "severity_score": severity_score,
            "specificity_score": specificity_score,
            "scope_score": scope_score,
            "candidate_confidence": candidate.confidence,
            "provider": cluster.provider,
            "model": cluster.model,
            "route": cluster.route,
        }),
    }
}
