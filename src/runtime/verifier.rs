use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExpectedEffectContract {
    pub kind: String,
    pub target: Option<String>,
    pub detail: String,
    pub verification_method: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerifierVerdict {
    pub status: String,
    pub summary: String,
    pub checked_at: String,
    pub checks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RollbackReason {
    pub kind: String,
    pub summary: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ActionVerification {
    pub executed: bool,
    pub verified: Option<bool>,
    pub expected_effect: Option<ExpectedEffectContract>,
    pub verifier_verdict: Option<VerifierVerdict>,
    pub rollback_reason: Option<RollbackReason>,
}

impl ActionVerification {
    pub fn verified(
        expected_effect: ExpectedEffectContract,
        summary: impl Into<String>,
        checks: Vec<String>,
    ) -> Self {
        Self {
            executed: true,
            verified: Some(true),
            expected_effect: Some(expected_effect),
            verifier_verdict: Some(VerifierVerdict {
                status: "verified".to_string(),
                summary: summary.into(),
                checked_at: verifier_timestamp(),
                checks,
            }),
            rollback_reason: None,
        }
    }

    pub fn failed(
        executed: bool,
        expected_effect: Option<ExpectedEffectContract>,
        summary: impl Into<String>,
        checks: Vec<String>,
        rollback_reason: Option<RollbackReason>,
    ) -> Self {
        let status = if executed { "failed" } else { "blocked" };
        Self {
            executed,
            verified: if executed { Some(false) } else { None },
            expected_effect,
            verifier_verdict: Some(VerifierVerdict {
                status: status.to_string(),
                summary: summary.into(),
                checked_at: verifier_timestamp(),
                checks,
            }),
            rollback_reason,
        }
    }

    pub fn deferred(
        expected_effect: Option<ExpectedEffectContract>,
        summary: impl Into<String>,
        checks: Vec<String>,
        rollback_reason: Option<RollbackReason>,
    ) -> Self {
        Self {
            executed: false,
            verified: None,
            expected_effect,
            verifier_verdict: Some(VerifierVerdict {
                status: "deferred".to_string(),
                summary: summary.into(),
                checked_at: verifier_timestamp(),
                checks,
            }),
            rollback_reason,
        }
    }
}

fn verifier_timestamp() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}
