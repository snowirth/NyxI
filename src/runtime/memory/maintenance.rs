use super::ingest::{
    infer_source_trust_tier, normalize_memory_url, normalize_refresh_statement,
    parse_web_search_results, publisher_from_url, stable_checksum, web_claim_ttl_secs,
    web_result_statement,
};
use super::*;

impl AppState {
    pub fn reconcile_web_refresh_job_output(
        &self,
        job_id: &str,
        raw_output: &str,
    ) -> Result<String> {
        let job = self
            .db
            .get_memory_refresh_job(job_id)?
            .with_context(|| format!("missing refresh job {}", job_id))?;
        let claim = self
            .db
            .get_memory_claim(&job.claim_id)?
            .with_context(|| format!("missing claim {}", job.claim_id))?;
        let source_id = claim
            .source_id
            .clone()
            .or(job.source_id.clone())
            .context("refresh job missing source_id")?;
        let source = self
            .db
            .get_memory_source(&source_id)?
            .with_context(|| format!("missing source {}", source_id))?;

        let observed_at = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        self.db
            .mark_memory_source_checked(&source.id, Some(&observed_at))
            .ok();

        let results = parse_web_search_results(raw_output);
        if results.is_empty() {
            anyhow::bail!("refresh search returned no parseable results");
        }

        let source_url = normalize_memory_url(&source.url_or_ref);
        let Some(matching_result) = results
            .iter()
            .find(|result| normalize_memory_url(&result.url) == source_url)
        else {
            self.db
                .invalidate_memory_claim(&claim.id, Some(&observed_at))?;
            self.db.complete_memory_refresh_job(&job.id)?;
            return Ok("invalidated missing source".to_string());
        };

        let publisher = publisher_from_url(&matching_result.url);
        let trust_tier = infer_source_trust_tier(&matching_result.url);
        let checksum = stable_checksum(&format!(
            "{}\n{}\n{}",
            matching_result.title, matching_result.body, matching_result.url
        ));
        let refreshed_source_id = self.db.remember_source(
            "web",
            &matching_result.url,
            Some(matching_result.title.trim()),
            publisher.as_deref(),
            trust_tier,
            Some(&checksum),
            Some(&observed_at),
            Some(&job.refresh_query),
        )?;
        let refreshed_statement = web_result_statement(matching_result);
        let refreshed_ttl = web_claim_ttl_secs(&job.refresh_query, matching_result);
        if refreshed_statement.is_empty() {
            self.db.reactivate_memory_claim(
                &claim.id,
                Some(&refreshed_source_id),
                Some(&observed_at),
                Some(refreshed_ttl),
            )?;
            self.db.complete_memory_refresh_job(&job.id)?;
            return Ok("retained existing claim with refreshed source".to_string());
        }

        if normalize_refresh_statement(&refreshed_statement)
            == normalize_refresh_statement(&claim.statement)
        {
            self.db.reactivate_memory_claim(
                &claim.id,
                Some(&refreshed_source_id),
                Some(&observed_at),
                Some(refreshed_ttl),
            )?;
            self.db.complete_memory_refresh_job(&job.id)?;
            return Ok("reactivated same claim".to_string());
        }

        let new_claim_id = self.db.remember_sourced_claim(
            &refreshed_statement,
            &claim.kind,
            &claim.scope,
            &refreshed_source_id,
            trust_tier,
            claim.importance.max(0.65),
            Some(refreshed_ttl),
            Some(&observed_at),
        )?;
        if let Some(new_claim_id) = new_claim_id {
            self.db
                .supersede_memory_claim(&claim.id, &new_claim_id, Some(&observed_at))?;
        } else {
            self.db.reactivate_memory_claim(
                &claim.id,
                Some(&refreshed_source_id),
                Some(&observed_at),
                Some(refreshed_ttl),
            )?;
        }
        self.db.complete_memory_refresh_job(&job.id)?;
        Ok("superseded with refreshed claim".to_string())
    }

    pub async fn refresh_due_web_memory_claims(&self, limit: usize) -> usize {
        let jobs = match self.db.list_due_memory_refresh_jobs(limit) {
            Ok(jobs) => jobs,
            Err(error) => {
                tracing::warn!("memory: failed to list due refresh jobs: {}", error);
                return 0;
            }
        };

        let mut completed = 0usize;
        for job in jobs {
            let claimed = match self.db.claim_memory_refresh_job(&job.id) {
                Ok(claimed) => claimed,
                Err(error) => {
                    tracing::warn!("memory: failed to claim refresh job {}: {}", job.id, error);
                    continue;
                }
            };
            if !claimed {
                continue;
            }

            match crate::tools::run(
                "web_search",
                &serde_json::json!({ "query": job.refresh_query }),
            )
            .await
            {
                Ok(value) => {
                    if let Some(output) = value["output"].as_str() {
                        match self.reconcile_web_refresh_job_output(&job.id, output) {
                            Ok(outcome) => {
                                completed += 1;
                                tracing::info!(
                                    "memory: refresh job {} completed for claim {} ({})",
                                    &job.id[..8],
                                    &job.claim_id[..8],
                                    outcome
                                );
                            }
                            Err(error) => {
                                let retry_delay =
                                    memory_refresh_retry_delay_secs(job.attempt_count + 1);
                                self.db
                                    .reschedule_memory_refresh_job(
                                        &job.id,
                                        &error.to_string(),
                                        retry_delay,
                                    )
                                    .ok();
                                tracing::warn!(
                                    "memory: refresh reconciliation failed for job {}: {}",
                                    &job.id[..8],
                                    error
                                );
                            }
                        }
                    } else {
                        let retry_delay = memory_refresh_retry_delay_secs(job.attempt_count + 1);
                        self.db
                            .reschedule_memory_refresh_job(
                                &job.id,
                                "web_search returned no output",
                                retry_delay,
                            )
                            .ok();
                    }
                }
                Err(error) => {
                    let retry_delay = memory_refresh_retry_delay_secs(job.attempt_count + 1);
                    self.db
                        .reschedule_memory_refresh_job(&job.id, &error.to_string(), retry_delay)
                        .ok();
                    tracing::warn!(
                        "memory: refresh query failed for job {}: {}",
                        &job.id[..8],
                        error
                    );
                }
            }
        }

        completed
    }
}

pub(super) fn memory_refresh_retry_delay_secs(attempt: i64) -> i64 {
    let exponent = attempt.clamp(1, 6) as u32;
    (15 * 60 * (1_i64 << (exponent - 1))).min(6 * 60 * 60)
}
