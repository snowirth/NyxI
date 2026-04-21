use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use anyhow::{Context, Result};

use crate::{AppState, db, forge, runtime::ChatFinalizeResult};

mod explain;
mod ingest;
mod maintenance;
mod planner;
mod provenance;
mod retrieval;
mod working_set;

pub(crate) use planner::{context_candidate_limit_for_plan, plan_memory_request};
pub(crate) use provenance::is_memory_provenance_request;
pub(crate) use retrieval::refine_context_for_plan;

#[cfg(test)]
use self::ingest::{parse_web_search_results, web_result_statement};
#[cfg(test)]
use self::working_set::render_memory_surface;

pub(crate) const MEMORY_WORKING_SET_SCHEMA_VERSION: &str = "nyx_memory_working_set.v6";
pub(crate) const MEMORY_PROVENANCE_BRIEF_SCHEMA_VERSION: &str = "nyx_memory_provenance_brief.v1";

const MEMORY_STOP_WORDS: &[&str] = &[
    "what",
    "when",
    "where",
    "which",
    "who",
    "whom",
    "whose",
    "with",
    "from",
    "that",
    "this",
    "then",
    "than",
    "have",
    "has",
    "had",
    "were",
    "was",
    "into",
    "about",
    "after",
    "before",
    "your",
    "their",
    "there",
    "would",
    "could",
    "should",
    "just",
    "really",
    "been",
    "being",
    "them",
    "they",
    "you",
    "our",
    "out",
    "for",
    "and",
    "the",
    "are",
    "did",
    "does",
    "how",
    "why",
    "his",
    "her",
    "she",
    "him",
    "its",
    "too",
    "can",
    "get",
    "got",
    "use",
    "using",
    "used",
    "want",
    "like",
    "need",
    "help",
    "some",
    "more",
    "than",
    "also",
    "over",
    "under",
    "month",
    "week",
    "year",
    "years",
    "days",
    "last",
    "next",
    "first",
    "second",
    "user",
    "assistant",
    "nyx",
];

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryRequestMode {
    General,
    Identity,
    Policy,
    Task,
    Fact,
    Temporal,
    Evidence,
    Repair,
    FreshLookup,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryRetrievalLane {
    HotState,
    Procedural,
    TaskState,
    MixedContext,
    CapsuleRecall,
    TemporalRecall,
    RepairHistory,
    FreshLookup,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct MemoryRetrievalPlan {
    pub mode: MemoryRequestMode,
    pub lane: MemoryRetrievalLane,
    pub expanded_query: String,
    pub focus_terms: Vec<String>,
    pub temporal: bool,
    pub evidence_required: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct MemoryExplanationPacket {
    pub lane_summary: String,
    pub surfaced_items: Vec<MemoryExplanationItem>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct MemoryExplanationItem {
    pub kind: String,
    pub label: String,
    pub reason: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct MemoryEvidenceAggregate {
    pub topic: String,
    pub summary: String,
    pub support_count: usize,
    pub session_count: usize,
    pub evidence_kinds: Vec<String>,
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct MemoryNegativeEvidence {
    pub missing_terms: Vec<String>,
    pub summary: String,
    pub related_topics: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct MemoryResourceCard {
    pub kind: String,
    pub name: String,
    pub source: String,
    pub handle: Option<String>,
    pub ready: bool,
    pub status: String,
    pub summary: String,
    pub reason: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub(crate) struct MemoryWorkingSet {
    pub schema_version: String,
    pub query: String,
    pub assembled_at: String,
    pub intent: String,
    pub plan: MemoryRetrievalPlan,
    pub entity_focus: Vec<String>,
    pub action_notes: Vec<String>,
    pub explanation: MemoryExplanationPacket,
    pub aggregated_evidence: Vec<MemoryEvidenceAggregate>,
    pub negative_evidence: Vec<MemoryNegativeEvidence>,
    pub prompt_context: String,
    pub capsules: Vec<db::MemorySessionCapsuleRecord>,
    pub resource_cards: Vec<MemoryResourceCard>,
    pub profile: Vec<String>,
    pub active_claims: Vec<db::MemoryClaimRecord>,
    pub procedures: Vec<db::MemoryProcedureRecord>,
    pub recent_episodes: Vec<db::MemoryEpisodeRecord>,
    pub supporting_sources: Vec<db::MemorySourceRecord>,
    pub uncertainties: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct MemoryProvenanceBrief {
    pub schema_version: String,
    pub generated_at: String,
    pub source: String,
    pub requested_query: String,
    pub resolved_query: String,
    pub inferred_from_history: bool,
    pub inferred_from_surface: Option<String>,
    pub basis_summary: String,
    pub evidence_items: Vec<MemoryExplanationItem>,
    pub supporting_source_count: usize,
    pub evidence_cluster_count: usize,
    pub coverage_gaps: Vec<String>,
    pub reply: String,
}

impl AppState {
    pub(crate) fn get_profile(&self, sender: &str) -> db::UserProfile {
        let key = if sender.is_empty() || sender == "web" || sender == "mcp" {
            "owner"
        } else {
            sender
        };
        if let Some(profile) = self.profiles.get(key) {
            return profile.clone();
        }
        let profile = db::UserProfile::load_for(&self.db, key);
        self.profiles.insert(key.to_string(), profile.clone());
        profile
    }

    pub(crate) fn save_profile(&self, sender: &str, profile: &db::UserProfile) {
        let key = if sender.is_empty() || sender == "web" || sender == "mcp" {
            "owner"
        } else {
            sender
        };
        profile.save_for(&self.db, key);
        self.profiles.insert(key.to_string(), profile.clone());
    }

    #[allow(dead_code)]
    pub(crate) async fn surface_memories(&self, text: &str) -> String {
        self.memory_working_set_for_query(text, 5)
            .await
            .prompt_context
    }

    pub(crate) fn embed_memory_background(&self, memory_id: String, content: String) {
        let embedder = self.embedder.clone();
        let db = self.db.clone();
        tokio::spawn(async move {
            if let Some(embedding) = embedder.embed(&content).await {
                db.store_embedding(&memory_id, &embedding);
                tracing::debug!("embedded memory: {}", &memory_id[..8]);
            }
        });
    }

    pub(crate) async fn finalize_response(
        &self,
        channel: &str,
        sender: &str,
        text: &str,
        lower: &str,
        response: &str,
        cache_hash: u64,
        depth: u8,
    ) -> ChatFinalizeResult {
        let is_substantial = text.len() > 30
            && response.len() > 30
            && !channel.starts_with("internal")
            && !lower.starts_with("hi")
            && !lower.starts_with("hey")
            && !lower.starts_with("hello")
            && !lower.starts_with("thanks");

        if is_substantial {
            if let Ok(extraction) = self
                .llm
                .chat_auto(
                    &format!(
                        "Should I remember anything from this exchange? If yes, reply as:\n\
                         REMEMBER [personal|task|tool]: [fact]\nREASON: [why this matters]\n\
                         If nothing worth remembering, reply: NONE\n\n\
                         User: {}\nAssistant: {}",
                        crate::trunc(text, 150),
                        crate::trunc(response, 150)
                    ),
                    60,
                )
                .await
            {
                let ext = extraction.trim();
                if ext.starts_with("REMEMBER") && ext.contains(':') {
                    let parts: Vec<&str> = ext.splitn(2, ':').collect();
                    if parts.len() == 2 {
                        let tier = if ext.contains("[personal]") {
                            "experience"
                        } else if ext.contains("[task]") {
                            "knowledge"
                        } else if ext.contains("[tool]") {
                            "lesson"
                        } else {
                            "experience"
                        };
                        let fact = parts[1].split("REASON:").next().unwrap_or("").trim();
                        if fact.len() > 5 {
                            if let Ok(Some(id)) = self.db.remember(fact, tier, 0.7) {
                                self.embed_memory_background(id, fact.to_string());
                            }
                            if tier == "experience" {
                                let mut profile = self.get_profile(sender);
                                profile.add_fact(fact);
                                self.save_profile(sender, &profile);
                            }
                        }
                    }
                }
            }
        }

        let strong_corrections = [
            "that's not right",
            "thats wrong",
            "that's wrong",
            "you got it wrong",
            "you misunderstood",
            "don't do that",
        ];
        let weak_corrections = ["no,", "nope,", "wrong", "actually,"];
        let has_strong = strong_corrections
            .iter()
            .any(|signal| lower.contains(signal));
        let weak_count = weak_corrections
            .iter()
            .filter(|signal| lower.contains(**signal))
            .count();
        let is_correction = (has_strong || weak_count >= 2) && text.len() > 15;

        if is_correction {
            if let Ok(lesson) = self
                .llm
                .chat_auto(
                    &format!(
                        "Extract ONE lesson from this correction in one sentence.\nMy response: \"{}\"\nCorrection: \"{}\"\nLesson:",
                        crate::trunc(response, 150),
                        text
                    ),
                    40,
                )
                .await
            {
                let lesson = lesson.trim();
                if lesson.len() > 10 {
                    let lesson_text = format!("lesson: {}", lesson);
                    if let Ok(Some(id)) = self.db.remember(&lesson_text, "lesson", 0.9) {
                        self.embed_memory_background(id, lesson_text.clone());
                    }
                    self.record_user_adaptation_growth(
                        "runtime",
                        "learned from user correction",
                        serde_json::json!({
                            "lesson": lesson,
                            "channel": channel,
                            "trigger": "correction",
                        }),
                    );
                    tracing::info!("learned: {}", crate::trunc(lesson, 60));
                }
            }
        }

        let registered_tool_mention =
            forge::inspect_requested_registered_tool(self.db.as_ref(), text).await;
        let mentions_healthy_registered_tool = registered_tool_mention
            .as_ref()
            .map(|inspection| inspection.healthy)
            .unwrap_or(false);
        let mentions_broken_registered_tool = registered_tool_mention
            .as_ref()
            .map(|inspection| !inspection.healthy)
            .unwrap_or(false);

        if depth == 0
            && !channel.starts_with("internal")
            && response.len() > 20
            && !mentions_healthy_registered_tool
        {
            if mentions_broken_registered_tool {
                if let Some(inspection) = &registered_tool_mention {
                    tracing::warn!(
                        "forge: requested registered tool {} is unhealthy: {}",
                        inspection.name,
                        inspection.issue.as_deref().unwrap_or("unknown issue")
                    );
                }
            }
            let should_build = if mentions_broken_registered_tool {
                "YES".to_string()
            } else {
                self.llm
                    .chat_auto(
                        &format!(
                            "The user asked: \"{}\"\nThe assistant replied: \"{}\"\n\n\
                             Could a new Python tool fix this? Reply YES only if the assistant \
                             failed to do something that a script could handle (API call, file operation, \
                             data processing). Reply NO if it's just a knowledge/conversation question.\n\
                             Reply with ONLY: YES or NO",
                            crate::trunc(text, 100),
                            crate::trunc(response, 100)
                        ),
                        5,
                    )
                    .await
                    .unwrap_or_default()
            };
            if should_build.trim().to_uppercase().starts_with("YES") {
                match forge::build_tool(&self.llm, text, response).await {
                    forge::ForgeResult::Success {
                        filename,
                        description,
                        auto_run,
                        telemetry,
                    } => {
                        tracing::info!("forge: built {} — {}", filename, description);
                        clear_built_tool_health_for_filename(self.db.as_ref(), &filename);
                        let growth_details = tool_growth_context_details(
                            registered_tool_mention.as_ref(),
                            auto_run.as_ref(),
                        );
                        self.record_tool_build_growth(
                            "runtime",
                            text,
                            &filename,
                            &description,
                            true,
                            &telemetry,
                            growth_details,
                        );
                        let built_response = tool_growth_success_response(
                            &filename,
                            &description,
                            auto_run.as_ref(),
                        );
                        self.store_and_cache(channel, text, &built_response, cache_hash)
                            .await;
                        return ChatFinalizeResult {
                            response: built_response,
                            route: "chat_tool_growth_success".to_string(),
                            outcome: "completed".to_string(),
                            details: serde_json::json!({
                                "tool_growth": {
                                    "attempted": true,
                                    "success": true,
                                    "filename": filename,
                                    "description": description,
                                    "auto_run": auto_run,
                                }
                            }),
                        };
                    }
                    forge::ForgeResult::Failed { reason, telemetry } => {
                        let growth_details =
                            tool_growth_context_details(registered_tool_mention.as_ref(), None);
                        self.record_tool_build_growth(
                            "runtime",
                            text,
                            "",
                            &reason,
                            false,
                            &telemetry,
                            growth_details,
                        );
                        tracing::warn!("forge: failed — {}", reason);
                        let revised_response = tool_growth_failure_response(&reason);
                        self.store_and_cache(channel, text, &revised_response, cache_hash)
                            .await;
                        return ChatFinalizeResult {
                            response: revised_response,
                            route: "chat_tool_growth_failed".to_string(),
                            outcome: "completed".to_string(),
                            details: serde_json::json!({
                                "tool_growth": {
                                    "attempted": true,
                                    "success": false,
                                    "reason": reason,
                                }
                            }),
                        };
                    }
                }
            }
        } else if mentions_healthy_registered_tool {
            tracing::info!("forge: skipped auto-build for request targeting existing tool");
        }

        self.store_and_cache(channel, text, response, cache_hash)
            .await;
        ChatFinalizeResult {
            response: response.to_string(),
            route: "chat_llm".to_string(),
            outcome: "completed".to_string(),
            details: serde_json::json!({
                "tool_growth": {
                    "attempted": false,
                },
                "registered_tool_mention": registered_tool_mention,
            }),
        }
    }

    pub(crate) async fn store_and_cache(
        &self,
        channel: &str,
        user_msg: &str,
        response: &str,
        cache_hash: u64,
    ) {
        self.db.store_message(channel, "user", user_msg);
        self.db.store_message(channel, "assistant", response);
        self.db.set_state(&format!("compressed:{}", channel), "");
        self.response_cache
            .insert(cache_hash, (response.to_string(), Instant::now()));
        self.response_cache
            .retain(|_, (_, created_at)| created_at.elapsed() < std::time::Duration::from_secs(60));

        let response_time = 0u64;
        let soul_state = self.soul.lock().await;
        let hour = chrono::Local::now()
            .format("%H")
            .to_string()
            .parse()
            .unwrap_or(0);
        let mut tracker = self.tracker.lock().await;
        tracker.on_response(
            channel,
            response.len(),
            response_time,
            soul_state.warmth,
            soul_state.verbosity,
            soul_state.assertiveness,
            hour,
        );
    }
}

fn tool_growth_success_response(
    filename: &str,
    description: &str,
    auto_run: Option<&forge::ToolRunPreview>,
) -> String {
    let mut response = format!("built {} — {}", filename, description);
    if let Some(preview) = auto_run {
        response.push_str(&format!(
            "\nran it with: {}\nresult: {}",
            crate::trunc(&preview.input.to_string(), 180),
            crate::trunc(&preview.output.to_string(), 180)
        ));
    }
    response
}

fn tool_growth_failure_response(reason: &str) -> String {
    if let Some((issue, expected, actual)) = structured_tool_growth_failure(reason) {
        return format!(
            "i can try building a tool for that, but this attempt didn't verify cleanly yet.\nissue: {}\nexpected: {}\nactual: {}",
            crate::trunc(issue.trim(), 140),
            crate::trunc(expected.trim(), 120),
            crate::trunc(actual.trim(), 120),
        );
    }

    let detail = crate::trunc(reason.trim(), 180);
    format!(
        "i can try building a tool for that, but this attempt didn't verify cleanly yet: {}",
        detail
    )
}

fn structured_tool_growth_failure(reason: &str) -> Option<(String, String, String)> {
    let expected = extract_reason_marker(reason, "expected")?;
    let actual = extract_reason_marker(reason, "actual")?;
    let issue = strip_reason_markers(reason)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    Some((issue, expected, actual))
}

fn extract_reason_marker(reason: &str, marker: &str) -> Option<String> {
    let start_marker = format!("[{}]", marker);
    let end_marker = format!("[/{}]", marker);
    let start = reason.find(&start_marker)? + start_marker.len();
    let end = reason[start..].find(&end_marker)? + start;
    Some(reason[start..end].trim().to_string())
}

fn strip_reason_markers(reason: &str) -> String {
    let mut cleaned = reason.to_string();
    for marker in ["expected", "actual"] {
        let start_marker = format!("[{}]", marker);
        let end_marker = format!("[/{}]", marker);
        while let Some(start) = cleaned.find(&start_marker) {
            let Some(end_rel) = cleaned[start..].find(&end_marker) else {
                break;
            };
            let end = start + end_rel + end_marker.len();
            cleaned.replace_range(start..end, "");
        }
    }
    cleaned
}

fn tool_growth_context_details(
    inspection: Option<&forge::RegisteredToolInspection>,
    auto_run: Option<&forge::ToolRunPreview>,
) -> Option<serde_json::Value> {
    let mut details = serde_json::Map::new();
    if let Some(inspection) = inspection {
        details.insert(
            "registered_tool_inspection".into(),
            serde_json::to_value(inspection).unwrap_or_else(|_| serde_json::json!({})),
        );
    }
    if let Some(preview) = auto_run {
        details.insert(
            "auto_run_preview".into(),
            serde_json::to_value(preview).unwrap_or_else(|_| serde_json::json!({})),
        );
    }

    if details.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(details))
    }
}

fn clear_built_tool_health_for_filename(db: &crate::db::Db, filename: &str) {
    let tool_name = std::path::Path::new(filename)
        .file_stem()
        .and_then(|value| value.to_str())
        .map(str::to_string);
    if let Some(tool_name) = tool_name {
        db.clear_built_tool_health(&tool_name).ok();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MemorySurfaceItem {
    section: &'static str,
    display: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedWebSearchResult {
    title: String,
    body: String,
    url: String,
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/memory.rs"]
mod tests;
