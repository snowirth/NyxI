use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;

use crate::{
    autonomy, awareness, consciousness, db, embed, filewatcher, intent, interaction, llm, mcp,
    plugins, soul, twitter, world,
};

mod autonomy_hooks;
pub(crate) mod browser_session;
mod chat;
mod delegation;
mod fast_paths;
pub(crate) mod memory;
pub(crate) mod self_model;
pub(crate) mod verifier;

pub use browser_session::{BrowserSessionManager, SessionOpenParams, SessionStepParams};

pub(crate) use self_model::SelfModelSnapshot;

pub(crate) const CHAT_EXECUTION_TRACE_SCHEMA_VERSION: &str = "nyx_execution_trace.v1";

#[derive(Debug, Clone)]
pub(crate) struct ChatFinalizeResult {
    pub response: String,
    pub route: String,
    pub outcome: String,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone)]
pub(crate) struct ChatResponseDraft {
    pub response: String,
    pub llm_trace: Option<llm::LlmResponseTrace>,
    pub llm_latency_ms: Option<u64>,
    pub tool_loop: Option<serde_json::Value>,
}

/// Load `.env`: fills missing vars first; if `NYX_DOTENV_OVERRIDE=1` (from shell or from that first load),
/// reload so `.env` wins over already-exported environment variables.
pub fn load_dotenv() {
    dotenvy::dotenv().ok();
    let want_override = std::env::var("NYX_DOTENV_OVERRIDE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if want_override {
        dotenvy::dotenv_override().ok();
    }
}

#[derive(Clone)]
pub struct Config {
    pub anthropic_key: String,
    pub anthropic_model: String,
    pub nim_key: String,
    pub nim_model: String,
    pub nim_base_url: String,
    pub chat_primary: llm::ChatPrimary,
    pub ollama_host: String,
    pub ollama_model: String,
    pub telegram_token: String,
    pub telegram_owner_ids: Vec<String>,
    pub web_port: u16,
    pub api_token: String,
    pub user_name: String,
    pub user_location: String,
    pub default_city: String,
    pub github_repo: String,
}

impl Config {
    pub fn from_env() -> Self {
        use std::env::var;

        let owner_ids: Vec<String> = var("NYX_TELEGRAM_OWNER_IDS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let anthropic_key = var("NYX_ANTHROPIC_API_KEY").unwrap_or_default();
        let nim_key = var("NYX_NIM_API_KEY").unwrap_or_default();
        let chat_primary = llm::ChatPrimary::from_env(
            &var("NYX_CHAT_PRIMARY").unwrap_or_default(),
            !anthropic_key.is_empty(),
            !nim_key.is_empty(),
        );

        Self {
            anthropic_key,
            anthropic_model: var("NYX_ANTHROPIC_MODEL")
                .unwrap_or_else(|_| "claude-haiku-4-5-20251001".into()),
            nim_key,
            nim_model: var("NYX_NIM_MODEL")
                .unwrap_or_else(|_| "moonshotai/kimi-k2-instruct-0905".into()),
            nim_base_url: var("NYX_NIM_BASE_URL")
                .unwrap_or_else(|_| "https://integrate.api.nvidia.com".into())
                .trim_end_matches('/')
                .to_string(),
            chat_primary,
            ollama_host: var("NYX_OLLAMA_HOST").unwrap_or_else(|_| "http://127.0.0.1:11434".into()),
            ollama_model: var("NYX_OLLAMA_MODEL")
                .unwrap_or_else(|_| "deepseek-v3.1:671b-cloud".into()),
            telegram_token: var("NYX_TELEGRAM_TOKEN").unwrap_or_default(),
            telegram_owner_ids: owner_ids,
            web_port: var("NYX_WEB_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8099),
            api_token: var("NYX_API_TOKEN").unwrap_or_default(),
            user_name: var("NYX_USER_NAME").unwrap_or_else(|_| "User".into()),
            user_location: var("NYX_USER_LOCATION").unwrap_or_default(),
            default_city: var("NYX_DEFAULT_CITY").unwrap_or_default(),
            github_repo: var("NYX_GITHUB_REPO").unwrap_or_default(),
        }
    }
}

pub async fn autodetect_location(config: &mut Config) {
    if !config.user_location.is_empty() && !config.default_city.is_empty() {
        return;
    }

    if let Ok(resp) = reqwest::Client::new()
        .get("http://ip-api.com/json/")
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
    {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            let city = data["city"].as_str().unwrap_or("");
            let region = data["regionName"].as_str().unwrap_or("");
            if !city.is_empty() {
                if config.default_city.is_empty() {
                    config.default_city = city.to_string();
                }
                if config.user_location.is_empty() {
                    config.user_location = if region.is_empty() {
                        city.to_string()
                    } else {
                        format!("{}, {}", city, region)
                    };
                }
                tracing::info!("location: {} (auto-detected)", config.user_location);
            }
        }
    }
}

pub type ProactiveQueue = Arc<tokio::sync::Mutex<Vec<String>>>;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<db::Db>,
    pub llm: Arc<llm::LlmGate>,
    pub profiles: Arc<dashmap::DashMap<String, db::UserProfile>>,
    pub config: Config,
    pub start_time: Instant,
    pub channel_locks: Arc<dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    pub response_cache: Arc<dashmap::DashMap<u64, (String, Instant)>>,
    pub awareness: awareness::SharedAwareness,
    pub proactive_queue: ProactiveQueue,
    pub soul: Arc<tokio::sync::Mutex<soul::Soul>>,
    pub consciousness: consciousness::SharedConsciousness,
    pub embedder: Arc<embed::Embedder>,
    pub tracker: Arc<tokio::sync::Mutex<interaction::InteractionTracker>>,
    pub plugins: Arc<plugins::PluginRegistry>,
    pub mcp_hub: Arc<mcp::McpHub>,
    pub browser_sessions: Arc<browser_session::BrowserSessionManager>,
}

/// Allowed imports for self-bootstrapped tools. Anything not on this list is rejected.
pub(crate) const BOOTSTRAP_ALLOWED_IMPORTS: &[&str] = &[
    "json",
    "sys",
    "os.path",
    "urllib.request",
    "urllib.parse",
    "datetime",
    "math",
    "re",
    "hashlib",
    "base64",
    "time",
    "pathlib",
    "collections",
    "itertools",
    "functools",
];

pub(crate) fn build_state_with_db_path(
    config: Config,
    start: Instant,
    db_path: &str,
) -> Result<AppState> {
    let db = Arc::new(db::Db::open(db_path)?);
    db.migrate()?;
    db.decay_memories();
    db.assign_missing_memory_entities(500).ok();
    db.merge_duplicate_memory_entities(200).ok();
    db.mark_due_memory_claims_stale(200).ok();
    db.merge_duplicate_memory_claims(200).ok();
    db.ingest_replay_failure_clusters(1000).ok();
    db.promote_replay_failure_clusters_to_procedures(2, 200)
        .ok();
    db.promote_replay_failure_clusters_to_policy_candidates(3, 200)
        .ok();

    let llm = Arc::new(llm::LlmGate::new(&config));

    let profiles: Arc<dashmap::DashMap<String, db::UserProfile>> =
        Arc::new(dashmap::DashMap::new());
    profiles.insert("owner".to_string(), db::UserProfile::load(&db));

    let awareness = awareness::new_shared();
    let proactive_queue: ProactiveQueue = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let mut soul_state = soul::Soul::default();
    {
        let interactions = db.get_interactions(200);
        if !interactions.is_empty() {
            let insights = interaction::InteractionInsights::from_interactions(&interactions);
            soul_state.learn_from(&insights);
        }
    }
    let soul = Arc::new(tokio::sync::Mutex::new(soul_state));
    let consciousness_state = consciousness::ConsciousnessState::new();
    let embedder = Arc::new(embed::Embedder::new(&config.ollama_host));
    let tracker = Arc::new(tokio::sync::Mutex::new(
        interaction::InteractionTracker::new(),
    ));
    let plugin_registry = Arc::new(plugins::PluginRegistry::load_from_dir(
        &std::env::current_dir().unwrap_or_default().join("plugins"),
    ));

    let state = AppState {
        db,
        llm,
        profiles,
        config,
        start_time: start,
        channel_locks: Arc::new(dashmap::DashMap::new()),
        response_cache: Arc::new(dashmap::DashMap::new()),
        awareness,
        proactive_queue,
        soul,
        consciousness: consciousness_state,
        embedder,
        tracker,
        plugins: plugin_registry,
        mcp_hub: mcp::McpHub::new(),
        browser_sessions: Arc::new(browser_session::BrowserSessionManager::new()),
    };

    if let Err(error) = state.db.register_runtime_start("runtime") {
        tracing::warn!("runtime: failed to register startup incident: {}", error);
    }
    if let Err(error) =
        world::state::compile_and_persist_project_graph(state.db.as_ref(), "runtime_startup")
    {
        tracing::warn!(
            "world: failed to compile project graph on startup: {}",
            error
        );
    } else if let Some(changes) = world::state::load_project_graph_changes(state.db.as_ref()) {
        if let Some(message) = changes.resume_brief() {
            if let Ok(mut queue) = state.proactive_queue.try_lock() {
                queue.push(message);
            }
        }
    }

    Ok(state)
}

pub fn build_state(config: Config, start: Instant) -> Result<AppState> {
    build_state_with_db_path(config, start, "workspace/nyx.db")
}

pub fn spawn_background_tasks(state: &AppState) {
    let awareness_state = state.clone();
    let awareness_ctx = state.awareness.clone();
    let awareness_pq = state.proactive_queue.clone();
    tokio::spawn(async move {
        awareness::run(awareness_state, awareness_ctx, awareness_pq).await;
    });

    let consciousness_st = state.clone();
    let consciousness_aw = state.awareness.clone();
    let consciousness_pq = state.proactive_queue.clone();
    let consciousness_cs = state.consciousness.clone();
    tokio::spawn(async move {
        consciousness::run(
            consciousness_st,
            consciousness_aw,
            consciousness_pq,
            consciousness_cs,
        )
        .await;
    });

    let overnight_st = state.clone();
    let overnight_aw = state.awareness.clone();
    let overnight_pq = state.proactive_queue.clone();
    tokio::spawn(async move {
        crate::overnight::run(overnight_st, overnight_aw, overnight_pq).await;
    });

    let fw_state = state.clone();
    let fw_pq = state.proactive_queue.clone();
    tokio::spawn(async move {
        filewatcher::run(fw_state, fw_pq).await;
    });

    let autonomy_state = state.clone();
    let autonomy_pq = state.proactive_queue.clone();
    tokio::spawn(async move {
        autonomy::run(autonomy_state, autonomy_pq).await;
    });

    let mcp_hub = state.mcp_hub.clone();
    tokio::spawn(async move {
        let config_path = std::env::current_dir()
            .unwrap_or_default()
            .join("mcp_servers.json");
        mcp_hub
            .connect_from_config(config_path.to_str().unwrap_or("mcp_servers.json"))
            .await;
    });

    let twitter_st = state.clone();
    let twitter_pq = state.proactive_queue.clone();
    tokio::spawn(async move {
        twitter::run(twitter_st, twitter_pq).await;
    });
}

impl AppState {
    pub async fn handle(&self, channel: &str, sender: &str, text: &str) -> String {
        self.handle_inner(channel, sender, text, 0).await
    }

    pub fn validate_bootstrap_imports(&self, content: &str) -> bool {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("import ") || trimmed.starts_with("from ") {
                let module = if trimmed.starts_with("from ") {
                    trimmed.split_whitespace().nth(1).unwrap_or("")
                } else {
                    trimmed
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("")
                        .split('.')
                        .next()
                        .unwrap_or("")
                };
                let base_module = module.split('.').next().unwrap_or("");
                if !BOOTSTRAP_ALLOWED_IMPORTS.iter().any(|allowed| {
                    let allowed_base = allowed.split('.').next().unwrap_or(allowed);
                    base_module == allowed_base
                }) {
                    return false;
                }
            }
        }
        true
    }

    pub fn extract_city(&self, text: &str) -> Option<String> {
        let lower = text.to_lowercase();
        for prep in [" in ", " at ", " for "] {
            if let Some(idx) = lower.find(prep) {
                let raw = &text[idx + prep.len()..];
                let city = raw
                    .trim()
                    .trim_end_matches(|c: char| "?.!".contains(c))
                    .replace("right now", "")
                    .replace("today", "")
                    .replace("tomorrow", "")
                    .trim()
                    .to_string();
                if !city.is_empty() {
                    return Some(city);
                }
            }
        }
        None
    }

    async fn handle_inner(&self, channel: &str, sender: &str, text: &str, depth: u8) -> String {
        let lock = self
            .channel_locks
            .entry(channel.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let Ok(_guard) =
            tokio::time::timeout(std::time::Duration::from_secs(120), lock.lock()).await
        else {
            return "busy with another message. try again.".into();
        };

        {
            let mut tracker = self.tracker.lock().await;
            if let Some(scored) = tracker.on_message(channel, text) {
                self.db.store_interaction(&scored);
                tracing::debug!(
                    "interaction: {:?} (warmth={:.2} verb={:.2})",
                    scored.outcome,
                    scored.warmth,
                    scored.verbosity
                );
            }
        }

        let proactive_prefix = self.drain_proactive_prefix().await;
        let lower = text.to_lowercase();
        let awareness_ctx = self.awareness.read().await.clone();
        let cache_hash = self
            .response_cache_key(channel, sender, text, &proactive_prefix, &awareness_ctx)
            .await;
        {
            let mut soul_state = self.soul.lock().await;
            soul_state.adapt(&awareness_ctx);
        }

        let intent = if let Some(i) = intent::classify_fast(text) {
            i
        } else {
            intent::classify_llm(text, &self.llm).await
        };
        let intent_label = intent_name(&intent).to_string();
        tracing::info!(
            "intent: {} for \"{}\"",
            intent_label,
            crate::trunc(text, 50)
        );

        if !is_time_sensitive_intent(&intent) {
            if let Some(entry) = self.response_cache.get(&cache_hash) {
                if entry.value().1.elapsed() < std::time::Duration::from_secs(30) {
                    let response = entry.value().0.clone();
                    let cache_age_ms = entry.value().1.elapsed().as_millis() as u64;
                    self.record_chat_execution_trace(
                        channel,
                        sender,
                        text,
                        &response,
                        &intent,
                        &intent_label,
                        "response_cache",
                        "cache_hit",
                        true,
                        cache_hash,
                        depth,
                        &proactive_prefix,
                        &awareness_ctx,
                        serde_json::json!({
                            "cache_age_ms": cache_age_ms,
                        }),
                    );
                    return response;
                }
            }
        }

        if let Some(response) = self
            .try_intent_fast_path(channel, sender, text, cache_hash, &intent)
            .await
        {
            self.record_chat_execution_trace(
                channel,
                sender,
                text,
                &response,
                &intent,
                &intent_label,
                "intent_fast_path",
                "completed",
                false,
                cache_hash,
                depth,
                &proactive_prefix,
                &awareness_ctx,
                serde_json::json!({}),
            );
            return response;
        }

        if let Some(response) = self
            .try_rule_fast_path(channel, sender, text, &lower, cache_hash, depth, &intent)
            .await
        {
            self.record_chat_execution_trace(
                channel,
                sender,
                text,
                &response,
                &intent,
                &intent_label,
                "rule_fast_path",
                "completed",
                false,
                cache_hash,
                depth,
                &proactive_prefix,
                &awareness_ctx,
                serde_json::json!({}),
            );
            return response;
        }

        let drafted = self
            .respond_with_chat(channel, sender, text, &proactive_prefix, &awareness_ctx)
            .await;
        let finalized = self
            .finalize_response(
                channel,
                sender,
                text,
                &lower,
                &drafted.response,
                cache_hash,
                depth,
            )
            .await;
        let mut execution_details = finalized.details.clone();
        if let Some(object) = execution_details.as_object_mut() {
            if let Some(llm_trace) = drafted.llm_trace.as_ref() {
                object.insert(
                    "llm".into(),
                    serde_json::json!({
                        "provider": llm_trace.provider,
                        "model": llm_trace.model,
                        "route": llm_trace.route,
                        "latency_ms": llm_trace.latency_ms.or(drafted.llm_latency_ms),
                    }),
                );
            }
            if let Some(tool_loop) = drafted.tool_loop.as_ref() {
                object.insert("tool_loop".into(), tool_loop.clone());
            }
        }
        self.record_chat_execution_trace(
            channel,
            sender,
            text,
            &finalized.response,
            &intent,
            &intent_label,
            &finalized.route,
            &finalized.outcome,
            false,
            cache_hash,
            depth,
            &proactive_prefix,
            &awareness_ctx,
            execution_details,
        );
        finalized.response
    }

    async fn drain_proactive_prefix(&self) -> String {
        let mut q = self.proactive_queue.lock().await;
        if q.is_empty() {
            String::new()
        } else {
            let messages = q.drain(..).collect::<Vec<_>>();
            let body = if messages.len() == 1 {
                messages[0].trim().to_string()
            } else {
                messages
                    .iter()
                    .map(|message| format_proactive_notice_item(message))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            format!("[nyx noticed]\n{}\n\n", body)
        }
    }
}

fn format_proactive_notice_item(message: &str) -> String {
    let mut lines = message.lines();
    let first = lines.next().unwrap_or("").trim();
    let mut item = format!("- {}", first);
    for line in lines {
        item.push('\n');
        if line.trim().is_empty() {
            item.push_str("  ");
        } else {
            item.push_str("  ");
            item.push_str(line.trim());
        }
    }
    item
}

impl AppState {
    async fn response_cache_key(
        &self,
        channel: &str,
        sender: &str,
        text: &str,
        proactive_prefix: &str,
        awareness_ctx: &awareness::AwarenessContext,
    ) -> u64 {
        let profile_prompt = self.get_profile(sender).to_prompt();
        let self_model_fingerprint = self
            .self_model_snapshot()
            .await
            .response_cache_fingerprint();
        let tone = awareness_ctx.tone_directive();

        let mut hasher = DefaultHasher::new();
        channel.hash(&mut hasher);
        sender.hash(&mut hasher);
        text.hash(&mut hasher);
        proactive_prefix.hash(&mut hasher);
        tone.hash(&mut hasher);
        profile_prompt.hash(&mut hasher);
        self_model_fingerprint.hash(&mut hasher);
        hasher.finish()
    }

    fn record_chat_execution_trace(
        &self,
        channel: &str,
        sender: &str,
        request_text: &str,
        response_text: &str,
        intent: &intent::Intent,
        intent_label: &str,
        route: &str,
        outcome: &str,
        cache_hit: bool,
        cache_hash: u64,
        depth: u8,
        proactive_prefix: &str,
        awareness_ctx: &awareness::AwarenessContext,
        details: serde_json::Value,
    ) {
        let trace = serde_json::json!({
            "schema_version": CHAT_EXECUTION_TRACE_SCHEMA_VERSION,
            "surface": "chat",
            "kind": "chat_turn",
            "summary": format!("{} handled via {}", intent_label, route),
            "outcome": outcome,
            "success": outcome != "failed",
            "trace": {
                "channel": channel,
                "sender": sender,
                "intent": intent_label,
                "route": route,
                "cache_hit": cache_hit,
                "cache_eligible": !is_time_sensitive_intent(intent),
                "cache_hash": cache_hash.to_string(),
                "depth": depth,
                "proactive_prefix_present": !proactive_prefix.is_empty(),
                "tone_applied": !awareness_ctx.tone_directive().is_empty(),
            },
            "request": {
                "text": request_text,
                "chars": request_text.chars().count(),
            },
            "response": {
                "text": response_text,
                "chars": response_text.chars().count(),
            },
            "details": details,
        });

        if let Err(error) = self.db.record_chat_trace(
            channel,
            sender,
            intent_label,
            route,
            outcome,
            cache_hit,
            depth,
            &trace,
        ) {
            tracing::warn!("chat trace: failed to persist trace: {}", error);
        }
    }
}

fn intent_name(intent: &intent::Intent) -> &'static str {
    match intent {
        intent::Intent::Time => "time",
        intent::Intent::Weather { .. } => "weather",
        intent::Intent::Remember { .. } => "remember",
        intent::Intent::Recall => "recall",
        intent::Intent::Search { .. } => "search",
        intent::Intent::Remind { .. } => "remind",
        intent::Intent::ListReminders => "list_reminders",
        intent::Intent::Schedule { .. } => "schedule",
        intent::Intent::ListSchedule => "list_schedule",
        intent::Intent::Git { .. } => "git",
        intent::Intent::GitHub => "github",
        intent::Intent::Tweet { .. } => "tweet",
        intent::Intent::Timeline => "timeline",
        intent::Intent::Mentions => "mentions",
        intent::Intent::Gif { .. } => "gif",
        intent::Intent::ImageGen { .. } => "image",
        intent::Intent::ReadFile { .. } => "readfile",
        intent::Intent::Evolve => "evolve",
        intent::Intent::Vision => "vision",
        intent::Intent::Sleep => "sleep",
        intent::Intent::Chat => "chat",
    }
}

fn is_time_sensitive_intent(intent: &intent::Intent) -> bool {
    matches!(
        intent,
        intent::Intent::Time
            | intent::Intent::Weather { .. }
            | intent::Intent::Remind { .. }
            | intent::Intent::Vision
    )
}

#[cfg(test)]
#[path = "../tests/unit/runtime.rs"]
mod tests;
