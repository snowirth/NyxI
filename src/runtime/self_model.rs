use std::collections::{BTreeSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{AppState, autonomy, forge, llm, tools, world};

mod diff;
mod gaps;
pub(crate) use diff::diff_snapshots;
use gaps::{detect_self_model_gaps, self_model_gap_signature, should_emit_self_model_gap};

const CORE_SELF_CAPABILITIES: &[(&str, &str)] = &[
    (
        "memory_recall",
        "Remember facts about the user and retrieve them later.",
    ),
    (
        "reminders_and_scheduling",
        "Set reminders and recurring scheduled tasks.",
    ),
    (
        "repo_and_file_inspection",
        "Inspect local repo state and read allowed files.",
    ),
    (
        "tool_growth",
        "Build new Python tools when a capability is missing.",
    ),
    (
        "protected_self_edit",
        "Self-edit protected core files only through the verified evolution path.",
    ),
    (
        "user_adaptation",
        "Adapt tone using awareness, interaction history, and the identity/soul engine.",
    ),
    (
        "background_reflection",
        "Run background reflection, memory consolidation, and growth review loops.",
    ),
];
const SELF_MODEL_GAP_COOLDOWN_SECS: i64 = 300;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CapabilitySource {
    Core,
    Builtin,
    SelfBuilt,
    Plugin,
    Mcp,
}

impl CapabilitySource {
    fn label(self) -> &'static str {
        match self {
            Self::Core => "Core abilities",
            Self::Builtin => "Built-in tools",
            Self::SelfBuilt => "Self-built tools",
            Self::Plugin => "Plugin tools",
            Self::Mcp => "Connected MCP tools",
        }
    }
}

impl Default for CapabilitySource {
    fn default() -> Self {
        Self::Core
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct SelfModelSnapshot {
    pub generated_at: String,
    pub identity: IdentityState,
    pub adaptation: AdaptationState,
    pub runtime: RuntimeState,
    pub capabilities: Vec<Capability>,
    pub constraints: Vec<Constraint>,
    pub growth: GrowthState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct IdentityState {
    pub primary_file: String,
    pub legacy_fallback_file: Option<String>,
    pub principle: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct AdaptationState {
    pub warmth: f32,
    pub verbosity: f32,
    pub assertiveness: f32,
    pub learned_from_history: bool,
    pub interaction_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct RuntimeState {
    pub user_chat_primary: String,
    pub hosted_tool_loop_ready: bool,
    pub autonomous_llm_ready: bool,
    pub providers: Vec<ProviderState>,
    pub memory_count: i64,
    pub message_count: i64,
    pub active_goal_count: usize,
    pub recent_growth_event_count: usize,
    pub built_tool_count: usize,
    pub plugin_count: usize,
    pub plugin_tool_count: usize,
    pub mcp_server_count: usize,
    pub mcp_tool_count: usize,
    pub default_city_configured: bool,
    pub github_repo_configured: bool,
    pub telegram_owner_count: usize,
    pub background_reflection_active: bool,
    pub background_thought_count: u64,
    pub world_focus: world::projects::WorldFocusSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct ProviderState {
    pub name: String,
    pub role: String,
    pub configured: bool,
    pub model: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct Capability {
    pub source: CapabilitySource,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct Constraint {
    pub kind: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct GrowthState {
    pub active_goals: Vec<GrowthGoal>,
    pub recent_events: Vec<GrowthEventSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct GrowthGoal {
    pub title: String,
    pub priority: f64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct GrowthEventSummary {
    pub kind: String,
    pub target: Option<String>,
    pub summary: String,
    pub success: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct SelfModelDiff {
    pub summary: Vec<String>,
    pub identity: Vec<SelfModelValueChange>,
    pub adaptation: Vec<SelfModelValueChange>,
    pub runtime: Vec<SelfModelValueChange>,
    pub providers: Vec<SelfModelProviderChange>,
    pub capabilities: SelfModelCollectionChange,
    pub constraints: SelfModelCollectionChange,
    pub growth_goals: SelfModelCollectionChange,
    pub recent_events: SelfModelCollectionChange,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct SelfModelValueChange {
    pub field: String,
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct SelfModelProviderChange {
    pub name: String,
    pub change: String,
    pub summary: String,
    pub details: Vec<SelfModelValueChange>,
    pub from: Option<ProviderState>,
    pub to: Option<ProviderState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub(crate) struct SelfModelCollectionChange {
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

impl SelfModelSnapshot {
    pub(crate) fn has_capability_named(&self, name: &str) -> bool {
        self.capabilities
            .iter()
            .any(|capability| capability.name.eq_ignore_ascii_case(name))
    }

    pub(crate) fn has_constraint(&self, kind: &str) -> bool {
        self.constraints
            .iter()
            .any(|constraint| constraint.kind == kind)
    }

    pub(crate) fn has_active_goal(&self, title: &str) -> bool {
        self.growth
            .active_goals
            .iter()
            .any(|goal| goal.title == title)
    }

    pub(crate) fn capability_count(&self) -> usize {
        self.capabilities.len()
    }

    pub(crate) fn response_cache_fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.identity.primary_file.hash(&mut hasher);
        self.identity.legacy_fallback_file.hash(&mut hasher);
        self.identity.principle.hash(&mut hasher);

        // Keep the cache keyed to stable response-shaping state, not hot counters
        // and growth telemetry that change every turn and would otherwise defeat
        // short-lived response reuse.
        self.runtime.user_chat_primary.hash(&mut hasher);
        self.runtime.hosted_tool_loop_ready.hash(&mut hasher);
        self.runtime.autonomous_llm_ready.hash(&mut hasher);
        self.runtime.default_city_configured.hash(&mut hasher);
        self.runtime.github_repo_configured.hash(&mut hasher);
        self.runtime.telegram_owner_count.hash(&mut hasher);
        self.runtime.world_focus.hash(&mut hasher);
        for provider in &self.runtime.providers {
            provider.name.hash(&mut hasher);
            provider.role.hash(&mut hasher);
            provider.configured.hash(&mut hasher);
            provider.model.hash(&mut hasher);
        }

        hasher.finish()
    }

    fn to_prompt(&self) -> String {
        let mut sections = Vec::new();

        sections.push(
            "Identity is stable and authored separately. Treat this self-model as the source of truth for current capabilities, readiness, and constraints."
                .to_string(),
        );
        sections.push(
            "Do not claim an action exists unless it appears here or in the active tool listings. If configuration, auth, network, or provider state may block something, say you need to check or try it."
                .to_string(),
        );
        if self.has_capability_named("tool_growth") {
            sections.push(
                "Core capability note: tool_growth is a real meta-capability. If a request is scriptable but no promoted tool exists yet, say you can try building a tool instead of saying you are categorically unable."
                    .to_string(),
            );
        }

        sections.push(format!(
            "Current runtime state:\n- user chat primary: {}\n- hosted tool loop ready: {}\n- autonomous LLM ready: {}\n- memories stored: {}\n- messages stored: {}\n- active autonomy goals: {}\n- recent growth events reviewed: {}\n- self-built tools: {}\n- plugins: {} ({} tools)\n- MCP connections: {} servers / {} tools\n- default city configured: {}\n- github repo configured: {}\n- telegram owners configured: {}\n- background reflection active: {}\n- background thought count: {}",
            self.runtime.user_chat_primary,
            yes_no(self.runtime.hosted_tool_loop_ready),
            yes_no(self.runtime.autonomous_llm_ready),
            self.runtime.memory_count,
            self.runtime.message_count,
            self.runtime.active_goal_count,
            self.runtime.recent_growth_event_count,
            self.runtime.built_tool_count,
            self.runtime.plugin_count,
            self.runtime.plugin_tool_count,
            self.runtime.mcp_server_count,
            self.runtime.mcp_tool_count,
            yes_no(self.runtime.default_city_configured),
            yes_no(self.runtime.github_repo_configured),
            self.runtime.telegram_owner_count,
            yes_no(self.runtime.background_reflection_active),
            self.runtime.background_thought_count,
        ));

        let world_focus_lines = world_focus_prompt_lines(&self.runtime.world_focus);
        if !world_focus_lines.is_empty() {
            sections.push(format!(
                "Current world focus:\n{}",
                world_focus_lines.join("\n")
            ));
        }

        let providers = self
            .runtime
            .providers
            .iter()
            .map(|provider| {
                let model = provider
                    .model
                    .as_deref()
                    .map(|value| format!(" ({})", value))
                    .unwrap_or_default();
                format!(
                    "- {}: configured={} role={}{}",
                    provider.name,
                    yes_no(provider.configured),
                    provider.role,
                    model
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        sections.push(format!("Provider readiness:\n{}", providers));

        sections.push(format!(
            "Current adaptation state:\n- warmth: {:.2}\n- verbosity: {:.2}\n- assertiveness: {:.2}\n- learned from interaction history: {}\n- interaction count this session: {}",
            self.adaptation.warmth,
            self.adaptation.verbosity,
            self.adaptation.assertiveness,
            yes_no(self.adaptation.learned_from_history),
            self.adaptation.interaction_count,
        ));

        for source in [
            CapabilitySource::Core,
            CapabilitySource::Builtin,
            CapabilitySource::SelfBuilt,
            CapabilitySource::Plugin,
            CapabilitySource::Mcp,
        ] {
            let items = self
                .capabilities
                .iter()
                .filter(|capability| capability.source == source)
                .map(|capability| format!("- {}: {}", capability.name, capability.description))
                .collect::<Vec<_>>();
            if !items.is_empty() {
                sections.push(format!("{}:\n{}", source.label(), items.join("\n")));
            }
        }

        if !self.constraints.is_empty() {
            let constraints = self
                .constraints
                .iter()
                .map(|constraint| format!("- {}: {}", constraint.kind, constraint.detail))
                .collect::<Vec<_>>()
                .join("\n");
            sections.push(format!("Operating boundaries:\n{}", constraints));
        }

        if !self.growth.active_goals.is_empty() {
            let goals = self
                .growth
                .active_goals
                .iter()
                .map(|goal| {
                    format!(
                        "- {} (priority {:.2}, source {})",
                        goal.title, goal.priority, goal.source
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            sections.push(format!("Growth goals currently active:\n{}", goals));
        }

        if !self.growth.recent_events.is_empty() {
            let events = self
                .growth
                .recent_events
                .iter()
                .map(|event| {
                    let target = event
                        .target
                        .as_deref()
                        .map(|value| format!(" target={}", value))
                        .unwrap_or_default();
                    format!(
                        "- [{}] {} success={}{}: {}",
                        event.created_at,
                        event.kind,
                        yes_no(event.success),
                        target,
                        event.summary
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            sections.push(format!("Recent growth history:\n{}", events));
        }

        format!("<self_model>\n{}\n</self_model>", sections.join("\n\n"))
    }
}

impl SelfModelDiff {
    pub(crate) fn has_changes(&self) -> bool {
        !self.identity.is_empty()
            || !self.adaptation.is_empty()
            || !self.runtime.is_empty()
            || !self.providers.is_empty()
            || !self.capabilities.added.is_empty()
            || !self.capabilities.removed.is_empty()
            || !self.constraints.added.is_empty()
            || !self.constraints.removed.is_empty()
            || !self.growth_goals.added.is_empty()
            || !self.growth_goals.removed.is_empty()
            || !self.recent_events.added.is_empty()
            || !self.recent_events.removed.is_empty()
    }
}

pub(crate) fn parse_self_model_snapshot(value: &serde_json::Value) -> Option<SelfModelSnapshot> {
    serde_json::from_value(value.clone()).ok()
}

pub(crate) fn parse_self_model_snapshot_record(
    record: &serde_json::Value,
) -> Option<SelfModelSnapshot> {
    record.get("snapshot").and_then(parse_self_model_snapshot)
}

impl AppState {
    pub(crate) async fn persist_self_model_snapshot_and_detect_gaps(
        &self,
        source: &str,
        trigger_kind: &str,
        trigger_target: Option<&str>,
        summary: &str,
        success: bool,
        growth_event_id: Option<i64>,
    ) {
        let snapshot = self.self_model_snapshot().await;
        let snapshot_json = match serde_json::to_value(&snapshot) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!("self-model: failed to serialize snapshot: {}", error);
                return;
            }
        };
        let snapshot_id = match self.db.record_self_model_snapshot(
            source,
            trigger_kind,
            trigger_target,
            summary,
            &snapshot_json,
            snapshot.capability_count(),
            snapshot.constraints.len(),
            snapshot.growth.active_goals.len(),
        ) {
            Ok(id) => id,
            Err(error) => {
                tracing::warn!("self-model: failed to persist snapshot: {}", error);
                return;
            }
        };

        for gap in detect_self_model_gaps(
            trigger_kind,
            trigger_target,
            summary,
            success,
            growth_event_id,
            &snapshot,
            snapshot_id,
        ) {
            let signature =
                self_model_gap_signature(trigger_kind, gap.target.as_deref(), gap.content.as_str());
            if !should_emit_self_model_gap(self.db.as_ref(), &signature) {
                continue;
            }

            if let Err(error) = autonomy::ingest_observation(
                self.db.as_ref(),
                autonomy::ObservationInput {
                    kind: "self_model_gap".to_string(),
                    source: source.to_string(),
                    content: gap.content,
                    context: gap.context,
                    priority: gap.priority,
                },
            ) {
                tracing::warn!("self-model: failed to promote gap observation: {}", error);
            }
        }
    }

    pub(crate) async fn self_model_snapshot(&self) -> SelfModelSnapshot {
        let built_tools = forge::visible_registered_tools(self.db.as_ref());
        let plugin_tools = self.plugins.all_tools();
        let mcp_tools = self.mcp_hub.list_tools().await;
        let active_goals = self.db.list_active_autonomy_goals().unwrap_or_default();
        let recent_growth = self.db.list_recent_growth_events(6).unwrap_or_default();
        let world_focus = world::state::load_world_focus(self.db.as_ref());

        let adaptation = {
            let soul_state = self.soul.lock().await;
            AdaptationState {
                warmth: round_metric(soul_state.warmth),
                verbosity: round_metric(soul_state.verbosity),
                assertiveness: round_metric(soul_state.assertiveness),
                learned_from_history: soul_state.learned,
                interaction_count: soul_state.interactions,
            }
        };

        let mcp_server_count = mcp_tools
            .iter()
            .map(|tool| tool.server_name.clone())
            .collect::<BTreeSet<_>>()
            .len();

        let mut capabilities = Vec::new();
        capabilities.extend(core_capabilities());
        capabilities.extend(
            tools::BUILTIN_TOOL_CAPABILITIES
                .iter()
                .map(|(name, description)| Capability {
                    source: CapabilitySource::Builtin,
                    name: (*name).to_string(),
                    description: (*description).to_string(),
                }),
        );
        capabilities.extend(built_tools.iter().map(|tool| Capability {
            source: CapabilitySource::SelfBuilt,
            name: tool.name.clone(),
            description: tool.description.clone(),
        }));
        capabilities.extend(plugin_tools.iter().map(|(plugin, tool)| Capability {
            source: CapabilitySource::Plugin,
            name: tool.name.clone(),
            description: format!("{} (plugin {})", tool.description, plugin.name),
        }));
        capabilities.extend(mcp_tools.iter().map(|tool| Capability {
            source: CapabilitySource::Mcp,
            name: format!("{}:{}", tool.server_name, tool.name),
            description: tool.description.clone(),
        }));
        capabilities.sort_by(|a, b| {
            a.source
                .cmp(&b.source)
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        });

        let runtime = RuntimeState {
            user_chat_primary: chat_primary_name(self.llm.chat_primary()).to_string(),
            hosted_tool_loop_ready: self.llm.user_chat_tool_loop_ready(),
            autonomous_llm_ready: self.llm.has_ollama() || self.llm.has_nim(),
            providers: provider_states(&self.config, &self.llm),
            memory_count: self.db.memory_count(),
            message_count: self.db.message_count(),
            active_goal_count: active_goals.len(),
            recent_growth_event_count: recent_growth.len(),
            built_tool_count: built_tools.len(),
            plugin_count: self.plugins.plugins.len(),
            plugin_tool_count: plugin_tools.len(),
            mcp_server_count,
            mcp_tool_count: mcp_tools.len(),
            default_city_configured: !self.config.default_city.trim().is_empty(),
            github_repo_configured: !self.config.github_repo.trim().is_empty(),
            telegram_owner_count: self.config.telegram_owner_ids.len(),
            background_reflection_active: self
                .consciousness
                .active
                .load(std::sync::atomic::Ordering::Relaxed),
            background_thought_count: self
                .consciousness
                .thought_count
                .load(std::sync::atomic::Ordering::Relaxed),
            world_focus,
        };

        let constraints = constraints_for(
            &runtime,
            built_tools.is_empty(),
            plugin_tools.is_empty(),
            mcp_tools.is_empty(),
        );

        let growth = GrowthState {
            active_goals: active_goals
                .into_iter()
                .take(6)
                .map(|goal| GrowthGoal {
                    title: goal.title,
                    priority: goal.priority,
                    source: goal.source,
                })
                .collect(),
            recent_events: recent_growth
                .into_iter()
                .map(|event| GrowthEventSummary {
                    kind: event.kind,
                    target: event.target,
                    summary: event.summary,
                    success: event.success,
                    created_at: event.created_at,
                })
                .collect(),
        };

        SelfModelSnapshot {
            generated_at: chrono::Utc::now().to_rfc3339(),
            identity: IdentityState {
                primary_file: "IDENTITY.md".to_string(),
                legacy_fallback_file: Some("SOUL.md".to_string()),
                principle: "Stable authored identity belongs in IDENTITY.md. Capability awareness, readiness, and limits come from live runtime state.".to_string(),
            },
            adaptation,
            runtime,
            capabilities,
            constraints,
            growth,
        }
    }

    pub(crate) async fn self_model_prompt(&self) -> String {
        self.self_model_snapshot().await.to_prompt()
    }
}

fn core_capabilities() -> Vec<Capability> {
    CORE_SELF_CAPABILITIES
        .iter()
        .map(|(name, description)| Capability {
            source: CapabilitySource::Core,
            name: (*name).to_string(),
            description: (*description).to_string(),
        })
        .collect()
}

fn provider_states(config: &crate::Config, llm: &Arc<llm::LlmGate>) -> Vec<ProviderState> {
    vec![
        ProviderState {
            name: "anthropic".to_string(),
            role: provider_role(llm.chat_primary(), llm::ChatPrimary::Anthropic, false).to_string(),
            configured: llm.has_anthropic(),
            model: if llm.has_anthropic() {
                Some(config.anthropic_model.clone())
            } else {
                None
            },
            reason: if llm.has_anthropic() {
                "Configured via NYX_ANTHROPIC_API_KEY for hosted/fallback chat.".to_string()
            } else {
                "Missing NYX_ANTHROPIC_API_KEY, so Anthropic is unavailable.".to_string()
            },
        },
        ProviderState {
            name: "nim".to_string(),
            role: provider_role(llm.chat_primary(), llm::ChatPrimary::Nim, true).to_string(),
            configured: llm.has_nim(),
            model: if llm.has_nim() {
                Some(config.nim_model.clone())
            } else {
                None
            },
            reason: if llm.has_nim() {
                "Configured via NYX_NIM_API_KEY for autonomous or fallback chat.".to_string()
            } else {
                "Missing NYX_NIM_API_KEY, so NIM is unavailable.".to_string()
            },
        },
        ProviderState {
            name: "ollama".to_string(),
            role: provider_role(llm.chat_primary(), llm::ChatPrimary::Ollama, true).to_string(),
            configured: llm.has_ollama(),
            model: if llm.has_ollama() {
                Some(config.ollama_model.clone())
            } else {
                None
            },
            reason: if llm.has_ollama() {
                "Configured via NYX_OLLAMA_HOST for local chat and fallback paths.".to_string()
            } else {
                "Missing NYX_OLLAMA_HOST, so Ollama is unavailable.".to_string()
            },
        },
    ]
}

fn provider_role(
    primary: llm::ChatPrimary,
    provider: llm::ChatPrimary,
    autonomous: bool,
) -> &'static str {
    if primary == provider {
        return "primary_user_chat";
    }
    if autonomous && matches!(provider, llm::ChatPrimary::Ollama | llm::ChatPrimary::Nim) {
        return "autonomous_or_fallback";
    }
    "fallback"
}

fn constraints_for(
    runtime: &RuntimeState,
    no_built_tools: bool,
    no_plugin_tools: bool,
    no_mcp_tools: bool,
) -> Vec<Constraint> {
    let mut constraints = vec![
        Constraint {
            kind: "capability_claims".to_string(),
            detail: "Only claim actions that appear in the live self-model or active tool listings.".to_string(),
        },
        Constraint {
            kind: "protected_core_writes".to_string(),
            detail: "Protected core files under src/, agents/, Cargo.toml, Cargo.lock, IDENTITY.md, and legacy SOUL.md require the verified forge self-edit path.".to_string(),
        },
        Constraint {
            kind: "tool_boundaries".to_string(),
            detail: "Tool execution stays inside per-tool interfaces, path policy, and sandboxing where available. Generic tools do not get unrestricted core write access.".to_string(),
        },
        Constraint {
            kind: "external_dependency_honesty".to_string(),
            detail: "Network access, provider availability, API keys, local auth, and local services can make an interface conditional even when the tool exists.".to_string(),
        },
        Constraint {
            kind: "autonomy_grounding".to_string(),
            detail: "Background autonomy should act from persisted observations, goals, tasks, and growth telemetry instead of inventing work.".to_string(),
        },
    ];

    if !runtime.hosted_tool_loop_ready {
        constraints.push(Constraint {
            kind: "hosted_tool_loop_unavailable".to_string(),
            detail: "Current user-chat provider state does not support the hosted tool loop, so user chat falls back to flat prompting plus fast paths.".to_string(),
        });
    }
    if !runtime.default_city_configured {
        constraints.push(Constraint {
            kind: "weather_needs_city".to_string(),
            detail: "Weather can still run, but it needs an explicit city until NYX_DEFAULT_CITY is configured.".to_string(),
        });
    }
    if no_built_tools {
        constraints.push(Constraint {
            kind: "no_self_built_tools".to_string(),
            detail: "No promoted self-built tools are currently registered.".to_string(),
        });
    }
    if no_plugin_tools {
        constraints.push(Constraint {
            kind: "no_plugins_loaded".to_string(),
            detail: "No plugin tools are currently loaded into the runtime.".to_string(),
        });
    }
    if no_mcp_tools {
        constraints.push(Constraint {
            kind: "no_mcp_connections".to_string(),
            detail: "No external MCP tools are currently connected.".to_string(),
        });
    }

    constraints
}

fn chat_primary_name(primary: llm::ChatPrimary) -> &'static str {
    match primary {
        llm::ChatPrimary::Anthropic => "anthropic",
        llm::ChatPrimary::Nim => "nim",
        llm::ChatPrimary::Ollama => "ollama",
    }
}

fn round_metric(value: f32) -> f32 {
    (value * 100.0).round() / 100.0
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn world_focus_prompt_lines(world_focus: &world::projects::WorldFocusSummary) -> Vec<String> {
    if world_focus.is_empty() {
        return Vec::new();
    }

    let mut lines = Vec::new();

    if let Some(title) = world_focus.active_project_title.as_deref() {
        let status = world_focus
            .active_project_status
            .as_deref()
            .unwrap_or("active");
        lines.push(format!("- active project: {} [{}]", title, status));
    }

    if let Some(title) = world_focus.active_workstream_title.as_deref() {
        let status = world_focus
            .active_workstream_status
            .as_deref()
            .unwrap_or("active");
        lines.push(format!("- active workstream: {} [{}]", title, status));
    }

    if let Some(title) = world_focus.resume_focus_title.as_deref() {
        let mut details = Vec::new();
        if let Some(status) = world_focus.resume_focus_status.as_deref() {
            details.push(status.to_string());
        }
        if let Some(kind) = world_focus.resume_focus_kind.as_deref() {
            details.push(kind.to_string());
        }
        if let Some(reason) = world_focus.resume_focus_reason.as_deref() {
            details.push(reason.to_string());
        }
        if details.is_empty() {
            lines.push(format!("- resume focus: {}", title));
        } else {
            lines.push(format!(
                "- resume focus: {} [{}]",
                title,
                details.join("; ")
            ));
        }
    }

    if world_focus.resume_focus_blocker_count > 0 {
        lines.push(format!(
            "- blockers touching current focus: {}",
            world_focus.resume_focus_blocker_count
        ));
    }

    if let Some(summary) = world_focus.top_blocker_summary.as_deref() {
        lines.push(format!("- top blocker: {}", crate::trunc(summary, 180)));
    }

    lines
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/self_model.rs"]
mod tests;
