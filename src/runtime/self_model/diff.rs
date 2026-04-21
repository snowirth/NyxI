use std::collections::{BTreeMap, BTreeSet};

use super::{
    Capability, CapabilitySource, GrowthEventSummary, ProviderState, SelfModelCollectionChange,
    SelfModelDiff, SelfModelProviderChange, SelfModelSnapshot, SelfModelValueChange,
};

pub(crate) fn diff_snapshots(from: &SelfModelSnapshot, to: &SelfModelSnapshot) -> SelfModelDiff {
    let mut diff = SelfModelDiff::default();

    push_change(
        &mut diff.identity,
        "primary_file",
        &from.identity.primary_file,
        &to.identity.primary_file,
    );
    push_optional_change(
        &mut diff.identity,
        "legacy_fallback_file",
        from.identity.legacy_fallback_file.as_deref(),
        to.identity.legacy_fallback_file.as_deref(),
    );
    push_change(
        &mut diff.identity,
        "principle",
        &from.identity.principle,
        &to.identity.principle,
    );

    push_change(
        &mut diff.adaptation,
        "warmth",
        &format!("{:.2}", from.adaptation.warmth),
        &format!("{:.2}", to.adaptation.warmth),
    );
    push_change(
        &mut diff.adaptation,
        "verbosity",
        &format!("{:.2}", from.adaptation.verbosity),
        &format!("{:.2}", to.adaptation.verbosity),
    );
    push_change(
        &mut diff.adaptation,
        "assertiveness",
        &format!("{:.2}", from.adaptation.assertiveness),
        &format!("{:.2}", to.adaptation.assertiveness),
    );
    push_change(
        &mut diff.adaptation,
        "learned_from_history",
        &from.adaptation.learned_from_history.to_string(),
        &to.adaptation.learned_from_history.to_string(),
    );
    push_change(
        &mut diff.adaptation,
        "interaction_count",
        &from.adaptation.interaction_count.to_string(),
        &to.adaptation.interaction_count.to_string(),
    );

    push_change(
        &mut diff.runtime,
        "user_chat_primary",
        &from.runtime.user_chat_primary,
        &to.runtime.user_chat_primary,
    );
    push_change(
        &mut diff.runtime,
        "hosted_tool_loop_ready",
        &from.runtime.hosted_tool_loop_ready.to_string(),
        &to.runtime.hosted_tool_loop_ready.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "autonomous_llm_ready",
        &from.runtime.autonomous_llm_ready.to_string(),
        &to.runtime.autonomous_llm_ready.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "memory_count",
        &from.runtime.memory_count.to_string(),
        &to.runtime.memory_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "message_count",
        &from.runtime.message_count.to_string(),
        &to.runtime.message_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "active_goal_count",
        &from.runtime.active_goal_count.to_string(),
        &to.runtime.active_goal_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "recent_growth_event_count",
        &from.runtime.recent_growth_event_count.to_string(),
        &to.runtime.recent_growth_event_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "built_tool_count",
        &from.runtime.built_tool_count.to_string(),
        &to.runtime.built_tool_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "plugin_count",
        &from.runtime.plugin_count.to_string(),
        &to.runtime.plugin_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "plugin_tool_count",
        &from.runtime.plugin_tool_count.to_string(),
        &to.runtime.plugin_tool_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "mcp_server_count",
        &from.runtime.mcp_server_count.to_string(),
        &to.runtime.mcp_server_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "mcp_tool_count",
        &from.runtime.mcp_tool_count.to_string(),
        &to.runtime.mcp_tool_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "default_city_configured",
        &from.runtime.default_city_configured.to_string(),
        &to.runtime.default_city_configured.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "github_repo_configured",
        &from.runtime.github_repo_configured.to_string(),
        &to.runtime.github_repo_configured.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "telegram_owner_count",
        &from.runtime.telegram_owner_count.to_string(),
        &to.runtime.telegram_owner_count.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "background_reflection_active",
        &from.runtime.background_reflection_active.to_string(),
        &to.runtime.background_reflection_active.to_string(),
    );
    push_change(
        &mut diff.runtime,
        "background_thought_count",
        &from.runtime.background_thought_count.to_string(),
        &to.runtime.background_thought_count.to_string(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.active_project_title",
        from.runtime.world_focus.active_project_title.as_deref(),
        to.runtime.world_focus.active_project_title.as_deref(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.active_project_status",
        from.runtime.world_focus.active_project_status.as_deref(),
        to.runtime.world_focus.active_project_status.as_deref(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.active_workstream_title",
        from.runtime.world_focus.active_workstream_title.as_deref(),
        to.runtime.world_focus.active_workstream_title.as_deref(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.active_workstream_status",
        from.runtime.world_focus.active_workstream_status.as_deref(),
        to.runtime.world_focus.active_workstream_status.as_deref(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.resume_focus_title",
        from.runtime.world_focus.resume_focus_title.as_deref(),
        to.runtime.world_focus.resume_focus_title.as_deref(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.resume_focus_kind",
        from.runtime.world_focus.resume_focus_kind.as_deref(),
        to.runtime.world_focus.resume_focus_kind.as_deref(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.resume_focus_status",
        from.runtime.world_focus.resume_focus_status.as_deref(),
        to.runtime.world_focus.resume_focus_status.as_deref(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.resume_focus_reason",
        from.runtime.world_focus.resume_focus_reason.as_deref(),
        to.runtime.world_focus.resume_focus_reason.as_deref(),
    );
    push_change(
        &mut diff.runtime,
        "world_focus.resume_focus_blocker_count",
        &from
            .runtime
            .world_focus
            .resume_focus_blocker_count
            .to_string(),
        &to.runtime
            .world_focus
            .resume_focus_blocker_count
            .to_string(),
    );
    push_change(
        &mut diff.runtime,
        "world_focus.blocker_count",
        &from.runtime.world_focus.blocker_count.to_string(),
        &to.runtime.world_focus.blocker_count.to_string(),
    );
    push_optional_change(
        &mut diff.runtime,
        "world_focus.top_blocker_summary",
        from.runtime.world_focus.top_blocker_summary.as_deref(),
        to.runtime.world_focus.top_blocker_summary.as_deref(),
    );

    diff.providers = diff_provider_states(&from.runtime.providers, &to.runtime.providers);
    diff.capabilities = diff_string_collections(
        from.capabilities.iter().map(capability_label),
        to.capabilities.iter().map(capability_label),
    );
    diff.constraints = diff_string_collections(
        from.constraints
            .iter()
            .map(|constraint| constraint.kind.clone()),
        to.constraints
            .iter()
            .map(|constraint| constraint.kind.clone()),
    );
    diff.growth_goals = diff_string_collections(
        from.growth
            .active_goals
            .iter()
            .map(|goal| goal.title.clone()),
        to.growth.active_goals.iter().map(|goal| goal.title.clone()),
    );
    diff.recent_events = diff_string_collections(
        from.growth.recent_events.iter().map(growth_event_label),
        to.growth.recent_events.iter().map(growth_event_label),
    );

    diff.summary = build_diff_summary(&diff);
    diff
}

fn push_change(changes: &mut Vec<SelfModelValueChange>, field: &str, from: &str, to: &str) {
    if from != to {
        changes.push(SelfModelValueChange {
            field: field.to_string(),
            from: from.to_string(),
            to: to.to_string(),
        });
    }
}

fn push_optional_change(
    changes: &mut Vec<SelfModelValueChange>,
    field: &str,
    from: Option<&str>,
    to: Option<&str>,
) {
    let from_value = from.unwrap_or("n/a");
    let to_value = to.unwrap_or("n/a");
    push_change(changes, field, from_value, to_value);
}

fn diff_provider_states(
    from: &[ProviderState],
    to: &[ProviderState],
) -> Vec<SelfModelProviderChange> {
    let from_map = from
        .iter()
        .map(|provider| (provider.name.clone(), provider))
        .collect::<BTreeMap<_, _>>();
    let to_map = to
        .iter()
        .map(|provider| (provider.name.clone(), provider))
        .collect::<BTreeMap<_, _>>();
    let mut names = from_map.keys().cloned().collect::<BTreeSet<_>>();
    names.extend(to_map.keys().cloned());

    names
        .into_iter()
        .filter_map(|name| match (from_map.get(&name), to_map.get(&name)) {
            (Some(from_provider), Some(to_provider)) => {
                let mut details = Vec::new();
                push_change(&mut details, "role", &from_provider.role, &to_provider.role);
                push_change(
                    &mut details,
                    "configured",
                    &from_provider.configured.to_string(),
                    &to_provider.configured.to_string(),
                );
                push_optional_change(
                    &mut details,
                    "model",
                    from_provider.model.as_deref(),
                    to_provider.model.as_deref(),
                );
                push_change(
                    &mut details,
                    "reason",
                    &from_provider.reason,
                    &to_provider.reason,
                );
                if details.is_empty() {
                    None
                } else {
                    Some(SelfModelProviderChange {
                        name: name.clone(),
                        change: "updated".to_string(),
                        summary: format!(
                            "{} changed: {}",
                            name,
                            details
                                .iter()
                                .map(|detail| detail.field.clone())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        details,
                        from: Some((*from_provider).clone()),
                        to: Some((*to_provider).clone()),
                    })
                }
            }
            (Some(from_provider), None) => Some(SelfModelProviderChange {
                name: name.clone(),
                change: "removed".to_string(),
                summary: format!("{} is no longer visible in provider readiness", name),
                details: Vec::new(),
                from: Some((*from_provider).clone()),
                to: None,
            }),
            (None, Some(to_provider)) => Some(SelfModelProviderChange {
                name: name.clone(),
                change: "added".to_string(),
                summary: format!("{} is now visible in provider readiness", name),
                details: Vec::new(),
                from: None,
                to: Some((*to_provider).clone()),
            }),
            (None, None) => None,
        })
        .collect()
}

fn diff_string_collections(
    from: impl IntoIterator<Item = String>,
    to: impl IntoIterator<Item = String>,
) -> SelfModelCollectionChange {
    let from_set = from.into_iter().collect::<BTreeSet<_>>();
    let to_set = to.into_iter().collect::<BTreeSet<_>>();
    SelfModelCollectionChange {
        added: to_set.difference(&from_set).cloned().collect(),
        removed: from_set.difference(&to_set).cloned().collect(),
    }
}

fn capability_label(capability: &Capability) -> String {
    format!(
        "{} [{}]",
        capability.name,
        capability_source_key(capability.source)
    )
}

fn capability_source_key(source: CapabilitySource) -> &'static str {
    match source {
        CapabilitySource::Core => "core",
        CapabilitySource::Builtin => "builtin",
        CapabilitySource::SelfBuilt => "self-built",
        CapabilitySource::Plugin => "plugin",
        CapabilitySource::Mcp => "mcp",
    }
}

fn growth_event_label(event: &GrowthEventSummary) -> String {
    event.target.as_ref().map_or_else(
        || event.kind.clone(),
        |target| format!("{} -> {}", event.kind, target),
    )
}

fn build_diff_summary(diff: &SelfModelDiff) -> Vec<String> {
    let mut summary = Vec::new();

    if !diff.providers.is_empty() {
        summary.push(format!(
            "Provider readiness changed: {}",
            diff.providers
                .iter()
                .map(|provider| provider.name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !diff.capabilities.added.is_empty() || !diff.capabilities.removed.is_empty() {
        summary.push(format!(
            "Capabilities changed: +{} / -{}",
            diff.capabilities.added.len(),
            diff.capabilities.removed.len()
        ));
    }
    if !diff.constraints.added.is_empty() || !diff.constraints.removed.is_empty() {
        summary.push(format!(
            "Constraints changed: +{} / -{}",
            diff.constraints.added.len(),
            diff.constraints.removed.len()
        ));
    }
    if !diff.runtime.is_empty() {
        summary.push(format!(
            "Runtime changed: {}",
            diff.runtime
                .iter()
                .take(4)
                .map(|change| change.field.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !diff.adaptation.is_empty() {
        summary.push(format!(
            "Adaptation changed: {}",
            diff.adaptation
                .iter()
                .take(3)
                .map(|change| change.field.clone())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !diff.growth_goals.added.is_empty() || !diff.growth_goals.removed.is_empty() {
        summary.push(format!(
            "Growth goals changed: +{} / -{}",
            diff.growth_goals.added.len(),
            diff.growth_goals.removed.len()
        ));
    }
    if !diff.recent_events.added.is_empty() || !diff.recent_events.removed.is_empty() {
        summary.push(format!(
            "Recent growth events changed: +{} / -{}",
            diff.recent_events.added.len(),
            diff.recent_events.removed.len()
        ));
    }
    if summary.is_empty() {
        summary.push("No self-model changes detected between the selected snapshots.".to_string());
    }
    summary
}
