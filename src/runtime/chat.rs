use anyhow::Result as AnyhowResult;
use std::path::Path;
use std::time::Instant;

use crate::{AppState, awareness, constitution, forge, llm, tools};

const TOOL_GROWTH_GUIDANCE: &str = "<capability_planning>\nIf the user asks for something scriptable that is not yet a registered tool, and the self-model lists tool_growth, say you can try building a tool instead of saying you are categorically unable.\n</capability_planning>\n\n";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ToolDispatchPreflight {
    pub requested_name: String,
    pub resolved_name: String,
    pub kind: String,
    pub ready: bool,
    pub summary: String,
    pub issue: Option<String>,
    pub arguments: serde_json::Value,
    pub requires_network: bool,
    pub sandboxed: bool,
    pub target: Option<String>,
    pub command: Option<String>,
    pub source: Option<String>,
    pub server_name: Option<String>,
    pub quarantined_until: Option<String>,
    pub policy_notes: Vec<String>,
    pub policy_triggers: Vec<String>,
    pub verification_mode: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ToolDispatchResult {
    pub tool_name: String,
    pub dry_run: bool,
    pub success: bool,
    pub executed: bool,
    pub verified: Option<bool>,
    pub telemetry: serde_json::Value,
    pub preflight: ToolDispatchPreflight,
    pub output: Option<String>,
    pub raw_result: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
enum PreparedToolCall {
    Blocked(ToolDispatchPreflight),
    Builtin {
        tool_name: String,
        args: serde_json::Value,
        preflight: ToolDispatchPreflight,
    },
    SelfBuilt {
        tool_name: String,
        args: serde_json::Value,
        preflight: ToolDispatchPreflight,
    },
    Plugin {
        tool_name: String,
        args: serde_json::Value,
        preflight: ToolDispatchPreflight,
    },
    Mcp {
        server_name: String,
        tool_name: String,
        args: serde_json::Value,
        preflight: ToolDispatchPreflight,
    },
}

impl PreparedToolCall {
    fn preflight(&self) -> &ToolDispatchPreflight {
        match self {
            Self::Blocked(preflight)
            | Self::Builtin { preflight, .. }
            | Self::SelfBuilt { preflight, .. }
            | Self::Plugin { preflight, .. }
            | Self::Mcp { preflight, .. } => preflight,
        }
    }
}

fn tool_dispatch_telemetry(executed: bool, verified: Option<bool>) -> serde_json::Value {
    serde_json::json!({
        "executed": executed,
        "verified": verified,
    })
}

impl AppState {
    pub(crate) async fn respond_with_chat(
        &self,
        channel: &str,
        sender: &str,
        text: &str,
        proactive_prefix: &str,
        awareness_ctx: &awareness::AwarenessContext,
    ) -> crate::runtime::ChatResponseDraft {
        let started_at = Instant::now();
        let profile = self.get_profile(sender);
        let profile_text = profile.to_prompt();
        let self_model = self.self_model_prompt().await;
        let now = chrono::Local::now();
        let time = now.format("%H:%M %A %d %B %Y").to_string();
        let tone = awareness_ctx.tone_directive();
        let memory_working_set = self.memory_working_set_for_query(text, 8).await;
        let relevant_memories = memory_working_set.prompt_context.clone();
        let approved_policy_directives = approved_live_policy_directives(self.db.as_ref(), 8);
        let approved_policy_prompt = approved_policy_prompt_fragment(&approved_policy_directives);
        let chat_routing_experiment = self
            .llm
            .select_routing_experiment(&approved_policy_directives, llm::RoutingSurface::Chat);
        let tool_loop_routing_experiment = self
            .llm
            .select_routing_experiment(&approved_policy_directives, llm::RoutingSurface::ToolLoop);

        let raw_history = self.db.get_history(channel, 20);
        let history = if raw_history.len() >= 20 {
            let summary_key = format!("compressed:{}", channel);
            let existing_summary = self.db.get_state(&summary_key);

            if let Some(summary) = existing_summary {
                let recent = raw_history[raw_history.len().saturating_sub(10)..].to_vec();
                let mut compressed =
                    vec![("system".to_string(), format!("[Earlier: {}]", summary))];
                compressed.extend(recent);
                compressed
            } else {
                let old: String = raw_history[..raw_history.len() - 10]
                    .iter()
                    .map(|(role, content)| format!("{}: {}", role, crate::trunc(content, 100)))
                    .collect::<Vec<_>>()
                    .join("\n");

                if let Ok(summary) = self
                    .llm
                    .chat_auto(
                        &format!(
                            "Compress this conversation into one paragraph. Focus on what matters:\n{}",
                            crate::trunc(&old, 1000)
                        ),
                        80,
                    )
                    .await
                {
                    self.db.set_state(&summary_key, summary.trim());
                    let mut compressed = vec![(
                        "system".to_string(),
                        format!("[Earlier: {}]", summary.trim()),
                    )];
                    compressed.extend(raw_history[raw_history.len() - 10..].to_vec());
                    compressed
                } else {
                    raw_history[raw_history.len().saturating_sub(12)..].to_vec()
                }
            }
        } else {
            raw_history
        };
        let cross_surface_prompt_context = if history.len() < 4 {
            crate::world::continuity::read_cross_surface_continuity(
                self.db.as_ref(),
                "chat_cross_surface_prompt",
            )
            .ok()
            .and_then(|brief| crate::world::continuity::render_prompt_context(&brief, channel))
        } else {
            None
        };

        let soul_prompt = {
            let soul_state = self.soul.lock().await;
            soul_state.to_prompt()
        };

        let mut prompt = format!(
            "<identity>\n{}\n</identity>\n\n{}\n\n{}\n\n<user_profile>\n{}\n</user_profile>\n\n<context>\nTime: {}{}\nChannel: {}\n</context>\n\n",
            soul_prompt,
            constitution::Constitution::to_prompt(),
            self_model,
            profile_text,
            time,
            if self.config.user_location.is_empty() {
                String::new()
            } else {
                format!(" {}", self.config.user_location)
            },
            channel
        );

        if !tone.is_empty() {
            prompt.push_str(&format!("{}\n\n", tone));
        }
        prompt.push_str(TOOL_GROWTH_GUIDANCE);
        if let Some(policy_prompt) = approved_policy_prompt.as_deref() {
            prompt.push_str(policy_prompt);
        }

        if !relevant_memories.is_empty() {
            prompt.push_str(&format!(
                "<memories>\nYou know these things. Don't announce them — just let them inform your response naturally. Only mention if directly relevant.\n{}\n</memories>\n\n",
                relevant_memories
            ));
        }
        if !memory_working_set.action_notes.is_empty() {
            prompt.push_str(&format!(
                "<memory_action_notes>\n{}\n</memory_action_notes>\n\n",
                memory_working_set.action_notes.join("\n")
            ));
        }
        if let Some(cross_surface_context) = cross_surface_prompt_context.as_deref() {
            prompt.push_str(&format!(
                "<cross_surface_continuity>\n{}\n</cross_surface_continuity>\n\n",
                cross_surface_context
            ));
        }

        if !proactive_prefix.is_empty() {
            prompt.push_str(&format!(
                "<proactive>\n{}</proactive>\n\n",
                proactive_prefix
            ));
        }

        for (role, content) in &history {
            prompt.push_str(&format!("{}: {}\n", role, crate::trunc(content, 200)));
        }
        prompt.push_str(&format!("user: {}\nassistant:", text));

        let plugin_tools = self.plugins.tools_for_prompt();
        let built_tools = forge::tools_for_prompt(self.db.as_ref());
        let mcp_tools = self.mcp_hub.tools_for_prompt().await;

        let mut system_for_tools = format!(
            "<identity>\n{}\n</identity>\n\n{}\n\n{}\n\n<user_profile>\n{}\n</user_profile>\n\n<context>\nTime: {}{}\nChannel: {}\n</context>\n\n",
            soul_prompt,
            constitution::Constitution::to_prompt(),
            self_model,
            profile_text,
            time,
            if self.config.user_location.is_empty() {
                String::new()
            } else {
                format!(" {}", self.config.user_location)
            },
            channel
        );
        system_for_tools.push_str(
            "<tool_instructions>\nYou may call tools whenever they improve the answer. \
             Use web_search for time-sensitive or uncertain facts; git_info and github for repo and PRs; \
             vision to see the user's screen; weather for weather; file_ops only to read allowed project paths; \
             twitter for timeline/search/post/reply/like; gif and image_gen when the user wants those. \
             For self-built tools, plugin tools, and MCP tools listed below, call external_tool with the exact tool name and an arguments object. \
             Chain tools if needed. After tool results, reply briefly in your voice — no XML.\n</tool_instructions>\n\n",
        );
        system_for_tools.push_str(TOOL_GROWTH_GUIDANCE);
        if let Some(policy_prompt) = approved_policy_prompt.as_deref() {
            system_for_tools.push_str(policy_prompt);
        }
        if !tone.is_empty() {
            system_for_tools.push_str(&format!("{}\n\n", tone));
        }
        if !relevant_memories.is_empty() {
            system_for_tools.push_str(&format!(
                "<memories>\nYou know these things. Don't announce them — weave in only when relevant.\n{}\n</memories>\n\n",
                relevant_memories
            ));
        }
        if let Some(cross_surface_context) = cross_surface_prompt_context.as_deref() {
            system_for_tools.push_str(&format!(
                "<cross_surface_continuity>\n{}\n</cross_surface_continuity>\n\n",
                cross_surface_context
            ));
        }
        if !memory_working_set.action_notes.is_empty() {
            system_for_tools.push_str(&format!(
                "<memory_action_notes>\n{}\n</memory_action_notes>\n\n",
                memory_working_set.action_notes.join("\n")
            ));
        }
        if !proactive_prefix.is_empty() {
            system_for_tools.push_str(&format!(
                "<proactive>\n{}</proactive>\n\n",
                proactive_prefix
            ));
        }
        if !built_tools.is_empty() {
            system_for_tools.push_str(&built_tools);
            system_for_tools.push_str("\n\n");
        }
        if !plugin_tools.is_empty() {
            system_for_tools.push_str(&plugin_tools);
            system_for_tools.push_str("\n\n");
        }
        if !mcp_tools.is_empty() {
            system_for_tools.push_str(&mcp_tools);
            system_for_tools.push_str("\n\n");
        }

        let mut convo_for_tools: Vec<(String, String)> = Vec::new();
        for (role, content) in &history {
            if role == "system" {
                system_for_tools.push_str(&format!(
                    "<session_context>\n{}\n</session_context>\n\n",
                    content
                ));
            } else if role == "user" || role == "assistant" {
                convo_for_tools.push((role.clone(), crate::trunc(content, 4000).to_string()));
            }
        }

        let mut tool_loop = None;
        let (raw_response, llm_trace) = if self.llm.user_chat_tool_loop_ready() {
            let state = self.clone();
            let mut run_tool = |name: String, input: serde_json::Value| {
                let state = state.clone();
                async move { state.dispatch_chat_tool(&name, input).await }
            };
            record_routing_experiment_selected(
                self.db.as_ref(),
                tool_loop_routing_experiment.as_ref(),
                "tool_loop",
            );
            match self
                .llm
                .chat_primary_with_tools_traced(
                    &system_for_tools,
                    &convo_for_tools,
                    text,
                    10,
                    2048,
                    tool_loop_routing_experiment.as_ref(),
                    &mut run_tool,
                )
                .await
            {
                Ok(response) if !response.text.trim().is_empty() => {
                    let tool_loop_latency_ms = started_at.elapsed().as_millis() as u64;
                    let mut response_trace = response.trace;
                    response_trace.latency_ms = Some(tool_loop_latency_ms);
                    tool_loop = Some(serde_json::json!({
                        "attempted": true,
                        "status": "completed",
                        "provider": response_trace.provider,
                        "model": response_trace.model,
                        "route": response_trace.route,
                        "latency_ms": tool_loop_latency_ms,
                        "used_fallback": response_trace.route.contains("fallback"),
                    }));
                    record_routing_experiment_applied(
                        self.db.as_ref(),
                        tool_loop_routing_experiment.as_ref(),
                        "tool_loop",
                        &response_trace,
                    );
                    (response.text, Some(response_trace))
                }
                Ok(_) => {
                    tool_loop = Some(serde_json::json!({
                        "attempted": true,
                        "status": "empty_response",
                        "fallback": "flat_prompt",
                    }));
                    record_routing_experiment_selected(
                        self.db.as_ref(),
                        chat_routing_experiment.as_ref(),
                        "chat",
                    );
                    match self
                        .llm
                        .chat_traced_with_routing_experiment(
                            &prompt,
                            512,
                            chat_routing_experiment.as_ref(),
                        )
                        .await
                    {
                        Ok(response) => {
                            record_routing_experiment_applied(
                                self.db.as_ref(),
                                chat_routing_experiment.as_ref(),
                                "chat",
                                &response.trace,
                            );
                            (response.text, Some(response.trace))
                        }
                        Err(_) => ("hmm, let me think about that.".into(), None),
                    }
                }
                Err(error) => {
                    tracing::warn!("chat tool loop failed: {}", error);
                    tool_loop = Some(serde_json::json!({
                        "attempted": true,
                        "status": "failed",
                        "error": error.to_string(),
                        "fallback": "flat_prompt",
                    }));
                    record_routing_experiment_selected(
                        self.db.as_ref(),
                        chat_routing_experiment.as_ref(),
                        "chat",
                    );
                    match self
                        .llm
                        .chat_traced_with_routing_experiment(
                            &prompt,
                            512,
                            chat_routing_experiment.as_ref(),
                        )
                        .await
                    {
                        Ok(response) => {
                            record_routing_experiment_applied(
                                self.db.as_ref(),
                                chat_routing_experiment.as_ref(),
                                "chat",
                                &response.trace,
                            );
                            (response.text, Some(response.trace))
                        }
                        Err(_) => ("hmm, let me think about that.".into(), None),
                    }
                }
            }
        } else {
            record_routing_experiment_selected(
                self.db.as_ref(),
                chat_routing_experiment.as_ref(),
                "chat",
            );
            match self
                .llm
                .chat_traced_with_routing_experiment(&prompt, 512, chat_routing_experiment.as_ref())
                .await
            {
                Ok(response) => {
                    record_routing_experiment_applied(
                        self.db.as_ref(),
                        chat_routing_experiment.as_ref(),
                        "chat",
                        &response.trace,
                    );
                    (response.text, Some(response.trace))
                }
                Err(_) => ("hmm, let me think about that.".into(), None),
            }
        };

        crate::runtime::ChatResponseDraft {
            response: constitution::Constitution::filter_response(&raw_response),
            llm_trace,
            llm_latency_ms: Some(started_at.elapsed().as_millis() as u64),
            tool_loop,
        }
    }

    pub(crate) async fn dispatch_chat_tool(&self, name: &str, input: serde_json::Value) -> String {
        let result = self.dispatch_chat_tool_action(name, input, false).await;
        pack_tool_dispatch_result(result)
    }

    pub(crate) async fn dispatch_chat_tool_action(
        &self,
        name: &str,
        input: serde_json::Value,
        dry_run: bool,
    ) -> ToolDispatchResult {
        let started_at = Instant::now();
        let prepared = self.prepare_chat_tool_action(name, input).await;
        let preflight = prepared.preflight().clone();

        let result = if !preflight.ready {
            ToolDispatchResult {
                tool_name: preflight.requested_name.clone(),
                dry_run,
                success: false,
                executed: false,
                verified: None,
                telemetry: tool_dispatch_telemetry(false, None),
                output: None,
                raw_result: None,
                error: preflight.issue.clone(),
                preflight,
            }
        } else if dry_run {
            ToolDispatchResult {
                tool_name: preflight.requested_name.clone(),
                dry_run: true,
                success: true,
                executed: false,
                verified: None,
                telemetry: tool_dispatch_telemetry(false, None),
                output: Some(format!("dry run: {}", preflight.summary)),
                raw_result: Some(serde_json::json!({
                    "dry_run": true,
                    "arguments": preflight.arguments.clone(),
                })),
                error: None,
                preflight,
            }
        } else {
            match prepared {
                PreparedToolCall::Blocked(preflight) => ToolDispatchResult {
                    tool_name: preflight.requested_name.clone(),
                    dry_run,
                    success: false,
                    executed: false,
                    verified: None,
                    telemetry: tool_dispatch_telemetry(false, None),
                    output: None,
                    raw_result: None,
                    error: preflight.issue.clone(),
                    preflight,
                },
                PreparedToolCall::Builtin {
                    tool_name,
                    args,
                    preflight,
                } => {
                    let result = tools::run_with_state(Some(self), &tool_name, &args).await;
                    if tool_name == "web_search" {
                        if let Ok(value) = &result {
                            if let Some(output) = value["output"].as_str() {
                                let query = args["query"].as_str().unwrap_or("");
                                if !query.is_empty() {
                                    self.ingest_web_search_memory(query, output);
                                }
                            }
                        }
                    }
                    tool_dispatch_result_from_json(preflight, false, result)
                }
                PreparedToolCall::SelfBuilt {
                    tool_name,
                    args,
                    preflight,
                } => {
                    let result =
                        forge::run_registered_tool_checked(self.db.as_ref(), &tool_name, &args)
                            .await
                            .unwrap_or_else(|| {
                                Err(anyhow::anyhow!("registered tool {} not found", tool_name))
                            });
                    tool_dispatch_result_from_json(preflight, false, result)
                }
                PreparedToolCall::Plugin {
                    tool_name,
                    args,
                    preflight,
                } => {
                    let result = self.plugins.run_tool(&tool_name, &args).await;
                    tool_dispatch_result_from_json(preflight, false, result)
                }
                PreparedToolCall::Mcp {
                    server_name,
                    tool_name,
                    args,
                    preflight,
                } => match self.mcp_hub.call_tool(&server_name, &tool_name, args).await {
                    Ok(output) => ToolDispatchResult {
                        tool_name: preflight.requested_name.clone(),
                        dry_run: false,
                        success: true,
                        executed: true,
                        verified: None,
                        telemetry: tool_dispatch_telemetry(true, None),
                        output: Some(output.clone()),
                        raw_result: Some(serde_json::json!({ "output": output })),
                        error: None,
                        preflight,
                    },
                    Err(error) => ToolDispatchResult {
                        tool_name: preflight.requested_name.clone(),
                        dry_run: false,
                        success: false,
                        executed: true,
                        verified: None,
                        telemetry: tool_dispatch_telemetry(true, None),
                        output: None,
                        raw_result: None,
                        error: Some(error.to_string()),
                        preflight,
                    },
                },
            }
        };

        self.record_tool_dispatch_execution(&result, started_at.elapsed().as_millis() as i64);
        result
    }

    async fn prepare_chat_tool_action(
        &self,
        name: &str,
        input: serde_json::Value,
    ) -> PreparedToolCall {
        match name {
            "weather" | "web_search" | "git_info" | "github" | "vision" | "gif" | "file_ops"
            | "twitter" | "image_gen" | "computer_use" | "transcribe" | "browser" => {
                self.prepare_builtin_tool_action(name, input)
            }
            "external_tool" => self.prepare_external_tool_action(input).await,
            _ => PreparedToolCall::Blocked(blocked_preflight(
                name,
                "builtin",
                format!("unknown tool: {}", name),
                serde_json::json!({}),
            )),
        }
    }

    fn prepare_builtin_tool_action(
        &self,
        name: &str,
        input: serde_json::Value,
    ) -> PreparedToolCall {
        let Some(status) = tools::builtin_tool_runtime_statuses()
            .into_iter()
            .find(|candidate| candidate.name == name)
        else {
            return PreparedToolCall::Blocked(blocked_preflight(
                name,
                "builtin",
                format!("unknown builtin tool: {}", name),
                serde_json::json!({}),
            ));
        };

        let mut issue = status.issue.clone();
        let mut args = serde_json::json!({});
        let mut summary = format!("{} {}", status.kind, status.name);

        match name {
            "weather" => {
                let city = input["city"]
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| self.config.default_city.clone());
                if city.trim().is_empty() {
                    issue.get_or_insert_with(|| "need a city or set NYX_DEFAULT_CITY".to_string());
                }
                args = serde_json::json!({ "city": city });
                summary = format!("weather lookup for {}", args["city"].as_str().unwrap_or(""));
            }
            "web_search" => {
                let query = input["query"].as_str().unwrap_or("").trim().to_string();
                if query.is_empty() {
                    issue.get_or_insert_with(|| "query required".to_string());
                }
                args = serde_json::json!({ "query": query });
                summary = format!("web search for {}", args["query"].as_str().unwrap_or(""));
            }
            "git_info" => {
                let action = input["action"]
                    .as_str()
                    .unwrap_or("status")
                    .trim()
                    .to_string();
                if !matches!(action.as_str(), "status" | "log" | "todos" | "diff") {
                    issue.get_or_insert_with(|| format!("unknown git_info action: {}", action));
                }
                args = serde_json::json!({ "action": action });
                summary = format!("git_info {}", args["action"].as_str().unwrap_or(""));
            }
            "github" => {
                let action = input["action"]
                    .as_str()
                    .unwrap_or("notifications")
                    .trim()
                    .to_string();
                let repo = input["repo"]
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| self.config.github_repo.clone());
                if !matches!(action.as_str(), "notifications" | "prs" | "issues") {
                    issue.get_or_insert_with(|| format!("unknown github action: {}", action));
                }
                if matches!(action.as_str(), "prs" | "issues") && repo.trim().is_empty() {
                    issue.get_or_insert_with(|| {
                        "set NYX_GITHUB_REPO or pass repo (owner/name)".to_string()
                    });
                }
                args = serde_json::json!({ "action": action, "repo": repo });
                summary = format!("github {}", args["action"].as_str().unwrap_or(""));
            }
            "vision" => {
                let prompt = input["prompt"].as_str().unwrap_or(
                    "Describe what you see on screen: apps, visible text, what the user is doing.",
                );
                args = serde_json::json!({ "prompt": prompt });
                summary = "vision screen capture".to_string();
            }
            "gif" => {
                let query = input["query"]
                    .as_str()
                    .unwrap_or("funny")
                    .trim()
                    .to_string();
                args = serde_json::json!({ "query": query });
                summary = format!("gif lookup for {}", args["query"].as_str().unwrap_or(""));
            }
            "file_ops" => {
                let action = input["action"].as_str().unwrap_or("read").trim();
                let path = input["path"].as_str().unwrap_or("").trim().to_string();
                if action != "read" {
                    issue.get_or_insert_with(|| {
                        "chat action layer only supports file_ops read".to_string()
                    });
                }
                if path.is_empty() {
                    issue.get_or_insert_with(|| "path required".to_string());
                }
                args = serde_json::json!({ "action": "read", "path": path });
                summary = format!("file read {}", args["path"].as_str().unwrap_or(""));
            }
            "twitter" => {
                let action = input["action"]
                    .as_str()
                    .unwrap_or("timeline")
                    .trim()
                    .to_string();
                if !matches!(
                    action.as_str(),
                    "timeline" | "search" | "post" | "reply" | "like"
                ) {
                    issue.get_or_insert_with(|| format!("unknown twitter action: {}", action));
                }
                let mut payload = serde_json::json!({ "action": action });
                if let Some(count) = input.get("count").and_then(|value| value.as_u64()) {
                    payload["count"] = serde_json::json!(count);
                }
                if let Some(query) = input["query"].as_str() {
                    if !query.trim().is_empty() {
                        payload["query"] = serde_json::json!(query.trim());
                    }
                }
                if let Some(text) = input["text"].as_str() {
                    if !text.trim().is_empty() {
                        payload["text"] = serde_json::json!(text.trim());
                    }
                }
                if let Some(tweet_id) = input["tweet_id"].as_str() {
                    if !tweet_id.trim().is_empty() {
                        payload["tweet_id"] = serde_json::json!(tweet_id.trim());
                    }
                }
                match action.as_str() {
                    "search" if payload.get("query").is_none() => {
                        issue
                            .get_or_insert_with(|| "query required for twitter search".to_string());
                    }
                    "post" if payload.get("text").is_none() => {
                        issue.get_or_insert_with(|| "text required for twitter post".to_string());
                    }
                    "reply" => {
                        if payload.get("tweet_id").is_none() {
                            issue.get_or_insert_with(|| {
                                "tweet_id required for twitter reply".to_string()
                            });
                        }
                        if payload.get("text").is_none() {
                            issue.get_or_insert_with(|| {
                                "text required for twitter reply".to_string()
                            });
                        }
                    }
                    "like" if payload.get("tweet_id").is_none() => {
                        issue.get_or_insert_with(|| {
                            "tweet_id required for twitter like".to_string()
                        });
                    }
                    _ => {}
                }
                args = payload;
                summary = format!("twitter {}", args["action"].as_str().unwrap_or(""));
            }
            "image_gen" => {
                let prompt = input["prompt"].as_str().unwrap_or("").trim().to_string();
                if prompt.is_empty() {
                    issue.get_or_insert_with(|| "prompt required".to_string());
                }
                let mut payload = serde_json::json!({ "prompt": prompt });
                if let Some(style) = input["style"].as_str() {
                    if !style.trim().is_empty() {
                        payload["style"] = serde_json::json!(style.trim());
                    }
                }
                args = payload;
                summary = "image generation request".to_string();
            }
            "computer_use" => {
                let task = input["task"]
                    .as_str()
                    .or_else(|| input["prompt"].as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if task.is_empty() {
                    issue.get_or_insert_with(|| "task required".to_string());
                }
                let mut payload = serde_json::json!({ "task": task });
                if let Some(max_steps) = input.get("max_steps").and_then(|value| value.as_u64()) {
                    payload["max_steps"] = serde_json::json!(max_steps);
                }
                args = payload;
                summary = "computer_use task".to_string();
            }
            "transcribe" => {
                let audio_path = input["audio_path"]
                    .as_str()
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if audio_path.is_empty() {
                    issue.get_or_insert_with(|| "audio_path required".to_string());
                } else if !Path::new(&audio_path).exists() {
                    issue.get_or_insert_with(|| format!("audio file not found: {}", audio_path));
                }
                args = serde_json::json!({ "audio_path": audio_path });
                summary = "audio transcription".to_string();
            }
            "browser" => {
                // Pass the caller's input through the shared validator so the
                // arguments object we attach to the dispatch (and the
                // preflight record) matches exactly what the Python tool
                // will receive on stdin.
                match tools::prepare_browser_dispatch(&input) {
                    Ok(dispatch) => {
                        let mut payload = serde_json::Map::new();
                        payload.insert(
                            "command".to_string(),
                            serde_json::Value::String(dispatch.command.clone()),
                        );
                        if !dispatch.url.is_empty() {
                            payload.insert(
                                "url".to_string(),
                                serde_json::Value::String(dispatch.url.clone()),
                            );
                        }
                        if let Some(out_path) = dispatch.out_path.clone() {
                            payload.insert(
                                "out_path".to_string(),
                                serde_json::Value::String(out_path),
                            );
                        }
                        if let Some(steps) = dispatch.steps.clone() {
                            payload.insert("steps".to_string(), serde_json::Value::Array(steps));
                        }
                        if let Some(session_id) = dispatch.session_id.clone() {
                            payload.insert(
                                "session_id".to_string(),
                                serde_json::Value::String(session_id),
                            );
                        }
                        if let Some(jar) = dispatch.jar.clone() {
                            payload.insert("jar".to_string(), serde_json::Value::String(jar));
                        }

                        let summary_target = if !dispatch.url.is_empty() {
                            dispatch.url.clone()
                        } else if let Some(session_id) = dispatch.session_id.as_deref() {
                            format!("session {}", session_id)
                        } else {
                            String::new()
                        };
                        summary = if summary_target.is_empty() {
                            format!("browser {}", dispatch.command)
                        } else {
                            format!("browser {} {}", dispatch.command, summary_target)
                        };
                        args = serde_json::Value::Object(payload);
                    }
                    Err(error) => {
                        issue.get_or_insert_with(|| error.to_string());
                        args = input.clone();
                        summary = "browser invocation (invalid)".to_string();
                    }
                }
            }
            _ => {}
        }

        let ready = issue.is_none();
        let preflight = ToolDispatchPreflight {
            requested_name: name.to_string(),
            resolved_name: status.name.clone(),
            kind: status.kind.clone(),
            ready,
            summary,
            issue,
            arguments: args.clone(),
            requires_network: status.requires_network,
            sandboxed: status.sandboxed,
            target: status.filename.clone(),
            command: status.command.clone(),
            source: status.source.clone(),
            server_name: status.server_name.clone(),
            quarantined_until: status.quarantined_until.clone(),
            policy_notes: Vec::new(),
            policy_triggers: Vec::new(),
            verification_mode: None,
        };
        let preflight = apply_live_policy_guards_to_preflight(
            self.db.as_ref(),
            None,
            Some(name),
            &args,
            preflight,
            "chat_tool_dispatch",
        );

        if preflight.ready {
            PreparedToolCall::Builtin {
                tool_name: status.name,
                args,
                preflight,
            }
        } else {
            PreparedToolCall::Blocked(preflight)
        }
    }

    async fn prepare_external_tool_action(&self, input: serde_json::Value) -> PreparedToolCall {
        let tool_name = input["tool_name"].as_str().unwrap_or("").trim().to_string();
        if tool_name.is_empty() {
            return PreparedToolCall::Blocked(blocked_preflight(
                "external_tool",
                "external",
                "tool_name required".to_string(),
                serde_json::json!({}),
            ));
        }
        let args = input
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        if let Some((server_name, resolved_name)) = tool_name.split_once(':') {
            let tool_status = self
                .mcp_hub
                .tool_statuses()
                .await
                .into_iter()
                .find(|status| {
                    status.server_name.as_deref() == Some(server_name)
                        && status.name == resolved_name
                });
            if let Some(status) = tool_status {
                let preflight = ToolDispatchPreflight {
                    requested_name: "external_tool".to_string(),
                    resolved_name: resolved_name.to_string(),
                    kind: status.kind,
                    ready: status.ready,
                    summary: format!("mcp tool {}:{}", server_name, resolved_name),
                    issue: status.issue,
                    arguments: args.clone(),
                    requires_network: status.requires_network,
                    sandboxed: status.sandboxed,
                    target: Some(format!("{}:{}", server_name, resolved_name)),
                    command: status.command,
                    source: status.source,
                    server_name: Some(server_name.to_string()),
                    quarantined_until: status.quarantined_until,
                    policy_notes: Vec::new(),
                    policy_triggers: Vec::new(),
                    verification_mode: None,
                };
                let preflight = apply_live_policy_guards_to_preflight(
                    self.db.as_ref(),
                    None,
                    Some(resolved_name),
                    &args,
                    preflight,
                    "chat_tool_dispatch",
                );
                if preflight.ready {
                    return PreparedToolCall::Mcp {
                        server_name: server_name.to_string(),
                        tool_name: resolved_name.to_string(),
                        args,
                        preflight,
                    };
                }
                return PreparedToolCall::Blocked(preflight);
            }
            return PreparedToolCall::Blocked(blocked_preflight(
                "external_tool",
                "mcp",
                format!("MCP tool {} is not connected", tool_name),
                args,
            ));
        }

        if let Ok(statuses) = forge::list_registered_tool_runtime_statuses(self.db.as_ref()) {
            if let Some(status) = statuses.into_iter().find(|status| status.name == tool_name) {
                let preflight = ToolDispatchPreflight {
                    requested_name: "external_tool".to_string(),
                    resolved_name: status.name.clone(),
                    kind: status.kind.clone(),
                    ready: status.ready,
                    summary: format!("self-built tool {}", status.name),
                    issue: status.issue.clone(),
                    arguments: args.clone(),
                    requires_network: status.requires_network,
                    sandboxed: status.sandboxed,
                    target: status.filename.clone(),
                    command: status.command.clone(),
                    source: status.source.clone(),
                    server_name: None,
                    quarantined_until: status.quarantined_until.clone(),
                    policy_notes: Vec::new(),
                    policy_triggers: Vec::new(),
                    verification_mode: None,
                };
                let preflight = apply_live_policy_guards_to_preflight(
                    self.db.as_ref(),
                    None,
                    Some(status.name.as_str()),
                    &args,
                    preflight,
                    "chat_tool_dispatch",
                );
                if preflight.ready {
                    return PreparedToolCall::SelfBuilt {
                        tool_name: status.name,
                        args,
                        preflight,
                    };
                }
                return PreparedToolCall::Blocked(preflight);
            }
        }

        if let Some(status) = self
            .plugins
            .tool_statuses()
            .into_iter()
            .find(|status| status.name == tool_name)
        {
            let preflight = ToolDispatchPreflight {
                requested_name: "external_tool".to_string(),
                resolved_name: status.name.clone(),
                kind: status.kind.clone(),
                ready: status.ready,
                summary: format!("plugin tool {}", status.name),
                issue: status.issue.clone(),
                arguments: args.clone(),
                requires_network: status.requires_network,
                sandboxed: status.sandboxed,
                target: None,
                command: status.command.clone(),
                source: status.source.clone(),
                server_name: None,
                quarantined_until: status.quarantined_until.clone(),
                policy_notes: Vec::new(),
                policy_triggers: Vec::new(),
                verification_mode: None,
            };
            let preflight = apply_live_policy_guards_to_preflight(
                self.db.as_ref(),
                None,
                Some(status.name.as_str()),
                &args,
                preflight,
                "chat_tool_dispatch",
            );
            if preflight.ready {
                return PreparedToolCall::Plugin {
                    tool_name: status.name,
                    args,
                    preflight,
                };
            }
            return PreparedToolCall::Blocked(preflight);
        }

        PreparedToolCall::Blocked(blocked_preflight(
            "external_tool",
            "external",
            format!(
                "external tool {} is not available in self-built tools, plugins, or MCP",
                tool_name
            ),
            args,
        ))
    }

    fn record_tool_dispatch_execution(&self, result: &ToolDispatchResult, latency_ms: i64) {
        let outcome = if result.dry_run {
            "dry_run"
        } else if !result.preflight.ready {
            "blocked"
        } else if result.success {
            "completed"
        } else {
            "failed"
        };
        let target = result
            .preflight
            .target
            .clone()
            .or_else(|| Some(result.preflight.resolved_name.clone()));
        let payload = serde_json::json!({
            "schema_version": crate::runtime::CHAT_EXECUTION_TRACE_SCHEMA_VERSION,
            "surface": "tool",
            "kind": "tool_dispatch",
            "summary": result.preflight.summary,
            "outcome": outcome,
            "success": result.success,
            "trace": {
                "source": "runtime",
                "target": target,
                "requested_name": result.preflight.requested_name,
                "resolved_name": result.preflight.resolved_name,
                "tool_kind": result.preflight.kind,
            },
            "details": {
                "dry_run": result.dry_run,
                "preflight": result.preflight,
                "output": result.output,
                "raw_result": result.raw_result,
                "error": result.error,
            }
        });
        let entry = crate::db::ExecutionLedgerWrite {
            surface: "tool".to_string(),
            kind: "tool_dispatch".to_string(),
            source: "runtime".to_string(),
            target: payload
                .pointer("/trace/target")
                .and_then(|value| value.as_str())
                .map(str::to_string),
            summary: result.preflight.summary.clone(),
            outcome: outcome.to_string(),
            success: result.success,
            correlation_id: Some(format!("tool:{}", result.preflight.resolved_name)),
            reference_kind: None,
            reference_id: None,
            channel: None,
            provider: None,
            model: None,
            route: Some(result.preflight.kind.clone()),
            latency_ms: Some(latency_ms),
            payload,
        };
        if let Err(error) = self.db.record_execution_ledger_entry(&entry) {
            tracing::warn!(
                "execution ledger: failed to record tool dispatch: {}",
                error
            );
        }
    }
}

fn blocked_preflight(
    requested_name: &str,
    kind: &str,
    issue: String,
    arguments: serde_json::Value,
) -> ToolDispatchPreflight {
    ToolDispatchPreflight {
        requested_name: requested_name.to_string(),
        resolved_name: requested_name.to_string(),
        kind: kind.to_string(),
        ready: false,
        summary: issue.clone(),
        issue: Some(issue),
        arguments,
        requires_network: false,
        sandboxed: false,
        target: None,
        command: None,
        source: None,
        server_name: None,
        quarantined_until: None,
        policy_notes: Vec::new(),
        policy_triggers: Vec::new(),
        verification_mode: None,
    }
}

fn record_routing_experiment_selected(
    db: &crate::db::Db,
    experiment: Option<&llm::RoutingExperimentPlan>,
    surface: &str,
) {
    let Some(experiment) = experiment else {
        return;
    };
    db.record_policy_runtime_event_by_trigger(
        &experiment.trigger,
        "live_experiment_selected",
        &format!(
            "Selected guarded {} routing experiment to prefer {} after replay evidence against {}.",
            surface, experiment.fallback_provider, experiment.matched_provider
        ),
        &serde_json::json!({
            "surface": surface,
            "matched_provider": experiment.matched_provider,
            "matched_route": experiment.matched_route,
            "fallback_provider": experiment.fallback_provider,
        }),
    )
    .ok();
}

fn record_routing_experiment_applied(
    db: &crate::db::Db,
    experiment: Option<&llm::RoutingExperimentPlan>,
    surface: &str,
    trace: &llm::LlmResponseTrace,
) {
    let Some(experiment) = experiment else {
        return;
    };
    db.record_policy_runtime_event_by_trigger(
        &experiment.trigger,
        "live_experiment_applied",
        &format!(
            "Applied guarded {} routing experiment and used {} via {}.",
            surface, trace.provider, trace.route
        ),
        &serde_json::json!({
            "surface": surface,
            "provider": trace.provider,
            "model": trace.model,
            "route": trace.route,
            "latency_ms": trace.latency_ms,
            "matched_provider": experiment.matched_provider,
            "matched_route": experiment.matched_route,
            "fallback_provider": experiment.fallback_provider,
        }),
    )
    .ok();
}

fn approved_live_policy_directives(
    db: &crate::db::Db,
    limit: usize,
) -> Vec<crate::improvement::policy::ApprovedPolicyDirective> {
    db.list_approved_policy_candidates(limit)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|candidate| crate::improvement::policy::directive_from_candidate(&candidate))
        .filter(|directive| crate::improvement::policy::is_safe_live_policy_kind(&directive.kind))
        .collect()
}

fn approved_policy_prompt_fragment(
    directives: &[crate::improvement::policy::ApprovedPolicyDirective],
) -> Option<String> {
    if directives.is_empty() {
        return None;
    }

    let lines = directives
        .iter()
        .take(6)
        .map(|directive| format!("- {}", directive.description))
        .collect::<Vec<_>>()
        .join("\n");
    Some(format!(
        "<approved_policies>\nUse these guarded runtime rules learned from replay when planning tool use or success claims:\n{}\n</approved_policies>\n\n",
        lines
    ))
}

fn apply_live_policy_guards_to_preflight(
    db: &crate::db::Db,
    task_kind: Option<&str>,
    tool_name: Option<&str>,
    args: &serde_json::Value,
    mut preflight: ToolDispatchPreflight,
    surface: &str,
) -> ToolDispatchPreflight {
    let Some(tool_name) = tool_name.filter(|value| !value.trim().is_empty()) else {
        return preflight;
    };
    let directives = approved_live_policy_directives(db, 16);
    for directive in directives.into_iter().filter(|directive| {
        crate::improvement::policy::directive_applies_to_task_tool(
            directive,
            task_kind,
            Some(tool_name),
        )
    }) {
        preflight.policy_notes.push(directive.description.clone());
        preflight.policy_triggers.push(directive.trigger.clone());
        if directive.kind == "verification_rule" && preflight.verification_mode.is_none() {
            preflight.verification_mode = Some(directive.rule.clone());
            db.record_policy_runtime_event_by_trigger(
                &directive.trigger,
                "live_verification_mode_selected",
                &format!(
                    "Applied verification mode {} for {} on {}.",
                    directive.rule, surface, tool_name
                ),
                &serde_json::json!({
                    "surface": surface,
                    "tool_name": tool_name,
                    "task_kind": task_kind,
                    "verification_mode": directive.rule,
                    "arguments": args,
                }),
            )
            .ok();
        }
        if !preflight.ready {
            continue;
        }
        if directive.rule == "preflight_path_exists" {
            let path = args
                .get("path")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty());
            if let Some(path) = path {
                if !Path::new(path).exists() {
                    let issue = format!(
                        "approved policy blocked {} because file not found: {}",
                        tool_name, path
                    );
                    preflight.ready = false;
                    preflight.summary = issue.clone();
                    preflight.issue = Some(issue.clone());
                    db.record_policy_runtime_event_by_trigger(
                        &directive.trigger,
                        "live_guard_blocked",
                        &issue,
                        &serde_json::json!({
                            "surface": surface,
                            "tool_name": tool_name,
                            "task_kind": task_kind,
                            "path": path,
                            "arguments": args,
                        }),
                    )
                    .ok();
                }
            }
        }
    }

    preflight
}

fn tool_dispatch_result_from_json(
    preflight: ToolDispatchPreflight,
    dry_run: bool,
    result: AnyhowResult<serde_json::Value>,
) -> ToolDispatchResult {
    match result {
        Ok(value) => {
            let success = value["success"]
                .as_bool()
                .unwrap_or(value.get("error").is_none());
            let output = extract_tool_output(&value);
            let error = if success {
                None
            } else {
                extract_tool_error(&value).or_else(|| output.clone())
            };
            ToolDispatchResult {
                tool_name: preflight.requested_name.clone(),
                dry_run,
                success,
                executed: true,
                verified: None,
                telemetry: tool_dispatch_telemetry(true, None),
                output: if success { output } else { None },
                raw_result: Some(value),
                error,
                preflight,
            }
        }
        Err(error) => ToolDispatchResult {
            tool_name: preflight.requested_name.clone(),
            dry_run,
            success: false,
            executed: true,
            verified: None,
            telemetry: tool_dispatch_telemetry(true, None),
            output: None,
            raw_result: None,
            error: Some(error.to_string()),
            preflight,
        },
    }
}

fn extract_tool_output(value: &serde_json::Value) -> Option<String> {
    value["output"]
        .as_str()
        .filter(|output| !output.is_empty())
        .map(str::to_string)
        .or_else(|| {
            if value.is_object() {
                None
            } else {
                Some(value.to_string())
            }
        })
}

fn extract_tool_error(value: &serde_json::Value) -> Option<String> {
    value["error"]
        .as_str()
        .filter(|error| !error.is_empty())
        .map(str::to_string)
}

fn pack_tool_dispatch_result(result: ToolDispatchResult) -> String {
    if let Some(output) = result.output {
        return output;
    }
    if let Some(error) = result.error {
        return format!("error: {}", error);
    }
    result
        .raw_result
        .map(|value| value.to_string())
        .unwrap_or_else(|| "error: tool produced no output".to_string())
}
