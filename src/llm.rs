//! LLM provider chain with priority routing.
//!
//! Two paths:
//! - chat()      → order set by [`ChatPrimary`] (`NYX_CHAT_PRIMARY`): Anthropic, NIM (e.g. Gemma on `integrate.api.nvidia.com`), or Ollama first
//! - chat_auto() → Ollama → NIM (autonomous work, free/cheap, volume matters). Never Anthropic.
//!
//! User chat tool loop: Anthropic native tools, or NIM OpenAI-style `tools` / `tool_calls`, depending on [`ChatPrimary`] and which API keys are set.
//!
//! All model names read from .env — swap models without recompiling.

use anyhow::{Context, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

fn extract_anthropic_assistant_text(content: &serde_json::Value) -> String {
    let Some(arr) = content.as_array() else {
        return String::new();
    };
    let mut parts: Vec<&str> = Vec::new();
    for b in arr {
        if b.get("type").and_then(|t| t.as_str()) == Some("text") {
            if let Some(t) = b.get("text").and_then(|x| x.as_str()) {
                parts.push(t);
            }
        }
    }
    parts.join("\n").trim().to_string()
}

/// Which provider handles user-facing `chat()` and the tool loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ChatPrimary {
    /// Claude (Anthropic Messages API + native tools).
    #[default]
    Anthropic,
    /// NVIDIA NIM OpenAI-compatible chat + tools (e.g. `google/gemma-4-31b-it`).
    Nim,
    /// Local Ollama only — no hosted tool loop (flat prompts).
    Ollama,
}

impl ChatPrimary {
    /// `NYX_CHAT_PRIMARY`: `anthropic` | `claude` | `haiku`, `nim` | `nvidia` | `gemma`, `ollama` | `local`, or empty for auto.
    pub fn from_env(raw: &str, has_anthropic_key: bool, has_nim_key: bool) -> Self {
        match raw.trim().to_lowercase().as_str() {
            "nim" | "nvidia" | "gemma" => ChatPrimary::Nim,
            "ollama" | "local" => ChatPrimary::Ollama,
            "anthropic" | "claude" | "haiku" => ChatPrimary::Anthropic,
            "" => {
                if has_anthropic_key {
                    ChatPrimary::Anthropic
                } else if has_nim_key {
                    ChatPrimary::Nim
                } else {
                    ChatPrimary::Ollama
                }
            }
            other => {
                tracing::warn!("NYX_CHAT_PRIMARY={:?} unknown; using auto", other);
                if has_anthropic_key {
                    ChatPrimary::Anthropic
                } else if has_nim_key {
                    ChatPrimary::Nim
                } else {
                    ChatPrimary::Ollama
                }
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct LlmResponseTrace {
    pub provider: String,
    pub model: String,
    pub route: String,
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct LlmTextResponse {
    pub text: String,
    pub trace: LlmResponseTrace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingSurface {
    Chat,
    ToolLoop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingExperimentPlan {
    pub trigger: String,
    pub surface: RoutingSurface,
    pub matched_provider: String,
    pub matched_route: String,
    pub fallback_provider: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LlmProviderChoice {
    Anthropic,
    Nim,
    Ollama,
}

#[derive(Debug, Clone)]
struct LlmAttempt {
    provider: LlmProviderChoice,
    route: String,
}

impl LlmProviderChoice {
    fn label(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::Nim => "nim",
            Self::Ollama => "ollama",
        }
    }
}

pub struct LlmGate {
    anthropic_key: String,
    anthropic_model: String,
    nim_key: String,
    nim_model: String,
    nim_base_url: String,
    ollama_host: String,
    ollama_model: String,
    chat_primary: ChatPrimary,
    client: reqwest::Client,
    input_tokens: AtomicU64,
    output_tokens: AtomicU64,
}

impl LlmGate {
    pub fn new(config: &crate::Config) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();

        let chat_primary = config.chat_primary;
        if !config.anthropic_key.is_empty() {
            tracing::info!("LLM: Anthropic ({}) — available", config.anthropic_model);
        }
        if !config.nim_key.is_empty() {
            tracing::info!("LLM: NIM ({}) — available", config.nim_model);
        }
        tracing::info!(
            "LLM: user chat primary = {:?} | Ollama ({}) — autonomous",
            chat_primary,
            config.ollama_model
        );

        Self {
            anthropic_key: config.anthropic_key.clone(),
            anthropic_model: config.anthropic_model.clone(),
            nim_key: config.nim_key.clone(),
            nim_model: config.nim_model.clone(),
            nim_base_url: config.nim_base_url.clone(),
            ollama_host: config.ollama_host.clone(),
            ollama_model: config.ollama_model.clone(),
            chat_primary,
            client,
            input_tokens: AtomicU64::new(0),
            output_tokens: AtomicU64::new(0),
        }
    }

    pub fn chat_primary(&self) -> ChatPrimary {
        self.chat_primary
    }

    pub fn has_anthropic(&self) -> bool {
        !self.anthropic_key.is_empty()
    }

    pub fn has_nim(&self) -> bool {
        !self.nim_key.is_empty()
    }

    pub fn has_ollama(&self) -> bool {
        !self.ollama_host.is_empty()
    }

    fn provider_model_label(provider: &str, model: &str) -> String {
        format!("{}:{}", provider, model)
    }

    fn traced_response(
        &self,
        provider: &str,
        model: &str,
        route: &str,
        latency_ms: Option<u64>,
        text: String,
    ) -> LlmTextResponse {
        LlmTextResponse {
            text,
            trace: LlmResponseTrace {
                provider: provider.to_string(),
                model: model.to_string(),
                route: route.to_string(),
                latency_ms,
            },
        }
    }

    pub fn preferred_chat_model_label(&self) -> String {
        match self.chat_primary {
            ChatPrimary::Nim => {
                if self.has_nim() {
                    Self::provider_model_label("nim", &self.nim_model)
                } else if self.has_ollama() {
                    Self::provider_model_label("ollama", &self.ollama_model)
                } else if self.has_anthropic() {
                    Self::provider_model_label("anthropic", &self.anthropic_model)
                } else {
                    "unavailable".to_string()
                }
            }
            ChatPrimary::Ollama => {
                if self.has_ollama() {
                    Self::provider_model_label("ollama", &self.ollama_model)
                } else if self.has_nim() {
                    Self::provider_model_label("nim", &self.nim_model)
                } else if self.has_anthropic() {
                    Self::provider_model_label("anthropic", &self.anthropic_model)
                } else {
                    "unavailable".to_string()
                }
            }
            ChatPrimary::Anthropic => {
                if self.has_anthropic() {
                    Self::provider_model_label("anthropic", &self.anthropic_model)
                } else if self.has_nim() {
                    Self::provider_model_label("nim", &self.nim_model)
                } else if self.has_ollama() {
                    Self::provider_model_label("ollama", &self.ollama_model)
                } else {
                    "unavailable".to_string()
                }
            }
        }
    }

    pub fn preferred_autonomous_model_label(&self) -> String {
        if self.has_ollama() {
            Self::provider_model_label("ollama", &self.ollama_model)
        } else if self.has_nim() {
            Self::provider_model_label("nim", &self.nim_model)
        } else if self.has_anthropic() {
            Self::provider_model_label("anthropic", &self.anthropic_model)
        } else {
            "unavailable".to_string()
        }
    }

    fn nim_chat_completions_url(&self) -> String {
        format!(
            "{}/v1/chat/completions",
            self.nim_base_url.trim_end_matches('/')
        )
    }

    /// User-facing chat — order depends on [`ChatPrimary`].
    pub async fn chat(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        self.chat_traced(prompt, max_tokens)
            .await
            .map(|response| response.text)
    }

    pub async fn chat_traced(&self, prompt: &str, max_tokens: u32) -> Result<LlmTextResponse> {
        self.chat_traced_with_routing_experiment(prompt, max_tokens, None)
            .await
    }

    pub async fn chat_traced_with_routing_experiment(
        &self,
        prompt: &str,
        max_tokens: u32,
        routing_experiment: Option<&RoutingExperimentPlan>,
    ) -> Result<LlmTextResponse> {
        let attempts = self.chat_attempts(routing_experiment);
        let mut last_error = None;
        for attempt in attempts {
            match self.call_chat_attempt(&attempt, prompt, max_tokens).await {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error),
            }
        }

        if let Some(error) = last_error {
            return Err(error);
        }

        match self.chat_primary {
            ChatPrimary::Nim => anyhow::bail!("no LLM available for nim-primary chat"),
            _ => anyhow::bail!("no LLM available"),
        }
    }

    /// Autonomous work — free/cheap models only. Ollama → NIM. Never Anthropic.
    pub async fn chat_auto(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        self.chat_auto_traced(prompt, max_tokens)
            .await
            .map(|response| response.text)
    }

    pub async fn chat_auto_traced(&self, prompt: &str, max_tokens: u32) -> Result<LlmTextResponse> {
        let started_at = Instant::now();
        if let Ok(r) = self.call_ollama(prompt, max_tokens).await {
            return Ok(self.traced_response(
                "ollama",
                &self.ollama_model,
                "chat_auto",
                Some(started_at.elapsed().as_millis() as u64),
                r,
            ));
        }
        if !self.nim_key.is_empty() {
            let started_at = Instant::now();
            if let Ok(r) = self.call_nim(prompt, max_tokens).await {
                return Ok(self.traced_response(
                    "nim",
                    &self.nim_model,
                    "chat_auto",
                    Some(started_at.elapsed().as_millis() as u64),
                    r,
                ));
            }
        }
        anyhow::bail!("no autonomous LLM available")
    }

    pub async fn chat_auto_with_fallback(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        self.chat_auto_with_fallback_traced(prompt, max_tokens)
            .await
            .map(|response| response.text)
    }

    pub async fn chat_auto_with_fallback_traced(
        &self,
        prompt: &str,
        max_tokens: u32,
    ) -> Result<LlmTextResponse> {
        match self.chat_auto_traced(prompt, max_tokens).await {
            Ok(response) => Ok(response),
            Err(auto_error) => self
                .chat_traced(prompt, max_tokens)
                .await
                .map(|mut response| {
                    response.trace.route = "chat_auto_fallback".to_string();
                    response
                })
                .with_context(|| {
                    format!(
                        "autonomous lane unavailable ({}) and primary fallback {} also failed",
                        auto_error,
                        self.preferred_chat_model_label()
                    )
                }),
        }
    }

    pub fn usage(&self) -> (u64, u64) {
        (
            self.input_tokens.load(Ordering::Relaxed),
            self.output_tokens.load(Ordering::Relaxed),
        )
    }

    pub fn select_routing_experiment(
        &self,
        directives: &[crate::improvement::policy::ApprovedPolicyDirective],
        surface: RoutingSurface,
    ) -> Option<RoutingExperimentPlan> {
        let attempts = match surface {
            RoutingSurface::Chat => self.chat_attempts(None),
            RoutingSurface::ToolLoop => self.tool_loop_attempts(None),
        };
        let primary = attempts.first()?;
        let fallback = attempts.get(1)?;
        let primary_model = self.model_for_provider(primary.provider);

        directives.iter().find_map(|directive| {
            if !crate::improvement::policy::directive_applies_to_llm_route(
                directive,
                primary.provider.label(),
                &primary.route,
                Some(primary_model),
            ) {
                return None;
            }

            Some(RoutingExperimentPlan {
                trigger: directive.trigger.clone(),
                surface,
                matched_provider: primary.provider.label().to_string(),
                matched_route: primary.route.clone(),
                fallback_provider: fallback.provider.label().to_string(),
            })
        })
    }

    /// True when the handler can run a hosted tool loop (NIM OpenAI tools or Anthropic tools).
    pub fn user_chat_tool_loop_ready(&self) -> bool {
        match self.chat_primary {
            ChatPrimary::Ollama => false,
            ChatPrimary::Nim => !self.nim_key.is_empty() || !self.anthropic_key.is_empty(),
            ChatPrimary::Anthropic => !self.anthropic_key.is_empty() || !self.nim_key.is_empty(),
        }
    }

    /// OpenAI-style `tools` array for NIM `/v1/chat/completions` (built from [`Self::chat_tool_definitions`]).
    pub fn chat_tools_openai_functions() -> Vec<serde_json::Value> {
        Self::chat_tool_definitions()
            .into_iter()
            .map(|t| {
                serde_json::json!({
                    "type": "function",
                    "function": {
                        "name": t["name"],
                        "description": t["description"],
                        "parameters": t["input_schema"]
                    }
                })
            })
            .collect()
    }

    /// Tool loop for user chat: NIM (OpenAI tools) or Anthropic depending on [`ChatPrimary`] and keys.
    pub async fn chat_primary_with_tools<F, Fut>(
        &self,
        system: &str,
        conversation: &[(String, String)],
        user_turn: &str,
        max_tool_rounds: u32,
        max_tokens: u32,
        run_tool: &mut F,
    ) -> Result<String>
    where
        F: FnMut(String, serde_json::Value) -> Fut,
        Fut: std::future::Future<Output = String> + Send,
    {
        self.chat_primary_with_tools_traced(
            system,
            conversation,
            user_turn,
            max_tool_rounds,
            max_tokens,
            None,
            run_tool,
        )
        .await
        .map(|response| response.text)
    }

    pub async fn chat_primary_with_tools_traced<F, Fut>(
        &self,
        system: &str,
        conversation: &[(String, String)],
        user_turn: &str,
        max_tool_rounds: u32,
        max_tokens: u32,
        routing_experiment: Option<&RoutingExperimentPlan>,
        run_tool: &mut F,
    ) -> Result<LlmTextResponse>
    where
        F: FnMut(String, serde_json::Value) -> Fut,
        Fut: std::future::Future<Output = String> + Send,
    {
        let attempts = self.tool_loop_attempts(routing_experiment);
        let mut last_error = None;
        for attempt in attempts {
            match self
                .call_tool_loop_attempt(
                    &attempt,
                    system,
                    conversation,
                    user_turn,
                    max_tool_rounds,
                    max_tokens,
                    run_tool,
                )
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error),
            }
        }

        if let Some(error) = last_error {
            return Err(error);
        }

        anyhow::bail!(
            "no provider keys for tool loop (set NYX_NIM_API_KEY and/or NYX_ANTHROPIC_API_KEY)"
        )
    }

    /// NIM OpenAI-compatible chat completions with `tools` / `tool_calls` rounds.
    pub async fn chat_nim_openai_tools<F, Fut>(
        &self,
        system: &str,
        conversation: &[(String, String)],
        user_turn: &str,
        max_tool_rounds: u32,
        max_tokens: u32,
        run_tool: &mut F,
    ) -> Result<String>
    where
        F: FnMut(String, serde_json::Value) -> Fut,
        Fut: std::future::Future<Output = String> + Send,
    {
        if self.nim_key.is_empty() {
            anyhow::bail!("nim not configured");
        }

        let tools = Self::chat_tools_openai_functions();
        let mut messages: Vec<serde_json::Value> = Vec::new();
        messages.push(serde_json::json!({
            "role": "system",
            "content": system
        }));

        for (role, content) in conversation {
            let r = role.as_str();
            if r != "user" && r != "assistant" {
                continue;
            }
            if content.is_empty() {
                continue;
            }
            messages.push(serde_json::json!({
                "role": r,
                "content": content
            }));
        }
        messages.push(serde_json::json!({
            "role": "user",
            "content": user_turn
        }));

        let max_calls = max_tool_rounds.clamp(1, 24);
        let cap_tokens = max_tokens.clamp(256, 8192);
        let mut api_calls = 0u32;
        let mut last_text = String::new();

        loop {
            api_calls += 1;
            if api_calls > max_calls {
                tracing::warn!("nim tool loop: API call cap {}", max_calls);
                if !last_text.trim().is_empty() {
                    return Ok(last_text);
                }
                anyhow::bail!("nim tool loop: exceeded API call cap");
            }

            let body = serde_json::json!({
                "model": self.nim_model,
                "max_tokens": cap_tokens,
                "messages": messages,
                "tools": tools,
                "tool_choice": "auto",
            });

            let resp = self
                .client
                .post(self.nim_chat_completions_url())
                .bearer_auth(&self.nim_key)
                .header("content-type", "application/json")
                .timeout(std::time::Duration::from_secs(45))
                .json(&body)
                .send()
                .await?;

            let status = resp.status();
            let data: serde_json::Value = resp.json().await?;
            if !status.is_success() {
                let err = data["error"]["message"]
                    .as_str()
                    .or(data["error"]["detail"].as_str())
                    .unwrap_or("request failed");
                anyhow::bail!("NIM {}: {}", status, err);
            }

            if let Some(u) = data.get("usage") {
                self.input_tokens
                    .fetch_add(u["prompt_tokens"].as_u64().unwrap_or(0), Ordering::Relaxed);
                self.output_tokens.fetch_add(
                    u["completion_tokens"].as_u64().unwrap_or(0),
                    Ordering::Relaxed,
                );
            }

            let choice = data
                .get("choices")
                .and_then(|c| c.as_array())
                .and_then(|a| a.first())
                .ok_or_else(|| anyhow::anyhow!("nim: missing choices"))?;
            let msg = &choice["message"];
            let finish = choice["finish_reason"].as_str().unwrap_or("");

            last_text = msg["content"].as_str().unwrap_or("").to_string();

            if let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                if !tool_calls.is_empty() {
                    messages.push(msg.clone());
                    for tc in tool_calls {
                        let id = tc["id"].as_str().unwrap_or("").to_string();
                        let name = tc["function"]["name"].as_str().unwrap_or("").to_string();
                        let args_raw = tc["function"]["arguments"].as_str().unwrap_or("{}");
                        let input: serde_json::Value =
                            serde_json::from_str(args_raw).unwrap_or(serde_json::json!({}));
                        if id.is_empty() || name.is_empty() {
                            continue;
                        }
                        let out = run_tool(name, input).await;
                        messages.push(serde_json::json!({
                            "role": "tool",
                            "tool_call_id": id,
                            "content": out
                        }));
                    }
                    continue;
                }
            }

            if finish == "tool_calls" {
                tracing::warn!("nim: finish_reason=tool_calls but no tool_calls in message");
            }

            return Ok(last_text);
        }
    }

    /// Tool definitions for Anthropic Messages API (`tools` field).
    pub fn chat_tool_definitions() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({
                "name": "weather",
                "description": "Current weather for a city.",
                "input_schema": {
                    "type": "object",
                    "properties": { "city": { "type": "string", "description": "City name" } },
                    "required": ["city"]
                }
            }),
            serde_json::json!({
                "name": "web_search",
                "description": "Search the web via DuckDuckGo for recent facts, news, or unknown entities.",
                "input_schema": {
                    "type": "object",
                    "properties": { "query": { "type": "string", "description": "Search query" } },
                    "required": ["query"]
                }
            }),
            serde_json::json!({
                "name": "git_info",
                "description": "Git repository info: status, recent log, TODOs in code, or diff.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["status", "log", "todos", "diff"],
                            "description": "Which git view to return"
                        }
                    },
                    "required": ["action"]
                }
            }),
            serde_json::json!({
                "name": "github",
                "description": "GitHub via gh CLI: unread notifications, open PRs, or open issues. Repo (owner/name) is required for prs/issues.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["notifications", "prs", "issues"],
                            "description": "What to fetch"
                        },
                        "repo": {
                            "type": "string",
                            "description": "Repository like owner/name — required for prs and issues"
                        }
                    },
                    "required": ["action"]
                }
            }),
            serde_json::json!({
                "name": "vision",
                "description": "Capture the user's screen and describe it with a vision model.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "prompt": {
                            "type": "string",
                            "description": "What to look for on screen (e.g. summarize visible code, list open apps)"
                        }
                    },
                    "required": ["prompt"]
                }
            }),
            serde_json::json!({
                "name": "gif",
                "description": "Search Tenor for a GIF; returns a URL or message suitable to share.",
                "input_schema": {
                    "type": "object",
                    "properties": { "query": { "type": "string", "description": "GIF search query" } },
                    "required": ["query"]
                }
            }),
            serde_json::json!({
                "name": "file_ops",
                "description": "Read a project file (read-only). Paths must stay under allowed project dirs.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": "Relative path from project root" }
                    },
                    "required": ["path"]
                }
            }),
            serde_json::json!({
                "name": "twitter",
                "description": "Twitter/X: read home timeline, search tweets, post, reply, or like.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["timeline", "search", "post", "reply", "like"],
                            "description": "timeline=home feed; search=query tweets; post=new tweet; reply=needs tweet_id+text; like=needs tweet_id"
                        },
                        "count": { "type": "integer", "description": "Max items for timeline/search" },
                        "query": { "type": "string", "description": "Search query (action=search)" },
                        "text": { "type": "string", "description": "Tweet or reply body (post/reply)" },
                        "tweet_id": { "type": "string", "description": "Target tweet id (reply/like)" }
                    },
                    "required": ["action"]
                }
            }),
            serde_json::json!({
                "name": "image_gen",
                "description": "Generate an image with FLUX from a text prompt.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "prompt": { "type": "string", "description": "Image description" },
                        "style": {
                            "type": "string",
                            "description": "Optional: realistic, anime, cinematic, pixel, artistic"
                        }
                    },
                    "required": ["prompt"]
                }
            }),
            serde_json::json!({
                "name": "external_tool",
                "description": "Call a dynamically listed plugin or MCP tool by exact name. For MCP tools use server:tool.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "tool_name": {
                            "type": "string",
                            "description": "Exact tool name. Plugin tools use the bare name. MCP tools use server:tool."
                        },
                        "arguments": {
                            "type": "object",
                            "description": "JSON object arguments for that tool"
                        }
                    },
                    "required": ["tool_name", "arguments"]
                }
            }),
        ]
    }

    /// Anthropic Messages API with tool_use / tool_result rounds. Only uses Anthropic (no NIM fallback inside the loop).
    pub async fn chat_anthropic_with_tools<F, Fut>(
        &self,
        system: &str,
        conversation: &[(String, String)],
        user_turn: &str,
        max_tool_rounds: u32,
        max_tokens: u32,
        run_tool: &mut F,
    ) -> Result<String>
    where
        F: FnMut(String, serde_json::Value) -> Fut,
        Fut: std::future::Future<Output = String> + Send,
    {
        if self.anthropic_key.is_empty() {
            anyhow::bail!("anthropic not configured");
        }

        let tools = Self::chat_tool_definitions();
        let mut messages: Vec<serde_json::Value> = Vec::new();

        for (role, content) in conversation {
            let r = role.as_str();
            if r != "user" && r != "assistant" {
                continue;
            }
            if content.is_empty() {
                continue;
            }
            messages.push(serde_json::json!({
                "role": r,
                "content": content
            }));
        }
        messages.push(serde_json::json!({
            "role": "user",
            "content": user_turn
        }));

        let max_calls = max_tool_rounds.clamp(1, 24);
        let cap_tokens = max_tokens.clamp(256, 8192);

        let mut api_calls = 0u32;
        let mut last_content = serde_json::Value::Null;

        loop {
            api_calls += 1;
            if api_calls > max_calls {
                tracing::warn!("anthropic tool loop: API call cap {}", max_calls);
                let fallback = extract_anthropic_assistant_text(&last_content);
                if !fallback.is_empty() {
                    return Ok(fallback);
                }
                anyhow::bail!("anthropic tool loop: exceeded API call cap");
            }

            let body = serde_json::json!({
                "model": self.anthropic_model,
                "max_tokens": cap_tokens,
                "system": system,
                "tools": tools,
                "messages": messages,
            });

            let resp = self
                .client
                .post("https://api.anthropic.com/v1/messages")
                .header("x-api-key", &self.anthropic_key)
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .timeout(std::time::Duration::from_secs(45))
                .json(&body)
                .send()
                .await?;

            let status = resp.status();
            let data: serde_json::Value = resp.json().await?;
            if !status.is_success() {
                let err = data["error"]["message"]
                    .as_str()
                    .unwrap_or("request failed");
                anyhow::bail!("Anthropic {}: {}", status, err);
            }

            if let Some(usage) = data.get("usage") {
                self.input_tokens.fetch_add(
                    usage["input_tokens"].as_u64().unwrap_or(0),
                    Ordering::Relaxed,
                );
                self.output_tokens.fetch_add(
                    usage["output_tokens"].as_u64().unwrap_or(0),
                    Ordering::Relaxed,
                );
            }

            last_content = data["content"].clone();
            let assistant_content = data["content"].clone();

            messages.push(serde_json::json!({
                "role": "assistant",
                "content": assistant_content
            }));

            let tool_blocks: Vec<serde_json::Value> = data["content"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();

            if tool_blocks.is_empty() {
                return Ok(extract_anthropic_assistant_text(&data["content"]));
            }

            let mut tool_results: Vec<serde_json::Value> = Vec::new();
            for block in &tool_blocks {
                let id = block["id"].as_str().unwrap_or("").to_string();
                let name = block["name"].as_str().unwrap_or("").to_string();
                let input = block.get("input").cloned().unwrap_or(serde_json::json!({}));
                if id.is_empty() || name.is_empty() {
                    continue;
                }
                let out = run_tool(name, input).await;
                tool_results.push(serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": id,
                    "content": out
                }));
            }

            if tool_results.is_empty() {
                return Ok(extract_anthropic_assistant_text(&data["content"]));
            }

            messages.push(serde_json::json!({
                "role": "user",
                "content": tool_results
            }));
        }
    }

    async fn call_anthropic(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        let body = serde_json::json!({
            "model": self.anthropic_model,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        });

        let resp = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.anthropic_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("Anthropic {}", resp.status());
        }

        let data: serde_json::Value = resp.json().await?;
        let content = data["content"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|c| c["text"].as_str())
            .unwrap_or("")
            .to_string();

        if let Some(usage) = data.get("usage") {
            self.input_tokens.fetch_add(
                usage["input_tokens"].as_u64().unwrap_or(0),
                Ordering::Relaxed,
            );
            self.output_tokens.fetch_add(
                usage["output_tokens"].as_u64().unwrap_or(0),
                Ordering::Relaxed,
            );
        }

        Ok(content)
    }

    async fn call_nim(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        let body = serde_json::json!({
            "model": self.nim_model,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        });

        let resp = self
            .client
            .post(self.nim_chat_completions_url())
            .bearer_auth(&self.nim_key)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("NIM {}", resp.status());
        }

        let data: serde_json::Value = resp.json().await?;
        Ok(data["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .to_string())
    }

    async fn call_ollama(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        let body = serde_json::json!({
            "model": self.ollama_model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": false,
            "options": {"num_predict": max_tokens},
        });

        let url = format!("{}/api/chat", self.ollama_host);
        let mut last_error = None;

        for attempt in 0..2 {
            match self.client.post(&url).json(&body).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if !status.is_success() {
                        last_error = Some(anyhow::anyhow!("Ollama {}", status));
                    } else {
                        let data: serde_json::Value = resp.json().await?;
                        let content = data["message"]["content"]
                            .as_str()
                            .map(str::trim)
                            .unwrap_or("");
                        if !content.is_empty() {
                            return Ok(content.to_string());
                        }
                        last_error = Some(anyhow::anyhow!("Ollama returned empty content"));
                    }
                }
                Err(error) => {
                    last_error = Some(error.into());
                }
            }

            if attempt == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(350)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Ollama request failed")))
    }

    pub async fn chat_anthropic_direct(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        self.chat_anthropic_direct_traced(prompt, max_tokens)
            .await
            .map(|response| response.text)
    }

    pub async fn chat_anthropic_direct_traced(
        &self,
        prompt: &str,
        max_tokens: u32,
    ) -> Result<LlmTextResponse> {
        if self.anthropic_key.is_empty() {
            anyhow::bail!("anthropic unavailable");
        }
        let started_at = Instant::now();
        self.call_anthropic(prompt, max_tokens)
            .await
            .map(|response| {
                self.traced_response(
                    "anthropic",
                    &self.anthropic_model,
                    "direct",
                    Some(started_at.elapsed().as_millis() as u64),
                    response,
                )
            })
    }

    pub async fn chat_nim_direct(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        self.chat_nim_direct_traced(prompt, max_tokens)
            .await
            .map(|response| response.text)
    }

    pub async fn chat_nim_direct_traced(
        &self,
        prompt: &str,
        max_tokens: u32,
    ) -> Result<LlmTextResponse> {
        if self.nim_key.is_empty() {
            anyhow::bail!("nim unavailable");
        }
        let started_at = Instant::now();
        self.call_nim(prompt, max_tokens).await.map(|response| {
            self.traced_response(
                "nim",
                &self.nim_model,
                "direct",
                Some(started_at.elapsed().as_millis() as u64),
                response,
            )
        })
    }

    pub async fn chat_ollama_direct(&self, prompt: &str, max_tokens: u32) -> Result<String> {
        self.chat_ollama_direct_traced(prompt, max_tokens)
            .await
            .map(|response| response.text)
    }

    pub async fn chat_ollama_direct_traced(
        &self,
        prompt: &str,
        max_tokens: u32,
    ) -> Result<LlmTextResponse> {
        let started_at = Instant::now();
        self.call_ollama(prompt, max_tokens).await.map(|response| {
            self.traced_response(
                "ollama",
                &self.ollama_model,
                "direct",
                Some(started_at.elapsed().as_millis() as u64),
                response,
            )
        })
    }

    fn chat_attempts(&self, routing_experiment: Option<&RoutingExperimentPlan>) -> Vec<LlmAttempt> {
        let attempts = match self.chat_primary {
            ChatPrimary::Nim => {
                let mut attempts = Vec::new();
                if !self.nim_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Nim,
                        route: "chat_primary".to_string(),
                    });
                }
                if self.has_ollama() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Ollama,
                        route: "chat_fallback".to_string(),
                    });
                }
                if !self.anthropic_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Anthropic,
                        route: "chat_fallback".to_string(),
                    });
                }
                attempts
            }
            ChatPrimary::Ollama => {
                let mut attempts = Vec::new();
                if self.has_ollama() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Ollama,
                        route: "chat_primary".to_string(),
                    });
                }
                if !self.nim_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Nim,
                        route: "chat_fallback".to_string(),
                    });
                }
                if !self.anthropic_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Anthropic,
                        route: "chat_fallback".to_string(),
                    });
                }
                attempts
            }
            ChatPrimary::Anthropic => {
                let mut attempts = Vec::new();
                if !self.anthropic_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Anthropic,
                        route: "chat_primary".to_string(),
                    });
                }
                if !self.nim_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Nim,
                        route: "chat_fallback".to_string(),
                    });
                }
                if self.has_ollama() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Ollama,
                        route: "chat_fallback".to_string(),
                    });
                }
                attempts
            }
        };
        apply_routing_experiment(
            attempts,
            routing_experiment,
            "chat_policy_fallback",
            "chat_policy_recovery",
        )
    }

    fn tool_loop_attempts(
        &self,
        routing_experiment: Option<&RoutingExperimentPlan>,
    ) -> Vec<LlmAttempt> {
        let attempts = match self.chat_primary {
            ChatPrimary::Nim => {
                let mut attempts = Vec::new();
                if !self.nim_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Nim,
                        route: "tool_loop_primary".to_string(),
                    });
                }
                if !self.anthropic_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Anthropic,
                        route: "tool_loop_config_fallback".to_string(),
                    });
                }
                attempts
            }
            ChatPrimary::Anthropic => {
                let mut attempts = Vec::new();
                if !self.anthropic_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Anthropic,
                        route: "tool_loop_primary".to_string(),
                    });
                }
                if !self.nim_key.is_empty() {
                    attempts.push(LlmAttempt {
                        provider: LlmProviderChoice::Nim,
                        route: "tool_loop_config_fallback".to_string(),
                    });
                }
                attempts
            }
            ChatPrimary::Ollama => Vec::new(),
        };
        apply_routing_experiment(
            attempts,
            routing_experiment,
            "tool_loop_policy_fallback",
            "tool_loop_policy_recovery",
        )
    }

    fn model_for_provider(&self, provider: LlmProviderChoice) -> &str {
        match provider {
            LlmProviderChoice::Anthropic => &self.anthropic_model,
            LlmProviderChoice::Nim => &self.nim_model,
            LlmProviderChoice::Ollama => &self.ollama_model,
        }
    }

    async fn call_chat_attempt(
        &self,
        attempt: &LlmAttempt,
        prompt: &str,
        max_tokens: u32,
    ) -> Result<LlmTextResponse> {
        let started_at = Instant::now();
        match attempt.provider {
            LlmProviderChoice::Anthropic => {
                self.call_anthropic(prompt, max_tokens).await.map(|text| {
                    self.traced_response(
                        "anthropic",
                        &self.anthropic_model,
                        &attempt.route,
                        Some(started_at.elapsed().as_millis() as u64),
                        text,
                    )
                })
            }
            LlmProviderChoice::Nim => self.call_nim(prompt, max_tokens).await.map(|text| {
                self.traced_response(
                    "nim",
                    &self.nim_model,
                    &attempt.route,
                    Some(started_at.elapsed().as_millis() as u64),
                    text,
                )
            }),
            LlmProviderChoice::Ollama => self.call_ollama(prompt, max_tokens).await.map(|text| {
                self.traced_response(
                    "ollama",
                    &self.ollama_model,
                    &attempt.route,
                    Some(started_at.elapsed().as_millis() as u64),
                    text,
                )
            }),
        }
    }

    async fn call_tool_loop_attempt<F, Fut>(
        &self,
        attempt: &LlmAttempt,
        system: &str,
        conversation: &[(String, String)],
        user_turn: &str,
        max_tool_rounds: u32,
        max_tokens: u32,
        run_tool: &mut F,
    ) -> Result<LlmTextResponse>
    where
        F: FnMut(String, serde_json::Value) -> Fut,
        Fut: std::future::Future<Output = String> + Send,
    {
        let text = match attempt.provider {
            LlmProviderChoice::Anthropic => {
                self.chat_anthropic_with_tools(
                    system,
                    conversation,
                    user_turn,
                    max_tool_rounds,
                    max_tokens,
                    run_tool,
                )
                .await?
            }
            LlmProviderChoice::Nim => {
                self.chat_nim_openai_tools(
                    system,
                    conversation,
                    user_turn,
                    max_tool_rounds,
                    max_tokens,
                    run_tool,
                )
                .await?
            }
            LlmProviderChoice::Ollama => anyhow::bail!("ollama cannot run the hosted tool loop"),
        };

        Ok(self.traced_response(
            attempt.provider.label(),
            self.model_for_provider(attempt.provider),
            &attempt.route,
            None,
            text,
        ))
    }
}

fn apply_routing_experiment(
    mut attempts: Vec<LlmAttempt>,
    routing_experiment: Option<&RoutingExperimentPlan>,
    fallback_route: &str,
    recovery_route: &str,
) -> Vec<LlmAttempt> {
    let Some(routing_experiment) = routing_experiment else {
        return attempts;
    };
    if attempts.len() < 2 {
        return attempts;
    }
    if attempts[0].provider.label() != routing_experiment.matched_provider {
        return attempts;
    }

    let fallback_attempt = attempts.remove(1);
    let mut primary_attempt = attempts.remove(0);
    let mut reordered = vec![LlmAttempt {
        provider: fallback_attempt.provider,
        route: fallback_route.to_string(),
    }];
    primary_attempt.route = recovery_route.to_string();
    reordered.push(primary_attempt);
    reordered.extend(attempts);
    reordered
}

#[cfg(test)]
#[path = "../tests/unit/llm.rs"]
mod tests;
