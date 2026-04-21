//! MCP Server — expose Nyx tools via Model Context Protocol.
//!
//! This runs as a separate process (stdio transport). Claude Code, Cursor,
//! or any MCP client can connect and use Nyx's tools.
//!
//! Usage:
//!   ./target/release/nyx --mcp    (starts MCP server on stdio)
//!
//! Or in Claude Code's MCP config:
//!   { "command": "./target/release/nyx", "args": ["--mcp"] }

use rmcp::{
    ServerHandler,
    model::{ServerCapabilities, ServerInfo},
    schemars, tool,
};

/// MCP server that wraps Nyx's tools.
#[derive(Debug, Clone, Default)]
pub struct NyxMcpServer;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ChatRequest {
    #[schemars(description = "Message to send to Nyx")]
    pub message: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct WeatherRequest {
    #[schemars(description = "City name")]
    pub city: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SearchRequest {
    #[schemars(description = "Search query")]
    pub query: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct MemoryProvenanceRequest {
    #[schemars(description = "Topic or claim to trace back to Nyx's memory evidence")]
    pub query: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GitRequest {
    #[schemars(description = "Action: status, log, todos, diff")]
    pub action: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct RememberRequest {
    #[schemars(description = "Fact to remember")]
    pub fact: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GifRequest {
    #[schemars(description = "Search query for GIF")]
    pub query: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ImageRequest {
    #[schemars(description = "Image generation prompt")]
    pub prompt: String,
    #[schemars(description = "Style: realistic, anime, cinematic, pixel, artistic")]
    pub style: Option<String>,
}

/// Call Nyx's HTTP API from the MCP server.
async fn call_nyx(endpoint: &str, body: serde_json::Value) -> String {
    let port = std::env::var("NYX_WEB_PORT").unwrap_or_else(|_| "8099".into());
    let url = format!("http://127.0.0.1:{}{}", port, endpoint);
    let client = reqwest::Client::new();
    let api_token = std::env::var("NYX_API_TOKEN").unwrap_or_default();

    let mut request = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&body)
        .timeout(std::time::Duration::from_secs(30));
    if !api_token.is_empty() {
        request = request.bearer_auth(api_token);
    }

    match request.send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(data) => data["response"]
                .as_str()
                .or(data["output"].as_str())
                .unwrap_or("no response")
                .to_string(),
            Err(_) => "failed to parse response".to_string(),
        },
        Err(e) => format!("nyx not reachable: {}", e),
    }
}

async fn call_nyx_get_json(endpoint: &str) -> Result<serde_json::Value, String> {
    let port = std::env::var("NYX_WEB_PORT").unwrap_or_else(|_| "8099".into());
    let url = format!("http://127.0.0.1:{}{}", port, endpoint);
    let client = reqwest::Client::new();
    let api_token = std::env::var("NYX_API_TOKEN").unwrap_or_default();

    let mut request = client.get(&url).timeout(std::time::Duration::from_secs(30));
    if !api_token.is_empty() {
        request = request.bearer_auth(api_token);
    }

    match request.send().await {
        Ok(resp) => resp
            .json::<serde_json::Value>()
            .await
            .map_err(|_| "failed to parse response".to_string()),
        Err(e) => Err(format!("nyx not reachable: {}", e)),
    }
}

async fn call_nyx_get_json_with_query(
    endpoint: &str,
    params: &[(&str, &str)],
) -> Result<serde_json::Value, String> {
    let port = std::env::var("NYX_WEB_PORT").unwrap_or_else(|_| "8099".into());
    let url = format!("http://127.0.0.1:{}{}", port, endpoint);
    let client = reqwest::Client::new();
    let api_token = std::env::var("NYX_API_TOKEN").unwrap_or_default();

    let mut request = client
        .get(&url)
        .query(params)
        .timeout(std::time::Duration::from_secs(30));
    if !api_token.is_empty() {
        request = request.bearer_auth(api_token);
    }

    match request.send().await {
        Ok(resp) => resp
            .json::<serde_json::Value>()
            .await
            .map_err(|_| "failed to parse response".to_string()),
        Err(e) => Err(format!("nyx not reachable: {}", e)),
    }
}

/// Run a Python tool directly (for tools that don't need the full handler).
async fn run_tool(name: &str, args: serde_json::Value) -> String {
    match crate::tools::run(name, &args).await {
        Ok(r) => r["output"]
            .as_str()
            .or(r["error"].as_str())
            .unwrap_or("no output")
            .to_string(),
        Err(e) => format!("tool error: {}", e),
    }
}

#[tool(tool_box)]
impl NyxMcpServer {
    #[tool(description = "Chat with Nyx — send a message and get a response")]
    async fn nyx_chat(&self, #[tool(aggr)] req: ChatRequest) -> String {
        call_nyx(
            "/api/chat",
            serde_json::json!({"message": req.message, "user": "mcp"}),
        )
        .await
    }

    #[tool(description = "Get Nyx's grounded current status and what matters right now")]
    async fn nyx_status(&self) -> String {
        match call_nyx_get_json("/api/operator/brief").await {
            Ok(data) => data
                .get("status_reply")
                .and_then(|value| value.as_str())
                .unwrap_or("no grounded operator brief is available right now")
                .to_string(),
            Err(error) => error,
        }
    }

    #[tool(description = "Explain Nyx's most recent autonomous action and why it happened")]
    async fn nyx_explain_recent_action(&self) -> String {
        match call_nyx_get_json("/api/operator/brief").await {
            Ok(data) => data
                .get("recent_action_reply")
                .and_then(|value| value.as_str())
                .unwrap_or("no recent autonomy action explanation is available right now")
                .to_string(),
            Err(error) => error,
        }
    }

    #[tool(
        description = "Get Nyx's cross-surface continuity summary across web, bots, voice, and MCP"
    )]
    async fn nyx_continuity(&self) -> String {
        match call_nyx_get_json("/api/operator/continuity").await {
            Ok(data) => data
                .get("continuity_reply")
                .and_then(|value| value.as_str())
                .unwrap_or("no cross-surface continuity summary is available right now")
                .to_string(),
            Err(error) => error,
        }
    }

    #[tool(
        description = "Trace what Nyx's memory answer is based on for a specific topic or claim"
    )]
    async fn nyx_memory_provenance(&self, #[tool(aggr)] req: MemoryProvenanceRequest) -> String {
        match call_nyx_get_json_with_query("/api/memory/provenance", &[("query", &req.query)]).await
        {
            Ok(data) => data
                .get("reply")
                .and_then(|value| value.as_str())
                .unwrap_or("no memory provenance explanation is available right now")
                .to_string(),
            Err(error) => error,
        }
    }

    #[tool(description = "Get weather for a city")]
    async fn nyx_weather(&self, #[tool(aggr)] req: WeatherRequest) -> String {
        run_tool("weather", serde_json::json!({"city": req.city})).await
    }

    #[tool(description = "Search the web using DuckDuckGo")]
    async fn nyx_search(&self, #[tool(aggr)] req: SearchRequest) -> String {
        run_tool("web_search", serde_json::json!({"query": req.query})).await
    }

    #[tool(description = "Git info — status, log, todos, or diff")]
    async fn nyx_git(&self, #[tool(aggr)] req: GitRequest) -> String {
        run_tool("git_info", serde_json::json!({"action": req.action})).await
    }

    #[tool(description = "Remember a fact about the user")]
    async fn nyx_remember(&self, #[tool(aggr)] req: RememberRequest) -> String {
        call_nyx(
            "/api/chat",
            serde_json::json!({"message": format!("remember that {}", req.fact), "user": "mcp"}),
        )
        .await
    }

    #[tool(description = "Search for a GIF")]
    async fn nyx_gif(&self, #[tool(aggr)] req: GifRequest) -> String {
        run_tool("gif", serde_json::json!({"query": req.query})).await
    }

    #[tool(description = "Generate an image with FLUX")]
    async fn nyx_image(&self, #[tool(aggr)] req: ImageRequest) -> String {
        let mut args = serde_json::json!({"prompt": req.prompt});
        if let Some(style) = req.style {
            args["style"] = serde_json::json!(style);
        }
        run_tool("image_gen", args).await
    }
}

#[tool(tool_box)]
impl ServerHandler for NyxMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some("Nyx — personal AI assistant tools. Chat, weather, search, git, memory, GIFs, image generation.".into()),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

/// Start the MCP server on stdio.
pub async fn serve() -> anyhow::Result<()> {
    let (stdin, stdout) = rmcp::transport::io::stdio();
    let server = NyxMcpServer;
    let service = rmcp::serve_server(server, (stdin, stdout))
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    service.waiting().await?;
    Ok(())
}

// ── MCP Client ──────────────────────────────────────────────
//
// Connects to external MCP servers, discovers their tools,
// makes them callable from Nyx.

use crate::tools::ToolRuntimeStatus;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A discovered tool from an external MCP server.
#[derive(Debug, Clone)]
pub struct ExternalTool {
    pub server_name: String,
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// MCP client hub — manages connections to external MCP servers.
pub struct McpHub {
    tools: RwLock<Vec<ExternalTool>>,
    clients: RwLock<Vec<(String, rmcp::service::RunningService<rmcp::RoleClient, ()>)>>,
}

impl McpHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tools: RwLock::new(Vec::new()),
            clients: RwLock::new(Vec::new()),
        })
    }

    /// Connect to MCP servers defined in config.
    /// Config format (JSON file or env var):
    ///   { "server_name": { "command": "npx", "args": ["@mcp/server-xyz"] } }
    pub async fn connect_from_config(&self, config_path: &str) {
        let config = match std::fs::read_to_string(config_path) {
            Ok(c) => c,
            Err(_) => {
                tracing::debug!("mcp client: no config at {}", config_path);
                return;
            }
        };

        let servers: HashMap<String, McpServerConfig> = match serde_json::from_str(&config) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("mcp client: invalid config: {}", e);
                return;
            }
        };

        for (name, cfg) in servers {
            if let Err(e) = self.connect(&name, &cfg).await {
                tracing::warn!("mcp client: failed to connect to {}: {}", name, e);
            }
        }
    }

    /// Connect to a single MCP server.
    async fn connect(&self, name: &str, cfg: &McpServerConfig) -> anyhow::Result<()> {
        tracing::info!(
            "mcp client: connecting to {} ({} {})",
            name,
            cfg.command,
            cfg.args.join(" ")
        );

        let mut cmd = tokio::process::Command::new(&cfg.command);
        cmd.args(&cfg.args);
        if let Some(env) = &cfg.env {
            for (k, v) in env {
                cmd.env(k, v);
            }
        }

        let transport = rmcp::transport::TokioChildProcess::new(&mut cmd)?;
        let client = rmcp::serve_client((), transport)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Discover tools
        let tools_result = client
            .list_tools(Default::default())
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let mut discovered = Vec::new();
        for tool in &tools_result.tools {
            let ext_tool = ExternalTool {
                server_name: name.to_string(),
                name: tool.name.to_string(),
                description: tool.description.to_string(),
                input_schema: serde_json::to_value(&tool.input_schema).unwrap_or_default(),
            };
            tracing::info!(
                "mcp client: discovered tool {}:{} — {}",
                name,
                ext_tool.name,
                crate::trunc(&ext_tool.description, 60)
            );
            discovered.push(ext_tool);
        }

        self.tools.write().await.extend(discovered);
        self.clients.write().await.push((name.to_string(), client));

        Ok(())
    }

    /// List all discovered external tools.
    pub async fn list_tools(&self) -> Vec<ExternalTool> {
        self.tools.read().await.clone()
    }

    /// Call an external tool by server:tool name.
    pub async fn call_tool(
        &self,
        server_name: &str,
        tool_name: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<String> {
        let clients = self.clients.read().await;
        let (_, client) = clients
            .iter()
            .find(|(name, _)| name == server_name)
            .ok_or_else(|| anyhow::anyhow!("MCP server '{}' not connected", server_name))?;

        let arguments = match args {
            serde_json::Value::Object(map) => Some(map),
            _ => None,
        };

        let result = client
            .call_tool(rmcp::model::CallToolRequestParam {
                name: Cow::Owned(tool_name.to_string()),
                arguments,
            })
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Extract text from result content
        let text: Vec<String> = result
            .content
            .iter()
            .filter_map(|c| {
                // Content is Annotated<RawContent> — access the inner raw value
                match &c.raw {
                    rmcp::model::RawContent::Text(t) => Some(t.text.clone()),
                    _ => None,
                }
            })
            .collect();

        Ok(text.join("\n"))
    }

    /// Get tool descriptions for the LLM prompt.
    pub async fn tools_for_prompt(&self) -> String {
        let tools = self.tools.read().await;
        if tools.is_empty() {
            return String::new();
        }

        let lines: Vec<String> = tools
            .iter()
            .map(|t| format!("- {}:{} — {}", t.server_name, t.name, t.description))
            .collect();
        format!(
            "<mcp_tools>\nExternal tools available:\n{}\n</mcp_tools>",
            lines.join("\n")
        )
    }

    pub async fn tool_statuses(&self) -> Vec<ToolRuntimeStatus> {
        let mut statuses: Vec<ToolRuntimeStatus> = self
            .tools
            .read()
            .await
            .iter()
            .map(|tool| ToolRuntimeStatus {
                kind: "mcp".to_string(),
                name: tool.name.clone(),
                description: tool.description.clone(),
                ready: true,
                status: "ready".to_string(),
                issue: None,
                requires_network: false,
                sandboxed: false,
                filename: None,
                command: None,
                server_name: Some(tool.server_name.clone()),
                source: Some(tool.server_name.clone()),
                quarantined_until: None,
            })
            .collect();
        statuses.sort_by(|a, b| {
            a.server_name
                .cmp(&b.server_name)
                .then_with(|| a.name.cmp(&b.name))
        });
        statuses
    }
}

#[derive(Debug, serde::Deserialize)]
struct McpServerConfig {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
}
