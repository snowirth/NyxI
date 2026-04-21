//! Plugin discovery — scan plugins/ directory, load manifests, register tools.
//!
//! Each plugin is a directory with a manifest.toml and a script/binary.
//! Plugins are loaded at startup and can be hot-reloaded.
//!
//! Format:
//!   plugins/
//!     my_plugin/
//!       manifest.toml    # name, description, tools
//!       script.py        # or binary
//!
//! manifest.toml:
//!   name = "my_plugin"
//!   version = "0.1.0"
//!   description = "Does something cool"
//!
//!   [[tools]]
//!   name = "my_tool"
//!   description = "Does the thing"
//!   command = "python3 script.py"
//!   network = false
//!
//!   [tools.parameters]
//!   query = { type = "string", description = "Search query" }

use crate::tools::{ToolRuntimeStatus, executable_available};
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A loaded plugin.
#[derive(Debug, Clone)]
pub struct Plugin {
    pub name: String,
    pub version: String,
    pub description: String,
    pub dir: PathBuf,
    pub tools: Vec<PluginTool>,
}

/// A tool defined by a plugin.
#[derive(Debug, Clone)]
pub struct PluginTool {
    pub name: String,
    pub description: String,
    pub command: String,
    pub network: bool,
    pub parameters: HashMap<String, ToolParam>,
}

#[derive(Debug, Clone)]
pub struct ToolParam {
    pub param_type: String,
    pub description: String,
}

/// Plugin registry — stores loaded plugins and their tools.
#[derive(Debug, Default)]
pub struct PluginRegistry {
    pub plugins: Vec<Plugin>,
}

impl PluginRegistry {
    /// Scan the plugins directory and load all valid plugins.
    pub fn load_from_dir(dir: &Path) -> Self {
        let mut registry = Self::default();

        if !dir.exists() {
            tracing::debug!("plugins: directory {:?} not found, skipping", dir);
            return registry;
        }

        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("plugins: can't read {:?}: {}", dir, e);
                return registry;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let manifest_path = path.join("manifest.toml");
            if !manifest_path.exists() {
                continue;
            }

            match load_plugin(&path, &manifest_path) {
                Ok(plugin) => {
                    tracing::info!(
                        "plugins: loaded {} v{} ({} tools)",
                        plugin.name,
                        plugin.version,
                        plugin.tools.len()
                    );
                    registry.plugins.push(plugin);
                }
                Err(e) => {
                    tracing::warn!("plugins: failed to load {:?}: {}", path, e);
                }
            }
        }

        if !registry.plugins.is_empty() {
            tracing::info!(
                "plugins: {} plugins loaded, {} total tools",
                registry.plugins.len(),
                registry
                    .plugins
                    .iter()
                    .map(|p| p.tools.len())
                    .sum::<usize>()
            );
        }

        registry
    }

    /// Get all tools from all plugins.
    pub fn all_tools(&self) -> Vec<(&Plugin, &PluginTool)> {
        self.plugins
            .iter()
            .flat_map(|p| p.tools.iter().map(move |t| (p, t)))
            .collect()
    }

    /// Find a tool by name.
    pub fn find_tool(&self, name: &str) -> Option<(&Plugin, &PluginTool)> {
        self.all_tools().into_iter().find(|(_, t)| t.name == name)
    }

    /// Execute a plugin tool — spawns the command, pipes JSON.
    pub async fn run_tool(
        &self,
        name: &str,
        args: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let (plugin, tool) = self
            .find_tool(name)
            .ok_or_else(|| anyhow::anyhow!("plugin tool '{}' not found", name))?;

        let args_json = serde_json::to_string(args)?;

        // Parse command into program + args
        let parts: Vec<&str> = tool.command.split_whitespace().collect();
        if parts.is_empty() {
            anyhow::bail!("empty command for tool {}", name);
        }

        let mut cmd = tokio::process::Command::new(parts[0]);
        if parts.len() > 1 {
            cmd.args(&parts[1..]);
        }
        cmd.current_dir(&plugin.dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn()?;

        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin.write_all(args_json.as_bytes()).await?;
            drop(stdin);
        }

        let output =
            tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
                .await??;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let result: serde_json::Value = serde_json::from_str(stdout.trim())
            .unwrap_or_else(|_| serde_json::json!({"success": false, "error": "parse error"}));

        Ok(result)
    }

    /// Get tool descriptions for the LLM (so it knows what plugins are available).
    pub fn tools_for_prompt(&self) -> String {
        if self.plugins.is_empty() {
            return String::new();
        }

        let mut lines = Vec::new();
        for (plugin, tool) in self.all_tools() {
            lines.push(format!(
                "- {} (plugin:{}): {}",
                tool.name, plugin.name, tool.description
            ));
        }
        format!(
            "<plugins>\nAvailable plugin tools:\n{}\n</plugins>",
            lines.join("\n")
        )
    }

    pub fn tool_statuses(&self) -> Vec<ToolRuntimeStatus> {
        let mut statuses = Vec::new();
        for plugin in &self.plugins {
            for tool in &plugin.tools {
                let issue = plugin_tool_issue(plugin, tool);
                let status = if issue.is_some() { "blocked" } else { "ready" };
                statuses.push(ToolRuntimeStatus {
                    kind: "plugin".to_string(),
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    ready: issue.is_none(),
                    status: status.to_string(),
                    issue,
                    requires_network: tool.network,
                    sandboxed: false,
                    filename: None,
                    command: Some(tool.command.clone()),
                    server_name: None,
                    source: Some(plugin.name.clone()),
                    quarantined_until: None,
                });
            }
        }
        statuses.sort_by(|a, b| a.name.cmp(&b.name));
        statuses
    }
}

/// Load a single plugin from its directory.
fn load_plugin(dir: &Path, manifest_path: &Path) -> Result<Plugin> {
    let content = std::fs::read_to_string(manifest_path)?;
    let manifest: toml::Value = content.parse()?;

    let name = manifest
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("unnamed")
        .to_string();
    let version = manifest
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("0.0.0")
        .to_string();
    let description = manifest
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let mut tools = Vec::new();
    if let Some(tools_array) = manifest.get("tools").and_then(|v| v.as_array()) {
        for tool_val in tools_array {
            let tool_name = tool_val
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unnamed")
                .to_string();
            let tool_desc = tool_val
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let command = tool_val
                .get("command")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let network = tool_val
                .get("network")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let mut parameters = HashMap::new();
            if let Some(params) = tool_val.get("parameters").and_then(|v| v.as_table()) {
                for (key, val) in params {
                    let param_type = val
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("string")
                        .to_string();
                    let param_desc = val
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    parameters.insert(
                        key.clone(),
                        ToolParam {
                            param_type,
                            description: param_desc,
                        },
                    );
                }
            }

            if !command.is_empty() {
                tools.push(PluginTool {
                    name: tool_name,
                    description: tool_desc,
                    command,
                    network,
                    parameters,
                });
            }
        }
    }

    Ok(Plugin {
        name,
        version,
        description,
        dir: dir.to_path_buf(),
        tools,
    })
}

fn plugin_tool_issue(plugin: &Plugin, tool: &PluginTool) -> Option<String> {
    let Some(program) = tool.command.split_whitespace().next() else {
        return Some("plugin command is empty".to_string());
    };

    let program_path = Path::new(program);
    let available = if program_path.is_absolute() || program.contains(std::path::MAIN_SEPARATOR) {
        let resolved = if program_path.is_absolute() {
            program_path.to_path_buf()
        } else {
            plugin.dir.join(program_path)
        };
        resolved.is_file()
    } else {
        executable_available(program)
    };

    if available {
        None
    } else {
        Some(format!("plugin command '{}' is not available", program))
    }
}
