use std::time::Instant;

use crate::{AppState, tools};

impl AppState {
    pub(crate) async fn delegate(
        &self,
        agent_type: &str,
        task: &str,
    ) -> std::result::Result<String, String> {
        self.delegate_inner(agent_type, task, 0).await
    }

    fn delegate_inner<'a>(
        &'a self,
        agent_type: &'a str,
        task: &'a str,
        depth: u8,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<String, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            if depth > 3 {
                return Err("max delegation depth reached".into());
            }

            let script = format!("agents/{}.py", agent_type);
            if !std::path::Path::new(&script).exists() {
                return Err(format!("agent {} not found", agent_type));
            }

            let request = serde_json::json!({ "task": task, "context": {} });
            let request_json =
                serde_json::to_string(&request).map_err(|error| error.to_string())?;

            let mut child = tokio::process::Command::new("python3")
                .arg(&script)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .current_dir(std::env::current_dir().unwrap_or_default())
                .spawn()
                .map_err(|error| error.to_string())?;

            let mut stdin = child.stdin.take().ok_or("no stdin")?;
            let stdout = child.stdout.take().ok_or("no stdout")?;

            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
            stdin
                .write_all(request_json.as_bytes())
                .await
                .map_err(|error| error.to_string())?;
            stdin.write_all(b"\n").await.ok();
            stdin.flush().await.ok();

            let mut reader = BufReader::new(stdout);
            let timeout = std::time::Duration::from_secs(60);
            let session_deadline = Instant::now() + std::time::Duration::from_secs(120);
            let mut final_output = String::new();

            loop {
                if Instant::now() > session_deadline {
                    tracing::warn!("agent {} hit session deadline", agent_type);
                    break;
                }

                let mut line = String::new();
                match tokio::time::timeout(timeout, reader.read_line(&mut line)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(_)) => {
                        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(line.trim()) {
                            match msg["type"].as_str() {
                                Some("tool_call") => {
                                    let name = msg["name"].as_str().unwrap_or("");
                                    let args = msg.get("arguments").cloned().unwrap_or_default();
                                    let result = self.execute_tool(name, &args).await;
                                    let payload =
                                        serde_json::to_string(&result).unwrap_or_default();
                                    stdin.write_all(payload.as_bytes()).await.ok();
                                    stdin.write_all(b"\n").await.ok();
                                    stdin.flush().await.ok();
                                }
                                Some("handoff") => {
                                    let target = msg["agent_type"].as_str().unwrap_or("research");
                                    let handoff_task = msg["task"].as_str().unwrap_or("");
                                    let result = match self
                                        .delegate_inner(target, handoff_task, depth + 1)
                                        .await
                                    {
                                        Ok(output) => serde_json::json!({ "output": output }),
                                        Err(error) => serde_json::json!({ "error": error }),
                                    };
                                    stdin
                                        .write_all(
                                            serde_json::to_string(&result)
                                                .unwrap_or_default()
                                                .as_bytes(),
                                        )
                                        .await
                                        .ok();
                                    stdin.write_all(b"\n").await.ok();
                                    stdin.flush().await.ok();
                                }
                                Some("response") => {
                                    final_output = msg["output"].as_str().unwrap_or("").to_string();
                                    break;
                                }
                                _ => tracing::debug!(
                                    "agent {}: unrecognized message: {}",
                                    agent_type,
                                    line.trim()
                                ),
                            }
                        } else {
                            tracing::debug!(
                                "agent {}: non-JSON output: {}",
                                agent_type,
                                line.trim()
                            );
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::warn!("agent {} read error: {}", agent_type, error);
                        break;
                    }
                    Err(_) => {
                        tracing::warn!("agent {} timed out on read", agent_type);
                        break;
                    }
                }
            }

            child.kill().await.ok();

            if final_output.is_empty() {
                if let Some(stderr) = child.stderr.take() {
                    let mut stderr_reader = BufReader::new(stderr);
                    let mut stderr_output = String::new();
                    tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        stderr_reader.read_line(&mut stderr_output),
                    )
                    .await
                    .ok();
                    if !stderr_output.is_empty() {
                        tracing::warn!("agent {} stderr: {}", agent_type, stderr_output.trim());
                    }
                }
                Err(format!("agent {} returned nothing", agent_type))
            } else {
                Ok(final_output)
            }
        })
    }

    pub(crate) async fn execute_tool(
        &self,
        name: &str,
        args: &serde_json::Value,
    ) -> serde_json::Value {
        match name {
            "_llm_chat" => {
                let prompt = args["prompt"].as_str().unwrap_or("");
                let max = args["max_tokens"].as_u64().unwrap_or(200) as u32;
                match self.llm.chat(prompt, max).await {
                    Ok(response) => serde_json::json!({ "output": response }),
                    Err(error) => serde_json::json!({ "error": error.to_string() }),
                }
            }
            "remember" => {
                let content = args["content"].as_str().unwrap_or("");
                let network = args["network"].as_str().unwrap_or("experience");
                let importance = args["importance"].as_f64().unwrap_or(0.5);
                if let Ok(Some(id)) = self.db.remember(content, network, importance) {
                    self.embed_memory_background(id, content.to_string());
                }
                serde_json::json!({ "success": true })
            }
            "shell" => {
                let command = args["command"].as_str().unwrap_or("");
                match tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(command)
                    .output()
                    .await
                {
                    Ok(output) => serde_json::json!({
                        "output": String::from_utf8_lossy(&output.stdout).to_string(),
                        "error": String::from_utf8_lossy(&output.stderr).to_string(),
                    }),
                    Err(error) => serde_json::json!({ "error": error.to_string() }),
                }
            }
            _ => {
                if let Some((server_name, tool_name)) = name.split_once(':') {
                    return match self
                        .mcp_hub
                        .call_tool(server_name, tool_name, args.clone())
                        .await
                    {
                        Ok(output) => serde_json::json!({ "output": output }),
                        Err(error) => serde_json::json!({ "error": error.to_string() }),
                    };
                }

                match tools::run_with_state(Some(self), name, args).await {
                    Ok(result) => result,
                    Err(_) => match self.plugins.run_tool(name, args).await {
                        Ok(result) => result,
                        Err(error) => serde_json::json!({ "error": error.to_string() }),
                    },
                }
            }
        }
    }
}
