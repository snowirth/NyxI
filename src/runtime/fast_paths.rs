use crate::{AppState, forge, intent, swarm, tools};

impl AppState {
    pub(crate) async fn try_intent_fast_path(
        &self,
        channel: &str,
        _sender: &str,
        text: &str,
        cache_hash: u64,
        intent: &intent::Intent,
    ) -> Option<String> {
        let response = match intent {
            intent::Intent::Gif { query } => {
                match tools::run("gif", &serde_json::json!({ "query": query })).await {
                    Ok(result) => Some(
                        result["output"]
                            .as_str()
                            .unwrap_or("no gif found")
                            .to_string(),
                    ),
                    Err(error) => {
                        tracing::debug!("tool error: {}", error);
                        None
                    }
                }
            }
            intent::Intent::ImageGen { prompt, style } => {
                let mut args = serde_json::json!({ "prompt": prompt });
                if let Some(style) = style {
                    args["style"] = serde_json::json!(style);
                }
                match tools::run("image_gen", &args).await {
                    Ok(result) => Some(
                        result["output"]
                            .as_str()
                            .unwrap_or("image gen failed")
                            .to_string(),
                    ),
                    Err(error) => {
                        tracing::debug!("tool error: {}", error);
                        None
                    }
                }
            }
            intent::Intent::Timeline => match tools::run(
                "twitter",
                &serde_json::json!({ "action": "timeline", "count": 10 }),
            )
            .await
            {
                Ok(result) => Some(
                    result["output"]
                        .as_str()
                        .unwrap_or("couldn't read timeline")
                        .to_string(),
                ),
                Err(error) => {
                    tracing::debug!("tool error: {}", error);
                    None
                }
            },
            intent::Intent::Mentions => {
                match tools::run("twitter", &serde_json::json!({ "action": "mentions" })).await {
                    Ok(result) => Some(
                        result["output"]
                            .as_str()
                            .unwrap_or("couldn't check mentions")
                            .to_string(),
                    ),
                    Err(error) => {
                        tracing::debug!("tool error: {}", error);
                        None
                    }
                }
            }
            intent::Intent::GitHub => {
                let repo = if !self.config.github_repo.is_empty() {
                    self.config.github_repo.clone()
                } else {
                    String::new()
                };
                match tools::run(
                    "github",
                    &serde_json::json!({ "action": "notifications", "repo": repo }),
                )
                .await
                {
                    Ok(result) => Some(
                        result["output"]
                            .as_str()
                            .unwrap_or("couldn't check github")
                            .to_string(),
                    ),
                    Err(error) => {
                        tracing::debug!("tool error: {}", error);
                        None
                    }
                }
            }
            intent::Intent::Git { action } => {
                match tools::run("git_info", &serde_json::json!({ "action": action })).await {
                    Ok(result) => Some(
                        result["output"]
                            .as_str()
                            .unwrap_or("no git info")
                            .to_string(),
                    ),
                    Err(error) => {
                        tracing::debug!("tool error: {}", error);
                        None
                    }
                }
            }
            intent::Intent::Vision => {
                tracing::info!("vision: dispatching tool");
                let vision_prompt = "Describe what you see on screen: which apps are open, what the user is working on, any visible text or content. Be specific and concise.";
                match tools::run("vision", &serde_json::json!({ "prompt": vision_prompt })).await {
                    Ok(result) => {
                        tracing::info!("vision: result={}", result);
                        let output = result["output"].as_str().unwrap_or("");
                        let error = result["error"].as_str().unwrap_or("");
                        Some(if !output.is_empty() {
                            output.to_string()
                        } else if !error.is_empty() {
                            format!("vision error: {}", error)
                        } else {
                            "couldn't capture screen".to_string()
                        })
                    }
                    Err(error) => Some(format!("vision tool failed: {}", error)),
                }
            }
            _ => None,
        }?;

        self.store_and_cache(channel, text, &response, cache_hash)
            .await;
        Some(response)
    }

    pub(crate) async fn try_rule_fast_path(
        &self,
        channel: &str,
        sender: &str,
        text: &str,
        lower: &str,
        cache_hash: u64,
        _depth: u8,
        intent: &intent::Intent,
    ) -> Option<String> {
        let sleep_signals = [
            "going to sleep",
            "going to bed",
            "heading to bed",
            "goodnight",
            "good night",
            "gn",
            "gonna sleep",
            "time to sleep",
            "im going to sleep",
            "i'm going to sleep",
            "night night",
            "gotta sleep",
        ];
        if sleep_signals.iter().any(|signal| lower.contains(signal)) || lower == "gn" {
            let recent = self.db.get_history(channel, 10);
            let context: String = recent
                .iter()
                .map(|(role, content)| format!("{}: {}", role, crate::trunc(content, 80)))
                .collect::<Vec<_>>()
                .join("\n");
            self.db.set_state("sleep_context", &context);
            self.db
                .set_state("sleep_time", &chrono::Utc::now().timestamp().to_string());

            let plan_prompt = format!(
                "The user is going to sleep. Based on recent conversation, \
                 what 1-2 things could you work on while they're away? \
                 Reply naturally — say goodnight briefly, then mention what you'll do. \
                 Keep it short.\n\nRecent:\n{}",
                crate::trunc(&context, 800)
            );
            let response = self
                .llm
                .chat(&plan_prompt, 100)
                .await
                .unwrap_or_else(|_| "night. i'll keep an eye on things.".into());
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if (lower.contains("what time") || lower.contains("the time") || lower == "time")
            && !lower.contains("times")
            && !lower.contains(" in ")
        {
            let loc = if self.config.user_location.is_empty() {
                String::new()
            } else {
                format!(". {}.", self.config.user_location)
            };
            let response = format!(
                "{}{}",
                chrono::Local::now().format("%H:%M, %A %d %B %Y"),
                loc
            );
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if !lower.contains("every ")
            && (lower.contains("weather")
                || (lower.contains("rain")
                    && (lower.contains("today") || lower.contains("tomorrow"))))
        {
            let city = self.extract_city(text).unwrap_or_else(|| {
                if self.config.default_city.is_empty() {
                    "New York".into()
                } else {
                    self.config.default_city.clone()
                }
            });
            match tools::run("weather", &serde_json::json!({ "city": city })).await {
                Ok(result) => {
                    let response = result["output"]
                        .as_str()
                        .unwrap_or("couldn't get weather")
                        .to_string();
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                Err(error) => tracing::debug!("tool error: {}", error),
            }
        }

        if is_operator_status_request(lower) {
            let response = crate::world::brief::compile_operator_brief(
                self.db.as_ref(),
                "chat_operator_status",
            )
            .map(|brief| crate::world::brief::render_status_reply(&brief))
            .unwrap_or_else(|error| {
                tracing::warn!("operator brief: failed to compile status reply: {}", error);
                "I could not compile the current operator brief just now.".to_string()
            });
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if is_recent_action_explanation_request(lower) {
            let response = crate::world::brief::compile_operator_brief(
                self.db.as_ref(),
                "chat_operator_action_explanation",
            )
            .map(|brief| crate::world::brief::render_recent_action_reply(&brief))
            .unwrap_or_else(|error| {
                tracing::warn!(
                    "operator brief: failed to compile action explanation: {}",
                    error
                );
                "I could not reconstruct the recent autonomy action trace just now.".to_string()
            });
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if is_cross_surface_continuity_request(lower) {
            let response = crate::world::continuity::read_cross_surface_continuity(
                self.db.as_ref(),
                "chat_cross_surface_continuity",
            )
            .map(|brief| crate::world::continuity::render_continuity_reply(&brief))
            .unwrap_or_else(|error| {
                tracing::warn!(
                    "cross-surface continuity: failed to compile continuity brief: {}",
                    error
                );
                "I could not reconstruct the recent cross-surface continuity thread just now."
                    .to_string()
            });
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if crate::runtime::memory::is_memory_provenance_request(lower) {
            let response = self
                .memory_provenance_brief(channel, text, 4, "chat_memory_provenance")
                .await
                .reply;
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if lower.contains("remember that") || lower.contains("remember this") {
            let fact = text
                .split("remember that ")
                .last()
                .or(text.split("remember this ").last())
                .unwrap_or(text)
                .trim();
            if fact.len() > 3 {
                if let Ok(Some(id)) = self.db.remember(fact, "experience", 0.8) {
                    self.embed_memory_background(id, fact.to_string());
                }
                let mut profile = self.get_profile(sender);
                profile.add_fact(fact);
                self.save_profile(sender, &profile);
                self.record_user_adaptation_growth(
                    "runtime",
                    "updated user profile from explicit remember",
                    serde_json::json!({
                        "fact": fact,
                        "sender": sender,
                        "channel": channel,
                        "trigger": "remember",
                    }),
                );
            }
            let response = "got it.".to_string();
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if lower.contains("know about me") || lower.contains("what do you know") {
            let profile = self.get_profile(sender);
            let prompt = profile.to_prompt();
            let memory_context = self.structured_memory_context_for_query(text).await;
            let memory_context = if memory_context.is_empty() {
                String::new()
            } else {
                format!("\n\nStructured memory:\n{}", memory_context)
            };
            let response = self
                .llm
                .chat(
                    &format!(
                        "Answer this question using the profile and memories. Be brief.\n\nProfile:\n{}{}\n\nQuestion: {}",
                        prompt, memory_context, text
                    ),
                    100,
                )
                .await
                .unwrap_or_else(|_| "not much yet.".into());
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if lower.contains("search for")
            || lower.contains("look up")
            || lower.contains("search about")
        {
            let query = text
                .replace("search for ", "")
                .replace("look up ", "")
                .replace("search about ", "");
            match tools::run("web_search", &serde_json::json!({ "query": query })).await {
                Ok(result) => {
                    let search_output = result["output"].as_str().unwrap_or("no results");
                    self.ingest_web_search_memory(&query, search_output);
                    let response = self
                        .llm
                        .chat(
                            &format!(
                                "Summarize these search results concisely:\n{}\n\nBe direct and factual.",
                                crate::trunc(search_output, 1500)
                            ),
                            200,
                        )
                        .await
                        .unwrap_or_else(|_| search_output.to_string());
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                Err(error) => tracing::debug!("tool error: {}", error),
            }
        }

        if lower.contains("remind me") || lower.starts_with("todo ") || lower.starts_with("todo:") {
            for prefix in ["remind me to ", "remind me ", "todo: ", "todo "] {
                if lower.contains(prefix) {
                    if let Some(index) = lower.find(prefix) {
                        let fact = &text[index + prefix.len()..];
                        let fact = fact.trim();
                        if fact.len() > 3 {
                            if let Ok(id) = self.db.add_reminder(fact) {
                                let response = format!("got it, #{}: {}", id, fact);
                                self.store_and_cache(channel, text, &response, cache_hash)
                                    .await;
                                return Some(response);
                            }
                        }
                    }
                    break;
                }
            }
        }

        if lower.contains("my reminders")
            || lower.contains("my todos")
            || lower == "reminders"
            || lower == "todos"
        {
            let reminders = self.db.get_reminders(false);
            let response = if reminders.is_empty() {
                "nothing pending.".to_string()
            } else {
                reminders
                    .iter()
                    .map(|(id, content, _)| format!("#{}: {}", id, content))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if lower.contains("every ") && (lower.contains("check") || lower.contains("run")) {
            let interval = if lower.contains("every morning") || lower.contains("every day") {
                86400i64
            } else if lower.contains("every hour") {
                3600
            } else if lower.contains("every 30 min") {
                1800
            } else if lower.contains("every 15 min") {
                900
            } else if lower.contains("every 5 min") {
                300
            } else {
                0
            };

            if interval > 0 {
                let (tool, args) = if lower.contains("github") || lower.contains("notification") {
                    ("github", serde_json::json!({ "action": "notifications" }))
                } else if lower.contains("weather") {
                    (
                        "weather",
                        serde_json::json!({
                            "city": if self.config.default_city.is_empty() {
                                "New York"
                            } else {
                                &self.config.default_city
                            }
                        }),
                    )
                } else if lower.contains("git") || lower.contains("commit") {
                    ("git_info", serde_json::json!({ "action": "status" }))
                } else {
                    ("", serde_json::Value::Null)
                };

                if !tool.is_empty() {
                    let desc = text.trim().to_string();
                    if let Ok(id) =
                        self.db
                            .add_scheduled_task(&desc, tool, &args.to_string(), interval)
                    {
                        let interval_str = if interval >= 86400 {
                            "daily".to_string()
                        } else if interval >= 3600 {
                            format!("every {}h", interval / 3600)
                        } else {
                            format!("every {}min", interval / 60)
                        };
                        let response = format!("scheduled #{}: {} ({})", id, desc, interval_str);
                        self.store_and_cache(channel, text, &response, cache_hash)
                            .await;
                        return Some(response);
                    }
                }
            }
        }

        if lower.contains("my schedule") || lower.contains("scheduled tasks") || lower == "schedule"
        {
            let tasks = self.db.list_scheduled_tasks();
            let response = if tasks.is_empty() {
                "no scheduled tasks.".to_string()
            } else {
                tasks
                    .iter()
                    .map(|(id, desc, _tool, interval, enabled)| {
                        let interval_str = if *interval >= 86400 {
                            "daily".to_string()
                        } else if *interval >= 3600 {
                            format!("every {}h", interval / 3600)
                        } else {
                            format!("every {}min", interval / 60)
                        };
                        let status = if *enabled { "" } else { " (disabled)" };
                        format!("#{}: {} [{}]{}", id, desc, interval_str, status)
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if (lower.starts_with("tweet ")
            || lower.starts_with("tweet:")
            || lower.starts_with("post tweet"))
            && !lower.contains("able to")
            && !lower.contains("can you")
            && !lower.contains('?')
        {
            let tweet_text = text
                .replace("tweet ", "")
                .replace("post tweet ", "")
                .replace("tweet:", "")
                .trim()
                .to_string();
            if tweet_text.len() > 3 {
                let draft = if tweet_text.len() < 20 {
                    match self
                        .llm
                        .chat_auto(
                            &format!(
                                "Write a single tweet (max 280 chars) about: {}. Casual, lowercase, no hashtags. Reply with ONLY the tweet text.",
                                tweet_text
                            ),
                            100,
                        )
                        .await
                    {
                        Ok(tweet) => tweet.trim().trim_matches('"').to_string(),
                        Err(_) => tweet_text.clone(),
                    }
                } else {
                    tweet_text.clone()
                };
                match tools::run(
                    "twitter",
                    &serde_json::json!({ "action": "post", "text": draft }),
                )
                .await
                {
                    Ok(result) => {
                        let response = result["output"]
                            .as_str()
                            .unwrap_or(result["error"].as_str().unwrap_or("tweet failed"))
                            .to_string();
                        self.store_and_cache(channel, text, &response, cache_hash)
                            .await;
                        return Some(response);
                    }
                    Err(error) => tracing::debug!("tool error: {}", error),
                }
            }
        }

        if lower.contains("my timeline") || lower.contains("twitter timeline") {
            match tools::run(
                "twitter",
                &serde_json::json!({ "action": "timeline", "count": 10 }),
            )
            .await
            {
                Ok(result) => {
                    let response = result["output"]
                        .as_str()
                        .unwrap_or("couldn't read timeline")
                        .to_string();
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                Err(error) => tracing::debug!("tool error: {}", error),
            }
        }

        if lower.contains("my mentions") || lower.contains("twitter mentions") {
            match tools::run("twitter", &serde_json::json!({ "action": "mentions" })).await {
                Ok(result) => {
                    let response = result["output"]
                        .as_str()
                        .unwrap_or("couldn't check mentions")
                        .to_string();
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                Err(error) => tracing::debug!("tool error: {}", error),
            }
        }

        if lower.contains("gif")
            && (lower.contains("send")
                || lower.contains("show")
                || lower.starts_with("gif")
                || lower.contains("random gif"))
        {
            let query = text
                .to_lowercase()
                .replace("send me", "")
                .replace("send a", "")
                .replace("send", "")
                .replace("show me", "")
                .replace("show a", "")
                .replace("show", "")
                .replace("random", "")
                .replace("gif of", "")
                .replace("gif:", "")
                .replace("gif", "")
                .replace("please", "")
                .trim()
                .to_string();
            let query = if query.is_empty() || query.len() < 2 {
                "funny".to_string()
            } else {
                query
            };
            match tools::run("gif", &serde_json::json!({ "query": query })).await {
                Ok(result) => {
                    let response = result["output"]
                        .as_str()
                        .unwrap_or("no gif found")
                        .to_string();
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                Err(error) => tracing::debug!("tool error: {}", error),
            }
        }

        if lower.starts_with("generate ")
            || lower.starts_with("draw ")
            || lower.starts_with("create image")
            || lower.starts_with("imagine ")
            || lower.contains("generate an image")
        {
            let prompt = text
                .replace("generate ", "")
                .replace("draw ", "")
                .replace("create image of ", "")
                .replace("create image ", "")
                .replace("imagine ", "")
                .replace("generate an image of ", "")
                .trim()
                .to_string();
            if prompt.len() > 3 {
                let style = if lower.contains("anime") {
                    Some("anime")
                } else if lower.contains("realistic") {
                    Some("realistic")
                } else if lower.contains("cinematic") {
                    Some("cinematic")
                } else if lower.contains("pixel") {
                    Some("pixel")
                } else if lower.contains("artistic") {
                    Some("artistic")
                } else {
                    None
                };
                let mut args = serde_json::json!({ "prompt": prompt });
                if let Some(style) = style {
                    args["style"] = serde_json::json!(style);
                }
                match tools::run("image_gen", &args).await {
                    Ok(result) => {
                        let output = result["output"].as_str().unwrap_or("image gen failed");
                        let response = if let Some(file) = result["file"].as_str() {
                            format!("[nyx:file:{}]\n{}", file, output)
                        } else {
                            output.to_string()
                        };
                        self.store_and_cache(channel, text, &response, cache_hash)
                            .await;
                        return Some(response);
                    }
                    Err(error) => tracing::debug!("tool error: {}", error),
                }
            }
        }

        if lower.contains("read file") || lower.contains("read the file") {
            let path = text.split("file ").last().unwrap_or("").trim().to_string();
            if !path.is_empty() {
                match tools::run(
                    "file_ops",
                    &serde_json::json!({ "action": "read", "path": path }),
                )
                .await
                {
                    Ok(result) => {
                        let response = result["output"]
                            .as_str()
                            .unwrap_or(result["error"].as_str().unwrap_or("couldn't read"))
                            .to_string();
                        self.store_and_cache(channel, text, &response, cache_hash)
                            .await;
                        return Some(response);
                    }
                    Err(error) => tracing::debug!("tool error: {}", error),
                }
            }
        }

        if matches!(intent, intent::Intent::Evolve)
            || lower.contains("evolve ")
            || (lower.contains("modify ") && lower.contains(".py"))
        {
            match forge::evolve_code(&self.llm, text).await {
                forge::EvolveResult::Success {
                    path,
                    description,
                    change_summary,
                    change_report,
                    telemetry,
                } => {
                    let response = format_evolve_response(
                        &path,
                        &description,
                        &change_summary,
                        &change_report,
                    );
                    self.record_self_edit_growth(
                        "runtime",
                        text,
                        &path,
                        &change_summary,
                        true,
                        &telemetry,
                    );
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                forge::EvolveResult::Failed { reason, telemetry } => {
                    self.record_self_edit_growth("runtime", text, "", &reason, false, &telemetry);
                    let response = format!("couldn't do that: {}", reason);
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
            }
        }

        if lower.contains("look at")
            || lower.contains("my screen")
            || lower.contains("what do you see")
        {
            match tools::run("vision", &serde_json::json!({ "prompt": text })).await {
                Ok(result) => {
                    let response = result["output"].as_str().unwrap_or("can't see").to_string();
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                Err(error) => tracing::debug!("tool error: {}", error),
            }
        }

        if !lower.contains("every ")
            && (lower.contains("git status")
                || lower.contains("git log")
                || lower.contains("what changed")
                || lower.contains("recent commits")
                || lower.contains("uncommitted"))
        {
            let action = if lower.contains("log") || lower.contains("commits") {
                "log"
            } else if lower.contains("todo") {
                "todos"
            } else if lower.contains("diff") || lower.contains("changed") {
                "diff"
            } else {
                "status"
            };
            match tools::run("git_info", &serde_json::json!({ "action": action })).await {
                Ok(result) => {
                    let response = result["output"]
                        .as_str()
                        .unwrap_or("no git info")
                        .to_string();
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                Err(error) => tracing::debug!("tool error: {}", error),
            }
        }

        if !lower.contains("every ")
            && (lower.contains("github")
                || lower.contains("notifications")
                || lower.contains("pull request")
                || lower.contains("my prs")
                || lower.contains("my issues"))
        {
            let action = if lower.contains("pr") || lower.contains("pull request") {
                "prs"
            } else if lower.contains("issue") {
                "issues"
            } else {
                "notifications"
            };
            let repo = if !self.config.github_repo.is_empty() {
                self.config.github_repo.clone()
            } else {
                String::new()
            };
            match tools::run(
                "github",
                &serde_json::json!({ "action": action, "repo": repo }),
            )
            .await
            {
                Ok(result) => {
                    let response = result["output"]
                        .as_str()
                        .unwrap_or("couldn't check github")
                        .to_string();
                    self.store_and_cache(channel, text, &response, cache_hash)
                        .await;
                    return Some(response);
                }
                Err(error) => tracing::debug!("tool error: {}", error),
            }
        }

        if lower.starts_with("research ")
            || lower.contains("deep dive")
            || lower.contains("investigate")
        {
            if let Ok(output) = self.delegate("research", text).await {
                self.store_and_cache(channel, text, &output, cache_hash)
                    .await;
                return Some(output);
            }
        }

        if (lower.contains("run ") && lower.contains("command")) || lower.contains("shell ") {
            if let Ok(output) = self.delegate("code", text).await {
                self.store_and_cache(channel, text, &output, cache_hash)
                    .await;
                return Some(output);
            }
        }

        if let Some(split) = swarm::try_split(text) {
            let response = swarm::execute(&self.llm, split).await;
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        if let Some(split) = swarm::try_split_llm(text, &self.llm).await {
            let response = swarm::execute(&self.llm, split).await;
            self.store_and_cache(channel, text, &response, cache_hash)
                .await;
            return Some(response);
        }

        None
    }
}

fn format_evolve_response(
    path: &str,
    description: &str,
    change_summary: &str,
    change_report: &forge::ChangeReport,
) -> String {
    let clean_description = description
        .trim()
        .trim_end_matches(|c: char| ".!?".contains(c));
    let line = change_report
        .line
        .map(|line| format!(" at line {}", line))
        .unwrap_or_default();
    if clean_description.is_empty() {
        format!(
            "done. modified {}{}. before: `{}`. after: `{}`. summary: {}.",
            path, line, change_report.before, change_report.after, change_summary
        )
    } else {
        format!(
            "done. modified {}{}.\nwhy: {}.\nbefore: `{}`\nafter: `{}`\nsummary: {}.",
            path,
            line,
            clean_description,
            change_report.before,
            change_report.after,
            change_summary
        )
    }
}

fn is_operator_status_request(lower: &str) -> bool {
    matches!(
        lower.trim(),
        "status" | "what matters" | "what matters right now" | "what should we focus on"
    ) || lower.contains("what matters right now")
        || lower.contains("what should i focus on")
        || lower.contains("what should we focus on")
        || lower.contains("what's the status right now")
        || lower.contains("whats the status right now")
        || lower.contains("grounded status")
}

fn is_recent_action_explanation_request(lower: &str) -> bool {
    lower.contains("why did you do that")
        || lower.contains("why did nyx do that")
        || lower.contains("why did you run that")
        || lower.contains("explain that action")
        || lower.contains("explain the last action")
        || lower.contains("why did that autonomous action happen")
}

fn is_cross_surface_continuity_request(lower: &str) -> bool {
    matches!(
        lower.trim(),
        "catch me up" | "what were we doing?" | "what were we doing"
    ) || lower.contains("catch me up")
        || lower.contains("what were we doing")
        || lower.contains("where were we up to")
        || lower.contains("what happened while i was away")
        || lower.contains("continue from the other surface")
        || lower.contains("resume across surfaces")
}
