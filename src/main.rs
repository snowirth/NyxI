use std::time::Instant;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    nyx::load_dotenv();

    if std::env::args().any(|a| a == "--mcp") {
        tracing_subscriber::fmt()
            .with_env_filter("nyx=warn")
            .with_writer(std::io::stderr)
            .init();
        return nyx::mcp::serve().await.map_err(|e| anyhow::anyhow!(e));
    }

    tracing_subscriber::fmt().with_env_filter("nyx=info").init();

    let start = Instant::now();
    let mut config = nyx::Config::from_env();
    nyx::autodetect_location(&mut config).await;

    tracing::info!("Nyx V2 starting");

    let state = nyx::build_state(config, start)?;
    install_runtime_panic_hook(state.db.clone());
    nyx::spawn_background_tasks(&state);

    let discord_token = std::env::var("NYX_DISCORD_TOKEN").unwrap_or_default();
    if !discord_token.is_empty() {
        let dc_state = state.clone();
        let dc_token = discord_token.clone();
        tokio::spawn(async move {
            if let Err(e) = discord::run(dc_state, &dc_token).await {
                tracing::error!("Discord error: {}", e);
            }
        });
    }

    if !state.config.telegram_token.is_empty() {
        let tg_state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = telegram::run(tg_state).await {
                tracing::error!("Telegram error: {}", e);
            }
        });
    }

    let addr = format!("127.0.0.1:{}", state.config.web_port);
    tracing::info!("Nyx V2 ready — http://{}", addr);

    let shutdown_state = state.clone();
    let app = nyx::web::router(state);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    if let Err(error) = shutdown_state
        .db
        .register_runtime_shutdown("main", "graceful_shutdown")
    {
        tracing::warn!("runtime: failed to register shutdown incident: {}", error);
    }

    Ok(())
}

fn install_runtime_panic_hook(db: std::sync::Arc<nyx::db::Db>) {
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let payload = panic_payload_summary(panic_info);
        let location = panic_info.location().map(|location| {
            serde_json::json!({
                "file": location.file(),
                "line": location.line(),
                "column": location.column(),
            })
        });
        let session = db
            .get_state("runtime.active_session")
            .filter(|raw| !raw.trim().is_empty())
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok());
        let summary = if let Some(location) = panic_info.location() {
            format!(
                "panic at {}:{}: {}",
                location.file(),
                location.line(),
                payload
            )
        } else {
            format!("panic: {}", payload)
        };
        let details = serde_json::json!({
            "captured_at": chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            "payload": payload,
            "thread": std::thread::current().name().map(str::to_string),
            "location": location,
            "session": session,
        });

        let record_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.record_system_incident("runtime_panic", "panic_hook", "error", &summary, &details)
        }));
        if let Err(error) = record_result {
            tracing::error!("runtime: failed to record panic incident: {:?}", error);
        } else if let Ok(Err(error)) = record_result {
            tracing::error!("runtime: failed to record panic incident: {}", error);
        }

        previous_hook(panic_info);
    }));
}

fn panic_payload_summary(panic_info: &std::panic::PanicHookInfo<'_>) -> String {
    if let Some(message) = panic_info.payload().downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = panic_info.payload().downcast_ref::<String>() {
        return message.clone();
    }
    "non-string panic payload".to_string()
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let terminate = async {
            if let Ok(mut stream) = signal(SignalKind::terminate()) {
                stream.recv().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

mod discord {
    use serenity::model::prelude::*;
    use serenity::prelude::*;
    use std::sync::Arc;

    struct Handler {
        state: Arc<nyx::AppState>,
    }

    #[serenity::async_trait]
    impl EventHandler for Handler {
        async fn message(&self, ctx: Context, msg: Message) {
            if msg.author.bot {
                return;
            }

            let text = msg.content.clone();
            if text.is_empty() {
                return;
            }

            let mentioned = msg.mentions_me(&ctx.http).await.unwrap_or(false);
            let is_dm = msg.guild_id.is_none();
            let name_mentioned = text.to_lowercase().contains("nyx");
            if !mentioned && !is_dm && !name_mentioned {
                return;
            }

            let clean = if text.contains('>') {
                text.split('>').last().unwrap_or(&text).trim()
            } else {
                &text
            };
            if clean.is_empty() {
                return;
            }

            let channel = format!("discord:{}", msg.channel_id);
            let sender = msg.author.name.clone();

            msg.channel_id.broadcast_typing(&ctx.http).await.ok();

            let response = self.state.handle(&channel, &sender, clean).await;
            let (file_path, text_body) = nyx::extract_file_marker(&response);
            if let Some(path) = file_path {
                let file = std::path::Path::new(&path);
                if file.exists() {
                    let files = vec![serenity::all::CreateAttachment::path(file).await.ok()];
                    let files: Vec<_> = files.into_iter().flatten().collect();
                    if !files.is_empty() {
                        msg.channel_id
                            .send_files(&ctx.http, files, serenity::all::CreateMessage::new())
                            .await
                            .ok();
                    }
                }
            }
            if !text_body.is_empty() {
                msg.channel_id
                    .say(&ctx.http, nyx::trunc(&text_body, 2000))
                    .await
                    .ok();
            }
        }

        async fn ready(&self, _: Context, ready: Ready) {
            tracing::info!("Discord: {} online", ready.user.name);
        }
    }

    pub async fn run(state: nyx::AppState, token: &str) -> anyhow::Result<()> {
        tracing::info!("Discord bot starting");
        let intents = GatewayIntents::GUILD_MESSAGES
            | GatewayIntents::DIRECT_MESSAGES
            | GatewayIntents::MESSAGE_CONTENT;
        let mut client = Client::builder(token, intents)
            .event_handler(Handler {
                state: Arc::new(state),
            })
            .await?;
        client.start().await?;
        Ok(())
    }
}

mod telegram {
    use std::sync::Arc;
    use teloxide::prelude::*;
    use teloxide::types::InputFile;

    pub async fn run(state: nyx::AppState) -> anyhow::Result<()> {
        let bot = Bot::new(&state.config.telegram_token);
        let owner_ids = state.config.telegram_owner_ids.clone();
        tracing::info!("Telegram bot starting ({} owners)", owner_ids.len());

        let handler = Update::filter_message().endpoint(
            move |bot: Bot,
                  msg: teloxide::types::Message,
                  state: Arc<nyx::AppState>,
                  owners: Vec<String>| async move {
                let text = msg.text().unwrap_or("").to_string();
                if text.is_empty() {
                    return respond(());
                }

                let user_id = msg
                    .from
                    .as_ref()
                    .map(|u| u.id.0.to_string())
                    .unwrap_or_default();
                let sender = msg
                    .from
                    .as_ref()
                    .map(|u| u.first_name.clone())
                    .unwrap_or("someone".into());
                let is_owner = owners.iter().any(|id| id == &user_id);

                if !owners.is_empty() && !is_owner {
                    return respond(());
                }

                bot.send_chat_action(msg.chat.id, teloxide::types::ChatAction::Typing)
                    .await
                    .ok();

                let channel = format!("telegram:{}", user_id);
                let response = state.handle(&channel, &sender, &text).await;

                let (file_path, text_body) = extract_file_marker(&response);
                if let Some(path) = file_path {
                    let file = std::path::Path::new(&path);
                    if file.exists() {
                        bot.send_photo(msg.chat.id, InputFile::file(file))
                            .await
                            .ok();
                    }
                }
                if !text_body.is_empty() {
                    let clean = text_body.replace("**", "").replace("__", "");
                    bot.send_message(msg.chat.id, clean).await.ok();
                }

                respond(())
            },
        );

        let state = Arc::new(state);
        Dispatcher::builder(bot, handler)
            .dependencies(teloxide::dptree::deps![state, owner_ids])
            .build()
            .dispatch()
            .await;

        Ok(())
    }

    fn extract_file_marker(response: &str) -> (Option<String>, String) {
        if let Some(rest) = response.strip_prefix("[nyx:file:") {
            if let Some(end) = rest.find(']') {
                let path = rest[..end].to_string();
                let body = rest[end + 1..].trim().to_string();
                return (Some(path), body);
            }
        }
        (None, response.to_string())
    }
}
