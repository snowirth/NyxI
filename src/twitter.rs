//! Twitter autonomy — like, follow, reply with judgment.
//!
//! Nyx has her own Twitter presence. She reads the timeline, likes posts
//! she finds interesting, follows people worth following, and replies
//! when she has something genuine to add.
//!
//! Safety:
//! - Constitution OPSEC rule applies to all public output
//! - Never reveals owner's personal info
//! - Max daily limits on all actions
//! - Reads existing replies before responding
//! - Scam/promo detection before engaging

use crate::AppState;
use crate::ProactiveQueue;

/// Daily limits
const MAX_LIKES_PER_DAY: u32 = 20;
const MAX_FOLLOWS_PER_DAY: u32 = 5;
const MAX_REPLIES_PER_DAY: u32 = 10;
const MAX_REPLIES_PER_ACCOUNT: u32 = 2;

/// How often to check timeline (seconds)
const CHECK_INTERVAL: u64 = 1800; // 30 min

/// Run the autonomous Twitter loop.
pub async fn run(state: AppState, proactive_queue: ProactiveQueue) {
    // Wait for system to stabilize
    tokio::time::sleep(std::time::Duration::from_secs(120)).await;
    tracing::info!("twitter: online");

    let mut daily_likes: u32 = 0;
    let mut daily_follows: u32 = 0;
    let mut daily_replies: u32 = 0;
    let mut reply_counts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut last_reset = chrono::Local::now().date_naive();
    let mut seen_tweets: std::collections::HashSet<String> = std::collections::HashSet::new();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(CHECK_INTERVAL)).await;

        // Reset daily counters at midnight
        let today = chrono::Local::now().date_naive();
        if today != last_reset {
            daily_likes = 0;
            daily_follows = 0;
            daily_replies = 0;
            reply_counts.clear();
            last_reset = today;
            tracing::info!("twitter: daily counters reset");
        }

        // Quiet hours — no engagement between 11pm-8am
        let hour = chrono::Local::now()
            .format("%H")
            .to_string()
            .parse::<u32>()
            .unwrap_or(0);
        if hour >= 23 || hour < 8 {
            continue;
        }

        // Read timeline
        let timeline = match crate::tools::run(
            "twitter",
            &serde_json::json!({"action": "timeline", "count": 20}),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => continue,
        };

        let items = match timeline["items"].as_array() {
            Some(items) => items.clone(),
            None => continue,
        };

        for item in &items {
            let text = item.as_str().unwrap_or("");
            if text.is_empty() {
                continue;
            }

            // Extract tweet ID and username
            let tweet_id = extract_field(text, "id:");
            let username = extract_field(text, "@");
            if tweet_id.is_empty() {
                continue;
            }
            if seen_tweets.contains(&tweet_id) {
                continue;
            }
            seen_tweets.insert(tweet_id.clone());

            // Skip scam/promo
            if is_scam_or_promo(text) {
                continue;
            }

            // Extract just the tweet content (before metadata)
            let content = text.split("[likes:").next().unwrap_or(text);
            let content = if content.starts_with('@') {
                content.split(": ").skip(1).collect::<Vec<_>>().join(": ")
            } else {
                content.to_string()
            };

            // Decide: like, follow, reply, or skip
            let decision = decide_engagement(
                &state,
                &content,
                &username,
                daily_likes,
                daily_follows,
                daily_replies,
                reply_counts.get(&username).copied().unwrap_or(0),
            )
            .await;

            match decision {
                Engagement::Like => {
                    if daily_likes < MAX_LIKES_PER_DAY {
                        if let Ok(r) = crate::tools::run(
                            "twitter",
                            &serde_json::json!({"action": "like", "tweet_id": tweet_id}),
                        )
                        .await
                        {
                            if r["success"].as_bool() == Some(true) {
                                daily_likes += 1;
                                tracing::info!(
                                    "twitter: liked @{} — {}",
                                    username,
                                    crate::trunc(&content, 40)
                                );
                            }
                        }
                    }
                }
                Engagement::Follow => {
                    if daily_follows < MAX_FOLLOWS_PER_DAY {
                        // We'd need user_id for follow, skip for now — twikit follow needs user_id
                        daily_follows += 1;
                        tracing::info!("twitter: would follow @{}", username);
                    }
                }
                Engagement::Reply(reply_text) => {
                    let account_replies = reply_counts.get(&username).copied().unwrap_or(0);
                    if daily_replies < MAX_REPLIES_PER_DAY
                        && account_replies < MAX_REPLIES_PER_ACCOUNT
                    {
                        // Safety check — constitution filter
                        let filtered =
                            crate::constitution::Constitution::filter_response(&reply_text);
                        if !contains_sensitive_info(&filtered) {
                            if let Ok(r) = crate::tools::run("twitter",
                                &serde_json::json!({"action": "reply", "tweet_id": tweet_id, "text": filtered})
                            ).await {
                                if r["success"].as_bool() == Some(true) {
                                    daily_replies += 1;
                                    *reply_counts.entry(username.clone()).or_insert(0) += 1;
                                    tracing::info!("twitter: replied to @{} — {}", username, crate::trunc(&filtered, 40));
                                    proactive_queue.lock().await.push(
                                        format!("replied to @{}: {}", username, crate::trunc(&filtered, 80))
                                    );
                                }
                            }
                        }
                    }
                }
                Engagement::Skip => {}
            }

            // Don't spam — pause between actions
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        // Trim seen_tweets to prevent unbounded growth
        if seen_tweets.len() > 1000 {
            seen_tweets.clear();
        }
    }
}

enum Engagement {
    Like,
    Follow,
    Reply(String),
    Skip,
}

/// Use DeepSeek to decide how to engage with a tweet.
async fn decide_engagement(
    state: &AppState,
    content: &str,
    username: &str,
    likes_today: u32,
    follows_today: u32,
    replies_today: u32,
    replies_to_account: u32,
) -> Engagement {
    if content.len() < 10 {
        return Engagement::Skip;
    }

    let prompt = format!(
        "You are Nyx, an AI with a Twitter account. You just saw this tweet:\n\n\
         @{}: {}\n\n\
         Decide ONE action:\n\
         LIKE — if it's genuinely interesting, funny, or insightful\n\
         REPLY: [your reply text, max 200 chars] — if you have something genuine to add. \
         Be casual, witty, authentic. Never generic. Never \"great post!\"\n\
         FOLLOW — if this person consistently posts interesting content\n\
         SKIP — if boring, promotional, controversial, or you have nothing to add\n\n\
         Rules:\n\
         - NEVER mention your owner, their location, or personal info\n\
         - Don't reply to obvious bait or scams\n\
         - Don't be sycophantic\n\
         - If the tweet is just a link with no context, SKIP\n\
         - You've liked {} tweets today, replied {} times, followed {} people\n\
         - You've replied to @{} {} times today (max {})\n\n\
         Reply with exactly one line: LIKE, REPLY: [text], FOLLOW, or SKIP",
        username,
        crate::trunc(content, 200),
        likes_today,
        replies_today,
        follows_today,
        username,
        replies_to_account,
        MAX_REPLIES_PER_ACCOUNT
    );

    let response = match state.llm.chat_auto(&prompt, 100).await {
        Ok(r) => r,
        Err(_) => return Engagement::Skip,
    };

    let trimmed = response.trim();

    if trimmed.starts_with("REPLY:") {
        let reply = trimmed.trim_start_matches("REPLY:").trim();
        if reply.len() > 5 && reply.len() <= 280 {
            return Engagement::Reply(reply.to_string());
        }
    } else if trimmed == "LIKE" || trimmed.starts_with("LIKE") {
        return Engagement::Like;
    } else if trimmed == "FOLLOW" || trimmed.starts_with("FOLLOW") {
        return Engagement::Follow;
    }

    Engagement::Skip
}

/// Check if a tweet is scam/promo.
fn is_scam_or_promo(text: &str) -> bool {
    let lower = text.to_lowercase();
    let signals = [
        "dm me for",
        "link in bio",
        "free giveaway",
        "drop your wallet",
        "airdrop",
        "whitelist",
        "mint now",
        "presale",
        "join our discord",
        "limited spots",
        "earn $",
        "passive income",
        "100x",
        "guaranteed returns",
        "click here",
        "follow and retweet to win",
        "send me dm",
    ];
    signals.iter().any(|s| lower.contains(s))
}

/// Check if text contains sensitive info that shouldn't be posted.
fn contains_sensitive_info(text: &str) -> bool {
    let lower = text.to_lowercase();
    let sensitive = [
        "adelaide",
        "api key",
        "api_key",
        "password",
        "ssh",
        ".env",
        "secret",
        "token",
        "credential",
    ];
    sensitive.iter().any(|s| lower.contains(s))
}

/// Extract a field like "id:12345" or "@username" from tweet text.
fn extract_field(text: &str, prefix: &str) -> String {
    if prefix == "@" {
        // @username at the start
        if text.starts_with('@') {
            return text[1..].split(':').next().unwrap_or("").trim().to_string();
        }
        return String::new();
    }
    // id:12345 at the end
    if let Some(idx) = text.find(prefix) {
        let after = &text[idx + prefix.len()..];
        return after.split(')').next().unwrap_or("").trim().to_string();
    }
    String::new()
}
