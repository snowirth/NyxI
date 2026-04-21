use super::*;

impl AppState {
    pub fn ingest_web_search_memory(&self, query: &str, raw_output: &str) {
        let observed_at = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let results = parse_web_search_results(raw_output);
        if results.is_empty() {
            return;
        }

        let mut stored_claims = 0usize;
        for result in results.into_iter().take(5) {
            let publisher = publisher_from_url(&result.url);
            let trust_tier = infer_source_trust_tier(&result.url);
            let checksum = stable_checksum(&format!(
                "{}\n{}\n{}",
                result.title, result.body, result.url
            ));
            let statement = web_result_statement(&result);
            if statement.is_empty() {
                continue;
            }
            let Ok(source_id) = self.db.remember_source(
                "web",
                &result.url,
                Some(result.title.trim()),
                publisher.as_deref(),
                trust_tier,
                Some(&checksum),
                Some(&observed_at),
                Some(query),
            ) else {
                continue;
            };

            let ttl = web_claim_ttl_secs(query, &result);
            if let Ok(Some(_claim_id)) = self.db.remember_sourced_claim(
                &statement,
                "fact",
                "global",
                &source_id,
                trust_tier,
                0.72,
                Some(ttl),
                Some(&observed_at),
            ) {
                stored_claims += 1;
            }
        }

        if stored_claims > 0 {
            tracing::info!(
                "memory: stored {} sourced claim(s) from web search query '{}'",
                stored_claims,
                crate::trunc(query, 80)
            );
        }
    }
}

pub(super) fn parse_web_search_results(raw_output: &str) -> Vec<ParsedWebSearchResult> {
    raw_output
        .split("\n---\n")
        .filter_map(|block| {
            let mut lines = block
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .collect::<Vec<_>>();
            if lines.len() < 2 {
                return None;
            }
            let url_index = lines
                .iter()
                .rposition(|line| line.starts_with("http://") || line.starts_with("https://"))?;
            let url = lines.remove(url_index).to_string();
            let title = lines.first()?.to_string();
            let body = if lines.len() > 1 {
                lines[1..].join(" ")
            } else {
                String::new()
            };
            Some(ParsedWebSearchResult { title, body, url })
        })
        .collect()
}

pub(super) fn web_result_statement(result: &ParsedWebSearchResult) -> String {
    let title = clean_web_claim_snippet(&result.title);
    let raw_body = clean_web_claim_snippet(&result.body);
    let body = strip_duplicate_title_prefix(&raw_body, &title);

    if !body.is_empty() {
        if title.is_empty()
            || looks_like_nonfactual_web_title(&title)
            || body.len() >= title.len().saturating_div(2).max(24)
        {
            return body;
        }
        return format!("{} — {}", title, body);
    }

    if looks_like_nonfactual_web_title(&title) {
        String::new()
    } else {
        title
    }
}

pub(super) fn clean_web_claim_snippet(text: &str) -> String {
    let compact = text
        .replace(['\n', '\r', '\t'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(|ch: char| matches!(ch, ' ' | '|' | '-' | '—'))
        .trim()
        .to_string();
    compact
        .trim_end_matches('.')
        .trim_end_matches("...")
        .trim()
        .to_string()
}

pub(super) fn looks_like_nonfactual_web_title(title: &str) -> bool {
    let lower = title.to_lowercase();
    lower.contains('?')
        || lower.starts_with("what ")
        || lower.starts_with("what's ")
        || lower.starts_with("how ")
        || lower.starts_with("why ")
        || lower.starts_with("who ")
        || lower.starts_with("which ")
        || lower.starts_with("introducing ")
        || lower.starts_with("announcing ")
}

pub(super) fn strip_duplicate_title_prefix(body: &str, title: &str) -> String {
    if body.is_empty() {
        return String::new();
    }

    let normalized_body = normalize_web_claim_match(body);
    let normalized_title = normalize_web_claim_match(title);
    if normalized_title.is_empty() {
        return body.to_string();
    }

    if normalized_body == normalized_title {
        return String::new();
    }

    let body_tokens = normalized_body.split_whitespace().collect::<Vec<_>>();
    let title_tokens = normalized_title.split_whitespace().collect::<Vec<_>>();
    if title_tokens.len() >= 3
        && body_tokens.len() <= title_tokens.len() + 4
        && body_tokens.starts_with(&title_tokens)
    {
        return String::new();
    }

    body.to_string()
}

pub(super) fn normalize_web_claim_match(text: &str) -> String {
    text.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn web_claim_ttl_secs(query: &str, result: &ParsedWebSearchResult) -> i64 {
    let combined = format!(
        "{} {} {}",
        query.to_lowercase(),
        result.title.to_lowercase(),
        result.body.to_lowercase()
    );
    if combined.contains("today")
        || combined.contains("current")
        || combined.contains("latest")
        || combined.contains("breaking")
        || combined.contains("price")
        || combined.contains("version")
        || combined.contains("ceo")
        || combined.contains("president")
        || combined.contains("release")
    {
        3 * 24 * 60 * 60
    } else {
        14 * 24 * 60 * 60
    }
}

pub(super) fn infer_source_trust_tier(url: &str) -> f64 {
    let lower = url.to_lowercase();
    if lower.contains(".gov/") || lower.ends_with(".gov") {
        0.9
    } else if lower.contains(".edu/") || lower.ends_with(".edu") {
        0.85
    } else if lower.contains("docs.") || lower.contains("/docs/") || lower.contains("github.com") {
        0.78
    } else {
        0.66
    }
}

pub(super) fn publisher_from_url(url: &str) -> Option<String> {
    let host = url
        .split("//")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("")
        .trim()
        .trim_start_matches("www.")
        .to_string();
    if host.is_empty() { None } else { Some(host) }
}

pub(super) fn stable_checksum(text: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub(super) fn normalize_memory_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_lowercase()
}

pub(super) fn normalize_refresh_statement(statement: &str) -> String {
    statement
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}
