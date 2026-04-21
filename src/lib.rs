//! Nyx library — shared runtime for the binary and integration tests.

/// Truncate a string at a valid UTF-8 char boundary.
pub fn trunc(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

pub fn extract_file_marker(response: &str) -> (Option<String>, String) {
    if let Some(rest) = response.strip_prefix("[nyx:file:") {
        if let Some(end) = rest.find(']') {
            let path = rest[..end].to_string();
            let body = rest[end + 1..].trim().to_string();
            return (Some(path), body);
        }
    }
    (None, response.to_string())
}

pub mod autonomy;
pub mod awareness;
pub mod built_tools_registry;
pub mod consciousness;
pub mod constitution;
pub mod db;
pub mod embed;
pub mod filewatcher;
pub mod forge;
pub mod improvement;
pub mod intent;
pub mod interaction;
pub mod llm;
#[path = "../benchmarks/longmemeval/mod.rs"]
pub mod longmemeval_benchmark;
pub mod mcp;
#[path = "../benchmarks/memory/mod.rs"]
pub mod memory_benchmark;
pub mod overnight;
pub mod patterns;
pub mod plugins;
pub mod runtime;
pub mod soul;
pub mod swarm;
pub mod tools;
pub mod twitter;
pub mod voice;
pub mod web;
pub mod world;

pub use runtime::{
    AppState, Config, ProactiveQueue, autodetect_location, build_state, load_dotenv,
    spawn_background_tasks,
};

pub(crate) use runtime::BOOTSTRAP_ALLOWED_IMPORTS;
