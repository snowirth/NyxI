//! Auto-generated registry of self-built tools promoted into core.
//!
//! Forge rewrites this file after a successful build-and-smoke-test cycle.
//! The runtime still uses manifest files for immediate access; this registry
//! is the protected-core copy that becomes part of the compiled system.

#[derive(Debug, Clone, Copy)]
pub struct CoreBuiltTool {
    pub name: &'static str,
    pub filename: &'static str,
    pub description: &'static str,
}

pub static CORE_BUILT_TOOLS: &[CoreBuiltTool] = &[];
