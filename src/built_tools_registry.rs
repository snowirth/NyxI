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

pub static CORE_BUILT_TOOLS: &[CoreBuiltTool] = &[
    CoreBuiltTool {
        name: "live_autonomy_probe_1776788813",
        filename: "tools/live_autonomy_probe_1776788813.py",
        description: "Recovered registration for self-built tool live_autonomy_probe_1776788813",
    },
    CoreBuiltTool {
        name: "make_markdown_todo_extractor_tool",
        filename: "tools/make_markdown_todo_extractor_tool.py",
        description: "Extract markdown checkbox items into structured todo entries.",
    },
    CoreBuiltTool {
        name: "tool_overview_probe_fa3f1232613343099b12212ee1de3ffc",
        filename: "tools/tool_overview_probe_fa3f1232613343099b12212ee1de3ffc.py",
        description: "Probe tool for readiness overview",
    },
    CoreBuiltTool {
        name: "tool_overview_probe_3147657fd099471fa566251c778a0e1e",
        filename: "tools/tool_overview_probe_3147657fd099471fa566251c778a0e1e.py",
        description: "Probe tool for readiness overview",
    },
    CoreBuiltTool {
        name: "live_autonomy_probe_1776787615",
        filename: "tools/live_autonomy_probe_1776787615.py",
        description: "Recovered registration for self-built tool live_autonomy_probe_1776787615",
    },
    CoreBuiltTool {
        name: "failing_probe_a407dc568ce2480f971eff6033b7f01c",
        filename: "tools/failing_probe_a407dc568ce2480f971eff6033b7f01c.py",
        description: "Recovered registration for self-built tool failing_probe_a407dc568ce2480f971eff6033b7f01c",
    },
    CoreBuiltTool {
        name: "slugifier",
        filename: "tools/slugifier.py",
        description: "Converts a given string into a URL-friendly slug by converting to lowercase, removing special characters, and replacing spaces with hyphens.",
    },
];
