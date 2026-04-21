use anyhow::{Context, Result};

use crate::db::Db;

use super::projects::{ProjectGraphChangeSet, ProjectGraphSnapshot, WorldFocusSummary};

pub const PROJECT_GRAPH_STATE_KEY: &str = "world.project_graph.latest";
pub const PROJECT_GRAPH_CHANGES_STATE_KEY: &str = "world.project_graph.changes.latest";

pub fn compile_and_persist_project_graph(db: &Db, source: &str) -> Result<ProjectGraphSnapshot> {
    let previous = load_project_graph(db);
    let snapshot = super::projects::compile_project_graph(db, source)?;
    let changes = super::projects::compile_project_graph_changes(previous.as_ref(), &snapshot);
    persist_project_graph(db, &snapshot)?;
    persist_project_graph_changes(db, &changes)?;
    Ok(snapshot)
}

pub fn load_project_graph(db: &Db) -> Option<ProjectGraphSnapshot> {
    let raw = db.get_state(PROJECT_GRAPH_STATE_KEY)?;
    serde_json::from_str(&raw).ok()
}

pub fn load_world_focus(db: &Db) -> WorldFocusSummary {
    load_project_graph(db)
        .map(|snapshot| super::projects::derive_world_focus(&snapshot))
        .unwrap_or_default()
}

pub fn load_project_graph_changes(db: &Db) -> Option<ProjectGraphChangeSet> {
    let raw = db.get_state(PROJECT_GRAPH_CHANGES_STATE_KEY)?;
    serde_json::from_str(&raw).ok()
}

pub fn persist_project_graph(db: &Db, snapshot: &ProjectGraphSnapshot) -> Result<()> {
    let raw = serde_json::to_string(snapshot).context("serialize project graph snapshot")?;
    db.set_state(PROJECT_GRAPH_STATE_KEY, &raw);
    Ok(())
}

pub fn persist_project_graph_changes(db: &Db, changes: &ProjectGraphChangeSet) -> Result<()> {
    let raw = serde_json::to_string(changes).context("serialize project graph changes")?;
    db.set_state(PROJECT_GRAPH_CHANGES_STATE_KEY, &raw);
    Ok(())
}
