use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    autonomy::{self, TaskStatus},
    db::{Db, ReplayFailureClusterRecord, SystemIncidentRecord},
};

mod changes;

pub use changes::{
    compile_project_graph_changes, derive_world_focus, task_relevant_change_summary,
};

pub const PROJECT_GRAPH_SCHEMA_VERSION: &str = "nyx_project_graph.v1";
pub const PROJECT_GRAPH_CHANGES_SCHEMA_VERSION: &str = "nyx_project_graph_changes.v1";

const PENDING_TASK_LIMIT: usize = 64;
const RUNNING_TASK_LIMIT: usize = 32;
const FAILURE_CLUSTER_LIMIT: usize = 8;
const INCIDENT_LIMIT: usize = 8;
const GROWTH_EVENT_LIMIT: usize = 8;
const SELF_MODEL_SNAPSHOT_LIMIT: usize = 6;
const WORKSTREAM_TASK_PREVIEW_LIMIT: usize = 4;
const PROJECT_MILESTONE_LIMIT: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphSnapshot {
    pub schema_version: String,
    pub compiled_at: String,
    pub source: String,
    pub counts: ProjectGraphCounts,
    pub projects: Vec<ProjectNode>,
    pub blockers: Vec<ProjectBlocker>,
    pub resume_queue: Vec<ResumeQueueItem>,
    pub recent_incidents: Vec<SystemIncidentRecord>,
    pub recent_growth_events: Vec<autonomy::GrowthEvent>,
    pub recent_self_model_snapshots: Vec<SelfModelSnapshotRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphCounts {
    pub projects: usize,
    pub active_goals: usize,
    pub workstreams: usize,
    pub pending_tasks: usize,
    pub ready_tasks: usize,
    pub running_tasks: usize,
    pub stale_running_tasks: usize,
    pub blockers: usize,
    pub resume_queue: usize,
    pub recent_incidents: usize,
    pub recent_growth_events: usize,
    pub recent_self_model_snapshots: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectNode {
    pub id: String,
    pub title: String,
    pub status: String,
    pub summary: String,
    pub active_goal_count: usize,
    pub workstreams: Vec<ProjectWorkstream>,
    pub milestones: Vec<ProjectMilestone>,
    pub blockers: Vec<String>,
    pub task_dependencies: Vec<ProjectTaskDependency>,
    pub last_activity_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectWorkstream {
    pub id: String,
    pub goal_id: Option<i64>,
    pub title: String,
    pub source: String,
    pub status: String,
    pub priority: f64,
    pub details: Option<String>,
    pub summary: String,
    pub task_counts: WorkstreamTaskCounts,
    pub blockers: Vec<String>,
    pub next_tasks: Vec<WorkstreamTaskPreview>,
    pub running_tasks: Vec<WorkstreamTaskPreview>,
    pub last_activity_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkstreamTaskCounts {
    pub pending: usize,
    pub ready: usize,
    pub running: usize,
    pub stale_running: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkstreamTaskPreview {
    pub id: i64,
    pub kind: String,
    pub title: String,
    pub status: String,
    pub priority: f64,
    pub tool: Option<String>,
    pub target: Option<String>,
    pub scheduled_for: Option<String>,
    pub last_run_at: Option<String>,
    pub latest_outcome: Option<String>,
    pub blocked_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeQueueItem {
    pub task_id: i64,
    pub goal_id: Option<i64>,
    pub goal_title: Option<String>,
    pub kind: String,
    pub title: String,
    pub status: String,
    pub priority: f64,
    pub tool: Option<String>,
    pub target: Option<String>,
    pub scheduled_for: Option<String>,
    pub last_run_at: Option<String>,
    pub latest_outcome: Option<String>,
    pub latest_summary: Option<String>,
    pub resume_reason: String,
    pub blocked_by: Vec<String>,
    #[serde(default)]
    pub active_blocker_count: usize,
    #[serde(default)]
    pub ready_now: bool,
    #[serde(default)]
    pub operator_state: String,
    #[serde(default)]
    pub retry_hint: Option<String>,
    #[serde(default)]
    pub top_blocker_summary: Option<String>,
    #[serde(default)]
    pub top_blocker_urgency: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectBlocker {
    pub id: String,
    pub kind: String,
    pub severity: String,
    pub status: String,
    pub summary: String,
    pub source: String,
    pub task_id: Option<i64>,
    pub task_kind: Option<String>,
    pub target: Option<String>,
    pub observed_at: String,
    #[serde(default)]
    pub age_bucket: String,
    #[serde(default)]
    pub urgency: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectTaskDependency {
    pub task_id: i64,
    pub task_kind: String,
    pub depends_on: String,
    pub dependency_kind: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectMilestone {
    pub id: String,
    pub kind: String,
    pub title: String,
    pub summary: String,
    pub target: Option<String>,
    pub recorded_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfModelSnapshotRef {
    pub id: Option<i64>,
    pub source: Option<String>,
    pub trigger_kind: Option<String>,
    pub trigger_target: Option<String>,
    pub summary: Option<String>,
    pub capability_count: Option<i64>,
    pub constraint_count: Option<i64>,
    pub active_goal_count: Option<i64>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct WorldFocusSummary {
    pub active_project_title: Option<String>,
    pub active_project_status: Option<String>,
    pub active_workstream_title: Option<String>,
    pub active_workstream_status: Option<String>,
    pub resume_focus_title: Option<String>,
    pub resume_focus_kind: Option<String>,
    pub resume_focus_status: Option<String>,
    pub resume_focus_reason: Option<String>,
    pub resume_focus_blocker_count: usize,
    pub blocker_count: usize,
    pub top_blocker_summary: Option<String>,
}

impl WorldFocusSummary {
    pub fn is_empty(&self) -> bool {
        self.active_project_title.is_none()
            && self.active_workstream_title.is_none()
            && self.resume_focus_title.is_none()
            && self.blocker_count == 0
            && self.top_blocker_summary.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphChangeSet {
    pub schema_version: String,
    pub compared_at: String,
    pub previous: Option<ProjectGraphSnapshotMarker>,
    pub current: ProjectGraphSnapshotMarker,
    pub changed: bool,
    pub summary: Vec<String>,
    pub focus_shift: Option<ProjectGraphFocusShift>,
    pub count_changes: Vec<ProjectGraphCountChange>,
    pub workstream_changes: Vec<ProjectGraphWorkstreamChange>,
    pub blocker_changes: Vec<ProjectGraphBlockerChange>,
    pub resume_changes: Vec<ProjectGraphResumeChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphSnapshotMarker {
    pub compiled_at: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphFocusShift {
    pub previous: WorldFocusSummary,
    pub current: WorldFocusSummary,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphCountChange {
    pub field: String,
    pub from: usize,
    pub to: usize,
    pub delta: isize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphWorkstreamChange {
    pub id: String,
    pub title: String,
    pub change: String,
    pub from_status: Option<String>,
    pub to_status: Option<String>,
    pub ready_from: usize,
    pub ready_to: usize,
    pub running_from: usize,
    pub running_to: usize,
    pub blocker_count_from: usize,
    pub blocker_count_to: usize,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphBlockerChange {
    pub id: String,
    pub kind: String,
    pub change: String,
    pub from_status: Option<String>,
    pub to_status: Option<String>,
    pub target: Option<String>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectGraphResumeChange {
    pub task_id: i64,
    pub title: String,
    pub kind: String,
    pub change: String,
    pub from_status: Option<String>,
    pub to_status: Option<String>,
    pub goal_title: Option<String>,
    pub summary: String,
}

impl ProjectGraphChangeSet {}

#[derive(Debug, Clone)]
struct LatestTaskOutcome {
    outcome: String,
    summary: String,
    executed: bool,
    verified: Option<bool>,
    rollback_reason: Option<crate::runtime::verifier::RollbackReason>,
}

pub fn compile_project_graph(db: &Db, source: &str) -> Result<ProjectGraphSnapshot> {
    let compiled_at = now_timestamp();
    let active_goals = db.list_active_autonomy_goals()?;
    let pending_tasks =
        db.list_autonomy_tasks_with_status(TaskStatus::Pending, PENDING_TASK_LIMIT)?;
    let running_tasks =
        db.list_autonomy_tasks_with_status(TaskStatus::Running, RUNNING_TASK_LIMIT)?;
    let stale_before = stale_running_before();
    let stale_running_tasks =
        db.list_stale_running_autonomy_tasks(&stale_before, RUNNING_TASK_LIMIT)?;
    let stale_running_ids = stale_running_tasks
        .iter()
        .map(|task| task.id)
        .collect::<HashSet<_>>();
    let recent_incidents =
        db.list_recent_system_incidents_filtered(None, None, None, INCIDENT_LIMIT)?;
    let recent_growth_events =
        db.list_recent_growth_events_filtered(None, None, None, None, GROWTH_EVENT_LIMIT)?;
    let recent_self_model_snapshots = db
        .list_recent_self_model_snapshots(SELF_MODEL_SNAPSHOT_LIMIT)?
        .into_iter()
        .map(SelfModelSnapshotRef::from_value)
        .collect::<Vec<_>>();
    let failure_clusters =
        db.list_recent_replay_failure_clusters_filtered(None, None, None, FAILURE_CLUSTER_LIMIT)?;

    let pending_task_count = db.count_autonomy_tasks_with_status(TaskStatus::Pending)?;
    let ready_task_count = db.count_ready_autonomy_tasks()?;
    let running_task_count = db.count_autonomy_tasks_with_status(TaskStatus::Running)?;
    let stale_running_task_count = db.count_stale_running_autonomy_tasks(&stale_before)?;

    let latest_outcomes = collect_latest_task_outcomes(db, &pending_tasks, &running_tasks)?;
    let (blockers, task_blockers) = collect_blockers(
        db,
        &pending_tasks,
        &running_tasks,
        &stale_running_ids,
        &failure_clusters,
        &recent_incidents,
        &compiled_at,
    )?;
    let blocker_index = blockers
        .iter()
        .cloned()
        .map(|blocker| (blocker.id.clone(), blocker))
        .collect::<HashMap<_, _>>();
    let resume_queue = build_resume_queue(
        &active_goals,
        &pending_tasks,
        &running_tasks,
        &stale_running_ids,
        &task_blockers,
        &latest_outcomes,
        &blocker_index,
        &compiled_at,
    );
    let task_dependencies = build_task_dependencies(&resume_queue, &blocker_index);
    let workstreams = build_workstreams(
        &active_goals,
        &pending_tasks,
        &running_tasks,
        &stale_running_ids,
        &task_blockers,
        &latest_outcomes,
        &blocker_index,
        &compiled_at,
    );
    let milestones = build_milestones(&recent_growth_events, &recent_self_model_snapshots);
    let project_status = summarize_project_status(&blockers, &resume_queue, &workstreams);
    let project_summary = format!(
        "{} workstreams, {} resume items, {} blockers",
        workstreams.len(),
        resume_queue.len(),
        blockers.len()
    );
    let project_last_activity_at = latest_timestamp(
        latest_workstream_activity(&workstreams),
        latest_milestone_activity(&milestones),
    );

    let projects = vec![ProjectNode {
        id: "project:nyx".to_string(),
        title: "Nyx".to_string(),
        status: project_status,
        summary: project_summary,
        active_goal_count: active_goals.len(),
        workstreams: workstreams.clone(),
        milestones,
        blockers: blockers.iter().map(|blocker| blocker.id.clone()).collect(),
        task_dependencies,
        last_activity_at: project_last_activity_at,
    }];

    let counts = ProjectGraphCounts {
        projects: projects.len(),
        active_goals: active_goals.len(),
        workstreams: workstreams.len(),
        pending_tasks: pending_task_count,
        ready_tasks: ready_task_count,
        running_tasks: running_task_count,
        stale_running_tasks: stale_running_task_count,
        blockers: blockers.len(),
        resume_queue: resume_queue.len(),
        recent_incidents: recent_incidents.len(),
        recent_growth_events: recent_growth_events.len(),
        recent_self_model_snapshots: recent_self_model_snapshots.len(),
    };

    Ok(ProjectGraphSnapshot {
        schema_version: PROJECT_GRAPH_SCHEMA_VERSION.to_string(),
        compiled_at,
        source: source.to_string(),
        counts,
        projects,
        blockers,
        resume_queue,
        recent_incidents,
        recent_growth_events,
        recent_self_model_snapshots,
    })
}

fn collect_latest_task_outcomes(
    db: &Db,
    pending_tasks: &[autonomy::Task],
    running_tasks: &[autonomy::Task],
) -> Result<HashMap<i64, LatestTaskOutcome>> {
    let mut outcomes = HashMap::new();
    for task in pending_tasks.iter().chain(running_tasks.iter()) {
        let Some(action_run) = db.list_autonomy_action_runs(task.id)?.into_iter().last() else {
            continue;
        };
        outcomes.insert(
            task.id,
            LatestTaskOutcome {
                outcome: action_run.outcome,
                summary: action_run.summary,
                executed: action_run.executed,
                verified: action_run.verified,
                rollback_reason: action_run.rollback_reason,
            },
        );
    }
    Ok(outcomes)
}

fn collect_blockers(
    db: &Db,
    pending_tasks: &[autonomy::Task],
    running_tasks: &[autonomy::Task],
    stale_running_ids: &HashSet<i64>,
    failure_clusters: &[ReplayFailureClusterRecord],
    recent_incidents: &[SystemIncidentRecord],
    now: &str,
) -> Result<(Vec<ProjectBlocker>, HashMap<i64, Vec<String>>)> {
    let mut blockers = HashMap::<String, ProjectBlocker>::new();
    let mut task_blockers = HashMap::<i64, Vec<String>>::new();
    let all_tasks = pending_tasks
        .iter()
        .chain(running_tasks.iter())
        .collect::<Vec<_>>();

    for task in &all_tasks {
        if let Some(retry_key) = autonomy::project_graph_retry_key(task) {
            if let Some(retry_state) = db.get_autonomy_retry_state(&retry_key)? {
                let blocker = blocker_from_retry_state(task, &retry_state, now);
                let blocker_id = blocker.id.clone();
                blockers.entry(blocker_id.clone()).or_insert(blocker);
                task_blockers.entry(task.id).or_default().push(blocker_id);
            }
        }

        if stale_running_ids.contains(&task.id) {
            let blocker = blocker_from_stale_task(task, now);
            let blocker_id = blocker.id.clone();
            blockers.entry(blocker_id.clone()).or_insert(blocker);
            task_blockers.entry(task.id).or_default().push(blocker_id);
        }
    }

    for cluster in failure_clusters {
        let blocker = blocker_from_failure_cluster(cluster, now);
        let blocker_id = blocker.id.clone();
        blockers
            .entry(blocker_id.clone())
            .or_insert_with(|| blocker.clone());

        for task in &all_tasks {
            if failure_cluster_matches_task(cluster, task) {
                task_blockers
                    .entry(task.id)
                    .or_default()
                    .push(blocker_id.clone());
            }
        }
    }

    for incident in recent_incidents
        .iter()
        .filter(|incident| matches!(incident.severity.as_str(), "warn" | "error" | "critical"))
    {
        let blocker = blocker_from_incident(incident, now);
        blockers
            .entry(blocker.id.clone())
            .or_insert_with(|| blocker.clone());
    }

    for blocker_ids in task_blockers.values_mut() {
        blocker_ids.sort();
        blocker_ids.dedup();
    }

    let mut blocker_list = blockers.into_values().collect::<Vec<_>>();
    blocker_list.sort_by(compare_blockers);
    Ok((blocker_list, task_blockers))
}

fn build_resume_queue(
    active_goals: &[autonomy::Goal],
    pending_tasks: &[autonomy::Task],
    running_tasks: &[autonomy::Task],
    stale_running_ids: &HashSet<i64>,
    task_blockers: &HashMap<i64, Vec<String>>,
    latest_outcomes: &HashMap<i64, LatestTaskOutcome>,
    blocker_index: &HashMap<String, ProjectBlocker>,
    now: &str,
) -> Vec<ResumeQueueItem> {
    let goal_titles = active_goals
        .iter()
        .map(|goal| (goal.id, goal.title.clone()))
        .collect::<HashMap<_, _>>();
    let mut items = pending_tasks
        .iter()
        .chain(running_tasks.iter())
        .map(|task| {
            let blocked_by = task_blockers.get(&task.id).cloned().unwrap_or_default();
            let latest_outcome = latest_outcomes.get(&task.id);
            let blocker_refs = blocked_by
                .iter()
                .filter_map(|blocker_id| blocker_index.get(blocker_id))
                .collect::<Vec<_>>();
            let primary_blocker = blocker_refs
                .iter()
                .copied()
                .min_by(|left, right| compare_blockers(left, right));
            let active_blocker_count = blocker_refs
                .iter()
                .filter(|blocker| blocker_counts_as_active_constraint(blocker))
                .count();
            let retry_hint = latest_outcome
                .and_then(latest_outcome_retry_hint)
                .or_else(|| primary_blocker.and_then(blocker_retry_hint));
            let ready_now = task.status == TaskStatus::Pending
                && task_is_ready(task, now)
                && active_blocker_count == 0;
            let operator_state = classify_resume_item(
                task,
                stale_running_ids.contains(&task.id),
                latest_outcome,
                ready_now,
                active_blocker_count,
                primary_blocker,
            );
            ResumeQueueItem {
                task_id: task.id,
                goal_id: task.goal_id,
                goal_title: task
                    .goal_id
                    .and_then(|goal_id| goal_titles.get(&goal_id).cloned()),
                kind: task.kind.clone(),
                title: task.title.clone(),
                status: task.status.as_str().to_string(),
                priority: task.priority,
                tool: task.tool.clone(),
                target: autonomy::project_graph_target(task),
                scheduled_for: task.scheduled_for.clone(),
                last_run_at: task.last_run_at.clone(),
                latest_outcome: latest_outcome.map(|value| value.outcome.clone()),
                latest_summary: latest_outcome.map(|value| value.summary.clone()),
                resume_reason: resume_reason(
                    task,
                    operator_state.as_str(),
                    retry_hint.as_deref(),
                    primary_blocker.map(|blocker| blocker.summary.as_str()),
                ),
                blocked_by,
                active_blocker_count,
                ready_now,
                operator_state,
                retry_hint,
                top_blocker_summary: primary_blocker.map(|blocker| blocker.summary.clone()),
                top_blocker_urgency: primary_blocker.map(|blocker| blocker.urgency.clone()),
            }
        })
        .collect::<Vec<_>>();

    items.sort_by(compare_resume_items);
    items
}

fn build_task_dependencies(
    resume_queue: &[ResumeQueueItem],
    blocker_index: &HashMap<String, ProjectBlocker>,
) -> Vec<ProjectTaskDependency> {
    let mut dependencies = Vec::new();
    for item in resume_queue {
        for blocker_id in &item.blocked_by {
            if let Some(blocker) = blocker_index.get(blocker_id) {
                if !blocker_counts_as_active_constraint(blocker) {
                    continue;
                }
                dependencies.push(ProjectTaskDependency {
                    task_id: item.task_id,
                    task_kind: item.kind.clone(),
                    depends_on: blocker.id.clone(),
                    dependency_kind: blocker.kind.clone(),
                    reason: blocker.summary.clone(),
                });
            }
        }
    }
    dependencies.sort_by(|left, right| {
        left.task_id
            .cmp(&right.task_id)
            .then_with(|| left.depends_on.cmp(&right.depends_on))
    });
    dependencies
}

fn build_workstreams(
    active_goals: &[autonomy::Goal],
    pending_tasks: &[autonomy::Task],
    running_tasks: &[autonomy::Task],
    stale_running_ids: &HashSet<i64>,
    task_blockers: &HashMap<i64, Vec<String>>,
    latest_outcomes: &HashMap<i64, LatestTaskOutcome>,
    blocker_index: &HashMap<String, ProjectBlocker>,
    now: &str,
) -> Vec<ProjectWorkstream> {
    let all_tasks = pending_tasks
        .iter()
        .chain(running_tasks.iter())
        .collect::<Vec<_>>();
    let active_goal_ids = active_goals
        .iter()
        .map(|goal| goal.id)
        .collect::<HashSet<_>>();

    let mut workstreams = active_goals
        .iter()
        .map(|goal| {
            let tasks = all_tasks
                .iter()
                .copied()
                .filter(|task| task.goal_id == Some(goal.id))
                .collect::<Vec<_>>();
            workstream_from_goal(
                Some(goal),
                &tasks,
                stale_running_ids,
                task_blockers,
                latest_outcomes,
                blocker_index,
                now,
            )
        })
        .collect::<Vec<_>>();

    let unassigned_tasks = all_tasks
        .iter()
        .copied()
        .filter(|task| {
            task.goal_id
                .map(|goal_id| !active_goal_ids.contains(&goal_id))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    if !unassigned_tasks.is_empty() {
        workstreams.push(workstream_from_goal(
            None,
            &unassigned_tasks,
            stale_running_ids,
            task_blockers,
            latest_outcomes,
            blocker_index,
            now,
        ));
    }

    workstreams.sort_by(compare_workstreams);
    workstreams
}

fn workstream_from_goal(
    goal: Option<&autonomy::Goal>,
    tasks: &[&autonomy::Task],
    stale_running_ids: &HashSet<i64>,
    task_blockers: &HashMap<i64, Vec<String>>,
    latest_outcomes: &HashMap<i64, LatestTaskOutcome>,
    blocker_index: &HashMap<String, ProjectBlocker>,
    now: &str,
) -> ProjectWorkstream {
    let mut next_tasks = tasks
        .iter()
        .copied()
        .filter(|task| task.status == TaskStatus::Pending)
        .map(|task| task_preview(task, task_blockers, latest_outcomes))
        .collect::<Vec<_>>();
    next_tasks.sort_by(compare_task_previews);
    next_tasks.truncate(WORKSTREAM_TASK_PREVIEW_LIMIT);

    let mut running_tasks = tasks
        .iter()
        .copied()
        .filter(|task| task.status == TaskStatus::Running)
        .map(|task| task_preview(task, task_blockers, latest_outcomes))
        .collect::<Vec<_>>();
    running_tasks.sort_by(compare_running_task_previews);
    running_tasks.truncate(WORKSTREAM_TASK_PREVIEW_LIMIT);

    let pending_count = tasks
        .iter()
        .copied()
        .filter(|task| task.status == TaskStatus::Pending)
        .count();
    let ready_count = tasks
        .iter()
        .copied()
        .filter(|task| {
            task.status == TaskStatus::Pending
                && task_is_ready(task, now)
                && !task_has_active_blockers(task.id, task_blockers, blocker_index)
        })
        .count();
    let running_count = tasks
        .iter()
        .copied()
        .filter(|task| task.status == TaskStatus::Running)
        .count();
    let stale_running_count = tasks
        .iter()
        .copied()
        .filter(|task| stale_running_ids.contains(&task.id))
        .count();

    let mut blocker_ids = tasks
        .iter()
        .flat_map(|task| {
            task_blockers
                .get(&task.id)
                .into_iter()
                .flat_map(|ids| ids.iter())
                .filter(|blocker_id| {
                    blocker_index
                        .get(*blocker_id)
                        .map(blocker_counts_as_active_constraint)
                        .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    blocker_ids.sort();
    blocker_ids.dedup();

    let last_activity_at = tasks.iter().fold(
        goal.map(|value| value.updated_at.clone()),
        |current, task| {
            latest_timestamp(
                current,
                latest_timestamp(Some(task.updated_at.clone()), task.last_run_at.clone()),
            )
        },
    );
    let priority = goal
        .map(|value| value.priority)
        .unwrap_or_else(|| tasks.iter().map(|task| task.priority).fold(0.0, f64::max));
    let title = goal
        .map(|value| value.title.clone())
        .unwrap_or_else(|| "Resume backlog".to_string());
    let source = goal
        .map(|value| value.source.clone())
        .unwrap_or_else(|| "world_compiler".to_string());
    let details = goal.and_then(|value| value.details.clone());
    let status = if !blocker_ids.is_empty() {
        "blocked"
    } else if running_count > 0 || pending_count > 0 {
        "active"
    } else {
        "tracked"
    };
    let summary = format!(
        "{} pending ({} ready), {} running, {} blockers",
        pending_count,
        ready_count,
        running_count,
        blocker_ids.len()
    );

    ProjectWorkstream {
        id: goal
            .map(|value| format!("goal:{}", value.id))
            .unwrap_or_else(|| "workstream:unassigned".to_string()),
        goal_id: goal.map(|value| value.id),
        title,
        source,
        status: status.to_string(),
        priority,
        details,
        summary,
        task_counts: WorkstreamTaskCounts {
            pending: pending_count,
            ready: ready_count,
            running: running_count,
            stale_running: stale_running_count,
        },
        blockers: blocker_ids,
        next_tasks,
        running_tasks,
        last_activity_at,
    }
}

fn task_preview(
    task: &autonomy::Task,
    task_blockers: &HashMap<i64, Vec<String>>,
    latest_outcomes: &HashMap<i64, LatestTaskOutcome>,
) -> WorkstreamTaskPreview {
    WorkstreamTaskPreview {
        id: task.id,
        kind: task.kind.clone(),
        title: task.title.clone(),
        status: task.status.as_str().to_string(),
        priority: task.priority,
        tool: task.tool.clone(),
        target: autonomy::project_graph_target(task),
        scheduled_for: task.scheduled_for.clone(),
        last_run_at: task.last_run_at.clone(),
        latest_outcome: latest_outcomes
            .get(&task.id)
            .map(|value| value.outcome.clone()),
        blocked_by: task_blockers.get(&task.id).cloned().unwrap_or_default(),
    }
}

fn build_milestones(
    growth_events: &[autonomy::GrowthEvent],
    self_model_snapshots: &[SelfModelSnapshotRef],
) -> Vec<ProjectMilestone> {
    let mut milestones = growth_events
        .iter()
        .filter(|event| event.success)
        .map(|event| ProjectMilestone {
            id: format!("growth:{}", event.id),
            kind: "growth_event".to_string(),
            title: event.kind.clone(),
            summary: event.summary.clone(),
            target: event.target.clone(),
            recorded_at: event.created_at.clone(),
        })
        .collect::<Vec<_>>();

    milestones.extend(self_model_snapshots.iter().filter_map(|snapshot| {
        let recorded_at = snapshot.created_at.clone()?;
        Some(ProjectMilestone {
            id: format!("self_model:{}", snapshot.id.unwrap_or_default()),
            kind: "self_model_snapshot".to_string(),
            title: snapshot
                .trigger_kind
                .clone()
                .unwrap_or_else(|| "self_model_snapshot".to_string()),
            summary: snapshot
                .summary
                .clone()
                .unwrap_or_else(|| "captured self-model snapshot".to_string()),
            target: snapshot.trigger_target.clone(),
            recorded_at,
        })
    }));

    milestones.sort_by(|left, right| right.recorded_at.cmp(&left.recorded_at));
    milestones.truncate(PROJECT_MILESTONE_LIMIT);
    milestones
}

fn blocker_from_retry_state(
    task: &autonomy::Task,
    retry_state: &autonomy::RetryState,
    now: &str,
) -> ProjectBlocker {
    let status = if retry_timestamp_is_past_or_now(retry_state.quarantined_until.as_deref(), now) {
        "open"
    } else if retry_state.quarantined_until.is_some() {
        "quarantined"
    } else if retry_timestamp_is_past_or_now(retry_state.next_retry_at.as_deref(), now) {
        "retry_ready"
    } else if retry_state.next_retry_at.is_some() {
        "waiting_retry_window"
    } else {
        "open"
    };
    let severity = match retry_state.failure_class {
        autonomy::FailureClass::Unsafe | autonomy::FailureClass::Permanent => "error",
        autonomy::FailureClass::Transient | autonomy::FailureClass::InconsistentState => "warn",
    };
    let summary = if let Some(until) = retry_state.quarantined_until.as_deref() {
        if retry_timestamp_is_past_or_now(Some(until), now) {
            format!(
                "{} already served its quarantine until {} and now needs intervention after {} failures",
                task.title,
                until,
                retry_state.failure_class.as_str()
            )
        } else {
            format!(
                "{} is quarantined until {} after {} failures",
                task.title,
                until,
                retry_state.failure_class.as_str()
            )
        }
    } else if let Some(next_retry_at) = retry_state.next_retry_at.as_deref() {
        if retry_timestamp_is_past_or_now(Some(next_retry_at), now) {
            format!(
                "{} is ready to retry now after {} failures; retry window reopened at {}",
                task.title,
                retry_state.failure_class.as_str(),
                next_retry_at
            )
        } else {
            format!(
                "{} is waiting for retry at {} after {} failures",
                task.title,
                next_retry_at,
                retry_state.failure_class.as_str()
            )
        }
    } else {
        format!(
            "{} has an open retry gate after {} failures",
            task.title,
            retry_state.failure_class.as_str()
        )
    };
    let age_bucket = blocker_age_bucket(&retry_state.updated_at, now);
    let urgency = blocker_urgency_for_retry_status(status, severity, &age_bucket);

    ProjectBlocker {
        id: format!("retry:{}", retry_state.key),
        kind: "retry_state".to_string(),
        severity: severity.to_string(),
        status: status.to_string(),
        summary,
        source: "autonomy_retry_state".to_string(),
        task_id: Some(task.id),
        task_kind: Some(task.kind.clone()),
        target: retry_state
            .target
            .clone()
            .or_else(|| autonomy::project_graph_target(task)),
        observed_at: retry_state.updated_at.clone(),
        age_bucket,
        urgency,
    }
}

fn blocker_from_stale_task(task: &autonomy::Task, now: &str) -> ProjectBlocker {
    let observed_at = task
        .last_run_at
        .clone()
        .unwrap_or_else(|| task.updated_at.clone());
    ProjectBlocker {
        id: format!("task:{}:stale", task.id),
        kind: "stale_running_task".to_string(),
        severity: "warn".to_string(),
        status: "stale".to_string(),
        summary: format!(
            "{} has been running without progress since {}",
            task.title,
            task.last_run_at
                .as_deref()
                .unwrap_or(task.updated_at.as_str())
        ),
        source: "autonomy_task".to_string(),
        task_id: Some(task.id),
        task_kind: Some(task.kind.clone()),
        target: autonomy::project_graph_target(task),
        observed_at: observed_at.clone(),
        age_bucket: blocker_age_bucket(&observed_at, now),
        urgency: "needs_intervention".to_string(),
    }
}

fn blocker_from_failure_cluster(cluster: &ReplayFailureClusterRecord, now: &str) -> ProjectBlocker {
    let severity = match cluster.failure_class.as_str() {
        "unsafe" | "permanent" => "error",
        _ => "warn",
    };
    let age_bucket = blocker_age_bucket(&cluster.last_seen_at, now);
    ProjectBlocker {
        id: format!("cluster:{}", cluster.id),
        kind: "replay_failure_cluster".to_string(),
        severity: severity.to_string(),
        status: "repeated_failure".to_string(),
        summary: format!(
            "{} repeated {} {} failures ({})",
            cluster.task_kind,
            cluster.occurrence_count,
            cluster.failure_class,
            cluster.exemplar_summary
        ),
        source: "replay_failure_clusters".to_string(),
        task_id: None,
        task_kind: Some(cluster.task_kind.clone()),
        target: cluster.target.clone(),
        observed_at: cluster.last_seen_at.clone(),
        age_bucket: age_bucket.clone(),
        urgency: blocker_urgency_from_severity(severity, &age_bucket),
    }
}

fn blocker_from_incident(incident: &SystemIncidentRecord, now: &str) -> ProjectBlocker {
    let age_bucket = blocker_age_bucket(&incident.created_at, now);
    ProjectBlocker {
        id: format!("incident:{}", incident.id),
        kind: "system_incident".to_string(),
        severity: incident.severity.clone(),
        status: "open".to_string(),
        summary: incident.summary.clone(),
        source: incident.source.clone(),
        task_id: None,
        task_kind: None,
        target: None,
        observed_at: incident.created_at.clone(),
        age_bucket: age_bucket.clone(),
        urgency: blocker_urgency_from_severity(&incident.severity, &age_bucket),
    }
}

fn failure_cluster_matches_task(
    cluster: &ReplayFailureClusterRecord,
    task: &autonomy::Task,
) -> bool {
    if cluster.task_kind != task.kind {
        return false;
    }
    if let Some(cluster_tool) = cluster.tool.as_deref() {
        if task.tool.as_deref() != Some(cluster_tool) {
            return false;
        }
    }
    if let Some(cluster_target) = cluster
        .target
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return autonomy::project_graph_target(task)
            .as_deref()
            .map(|target| target == cluster_target)
            .unwrap_or(false);
    }
    true
}

fn summarize_project_status(
    blockers: &[ProjectBlocker],
    resume_queue: &[ResumeQueueItem],
    workstreams: &[ProjectWorkstream],
) -> String {
    if blockers.iter().any(|blocker| blocker.severity == "error") {
        "blocked".to_string()
    } else if !blockers.is_empty() {
        "active_with_risk".to_string()
    } else if !resume_queue.is_empty() || !workstreams.is_empty() {
        "active".to_string()
    } else {
        "idle".to_string()
    }
}

fn resume_reason(
    task: &autonomy::Task,
    operator_state: &str,
    retry_hint: Option<&str>,
    top_blocker_summary: Option<&str>,
) -> String {
    match operator_state {
        "needs_intervention" => top_blocker_summary
            .map(|summary| format!("intervene on: {}", crate::trunc(summary, 140)))
            .unwrap_or_else(|| "recover stale running task".to_string()),
        "retry_needed" => retry_hint
            .map(|hint| format!("retry follow-through: {}", crate::trunc(hint, 140)))
            .unwrap_or_else(|| "retry unresolved follow-through".to_string()),
        "ready_now" => "dispatch ready task".to_string(),
        "in_flight" => "resume in-flight task".to_string(),
        "waiting_retry_window" => "wait for retry window".to_string(),
        "blocked" => top_blocker_summary
            .map(|summary| format!("clear blocker: {}", crate::trunc(summary, 140)))
            .unwrap_or_else(|| "wait for blocker resolution".to_string()),
        "scheduled" => "resume when scheduled".to_string(),
        _ if task.status == TaskStatus::Running => "resume in-flight task".to_string(),
        _ => "track task state".to_string(),
    }
}

fn compare_blockers(left: &ProjectBlocker, right: &ProjectBlocker) -> Ordering {
    blocker_urgency_rank(&left.urgency)
        .cmp(&blocker_urgency_rank(&right.urgency))
        .then_with(|| severity_rank(&left.severity).cmp(&severity_rank(&right.severity)))
        .then_with(|| blocker_age_rank(&left.age_bucket).cmp(&blocker_age_rank(&right.age_bucket)))
        .then_with(|| right.observed_at.cmp(&left.observed_at))
        .then_with(|| left.id.cmp(&right.id))
}

fn compare_resume_items(left: &ResumeQueueItem, right: &ResumeQueueItem) -> Ordering {
    resume_rank(left)
        .cmp(&resume_rank(right))
        .then_with(|| {
            right
                .priority
                .partial_cmp(&left.priority)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            option_timestamp_desc(left.scheduled_for.as_ref(), right.scheduled_for.as_ref())
        })
        .then_with(|| left.task_id.cmp(&right.task_id))
}

fn compare_workstreams(left: &ProjectWorkstream, right: &ProjectWorkstream) -> Ordering {
    workstream_rank(left)
        .cmp(&workstream_rank(right))
        .then_with(|| {
            right
                .priority
                .partial_cmp(&left.priority)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            option_timestamp_desc(
                left.last_activity_at.as_ref(),
                right.last_activity_at.as_ref(),
            )
        })
        .then_with(|| left.title.cmp(&right.title))
}

fn compare_task_previews(left: &WorkstreamTaskPreview, right: &WorkstreamTaskPreview) -> Ordering {
    task_preview_rank(left)
        .cmp(&task_preview_rank(right))
        .then_with(|| {
            right
                .priority
                .partial_cmp(&left.priority)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            option_timestamp_desc(left.scheduled_for.as_ref(), right.scheduled_for.as_ref())
        })
        .then_with(|| left.id.cmp(&right.id))
}

fn compare_running_task_previews(
    left: &WorkstreamTaskPreview,
    right: &WorkstreamTaskPreview,
) -> Ordering {
    let left_stale = left.blocked_by.iter().any(|value| value.contains(":stale"));
    let right_stale = right
        .blocked_by
        .iter()
        .any(|value| value.contains(":stale"));
    right_stale
        .cmp(&left_stale)
        .then_with(|| option_timestamp_desc(left.last_run_at.as_ref(), right.last_run_at.as_ref()))
        .then_with(|| left.id.cmp(&right.id))
}

fn task_preview_rank(task: &WorkstreamTaskPreview) -> usize {
    if task.blocked_by.is_empty() { 0 } else { 1 }
}

fn resume_rank(item: &ResumeQueueItem) -> usize {
    match item.operator_state.as_str() {
        "needs_intervention" => 0,
        "retry_needed" => 1,
        "ready_now" => 2,
        "in_flight" => 3,
        "waiting_retry_window" => 4,
        "blocked" => 5,
        "scheduled" => 6,
        _ => 7,
    }
}

fn workstream_rank(workstream: &ProjectWorkstream) -> usize {
    if workstream.task_counts.ready > 0 {
        0
    } else if workstream.status == "active" {
        1
    } else if workstream.status == "blocked" {
        2
    } else {
        3
    }
}

fn blocker_counts_as_active_constraint(blocker: &ProjectBlocker) -> bool {
    !(blocker.kind == "retry_state" && blocker.status == "retry_ready")
}

fn task_has_active_blockers(
    task_id: i64,
    task_blockers: &HashMap<i64, Vec<String>>,
    blocker_index: &HashMap<String, ProjectBlocker>,
) -> bool {
    task_blockers
        .get(&task_id)
        .into_iter()
        .flat_map(|ids| ids.iter())
        .any(|blocker_id| {
            blocker_index
                .get(blocker_id)
                .map(blocker_counts_as_active_constraint)
                .unwrap_or(true)
        })
}

fn classify_resume_item(
    task: &autonomy::Task,
    stale: bool,
    latest_outcome: Option<&LatestTaskOutcome>,
    ready_now: bool,
    active_blocker_count: usize,
    primary_blocker: Option<&ProjectBlocker>,
) -> String {
    if stale
        || matches!(
            primary_blocker.map(|blocker| blocker.urgency.as_str()),
            Some("needs_intervention")
        )
    {
        return "needs_intervention".to_string();
    }
    if ready_now && latest_outcome_requires_follow_up(latest_outcome) {
        return "retry_needed".to_string();
    }
    if ready_now {
        return "ready_now".to_string();
    }
    if task.status == TaskStatus::Running {
        if latest_outcome_requires_follow_up(latest_outcome) {
            return "retry_needed".to_string();
        }
        return "in_flight".to_string();
    }
    if matches!(
        primary_blocker.map(|blocker| blocker.status.as_str()),
        Some("waiting_retry_window")
    ) {
        return "waiting_retry_window".to_string();
    }
    if active_blocker_count > 0 {
        return "blocked".to_string();
    }
    if task.scheduled_for.is_some() {
        return "scheduled".to_string();
    }
    if latest_outcome_requires_follow_up(latest_outcome) {
        return "retry_needed".to_string();
    }
    "queued".to_string()
}

fn latest_outcome_requires_follow_up(outcome: Option<&LatestTaskOutcome>) -> bool {
    let Some(outcome) = outcome else {
        return false;
    };
    if outcome.verified == Some(true) {
        return false;
    }
    outcome.rollback_reason.is_some()
        || outcome.verified == Some(false)
        || (outcome.executed && outcome.verified.is_none())
        || matches!(
            outcome.outcome.as_str(),
            "failed" | "retry_scheduled" | "quarantined" | "dependency_blocked"
        )
}

fn latest_outcome_retry_hint(outcome: &LatestTaskOutcome) -> Option<String> {
    outcome
        .rollback_reason
        .as_ref()
        .map(|reason| reason.summary.clone())
        .or_else(|| {
            if latest_outcome_requires_follow_up(Some(outcome)) {
                Some(outcome.summary.clone())
            } else {
                None
            }
        })
}

fn blocker_retry_hint(blocker: &ProjectBlocker) -> Option<String> {
    (blocker.kind == "retry_state")
        .then(|| blocker.summary.clone())
        .filter(|summary| !summary.trim().is_empty())
}

fn blocker_urgency_for_retry_status(status: &str, severity: &str, age_bucket: &str) -> String {
    match status {
        "quarantined" => "needs_intervention".to_string(),
        "retry_ready" => "ready_to_retry".to_string(),
        _ => blocker_urgency_from_severity(severity, age_bucket),
    }
}

fn blocker_urgency_from_severity(severity: &str, age_bucket: &str) -> String {
    match severity {
        "critical" | "error" => "critical".to_string(),
        _ if matches!(age_bucket, "aging" | "stale") => "aging".to_string(),
        _ => "active".to_string(),
    }
}

fn blocker_age_bucket(observed_at: &str, now: &str) -> String {
    let Some(age_minutes) = age_minutes_between(observed_at, now) else {
        return "unknown".to_string();
    };
    if age_minutes >= 24 * 60 {
        "stale".to_string()
    } else if age_minutes >= 2 * 60 {
        "aging".to_string()
    } else {
        "fresh".to_string()
    }
}

fn blocker_urgency_rank(urgency: &str) -> usize {
    match urgency {
        "critical" => 0,
        "needs_intervention" => 1,
        "ready_to_retry" => 2,
        "aging" => 3,
        _ => 4,
    }
}

fn blocker_age_rank(age_bucket: &str) -> usize {
    match age_bucket {
        "stale" => 0,
        "aging" => 1,
        "fresh" => 2,
        _ => 3,
    }
}

fn severity_rank(severity: &str) -> usize {
    match severity {
        "critical" | "error" => 0,
        "warn" => 1,
        _ => 2,
    }
}

fn option_timestamp_desc(left: Option<&String>, right: Option<&String>) -> Ordering {
    right.cmp(&left)
}

fn latest_workstream_activity(workstreams: &[ProjectWorkstream]) -> Option<String> {
    workstreams.iter().fold(None, |current, workstream| {
        latest_timestamp(current, workstream.last_activity_at.clone())
    })
}

fn latest_milestone_activity(milestones: &[ProjectMilestone]) -> Option<String> {
    milestones.iter().fold(None, |current, milestone| {
        latest_timestamp(current, Some(milestone.recorded_at.clone()))
    })
}

fn latest_timestamp(left: Option<String>, right: Option<String>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left >= right { left } else { right }),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn task_is_ready(task: &autonomy::Task, now: &str) -> bool {
    task.status == TaskStatus::Pending
        && task
            .scheduled_for
            .as_deref()
            .map(|scheduled_for| scheduled_for <= now)
            .unwrap_or(true)
}

fn age_minutes_between(observed_at: &str, now: &str) -> Option<i64> {
    let observed_at = parse_timestamp(observed_at)?;
    let now = parse_timestamp(now)?;
    Some((now - observed_at).num_minutes().max(0))
}

fn parse_timestamp(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let naive = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").ok()?;
    Some(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        naive,
        chrono::Utc,
    ))
}

fn retry_timestamp_is_past_or_now(value: Option<&str>, now: &str) -> bool {
    value.map(|value| value <= now).unwrap_or(false)
}

fn stale_running_before() -> String {
    (chrono::Utc::now()
        - chrono::Duration::seconds(crate::autonomy::STALE_RUNNING_TASK_TIMEOUT_SECS))
    .format("%Y-%m-%d %H:%M:%S")
    .to_string()
}

fn now_timestamp() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

impl SelfModelSnapshotRef {
    fn from_value(value: serde_json::Value) -> Self {
        Self {
            id: value.get("id").and_then(|field| field.as_i64()),
            source: value
                .get("source")
                .and_then(|field| field.as_str())
                .map(str::to_string),
            trigger_kind: value
                .get("trigger_kind")
                .and_then(|field| field.as_str())
                .map(str::to_string),
            trigger_target: value
                .get("trigger_target")
                .and_then(|field| field.as_str())
                .map(str::to_string),
            summary: value
                .get("summary")
                .and_then(|field| field.as_str())
                .map(str::to_string),
            capability_count: value
                .get("capability_count")
                .and_then(|field| field.as_i64()),
            constraint_count: value
                .get("constraint_count")
                .and_then(|field| field.as_i64()),
            active_goal_count: value
                .get("active_goal_count")
                .and_then(|field| field.as_i64()),
            created_at: value
                .get("created_at")
                .and_then(|field| field.as_str())
                .map(str::to_string),
        }
    }
}
