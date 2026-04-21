use super::{Db, Task, WorldTaskContext};

pub(super) fn append_world_planning_notes(
    notes: &mut Vec<String>,
    task: &Task,
    world_snapshot: Option<&crate::world::projects::ProjectGraphSnapshot>,
    world_changes: Option<&crate::world::projects::ProjectGraphChangeSet>,
) {
    let Some(snapshot) = world_snapshot else {
        return;
    };

    notes.push(format!(
        "world_snapshot {} ({})",
        snapshot.compiled_at, snapshot.source
    ));

    if let Some(context) = world_task_context(snapshot, task) {
        notes.push(format!(
            "world_project={} status={}",
            context.project_title, context.project_status
        ));
        notes.push(format!(
            "world_workstream={} status={} ready={} running={}",
            context.workstream_title,
            context.workstream_status,
            context.workstream_ready_count,
            context.workstream_running_count
        ));
        notes.push(format!("world_resume_rank={}", context.resume_index + 1));
        notes.push(format!("world_resume_reason={}", context.resume_reason));
        notes.push(format!("world_operator_state={}", context.operator_state));
        notes.push(format!(
            "world_blocker_count={}",
            context.active_blocker_count
        ));
        if !context.blocked_by.is_empty() {
            notes.push(format!("world_blockers={}", context.blocked_by.join(",")));
        }
        if let Some(retry_hint) = context.retry_hint.as_deref() {
            notes.push(format!("world_retry_hint={}", retry_hint));
        }
        if let Some(changes) = world_changes {
            for summary in crate::world::projects::task_relevant_change_summary(
                changes,
                task.id,
                Some(context.workstream_id.as_str()),
            ) {
                notes.push(format!("world_change={}", summary));
            }
        }
    } else {
        notes.push(format!("world_task_untracked={}", task.id));
        if let Some(changes) = world_changes {
            for summary in changes.summary.iter().take(2) {
                notes.push(format!("world_change={}", summary));
            }
        }
    }
}

pub(super) fn refresh_project_graph_snapshot(
    db: &Db,
    source: &str,
) -> Option<crate::world::projects::ProjectGraphSnapshot> {
    match crate::world::state::compile_and_persist_project_graph(db, source) {
        Ok(snapshot) => Some(snapshot),
        Err(error) => {
            tracing::warn!(
                "world: failed to compile project graph during {}: {}",
                source,
                error
            );
            crate::world::state::load_project_graph(db)
        }
    }
}

pub(super) fn order_ready_tasks_for_dispatch(
    tasks: &mut Vec<Task>,
    world_snapshot: Option<&crate::world::projects::ProjectGraphSnapshot>,
) {
    tasks.sort_by(|left, right| compare_ready_tasks_for_dispatch(left, right, world_snapshot));
}

fn compare_ready_tasks_for_dispatch(
    left: &Task,
    right: &Task,
    world_snapshot: Option<&crate::world::projects::ProjectGraphSnapshot>,
) -> std::cmp::Ordering {
    let left_context = world_snapshot.and_then(|snapshot| world_task_context(snapshot, left));
    let right_context = world_snapshot.and_then(|snapshot| world_task_context(snapshot, right));

    ready_task_blocked_rank(left_context.as_ref())
        .cmp(&ready_task_blocked_rank(right_context.as_ref()))
        .then_with(|| {
            ready_task_operator_state_rank(left_context.as_ref())
                .cmp(&ready_task_operator_state_rank(right_context.as_ref()))
        })
        .then_with(|| {
            ready_task_workstream_status_rank(left_context.as_ref())
                .cmp(&ready_task_workstream_status_rank(right_context.as_ref()))
        })
        .then_with(|| {
            ready_task_workstream_index(left_context.as_ref())
                .cmp(&ready_task_workstream_index(right_context.as_ref()))
        })
        .then_with(|| {
            ready_task_preview_index(left_context.as_ref())
                .cmp(&ready_task_preview_index(right_context.as_ref()))
        })
        .then_with(|| {
            ready_task_resume_index(left_context.as_ref())
                .cmp(&ready_task_resume_index(right_context.as_ref()))
        })
        .then_with(|| {
            right
                .priority
                .partial_cmp(&left.priority)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| left.created_at.cmp(&right.created_at))
        .then_with(|| left.id.cmp(&right.id))
}

fn world_task_context(
    snapshot: &crate::world::projects::ProjectGraphSnapshot,
    task: &Task,
) -> Option<WorldTaskContext> {
    let (resume_index, resume_item) = snapshot
        .resume_queue
        .iter()
        .enumerate()
        .find(|(_, item)| item.task_id == task.id)?;
    let target_goal_id = resume_item.goal_id.or(task.goal_id);

    for project in &snapshot.projects {
        for (workstream_index, workstream) in project.workstreams.iter().enumerate() {
            let matches_task = workstream
                .next_tasks
                .iter()
                .any(|preview| preview.id == task.id)
                || workstream
                    .running_tasks
                    .iter()
                    .any(|preview| preview.id == task.id)
                || (target_goal_id.is_some() && workstream.goal_id == target_goal_id)
                || (target_goal_id.is_none()
                    && workstream.goal_id.is_none()
                    && workstream.id == "workstream:unassigned");
            if !matches_task {
                continue;
            }

            let preview_index = workstream
                .next_tasks
                .iter()
                .position(|preview| preview.id == task.id);

            return Some(WorldTaskContext {
                project_title: project.title.clone(),
                project_status: project.status.clone(),
                workstream_id: workstream.id.clone(),
                workstream_title: workstream.title.clone(),
                workstream_status: workstream.status.clone(),
                workstream_index,
                workstream_ready_count: workstream.task_counts.ready,
                workstream_running_count: workstream.task_counts.running,
                resume_index,
                preview_index,
                blocked_by: resume_item.blocked_by.clone(),
                active_blocker_count: resume_item.active_blocker_count,
                resume_reason: resume_item.resume_reason.clone(),
                operator_state: resume_item.operator_state.clone(),
                retry_hint: resume_item.retry_hint.clone(),
            });
        }
    }

    snapshot.projects.first().map(|project| WorldTaskContext {
        project_title: project.title.clone(),
        project_status: project.status.clone(),
        workstream_id: "workstream:resume_fallback".to_string(),
        workstream_title: resume_item
            .goal_title
            .clone()
            .unwrap_or_else(|| "Resume backlog".to_string()),
        workstream_status: "tracked".to_string(),
        workstream_index: usize::MAX / 4,
        workstream_ready_count: 0,
        workstream_running_count: 0,
        resume_index,
        preview_index: None,
        blocked_by: resume_item.blocked_by.clone(),
        active_blocker_count: resume_item.active_blocker_count,
        resume_reason: resume_item.resume_reason.clone(),
        operator_state: resume_item.operator_state.clone(),
        retry_hint: resume_item.retry_hint.clone(),
    })
}

fn ready_task_blocked_rank(context: Option<&WorldTaskContext>) -> usize {
    context
        .map(|context| usize::from(context.active_blocker_count > 0))
        .unwrap_or(0)
}

fn ready_task_operator_state_rank(context: Option<&WorldTaskContext>) -> usize {
    match context.map(|context| context.operator_state.as_str()) {
        Some("retry_needed") => 0,
        Some("ready_now") => 1,
        Some("needs_intervention") => 2,
        Some("in_flight") => 3,
        Some("waiting_retry_window") => 4,
        Some("blocked") => 5,
        Some("scheduled") => 6,
        _ => 7,
    }
}

fn ready_task_workstream_status_rank(context: Option<&WorldTaskContext>) -> usize {
    match context.map(|context| context.workstream_status.as_str()) {
        Some("active") => 0,
        Some("blocked") => 1,
        Some("tracked") => 2,
        _ => 3,
    }
}

fn ready_task_workstream_index(context: Option<&WorldTaskContext>) -> usize {
    context
        .map(|context| context.workstream_index)
        .unwrap_or(usize::MAX / 2)
}

fn ready_task_preview_index(context: Option<&WorldTaskContext>) -> usize {
    context
        .and_then(|context| context.preview_index)
        .unwrap_or(usize::MAX / 2)
}

fn ready_task_resume_index(context: Option<&WorldTaskContext>) -> usize {
    context
        .map(|context| context.resume_index)
        .unwrap_or(usize::MAX / 2)
}

pub(super) fn unresolved_dependency_blockers<'a>(
    world_snapshot: Option<&'a crate::world::projects::ProjectGraphSnapshot>,
    task: &Task,
) -> Vec<&'a crate::world::projects::ProjectBlocker> {
    let Some(snapshot) = world_snapshot else {
        return Vec::new();
    };
    let Some(context) = world_task_context(snapshot, task) else {
        return Vec::new();
    };

    context
        .blocked_by
        .iter()
        .filter_map(|blocker_id| {
            snapshot
                .blockers
                .iter()
                .find(|blocker| blocker.id == *blocker_id)
        })
        .filter(|blocker| blocker.kind != "retry_state")
        .collect()
}

pub(super) fn action_run_world_context(
    world_snapshot: Option<&crate::world::projects::ProjectGraphSnapshot>,
    world_changes: Option<&crate::world::projects::ProjectGraphChangeSet>,
    task: &Task,
) -> Option<serde_json::Value> {
    let snapshot = world_snapshot?;
    let context = world_task_context(snapshot, task)?;
    let relevant_changes = world_changes
        .map(|changes| {
            crate::world::projects::task_relevant_change_summary(
                changes,
                task.id,
                Some(context.workstream_id.as_str()),
            )
        })
        .unwrap_or_default();
    let blockers = context
        .blocked_by
        .iter()
        .filter_map(|blocker_id| {
            snapshot
                .blockers
                .iter()
                .find(|blocker| blocker.id == *blocker_id)
        })
        .map(|blocker| {
            serde_json::json!({
                "id": blocker.id,
                "kind": blocker.kind,
                "severity": blocker.severity,
                "status": blocker.status,
                "summary": blocker.summary,
                "source": blocker.source,
                "target": blocker.target,
                "observed_at": blocker.observed_at,
            })
        })
        .collect::<Vec<_>>();

    Some(serde_json::json!({
        "compiled_at": snapshot.compiled_at,
        "source": snapshot.source,
        "project": {
            "title": context.project_title,
            "status": context.project_status,
        },
        "workstream": {
            "id": context.workstream_id,
            "title": context.workstream_title,
            "status": context.workstream_status,
            "index": context.workstream_index,
            "ready_tasks": context.workstream_ready_count,
            "running_tasks": context.workstream_running_count,
        },
        "resume": {
            "index": context.resume_index,
            "reason": context.resume_reason,
            "preview_index": context.preview_index,
            "operator_state": context.operator_state,
            "active_blocker_count": context.active_blocker_count,
            "retry_hint": context.retry_hint,
        },
        "blocked_by": context.blocked_by,
        "blockers": blockers,
        "changes": world_changes.map(|changes| serde_json::json!({
            "changed": changes.changed,
            "summary": changes.summary.iter().take(4).cloned().collect::<Vec<_>>(),
            "relevant_summary": relevant_changes,
            "previous_compiled_at": changes.previous.as_ref().map(|marker| marker.compiled_at.clone()),
            "previous_source": changes.previous.as_ref().map(|marker| marker.source.clone()),
            "current_compiled_at": changes.current.compiled_at.clone(),
            "current_source": changes.current.source.clone(),
        })),
    }))
}
