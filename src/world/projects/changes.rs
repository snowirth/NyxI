use std::collections::HashMap;

use super::{
    PROJECT_GRAPH_CHANGES_SCHEMA_VERSION, ProjectGraphBlockerChange, ProjectGraphChangeSet,
    ProjectGraphCountChange, ProjectGraphFocusShift, ProjectGraphResumeChange,
    ProjectGraphSnapshot, ProjectGraphSnapshotMarker, ProjectGraphWorkstreamChange, ProjectNode,
    ProjectWorkstream, ResumeQueueItem, WorldFocusSummary, now_timestamp,
};

impl ProjectGraphChangeSet {
    pub fn has_changes(&self) -> bool {
        self.changed
    }

    pub fn resume_brief(&self) -> Option<String> {
        if !self.changed {
            return None;
        }

        let mut lines = vec!["Since last time:".to_string()];

        if let Some(focus_shift) = self.focus_shift.as_ref() {
            lines.push(format!(
                "- focus: {} -> {}",
                crate::trunc(&world_focus_label(&focus_shift.previous), 48),
                crate::trunc(&world_focus_label(&focus_shift.current), 48)
            ));
        }

        if let Some(counts_line) = resume_brief_count_line(&self.count_changes) {
            lines.push(format!("- counts: {}", counts_line));
        }

        if let Some(blockers_line) = resume_brief_blocker_line(&self.blocker_changes) {
            lines.push(format!("- blockers: {}", blockers_line));
        }

        if let Some(resume_line) = resume_brief_resume_line(&self.resume_changes) {
            lines.push(format!("- resume: {}", resume_line));
        }

        if lines.len() == 1 {
            lines.extend(
                self.summary
                    .iter()
                    .take(3)
                    .map(|line| format!("- {}", crate::trunc(line, 120))),
            );
        }

        Some(lines.join("\n"))
    }
}

fn resume_brief_count_line(count_changes: &[ProjectGraphCountChange]) -> Option<String> {
    let mut selected = count_changes
        .iter()
        .filter(|change| {
            matches!(
                change.field.as_str(),
                "ready_tasks" | "running_tasks" | "blockers" | "resume_queue" | "active_goals"
            )
        })
        .take(3)
        .map(|change| {
            format!(
                "{} {} -> {}",
                resume_brief_count_label(&change.field),
                change.from,
                change.to
            )
        })
        .collect::<Vec<_>>();

    if selected.is_empty() {
        selected = count_changes
            .iter()
            .take(3)
            .map(|change| {
                format!(
                    "{} {} -> {}",
                    resume_brief_count_label(&change.field),
                    change.from,
                    change.to
                )
            })
            .collect();
    }

    (!selected.is_empty()).then(|| selected.join(", "))
}

fn resume_brief_blocker_line(blocker_changes: &[ProjectGraphBlockerChange]) -> Option<String> {
    if blocker_changes.is_empty() {
        return None;
    }

    let opened = blocker_changes
        .iter()
        .filter(|change| change.change == "opened")
        .count();
    let cleared = blocker_changes
        .iter()
        .filter(|change| change.change == "cleared")
        .count();
    let updated = blocker_changes
        .iter()
        .filter(|change| change.change == "updated")
        .count();

    let mut parts = Vec::new();
    if opened > 0 {
        parts.push(format!("{} opened", opened));
    }
    if cleared > 0 {
        parts.push(format!("{} cleared", cleared));
    }
    if updated > 0 {
        parts.push(format!("{} updated", updated));
    }

    let mut line = parts.join(", ");
    if let Some(highlight) = blocker_changes
        .iter()
        .find(|change| change.change == "opened")
        .or_else(|| {
            blocker_changes
                .iter()
                .find(|change| change.change == "cleared")
        })
        .or_else(|| blocker_changes.first())
    {
        let detail = crate::trunc(resume_brief_change_detail(&highlight.summary), 88);
        if !detail.is_empty() {
            line.push_str(&format!(" ({})", detail));
        }
    }

    Some(line)
}

fn resume_brief_resume_line(resume_changes: &[ProjectGraphResumeChange]) -> Option<String> {
    let items = resume_changes
        .iter()
        .take(2)
        .map(|change| {
            let title = crate::trunc(&change.title, 56);
            match (&change.from_status, &change.to_status) {
                (Some(from), Some(to)) if from != to => {
                    format!("{} [{} -> {}]", title, from, to)
                }
                (None, Some(to)) => format!("{} [added as {}]", title, to),
                (Some(from), None) => format!("{} [cleared from {}]", title, from),
                (_, Some(to)) => format!("{} [{}]", title, to),
                (None, None) => format!("{} [{}]", title, change.change),
            }
        })
        .collect::<Vec<_>>();

    (!items.is_empty()).then(|| items.join("; "))
}

fn resume_brief_count_label(field: &str) -> &str {
    match field {
        "projects" => "projects",
        "active_goals" => "active goals",
        "workstreams" => "workstreams",
        "pending_tasks" => "pending",
        "ready_tasks" => "ready",
        "running_tasks" => "running",
        "stale_running_tasks" => "stale running",
        "blockers" => "blockers",
        "resume_queue" => "resume queue",
        "recent_incidents" => "incidents",
        "recent_growth_events" => "growth events",
        "recent_self_model_snapshots" => "self-model snapshots",
        _ => field,
    }
}

fn resume_brief_change_detail(summary: &str) -> &str {
    summary
        .strip_prefix("blocker opened: ")
        .or_else(|| summary.strip_prefix("blocker cleared: "))
        .or_else(|| summary.strip_prefix("blocker updated: "))
        .or_else(|| summary.strip_prefix("resume item added: "))
        .or_else(|| summary.strip_prefix("resume item cleared: "))
        .or_else(|| summary.strip_prefix("resume item updated: "))
        .unwrap_or(summary)
}

pub fn derive_world_focus(snapshot: &ProjectGraphSnapshot) -> WorldFocusSummary {
    let active_project = snapshot.projects.first();
    let resume_focus = snapshot.resume_queue.first();
    let active_workstream = select_focus_workstream(active_project, resume_focus);

    WorldFocusSummary {
        active_project_title: active_project.map(|project| project.title.clone()),
        active_project_status: active_project.map(|project| project.status.clone()),
        active_workstream_title: active_workstream
            .map(|workstream| workstream.title.clone())
            .or_else(|| resume_focus.and_then(|item| item.goal_title.clone())),
        active_workstream_status: active_workstream.map(|workstream| workstream.status.clone()),
        resume_focus_title: resume_focus.map(|item| item.title.clone()),
        resume_focus_kind: resume_focus.map(|item| item.kind.clone()),
        resume_focus_status: resume_focus.map(|item| item.status.clone()),
        resume_focus_reason: resume_focus.map(|item| item.resume_reason.clone()),
        resume_focus_blocker_count: resume_focus
            .map(|item| item.active_blocker_count)
            .unwrap_or_else(|| {
                active_workstream
                    .map(|workstream| workstream.blockers.len())
                    .unwrap_or(0)
            }),
        blocker_count: snapshot.blockers.len(),
        top_blocker_summary: resume_focus
            .and_then(|item| item.top_blocker_summary.clone())
            .or_else(|| {
                snapshot
                    .blockers
                    .first()
                    .map(|blocker| blocker.summary.clone())
            }),
    }
}

pub fn compile_project_graph_changes(
    previous: Option<&ProjectGraphSnapshot>,
    current: &ProjectGraphSnapshot,
) -> ProjectGraphChangeSet {
    let current_marker = ProjectGraphSnapshotMarker {
        compiled_at: current.compiled_at.clone(),
        source: current.source.clone(),
    };
    let previous_marker = previous.map(|snapshot| ProjectGraphSnapshotMarker {
        compiled_at: snapshot.compiled_at.clone(),
        source: snapshot.source.clone(),
    });
    let Some(previous_snapshot) = previous else {
        return ProjectGraphChangeSet {
            schema_version: PROJECT_GRAPH_CHANGES_SCHEMA_VERSION.to_string(),
            compared_at: now_timestamp(),
            previous: previous_marker,
            current: current_marker,
            changed: false,
            summary: Vec::new(),
            focus_shift: None,
            count_changes: Vec::new(),
            workstream_changes: Vec::new(),
            blocker_changes: Vec::new(),
            resume_changes: Vec::new(),
        };
    };

    let previous_focus = derive_world_focus(previous_snapshot);
    let current_focus = derive_world_focus(current);
    let focus_shift = (previous_focus != current_focus).then(|| ProjectGraphFocusShift {
        summary: format!(
            "focus shifted from {} to {}",
            world_focus_label(&previous_focus),
            world_focus_label(&current_focus)
        ),
        previous: previous_focus,
        current: current_focus,
    });
    let count_changes = collect_count_changes(&previous_snapshot.counts, &current.counts);
    let workstream_changes = collect_workstream_changes(previous_snapshot, current);
    let blocker_changes = collect_blocker_changes(previous_snapshot, current);
    let resume_changes = collect_resume_changes(previous_snapshot, current);
    let summary = build_project_change_summary(
        focus_shift.as_ref(),
        &count_changes,
        &workstream_changes,
        &blocker_changes,
        &resume_changes,
    );
    let changed = focus_shift.is_some()
        || !count_changes.is_empty()
        || !workstream_changes.is_empty()
        || !blocker_changes.is_empty()
        || !resume_changes.is_empty();

    ProjectGraphChangeSet {
        schema_version: PROJECT_GRAPH_CHANGES_SCHEMA_VERSION.to_string(),
        compared_at: now_timestamp(),
        previous: previous_marker,
        current: current_marker,
        changed,
        summary,
        focus_shift,
        count_changes,
        workstream_changes,
        blocker_changes,
        resume_changes,
    }
}

pub fn task_relevant_change_summary(
    changes: &ProjectGraphChangeSet,
    task_id: i64,
    workstream_id: Option<&str>,
) -> Vec<String> {
    let mut relevant = changes
        .resume_changes
        .iter()
        .filter(|change| change.task_id == task_id)
        .map(|change| change.summary.clone())
        .collect::<Vec<_>>();

    if let Some(workstream_id) = workstream_id {
        relevant.extend(
            changes
                .workstream_changes
                .iter()
                .filter(|change| change.id == workstream_id)
                .map(|change| change.summary.clone()),
        );
    }

    if relevant.is_empty() {
        relevant.extend(changes.summary.iter().take(2).cloned());
    }

    relevant.truncate(3);
    relevant
}

fn world_focus_label(focus: &WorldFocusSummary) -> String {
    focus
        .resume_focus_title
        .clone()
        .or_else(|| focus.active_workstream_title.clone())
        .or_else(|| focus.active_project_title.clone())
        .unwrap_or_else(|| "idle".to_string())
}

fn collect_count_changes(
    previous: &super::ProjectGraphCounts,
    current: &super::ProjectGraphCounts,
) -> Vec<ProjectGraphCountChange> {
    let mut changes = Vec::new();
    for (field, from, to) in [
        ("projects", previous.projects, current.projects),
        ("active_goals", previous.active_goals, current.active_goals),
        ("workstreams", previous.workstreams, current.workstreams),
        (
            "pending_tasks",
            previous.pending_tasks,
            current.pending_tasks,
        ),
        ("ready_tasks", previous.ready_tasks, current.ready_tasks),
        (
            "running_tasks",
            previous.running_tasks,
            current.running_tasks,
        ),
        (
            "stale_running_tasks",
            previous.stale_running_tasks,
            current.stale_running_tasks,
        ),
        ("blockers", previous.blockers, current.blockers),
        ("resume_queue", previous.resume_queue, current.resume_queue),
        (
            "recent_incidents",
            previous.recent_incidents,
            current.recent_incidents,
        ),
        (
            "recent_growth_events",
            previous.recent_growth_events,
            current.recent_growth_events,
        ),
        (
            "recent_self_model_snapshots",
            previous.recent_self_model_snapshots,
            current.recent_self_model_snapshots,
        ),
    ] {
        if from != to {
            changes.push(ProjectGraphCountChange {
                field: field.to_string(),
                from,
                to,
                delta: to as isize - from as isize,
            });
        }
    }
    changes
}

fn collect_workstream_changes(
    previous: &ProjectGraphSnapshot,
    current: &ProjectGraphSnapshot,
) -> Vec<ProjectGraphWorkstreamChange> {
    let previous_workstreams = previous
        .projects
        .iter()
        .flat_map(|project| project.workstreams.iter())
        .map(|workstream| (workstream.id.clone(), workstream))
        .collect::<HashMap<_, _>>();
    let current_workstreams = current
        .projects
        .iter()
        .flat_map(|project| project.workstreams.iter())
        .map(|workstream| (workstream.id.clone(), workstream))
        .collect::<HashMap<_, _>>();
    let mut ids = previous_workstreams.keys().cloned().collect::<Vec<_>>();
    ids.extend(current_workstreams.keys().cloned());
    ids.sort();
    ids.dedup();

    let mut changes = Vec::new();
    for id in ids {
        match (previous_workstreams.get(&id), current_workstreams.get(&id)) {
            (None, Some(workstream)) => changes.push(ProjectGraphWorkstreamChange {
                id: workstream.id.clone(),
                title: workstream.title.clone(),
                change: "added".to_string(),
                from_status: None,
                to_status: Some(workstream.status.clone()),
                ready_from: 0,
                ready_to: workstream.task_counts.ready,
                running_from: 0,
                running_to: workstream.task_counts.running,
                blocker_count_from: 0,
                blocker_count_to: workstream.blockers.len(),
                summary: format!(
                    "workstream added: {} [{}]",
                    workstream.title, workstream.status
                ),
            }),
            (Some(workstream), None) => changes.push(ProjectGraphWorkstreamChange {
                id: workstream.id.clone(),
                title: workstream.title.clone(),
                change: "removed".to_string(),
                from_status: Some(workstream.status.clone()),
                to_status: None,
                ready_from: workstream.task_counts.ready,
                ready_to: 0,
                running_from: workstream.task_counts.running,
                running_to: 0,
                blocker_count_from: workstream.blockers.len(),
                blocker_count_to: 0,
                summary: format!(
                    "workstream removed: {} [{}]",
                    workstream.title, workstream.status
                ),
            }),
            (Some(previous_workstream), Some(current_workstream)) => {
                let changed = previous_workstream.status != current_workstream.status
                    || previous_workstream.task_counts.ready
                        != current_workstream.task_counts.ready
                    || previous_workstream.task_counts.running
                        != current_workstream.task_counts.running
                    || previous_workstream.blockers.len() != current_workstream.blockers.len();
                if !changed {
                    continue;
                }

                let mut details = Vec::new();
                if previous_workstream.status != current_workstream.status {
                    details.push(format!(
                        "status {} -> {}",
                        previous_workstream.status, current_workstream.status
                    ));
                }
                if previous_workstream.task_counts.ready != current_workstream.task_counts.ready {
                    details.push(format!(
                        "ready {} -> {}",
                        previous_workstream.task_counts.ready, current_workstream.task_counts.ready
                    ));
                }
                if previous_workstream.task_counts.running != current_workstream.task_counts.running
                {
                    details.push(format!(
                        "running {} -> {}",
                        previous_workstream.task_counts.running,
                        current_workstream.task_counts.running
                    ));
                }
                if previous_workstream.blockers.len() != current_workstream.blockers.len() {
                    details.push(format!(
                        "blockers {} -> {}",
                        previous_workstream.blockers.len(),
                        current_workstream.blockers.len()
                    ));
                }

                changes.push(ProjectGraphWorkstreamChange {
                    id: current_workstream.id.clone(),
                    title: current_workstream.title.clone(),
                    change: "updated".to_string(),
                    from_status: Some(previous_workstream.status.clone()),
                    to_status: Some(current_workstream.status.clone()),
                    ready_from: previous_workstream.task_counts.ready,
                    ready_to: current_workstream.task_counts.ready,
                    running_from: previous_workstream.task_counts.running,
                    running_to: current_workstream.task_counts.running,
                    blocker_count_from: previous_workstream.blockers.len(),
                    blocker_count_to: current_workstream.blockers.len(),
                    summary: format!(
                        "workstream updated: {} ({})",
                        current_workstream.title,
                        details.join(", ")
                    ),
                });
            }
            (None, None) => {}
        }
    }
    changes
}

fn collect_blocker_changes(
    previous: &ProjectGraphSnapshot,
    current: &ProjectGraphSnapshot,
) -> Vec<ProjectGraphBlockerChange> {
    let previous_blockers = previous
        .blockers
        .iter()
        .map(|blocker| (blocker.id.clone(), blocker))
        .collect::<HashMap<_, _>>();
    let current_blockers = current
        .blockers
        .iter()
        .map(|blocker| (blocker.id.clone(), blocker))
        .collect::<HashMap<_, _>>();
    let mut ids = previous_blockers.keys().cloned().collect::<Vec<_>>();
    ids.extend(current_blockers.keys().cloned());
    ids.sort();
    ids.dedup();

    let mut changes = Vec::new();
    for id in ids {
        match (previous_blockers.get(&id), current_blockers.get(&id)) {
            (None, Some(blocker)) => changes.push(ProjectGraphBlockerChange {
                id: blocker.id.clone(),
                kind: blocker.kind.clone(),
                change: "opened".to_string(),
                from_status: None,
                to_status: Some(blocker.status.clone()),
                target: blocker.target.clone(),
                summary: format!("blocker opened: {}", blocker.summary),
            }),
            (Some(blocker), None) => changes.push(ProjectGraphBlockerChange {
                id: blocker.id.clone(),
                kind: blocker.kind.clone(),
                change: "cleared".to_string(),
                from_status: Some(blocker.status.clone()),
                to_status: None,
                target: blocker.target.clone(),
                summary: format!("blocker cleared: {}", blocker.summary),
            }),
            (Some(previous_blocker), Some(current_blocker)) => {
                let changed = previous_blocker.status != current_blocker.status
                    || previous_blocker.severity != current_blocker.severity
                    || previous_blocker.summary != current_blocker.summary;
                if !changed {
                    continue;
                }

                changes.push(ProjectGraphBlockerChange {
                    id: current_blocker.id.clone(),
                    kind: current_blocker.kind.clone(),
                    change: "updated".to_string(),
                    from_status: Some(previous_blocker.status.clone()),
                    to_status: Some(current_blocker.status.clone()),
                    target: current_blocker.target.clone(),
                    summary: format!(
                        "blocker updated: {} ({} -> {})",
                        current_blocker.summary, previous_blocker.status, current_blocker.status
                    ),
                });
            }
            (None, None) => {}
        }
    }
    changes
}

fn collect_resume_changes(
    previous: &ProjectGraphSnapshot,
    current: &ProjectGraphSnapshot,
) -> Vec<ProjectGraphResumeChange> {
    let previous_items = previous
        .resume_queue
        .iter()
        .map(|item| (item.task_id, item))
        .collect::<HashMap<_, _>>();
    let current_items = current
        .resume_queue
        .iter()
        .map(|item| (item.task_id, item))
        .collect::<HashMap<_, _>>();
    let mut task_ids = previous_items.keys().cloned().collect::<Vec<_>>();
    task_ids.extend(current_items.keys().cloned());
    task_ids.sort();
    task_ids.dedup();

    let mut changes = Vec::new();
    for task_id in task_ids {
        match (previous_items.get(&task_id), current_items.get(&task_id)) {
            (None, Some(item)) => changes.push(ProjectGraphResumeChange {
                task_id: item.task_id,
                title: item.title.clone(),
                kind: item.kind.clone(),
                change: "added".to_string(),
                from_status: None,
                to_status: Some(item.status.clone()),
                goal_title: item.goal_title.clone(),
                summary: format!("resume item added: {} [{}]", item.title, item.status),
            }),
            (Some(item), None) => changes.push(ProjectGraphResumeChange {
                task_id: item.task_id,
                title: item.title.clone(),
                kind: item.kind.clone(),
                change: "removed".to_string(),
                from_status: Some(item.status.clone()),
                to_status: None,
                goal_title: item.goal_title.clone(),
                summary: format!("resume item cleared: {} [{}]", item.title, item.status),
            }),
            (Some(previous_item), Some(current_item)) => {
                let changed = previous_item.status != current_item.status
                    || previous_item.resume_reason != current_item.resume_reason
                    || previous_item.blocked_by != current_item.blocked_by
                    || previous_item.latest_outcome != current_item.latest_outcome;
                if !changed {
                    continue;
                }

                let mut details = Vec::new();
                if previous_item.status != current_item.status {
                    details.push(format!(
                        "status {} -> {}",
                        previous_item.status, current_item.status
                    ));
                }
                if previous_item.resume_reason != current_item.resume_reason {
                    details.push(format!(
                        "reason {} -> {}",
                        previous_item.resume_reason, current_item.resume_reason
                    ));
                }
                if previous_item.blocked_by != current_item.blocked_by {
                    details.push(format!(
                        "blockers {} -> {}",
                        previous_item.blocked_by.len(),
                        current_item.blocked_by.len()
                    ));
                }
                if previous_item.latest_outcome != current_item.latest_outcome {
                    details.push(format!(
                        "outcome {} -> {}",
                        previous_item.latest_outcome.as_deref().unwrap_or("n/a"),
                        current_item.latest_outcome.as_deref().unwrap_or("n/a")
                    ));
                }

                changes.push(ProjectGraphResumeChange {
                    task_id: current_item.task_id,
                    title: current_item.title.clone(),
                    kind: current_item.kind.clone(),
                    change: "updated".to_string(),
                    from_status: Some(previous_item.status.clone()),
                    to_status: Some(current_item.status.clone()),
                    goal_title: current_item.goal_title.clone(),
                    summary: format!(
                        "resume item updated: {} ({})",
                        current_item.title,
                        details.join(", ")
                    ),
                });
            }
            (None, None) => {}
        }
    }
    changes
}

fn build_project_change_summary(
    focus_shift: Option<&ProjectGraphFocusShift>,
    count_changes: &[ProjectGraphCountChange],
    workstream_changes: &[ProjectGraphWorkstreamChange],
    blocker_changes: &[ProjectGraphBlockerChange],
    resume_changes: &[ProjectGraphResumeChange],
) -> Vec<String> {
    let mut summary = Vec::new();

    if let Some(focus_shift) = focus_shift {
        summary.push(focus_shift.summary.clone());
    }

    let count_summary = count_changes
        .iter()
        .filter(|change| {
            matches!(
                change.field.as_str(),
                "pending_tasks" | "ready_tasks" | "running_tasks" | "blockers" | "resume_queue"
            )
        })
        .take(4)
        .map(|change| format!("{} {} -> {}", change.field, change.from, change.to))
        .collect::<Vec<_>>();
    if !count_summary.is_empty() {
        summary.push(format!("counts changed: {}", count_summary.join(", ")));
    }

    summary.extend(
        blocker_changes
            .iter()
            .filter(|change| change.change == "opened")
            .take(2)
            .map(|change| change.summary.clone()),
    );
    summary.extend(
        blocker_changes
            .iter()
            .filter(|change| change.change == "cleared")
            .take(2)
            .map(|change| change.summary.clone()),
    );
    summary.extend(
        workstream_changes
            .iter()
            .take(2)
            .map(|change| change.summary.clone()),
    );
    summary.extend(
        resume_changes
            .iter()
            .take(3)
            .map(|change| change.summary.clone()),
    );

    summary.truncate(8);
    summary
}

fn select_focus_workstream<'a>(
    active_project: Option<&'a ProjectNode>,
    resume_focus: Option<&ResumeQueueItem>,
) -> Option<&'a ProjectWorkstream> {
    let project = active_project?;

    if let Some(item) = resume_focus {
        if let Some(workstream) = project
            .workstreams
            .iter()
            .find(|workstream| workstream.goal_id == item.goal_id)
        {
            return Some(workstream);
        }
        if item.goal_id.is_none() {
            if let Some(workstream) = project
                .workstreams
                .iter()
                .find(|workstream| workstream.id == "workstream:unassigned")
            {
                return Some(workstream);
            }
        }
    }

    project.workstreams.first()
}
