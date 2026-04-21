use super::projects::{ProjectGraphSnapshot, ProjectWorkstream, WorkstreamTaskPreview};

pub(crate) fn next_step_hint(snapshot: &ProjectGraphSnapshot) -> Option<String> {
    let project = snapshot.projects.first()?;
    let workstream = project.workstreams.first()?;
    Some(describe_workstream_next_step(&project.title, workstream))
}

fn describe_workstream_next_step(project_title: &str, workstream: &ProjectWorkstream) -> String {
    if let Some(task) = workstream.next_tasks.first() {
        return describe_pending_task_next_step(project_title, workstream, task);
    }

    if let Some(task) = workstream.running_tasks.first() {
        return format!(
            "check progress on {} in {} because it is already in flight",
            task.title, workstream.title
        );
    }

    if workstream.status == "blocked" {
        if !workstream.blockers.is_empty() {
            return format!(
                "clear blockers on {} in {} before new follow-through is queued",
                workstream.title, project_title
            );
        }

        return format!(
            "review {} in {} because it is blocked",
            workstream.title, project_title
        );
    }

    if workstream.status == "tracked" {
        return format!(
            "keep tracking {} in {} and wait for the next meaningful trigger",
            workstream.title, project_title
        );
    }

    format!(
        "review {} in {} for the next concrete follow-through",
        workstream.title, project_title
    )
}

fn describe_pending_task_next_step(
    project_title: &str,
    workstream: &ProjectWorkstream,
    task: &WorkstreamTaskPreview,
) -> String {
    if task.blocked_by.is_empty() {
        return format!(
            "do {} in {} because it is the strongest ready task",
            task.title, workstream.title
        );
    }

    if !workstream.blockers.is_empty() {
        return format!(
            "review blockers on {} in {} before {} can move",
            workstream.title, project_title, task.title
        );
    }

    format!(
        "review {} in {} because it is the next pending follow-through",
        task.title, workstream.title
    )
}
