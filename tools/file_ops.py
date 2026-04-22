#!/usr/bin/env python3
"""File operations — read, write, evolve with .bak backup."""
import datetime
import hashlib
import json
import os
import shutil
import sys
import uuid

try:
    import fcntl
except ImportError:  # pragma: no cover - only hits non-Unix platforms
    fcntl = None

ALLOWED_DIRS = ("agents", "tools", "skills", "static", "workspace")
BLOCKED_COMPONENTS = {".git", ".ssh", "target"}
BLOCKED_FILENAMES = {".env"}
BLOCKED_SUFFIXES = (".key", ".pem")
PROJECT_ROOT = os.path.realpath(os.getcwd())
ALLOWED_ROOTS = [os.path.join(PROJECT_ROOT, directory) for directory in ALLOWED_DIRS]


def _is_within(path, root):
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        return False


def _looks_blocked(path):
    parts = [part for part in path.split(os.sep) if part not in ("", ".")]
    if any(part in BLOCKED_COMPONENTS for part in parts):
        return True
    if any(part in BLOCKED_FILENAMES for part in parts):
        return True
    if any(part.endswith(BLOCKED_SUFFIXES) for part in parts):
        return True
    return False


def resolve_safe_path(path, allow_missing=False):
    requested = (path or "").strip()
    if not requested or os.path.isabs(requested):
        return None
    if ".." in requested.replace("\\", "/").split("/"):
        return None

    normalized = os.path.normpath(requested)
    if normalized in ("", ".", ".."):
        return None
    if _looks_blocked(normalized):
        return None

    absolute = os.path.abspath(os.path.join(PROJECT_ROOT, normalized))
    real_path = os.path.realpath(absolute)
    if not _is_within(real_path, PROJECT_ROOT):
        return None

    allowed_subpath = any(_is_within(real_path, root) for root in ALLOWED_ROOTS)
    direct_root_file = (
        os.path.dirname(normalized) in ("", ".")
        and not os.path.basename(normalized).startswith(".")
    )
    if not allowed_subpath and not direct_root_file:
        return None

    if not allow_missing and not os.path.exists(real_path):
        return None
    return real_path


def _cleanup_backup(path):
    backup = f"{path}.bak"
    if os.path.exists(backup):
        os.remove(backup)


def _restore_path(path, existed_before):
    backup = f"{path}.bak"
    if existed_before and os.path.exists(backup):
        shutil.copy2(backup, path)
    elif not existed_before and os.path.exists(path):
        os.remove(path)
    _cleanup_backup(path)


def _provenance_log_path():
    configured = os.environ.get("NYX_FILE_PROVENANCE_LOG", "").strip()
    if configured:
        return configured
    return os.path.join(PROJECT_ROOT, "workspace", "file_provenance_live.jsonl")


def _sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _line_count(text):
    return len(text.splitlines())


def _preview(text, max_chars):
    collapsed = " ".join(text.split())
    return collapsed[:max_chars]


def _summarize_diff(before, after):
    if before == after:
        return {
            "start_line": None,
            "before_preview": _preview(before, 140),
            "after_preview": _preview(after, 140),
            "summary": "no content change",
        }

    before_lines = before.splitlines()
    after_lines = after.splitlines()
    prefix = 0
    max_prefix = min(len(before_lines), len(after_lines))
    while prefix < max_prefix and before_lines[prefix] == after_lines[prefix]:
        prefix += 1

    before_suffix = len(before_lines)
    after_suffix = len(after_lines)
    while (
        before_suffix > prefix
        and after_suffix > prefix
        and before_lines[before_suffix - 1] == after_lines[after_suffix - 1]
    ):
        before_suffix -= 1
        after_suffix -= 1

    before_chunk = "\n".join(before_lines[prefix:before_suffix])
    after_chunk = "\n".join(after_lines[prefix:after_suffix])
    return {
        "start_line": prefix + 1,
        "before_preview": _preview(before_chunk, 140),
        "after_preview": _preview(after_chunk, 140),
        "summary": (
            f"replaced `{_preview(before_chunk, 70)}` "
            f"with `{_preview(after_chunk, 70)}`"
        ),
    }


def _relative_project_path(path):
    return os.path.relpath(path, PROJECT_ROOT).replace("\\", "/")


def _append_provenance_event(resolved_path, before_text, after_text, provenance):
    if not isinstance(provenance, dict) or not provenance:
        return None

    operation_id = str(provenance.get("operation_id") or uuid.uuid4())
    metadata = provenance.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    event = {
        "schema_version": "nyx_file_provenance.v1",
        "id": str(uuid.uuid4()),
        "actor": str(provenance.get("actor") or "nyx"),
        "source": str(provenance.get("source") or "tools.file_ops"),
        "action_kind": str(provenance.get("action_kind") or "file_mutation"),
        "operation_id": operation_id,
        "target_path": _relative_project_path(resolved_path),
        "description": provenance.get("description"),
        "outcome": str(provenance.get("outcome") or "committed"),
        "before_exists": before_text is not None,
        "after_exists": after_text is not None,
        "before_sha256": _sha256_text(before_text) if before_text is not None else None,
        "after_sha256": _sha256_text(after_text) if after_text is not None else None,
        "before_bytes": len(before_text) if before_text is not None else None,
        "after_bytes": len(after_text) if after_text is not None else None,
        "before_line_count": _line_count(before_text) if before_text is not None else None,
        "after_line_count": _line_count(after_text) if after_text is not None else None,
        "diff": _summarize_diff(before_text or "", after_text or ""),
        "metadata": metadata,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    log_path = _provenance_log_path()
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    payload = json.dumps(event, sort_keys=True) + "\n"
    with open(log_path, "a", encoding="utf-8") as handle:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.write(payload)
        handle.flush()
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return event["id"]


def read_file(path):
    resolved = resolve_safe_path(path)
    if not resolved:
        return {"success": False, "error": f"blocked: {path}"}
    try:
        with open(resolved, encoding="utf-8") as handle:
            content = handle.read()
        return {"success": True, "output": content[:5000]}
    except Exception as e:
        return {"success": False, "error": str(e)}


def write_file(path, content, provenance=None):
    resolved = resolve_safe_path(path, allow_missing=True)
    if not resolved:
        return {"success": False, "error": f"blocked: {path}"}
    existed_before = os.path.exists(resolved)
    before_text = None
    try:
        # Backup existing file
        if existed_before:
            shutil.copy2(resolved, f"{resolved}.bak")
            with open(resolved, encoding="utf-8") as handle:
                before_text = handle.read()
        # Write
        os.makedirs(os.path.dirname(resolved) or ".", exist_ok=True)
        with open(resolved, "w", encoding="utf-8") as handle:
            handle.write(content)
        # Validate Python files
        if resolved.endswith(".py"):
            try:
                compile(content, resolved, "exec")
            except SyntaxError as e:
                _restore_path(resolved, existed_before)
                return {"success": False, "error": f"syntax error: {e}. rolled back."}
        provenance_event_id = None
        if provenance:
            try:
                provenance_event_id = _append_provenance_event(
                    resolved, before_text, content, provenance
                )
            except Exception as e:
                _restore_path(resolved, existed_before)
                return {
                    "success": False,
                    "error": f"failed to record provenance: {e}. rolled back.",
                }
        # Success — clean up backup
        _cleanup_backup(resolved)
        return {
            "success": True,
            "output": f"wrote {len(content)} bytes to {path}",
            "provenance_event_id": provenance_event_id,
        }
    except Exception as e:
        _restore_path(resolved, existed_before)
        return {"success": False, "error": str(e)}


def evolve(path, search, replace, description="", provenance=None):
    resolved = resolve_safe_path(path)
    if not resolved:
        return {"success": False, "error": f"blocked: {path}"}
    try:
        if not os.path.exists(resolved):
            return {"success": False, "error": "file not found"}
        with open(resolved, encoding="utf-8") as handle:
            content = handle.read()
        if search not in content:
            return {"success": False, "error": "search text not found"}
        # Backup
        shutil.copy2(resolved, f"{resolved}.bak")
        # Replace
        new_content = content.replace(search, replace, 1)
        with open(resolved, "w", encoding="utf-8") as handle:
            handle.write(new_content)
        # Validate Python
        if resolved.endswith(".py"):
            try:
                compile(new_content, resolved, "exec")
            except SyntaxError as e:
                _restore_path(resolved, True)
                return {"success": False, "error": f"syntax error: {e}. rolled back."}
        provenance_event_id = None
        if provenance:
            try:
                provenance_event_id = _append_provenance_event(
                    resolved, content, new_content, provenance
                )
            except Exception as e:
                _restore_path(resolved, True)
                return {
                    "success": False,
                    "error": f"failed to record provenance: {e}. rolled back.",
                }
        # Clean backup
        _cleanup_backup(resolved)
        return {
            "success": True,
            "output": f"evolved {path}: {description}",
            "provenance_event_id": provenance_event_id,
        }
    except Exception as e:
        # Rollback on any error
        _restore_path(resolved, True)
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    try:
        req = json.loads(sys.stdin.read().strip())
    except:
        req = {}
    action = req.get("action", "read")
    if action == "read":
        print(json.dumps(read_file(req.get("path", ""))))
    elif action == "write":
        print(
            json.dumps(
                write_file(
                    req.get("path", ""),
                    req.get("content", ""),
                    req.get("provenance"),
                )
            )
        )
    elif action == "evolve":
        print(
            json.dumps(
                evolve(
                    req.get("path", ""),
                    req.get("search", ""),
                    req.get("replace", ""),
                    req.get("description", ""),
                    req.get("provenance"),
                )
            )
        )
    else:
        print(json.dumps({"success": False, "error": f"unknown action: {action}"}))
