#!/usr/bin/env python3
"""File operations — read, write, evolve with .bak backup."""
import json, sys, os, shutil

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


def write_file(path, content):
    resolved = resolve_safe_path(path, allow_missing=True)
    if not resolved:
        return {"success": False, "error": f"blocked: {path}"}
    existed_before = os.path.exists(resolved)
    try:
        # Backup existing file
        if existed_before:
            shutil.copy2(resolved, f"{resolved}.bak")
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
        # Success — clean up backup
        _cleanup_backup(resolved)
        return {"success": True, "output": f"wrote {len(content)} bytes to {path}"}
    except Exception as e:
        _restore_path(resolved, existed_before)
        return {"success": False, "error": str(e)}


def evolve(path, search, replace, description=""):
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
        # Clean backup
        _cleanup_backup(resolved)
        return {"success": True, "output": f"evolved {path}: {description}"}
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
        print(json.dumps(write_file(req.get("path", ""), req.get("content", ""))))
    elif action == "evolve":
        print(json.dumps(evolve(req.get("path", ""), req.get("search", ""), req.get("replace", ""), req.get("description", ""))))
    else:
        print(json.dumps({"success": False, "error": f"unknown action: {action}"}))
