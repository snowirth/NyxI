#!/usr/bin/env python3
"""Git awareness — status, log, todos, diff."""
import json, sys, subprocess, os, re

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TODO_EXTENSIONS = {".rs", ".py", ".js", ".ts", ".toml"}
TODO_SKIP_DIRS = {"node_modules", "target", ".git", "__pycache__", "dist", "build", ".venv", "venv"}
TODO_PATTERN = re.compile(r"#.*\b(TODO|FIXME|HACK)\b.*|//.*\b(TODO|FIXME|HACK)\b.*")


def _run(cmd, cwd=REPO_DIR):
    """Run a git command and return stdout or raise."""
    result = subprocess.run(
        cmd, cwd=cwd, capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git exited {result.returncode}")
    return result.stdout.strip()


def git_status():
    try:
        branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
        status = _run(["git", "status", "--porcelain"])
        lines = status.splitlines() if status else []
        modified = [l[3:] for l in lines if l[:2].strip() in ("M", "MM")]
        added = [l[3:] for l in lines if l[:2].strip() in ("A", "??")]
        deleted = [l[3:] for l in lines if l[:2].strip() == "D"]
        summary = f"branch: {branch}\n"
        summary += f"uncommitted changes: {len(lines)}\n"
        if modified:
            summary += f"modified: {', '.join(modified)}\n"
        if added:
            summary += f"new/untracked: {', '.join(added)}\n"
        if deleted:
            summary += f"deleted: {', '.join(deleted)}\n"
        if not lines:
            summary += "working tree clean"
        return {"success": True, "output": summary.strip()}
    except Exception as e:
        return {"success": False, "error": str(e)}


def git_log(count=10):
    try:
        count = min(int(count), 50)
        fmt = "%h|%an|%s|%cd"
        raw = _run(["git", "log", f"-{count}", f"--pretty=format:{fmt}", "--date=short"])
        if not raw:
            return {"success": True, "output": "no commits"}
        lines = []
        for entry in raw.splitlines():
            parts = entry.split("|", 3)
            if len(parts) == 4:
                lines.append(f"{parts[0]} ({parts[3]}) {parts[1]}: {parts[2]}")
            else:
                lines.append(entry)
        return {"success": True, "output": "\n".join(lines)}
    except Exception as e:
        return {"success": False, "error": str(e)}


def git_todos():
    try:
        raw = _run(["git", "ls-files"])
        if not raw:
            return {"success": True, "output": "no tracked files"}
        tracked = raw.splitlines()
        hits = []
        for fpath in tracked:
            # Filter by extension
            if os.path.splitext(fpath)[1] not in TODO_EXTENSIONS:
                continue
            # Skip excluded dirs
            parts = fpath.replace("\\", "/").split("/")
            if any(p in TODO_SKIP_DIRS for p in parts):
                continue
            full = os.path.join(REPO_DIR, fpath)
            if not os.path.isfile(full):
                continue
            try:
                with open(full, "r", errors="ignore") as f:
                    for i, line in enumerate(f, 1):
                        if TODO_PATTERN.search(line):
                            hits.append(f"{fpath}:{i}: {line.strip()}")
            except Exception:
                continue
        if not hits:
            return {"success": True, "output": "no TODO/FIXME/HACK found"}
        # Cap output
        output = "\n".join(hits[:100])
        if len(hits) > 100:
            output += f"\n... and {len(hits) - 100} more"
        return {"success": True, "output": output}
    except Exception as e:
        return {"success": False, "error": str(e)}


def git_diff():
    try:
        raw = _run(["git", "diff", "--stat"])
        if not raw:
            # Check staged too
            raw = _run(["git", "diff", "--cached", "--stat"])
        if not raw:
            return {"success": True, "output": "no uncommitted changes"}
        return {"success": True, "output": raw}
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    try:
        req = json.loads(sys.stdin.read().strip())
    except:
        req = {}
    action = req.get("action", "status")
    if action == "status":
        print(json.dumps(git_status()))
    elif action == "log":
        print(json.dumps(git_log(req.get("count", 10))))
    elif action == "todos":
        print(json.dumps(git_todos()))
    elif action == "diff":
        print(json.dumps(git_diff()))
    else:
        print(json.dumps({"success": False, "error": f"unknown action: {action}"}))
