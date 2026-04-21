#!/usr/bin/env python3
import json, sys, subprocess

def run_gh(args):
    try:
        r = subprocess.run(["gh"] + args, capture_output=True, text=True, timeout=15)
        if r.returncode != 0:
            err = r.stderr.strip()
            if "not found" in err or "not installed" in err:
                return None, "gh CLI not installed — install from https://cli.github.com"
            if "auth" in err.lower() or "login" in err.lower():
                return None, "gh not authenticated — run `gh auth login`"
            return None, err or "gh command failed"
        return r.stdout.strip(), None
    except FileNotFoundError:
        return None, "gh CLI not installed — install from https://cli.github.com"
    except subprocess.TimeoutExpired:
        return None, "gh command timed out"
    except Exception as e:
        return None, str(e)

def notifications():
    out, err = run_gh(["api", "notifications", "--jq",
        '.[] | {title: .subject.title, repo: .repository.full_name, type: .subject.type, reason: .reason}'])
    if err:
        return {"success": False, "error": err}
    if not out:
        return {"success": True, "output": "No unread notifications.", "items": []}
    items = [json.loads(line) for line in out.splitlines() if line.strip()]
    summary = f"{len(items)} unread notification(s):\n" + "\n".join(
        f"  [{n['type']}] {n['repo']}: {n['title']}" for n in items)
    return {"success": True, "output": summary, "items": items}

def list_prs(repo):
    if not repo:
        return {"success": False, "error": "repo is required (e.g. pvlata75/Nyx)"}
    out, err = run_gh(["pr", "list", "--repo", repo, "--json",
        "number,title,author,state,updatedAt", "--limit", "20"])
    if err:
        return {"success": False, "error": err}
    items = json.loads(out) if out else []
    if not items:
        return {"success": True, "output": f"No open PRs in {repo}.", "items": []}
    summary = f"{len(items)} open PR(s) in {repo}:\n" + "\n".join(
        f"  #{p['number']} {p['title']} (by {p['author'].get('login','?')})" for p in items)
    return {"success": True, "output": summary, "items": items}

def list_issues(repo):
    if not repo:
        return {"success": False, "error": "repo is required (e.g. pvlata75/Nyx)"}
    out, err = run_gh(["issue", "list", "--repo", repo, "--json",
        "number,title,labels,state,updatedAt", "--limit", "20"])
    if err:
        return {"success": False, "error": err}
    items = json.loads(out) if out else []
    if not items:
        return {"success": True, "output": f"No open issues in {repo}.", "items": []}
    for i in items:
        i["labels"] = [l["name"] for l in i.get("labels", [])]
    summary = f"{len(items)} open issue(s) in {repo}:\n" + "\n".join(
        f"  #{i['number']} {i['title']}" + (f" [{', '.join(i['labels'])}]" if i['labels'] else "")
        for i in items)
    return {"success": True, "output": summary, "items": items}

ACTIONS = {"notifications": lambda r: notifications(),
           "prs": lambda r: list_prs(r.get("repo")),
           "issues": lambda r: list_issues(r.get("repo"))}

if __name__ == "__main__":
    try:
        req = json.loads(sys.stdin.read().strip())
    except Exception:
        req = {}
    action = req.get("action", "")
    if action not in ACTIONS:
        print(json.dumps({"success": False, "error": f"Unknown action '{action}'. Use: notifications, prs, issues"}))
    else:
        print(json.dumps(ACTIONS[action](req)))
