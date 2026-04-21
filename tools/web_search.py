#!/usr/bin/env python3
import json, sys
def search(query, max_results=5):
    try:
        from ddgs import DDGS
        results = list(DDGS().text(query, max_results=max_results))
        output = "\n---\n".join(f"{r['title']}\n{r['body']}\n{r['href']}" for r in results)
        return {"success": True, "output": output if output else "no results"}
    except Exception as e:
        return {"success": False, "error": str(e)}
if __name__ == "__main__":
    try: req = json.loads(sys.stdin.read().strip())
    except: req = {}
    print(json.dumps(search(req.get("query", ""))))
