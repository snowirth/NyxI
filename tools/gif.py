#!/usr/bin/env python3
"""GIF search via Tenor API. Returns a random GIF URL for the query."""
import json, sys, os, random, re
try:
    import urllib.request
except ImportError:
    pass

TENOR_API = "https://g.tenor.com/v1/search"
TENOR_KEY = os.getenv("TENOR_API_KEY", "LIVDSRZULELA")

def search_gif(query, limit=10):
    query = re.sub(r'[^\w\s\-]', '', query).strip()
    if not query:
        return {"success": False, "error": "invalid query"}

    url = f"{TENOR_API}?q={urllib.parse.quote(query)}&key={TENOR_KEY}&limit={limit}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return {"success": False, "error": f"tenor failed: {e}"}

    results = data.get("results", [])
    if not results:
        return {"success": False, "error": f"no GIFs for '{query}'"}

    gif = random.choice(results)
    media = gif.get("media", [{}])
    gif_url = ""
    if media:
        gif_url = media[0].get("gif", {}).get("url", "")
        if not gif_url:
            gif_url = media[0].get("mediumgif", {}).get("url", "")
        if not gif_url:
            gif_url = media[0].get("tinygif", {}).get("url", "")
    if not gif_url:
        gif_url = gif.get("url", "")

    if gif_url:
        return {"success": True, "output": gif_url}
    return {"success": False, "error": "GIF found but no URL"}

if __name__ == "__main__":
    try:
        req = json.loads(sys.stdin.read().strip())
    except Exception:
        req = {}
    print(json.dumps(search_gif(req.get("query", "funny"))))
