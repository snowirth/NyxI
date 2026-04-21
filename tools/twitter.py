#!/usr/bin/env python3
"""Twitter/X — post, read timeline, search, reply via twikit 1.7.x.

Uses sync client (more reliable than async for v1).
Reads JSON from stdin, prints JSON to stdout.
Cookies cached in workspace/twitter_cookies.json.
"""
import json, sys, os

COOKIES_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "workspace", "twitter_cookies.json")

def get_client():
    try:
        from twikit import Client
    except ImportError:
        return None, "twikit not installed. run: pip install twikit"

    client = Client("en-US")

    if os.path.exists(COOKIES_PATH):
        try:
            client.load_cookies(COOKIES_PATH)
            return client, None
        except Exception:
            pass

    # Try login with env vars
    username = os.environ.get("NYX_TWITTER_USERNAME", "")
    password = os.environ.get("NYX_TWITTER_PASSWORD", "")
    email = os.environ.get("NYX_TWITTER_EMAIL", "")
    if not username or not password:
        return None, "no cookies and no credentials"

    try:
        client.login(
            auth_info_1=username,
            auth_info_2=email or None,
            password=password,
        )
        os.makedirs(os.path.dirname(COOKIES_PATH), exist_ok=True)
        client.save_cookies(COOKIES_PATH)
        return client, None
    except Exception as e:
        return None, f"login failed: {e}"


def post_tweet(client, text):
    if len(text) > 280:
        text = text[:277] + "..."
    try:
        tweet = client.create_tweet(text)
        return {"success": True, "output": f"posted: {text}", "tweet_id": str(tweet.id)}
    except KeyError:
        # twikit sometimes can't parse success response — tweet likely posted
        return {"success": True, "output": f"posted (unconfirmed): {text}"}
    except Exception as e:
        err = str(e)
        if "Duplicate" in err or "187" in err:
            return {"success": False, "error": "duplicate tweet"}
        return {"success": False, "error": f"post failed: {err[:200]}"}


def read_timeline(client, count=20):
    try:
        tweets = client.get_latest_timeline(count=count)
        lines = []
        for t in tweets:
            user = t.user.screen_name if t.user else "unknown"
            likes = getattr(t, 'favorite_count', 0)
            rts = getattr(t, 'retweet_count', 0)
            lines.append(f"@{user}: {t.text[:200]} [likes:{likes} rt:{rts}] (id:{t.id})")
        return {"success": True, "output": "\n---\n".join(lines) if lines else "empty timeline", "items": lines}
    except Exception as e:
        return {"success": False, "error": f"timeline failed: {str(e)[:200]}"}


def reply_to(client, tweet_id, text):
    if len(text) > 280:
        text = text[:277] + "..."
    try:
        tweet = client.create_tweet(text, reply_to=tweet_id)
        return {"success": True, "output": f"replied to {tweet_id}: {text}", "tweet_id": str(tweet.id)}
    except KeyError:
        return {"success": True, "output": f"replied (unconfirmed) to {tweet_id}: {text}"}
    except Exception as e:
        return {"success": False, "error": f"reply failed: {str(e)[:200]}"}


def like_tweet(client, tweet_id):
    try:
        client.favorite_tweet(tweet_id)
        return {"success": True, "output": f"liked {tweet_id}"}
    except Exception as e:
        return {"success": False, "error": f"like failed: {str(e)[:200]}"}


def search_tweets(client, query, count=10):
    try:
        tweets = client.search_tweet(query, product="Latest", count=count)
        lines = []
        for t in tweets:
            user = t.user.screen_name if t.user else "unknown"
            lines.append(f"@{user}: {t.text[:200]} (id:{t.id})")
        return {"success": True, "output": "\n---\n".join(lines) if lines else "no results", "items": lines}
    except Exception as e:
        return {"success": False, "error": f"search failed: {str(e)[:200]}"}


def main():
    try:
        req = json.loads(sys.stdin.read().strip())
    except Exception:
        req = {}

    action = req.get("action", "timeline")

    client, err = get_client()
    if err:
        print(json.dumps({"success": False, "error": err}))
        return

    if action == "post":
        result = post_tweet(client, req.get("text", ""))
    elif action == "timeline":
        result = read_timeline(client, req.get("count", 20))
    elif action == "reply":
        result = reply_to(client, req.get("tweet_id", ""), req.get("text", ""))
    elif action == "like":
        result = like_tweet(client, req.get("tweet_id", ""))
    elif action == "search":
        result = search_tweets(client, req.get("query", ""), req.get("count", 10))
    else:
        result = {"success": False, "error": f"unknown action: {action}"}

    print(json.dumps(result))

if __name__ == "__main__":
    main()
