#!/usr/bin/env python3
"""Twitter/X — post, read timeline/mentions, search, reply via twikit 1.7.x.

Uses the sync client (more reliable than async for v1).
Reads JSON from stdin, prints JSON to stdout.
Cookies cached in workspace/twitter_cookies.json.
"""
import copy
import json
import os
import sys

COOKIES_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "workspace", "twitter_cookies.json")
HTTP_TIMEOUT_SECS = 20.0
_TWIKIT_PATCHED = False


def safe_attr(obj, name, default=None):
    try:
        value = getattr(obj, name)
    except Exception:
        return default
    return default if value is None else value


def safe_user_screen_name(tweet):
    user = safe_attr(tweet, "user")
    if user is None:
        return "unknown"
    return safe_attr(user, "screen_name", "unknown")


def safe_tweet_text(tweet):
    for field in ("text", "full_text"):
        value = safe_attr(tweet, field)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def safe_tweet_id(tweet):
    value = safe_attr(tweet, "id")
    return "" if value is None else str(value)


def safe_notification_message(note):
    value = safe_attr(note, "message", "")
    if isinstance(value, str):
        return value.strip()
    text = safe_attr(value, "text", "")
    return text.strip() if isinstance(text, str) else ""


def normalized_count(value, default):
    try:
        count = int(value)
    except Exception:
        return default
    return max(1, min(count, 40))


def normalize_twikit_user_payload(data):
    payload = dict(data) if isinstance(data, dict) else {}
    legacy = payload.get("legacy")
    if not isinstance(legacy, dict):
        legacy = {}
    else:
        legacy = dict(legacy)
    payload["legacy"] = legacy

    entities = legacy.get("entities")
    if not isinstance(entities, dict):
        entities = {}
    else:
        entities = dict(entities)
    legacy["entities"] = entities

    description = entities.get("description")
    if not isinstance(description, dict):
        description = {}
    else:
        description = dict(description)
    entities["description"] = description
    if not isinstance(description.get("urls"), list):
        description["urls"] = []

    url_info = entities.get("url")
    if not isinstance(url_info, dict):
        url_info = {}
    else:
        url_info = dict(url_info)
    entities["url"] = url_info
    if not isinstance(url_info.get("urls"), list):
        url_info["urls"] = []

    defaults = {
        "created_at": "",
        "name": "",
        "screen_name": "unknown",
        "profile_image_url_https": "",
        "location": "",
        "description": "",
        "pinned_tweet_ids_str": [],
        "verified": False,
        "possibly_sensitive": False,
        "can_dm": False,
        "can_media_tag": False,
        "want_retweets": False,
        "default_profile": False,
        "default_profile_image": False,
        "has_custom_timelines": False,
        "followers_count": 0,
        "fast_followers_count": 0,
        "normal_followers_count": 0,
        "friends_count": 0,
        "favourites_count": 0,
        "listed_count": 0,
        "media_count": 0,
        "statuses_count": 0,
        "is_translator": False,
        "translator_type": "",
        "withheld_in_countries": [],
        "url": None,
        "profile_banner_url": None,
        "protected": False,
    }
    for key, default in defaults.items():
        if legacy.get(key) is None:
            legacy[key] = copy.deepcopy(default)

    rest_id = payload.get("rest_id")
    if rest_id is None:
        rest_id = payload.get("id") or legacy.get("screen_name") or "unknown"
    payload["rest_id"] = str(rest_id)

    if payload.get("is_blue_verified") is None:
        payload["is_blue_verified"] = bool(payload.get("ext_is_blue_verified", False))

    return payload


def patch_twikit():
    global _TWIKIT_PATCHED
    if _TWIKIT_PATCHED:
        return

    import twikit.client as twikit_client
    import twikit.user as twikit_user
    import twikit.utils as twikit_utils

    original_user_init = twikit_user.User.__init__
    original_build_user_data = getattr(twikit_utils, "build_user_data", None)

    def patched_user_init(self, client, data):
        return original_user_init(self, client, normalize_twikit_user_payload(data))

    twikit_user.User.__init__ = patched_user_init

    if callable(original_build_user_data):
        def patched_build_user_data(raw_data):
            built = original_build_user_data(raw_data)
            return normalize_twikit_user_payload(built)

        twikit_utils.build_user_data = patched_build_user_data
        twikit_client.build_user_data = patched_build_user_data

    _TWIKIT_PATCHED = True


def get_client():
    try:
        from twikit import Client
    except ImportError:
        return None, "twikit not installed. run: pip install twikit"

    patch_twikit()
    client = Client("en-US", timeout=HTTP_TIMEOUT_SECS)

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
        limit = normalized_count(count, 20)
        tweets = client.get_latest_timeline(count=limit)
        lines = []
        for t in tweets:
            user = safe_user_screen_name(t)
            text = safe_tweet_text(t)
            tweet_id = safe_tweet_id(t)
            if not text or not tweet_id:
                continue
            likes = safe_attr(t, "favorite_count", 0) or 0
            rts = safe_attr(t, "retweet_count", 0) or 0
            lines.append(f"@{user}: {text[:200]} [likes:{likes} rt:{rts}] (id:{tweet_id})")
            if len(lines) >= limit:
                break
        return {"success": True, "output": "\n---\n".join(lines) if lines else "empty timeline", "items": lines}
    except Exception as e:
        return {"success": False, "error": f"timeline failed: {str(e)[:200]}"}


def read_mentions(client, count=20):
    try:
        limit = normalized_count(count, 20)
        notifications = client.get_notifications("Mentions", count=limit)
        lines = []
        for note in notifications:
            from_user = safe_attr(note, "from_user")
            user = safe_attr(from_user, "screen_name", "unknown")
            message = safe_notification_message(note)
            tweet = safe_attr(note, "tweet")
            text = safe_tweet_text(tweet)
            tweet_id = safe_tweet_id(tweet)

            if text and tweet_id:
                prefix = message or "mentioned you"
                lines.append(f"@{user}: {prefix} — {text[:200]} (id:{tweet_id})")
            elif message:
                lines.append(f"@{user}: {message}")
            else:
                lines.append(f"@{user}: mention")
            if len(lines) >= limit:
                break
        return {"success": True, "output": "\n---\n".join(lines) if lines else "no mentions", "items": lines}
    except Exception as e:
        return {"success": False, "error": f"mentions failed: {str(e)[:200]}"}


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
        limit = normalized_count(count, 10)
        tweets = client.search_tweet(query, product="Latest", count=limit)
        lines = []
        for t in tweets:
            user = safe_user_screen_name(t)
            text = safe_tweet_text(t)
            tweet_id = safe_tweet_id(t)
            if not text or not tweet_id:
                continue
            lines.append(f"@{user}: {text[:200]} (id:{tweet_id})")
            if len(lines) >= limit:
                break
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
    elif action == "mentions":
        result = read_mentions(client, req.get("count", 20))
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
