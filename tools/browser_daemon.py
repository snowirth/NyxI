#!/usr/bin/env python3
"""Nyx browser daemon — long-running Playwright supervisor.

Unlike `tools/browser.py` which spawns a fresh Chromium per invocation,
this daemon keeps a per-session Browser+Page alive across many RPC calls
so a user can interact across chat turns.

Protocol
--------
JSON-RPC over stdin/stdout. One request per line, one response per line.
Requests are delimited by newlines so the Rust side can read incrementally.

Methods:
  session_open        {"session_id"?, "url"?, "jar"?}       -> {"session_id", "final_url", "title"}
  session_step        {"session_id", "steps":[{...}, ...]}  -> {"final_url", "title", "steps":[...]}
  session_close       {"session_id"}                         -> {}
  session_list        {}                                     -> {"sessions":[{..., "jar": ...}, ...]}
  session_list_saved  {}                                     -> {"sessions":[{<saved-metadata>}, ...]}
  session_restore     {"session_id"}                         -> {"session_id", "final_url", "title"}
  jar_list            {}                                     -> {"jars":[{"name", "session_count", "created_at_monotonic"}, ...]}
  shutdown            {}                                     -> {} (daemon exits 0 after)

Envelope:
  success -> {"id": <req_id>, "ok": true,  "result": {...}}
  error   -> {"id": <req_id>, "ok": false, "error": "<type>: <msg>"}

Cookie jars (opt-in)
--------------------
By default every `session_open` creates a fresh isolated `BrowserContext`,
so sessions do not share cookies or localStorage.

Passing `jar: "<name>"` to `session_open` groups sessions under a named
jar. Sessions in the same jar share one underlying `BrowserContext` and
therefore share cookies, localStorage, and any other context state.
Sessions in different jars remain fully isolated, same as today. The
jar name must match `[a-z0-9_-]{1,40}` (enforced by the Rust side, but
we re-validate defensively here too).

A jar's context lives as long as any session in it is alive: the last
session to close takes the context with it. Jars are not persisted across
daemon restarts.

Evaluate step (opt-in, doubly-gated)
------------------------------------
`evaluate` runs arbitrary JS against the current page via
`page.evaluate(expression)`. It is strictly more dangerous than every
other step (can exfiltrate DOM + cookies + localStorage, can mutate page
state invisibly to a recipe reader). It is therefore gated on BOTH:
  1. Server-side env: `NYX_BROWSER_ALLOW_JS_EVAL=1` must be set.
  2. Per-call: the step must carry `"confirm": "I know what this does"`.
Both gates must pass; either alone is insufficient.

Design contract
---------------
- Max 3 concurrent sessions (pages). Jars are free; sessions are bounded.
- Sessions idle > 600s are reaped on every RPC (inline; no background thread needed
  because the daemon is single-threaded by design — per-session serialisation is
  enforced by the RPC loop).
- Per-step timeout is 10s (STEP_TIMEOUT_MS), same as `tools/browser.py`.
- Navigation timeout is 30s.
- On shutdown (method or EOF), best-effort close all sessions.
- Never crash on bad input — always answer with an error envelope.
"""
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone

# Caps and timeouts mirror tools/browser.py so session_step behaves
# identically to a one-shot `interact` call.
NAVIGATE_TEXT_CAP = 4 * 1024
EXTRACT_TEXT_CAP = 16 * 1024
FETCH_HTML_CAP = 32 * 1024
EVALUATE_OUTPUT_CAP = 32 * 1024  # 32KB cap on JSON-encoded evaluate result
NAV_TIMEOUT_MS = 30_000
STEP_TIMEOUT_MS = 10_000

MAX_SESSIONS = 3
IDLE_TIMEOUT_SECS = 600  # 10 minutes

_JAR_NAME_RE = re.compile(r"^[a-z0-9_-]{1,40}$")
_EVALUATE_CONFIRM_STRING = "I know what this does"
_EVALUATE_ENV_VAR = "NYX_BROWSER_ALLOW_JS_EVAL"

# --- Persistence configuration ----------------------------------------------
#
# The daemon writes a small JSON file on every mutating RPC so a user can
# reopen a saved session after a Nyx restart. The restore is explicit: the
# file only carries metadata (session_id, last_url, timestamps, jar name,
# path to a storage-state blob). `session_restore` is what actually
# re-launches Chromium and rehydrates cookies + localStorage.
#
# Layout (rooted at STATE_DIR):
#   STATE_DIR/sessions_state.json        -- atomic index
#   STATE_DIR/storage/<session_id>.json  -- per-session storage_state
#   STATE_DIR/storage/jar_<name>.json    -- per-jar storage_state
#
# State dir resolution precedence (highest first):
#   1. NYX_BROWSER_STATE_DIR env var (used by tests)
#   2. NYX_SANDBOX_WORKDIR_ROOT/persistent (reuses sandbox workdir root)
#   3. $HOME/.nyx-sandbox
#   4. /tmp/nyx-sandbox (defensive fallback when nothing else is set)

_SCHEMA_VERSION = 1


def _resolve_state_dir():
    """Pick the state directory using the documented precedence."""
    override = os.environ.get("NYX_BROWSER_STATE_DIR")
    if override:
        return override
    sandbox_root = os.environ.get("NYX_SANDBOX_WORKDIR_ROOT")
    if sandbox_root:
        return os.path.join(sandbox_root, "persistent")
    home = os.environ.get("HOME")
    if home:
        return os.path.join(home, ".nyx-sandbox")
    # Extremely unusual: no HOME, no sandbox override, no test override.
    # Fall back to /tmp so we never crash, but warn once to stderr.
    print(
        "browser_daemon: no HOME/NYX_SANDBOX_WORKDIR_ROOT/NYX_BROWSER_STATE_DIR set; "
        "falling back to /tmp/nyx-sandbox for persistence",
        file=sys.stderr,
    )
    return "/tmp/nyx-sandbox"


STATE_DIR = _resolve_state_dir()
STATE_FILE = os.path.join(STATE_DIR, "sessions_state.json")
STORAGE_DIR = os.path.join(STATE_DIR, "storage")

# Saved-state in-memory index. Populated from disk at startup; updated on
# every mutating RPC. Shape mirrors the on-disk JSON entries:
#   _SAVED_SESSIONS[session_id] = {
#       "session_id": str,
#       "last_url": str,
#       "last_activity_at_epoch": float,
#       "jar": str | None,
#       "storage_state_path": str,
#   }
#   _SAVED_JARS[jar_name] = {
#       "name": str,
#       "created_at_monotonic_relative_secs": float,
#       "storage_state_path": str,
#   }
_SAVED_SESSIONS = {}
_SAVED_JARS = {}


class SessionSlot:
    """One live browsing session. Owns (or references) the Playwright objects.

    For jar-less sessions the slot owns the full (playwright, browser,
    context, page) stack and closes all of them on teardown.

    For jar-backed sessions only the `page` is owned by this slot; the
    playwright/browser/context are owned by the `_JARS` entry and are
    closed when the jar's session_count hits zero (see `_close_session`).
    """

    __slots__ = (
        "session_id",
        "playwright",
        "browser",
        "context",
        "page",
        "last_activity_at",
        "jar",
    )

    def __init__(self, session_id, playwright, browser, context, page, jar=None):
        self.session_id = session_id
        self.playwright = playwright
        self.browser = browser
        self.context = context
        self.page = page
        self.last_activity_at = time.monotonic()
        self.jar = jar

    def touch(self):
        self.last_activity_at = time.monotonic()

    def idle_seconds(self):
        return time.monotonic() - self.last_activity_at


# Module-level state: session_id -> SessionSlot.
_SESSIONS = {}

# Module-level state for cookie jars: jar_name -> dict with:
#   {"context": BrowserContext,
#    "browser": Browser,
#    "session_count": int,
#    "created_at_monotonic": float}
# Ref-counting: `session_count` tracks how many live sessions reference
# the jar. We increment on session_open(jar=...) and decrement on close
# (both explicit `session_close` and idle-reaper paths). When the count
# hits zero the context/browser are torn down and the jar is popped from
# `_JARS`. No scanning on every close — O(1) bookkeeping.
# The playwright driver is shared (see `_SHARED_PLAYWRIGHT`) because
# `sync_playwright().start()` uses a greenlet loop that only supports one
# instance per Python process.
_JARS = {}

# One Playwright driver per process — sync_playwright's greenlet runtime
# does not support multiple concurrent instances. Every session (jar or
# standalone) launches its own Chromium browser on top of this shared
# driver. Lazily initialised on first session.
_SHARED_PLAYWRIGHT = None


def _ensure_playwright():
    """Lazily start (and return) the process-wide Playwright driver."""
    global _SHARED_PLAYWRIGHT
    if _SHARED_PLAYWRIGHT is None:
        from playwright.sync_api import sync_playwright  # type: ignore
        _SHARED_PLAYWRIGHT = sync_playwright().start()
    return _SHARED_PLAYWRIGHT


def _goto(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)


def _validate_jar_name(jar):
    if not isinstance(jar, str) or not _JAR_NAME_RE.match(jar):
        raise ValueError("jar must match [a-z0-9_-]{1,40}")
    return jar


def _close_jar_stack(entry):
    """Tear down a jar's browser+context. Best-effort.

    Does NOT stop the shared Playwright driver — other jars + standalone
    sessions may still depend on it. The driver is stopped in
    `_close_all_sessions` once no sessions remain.
    """
    try:
        ctx = entry.get("context")
        if ctx is not None:
            ctx.close()
    except Exception:
        pass
    try:
        browser = entry.get("browser")
        if browser is not None:
            browser.close()
    except Exception:
        pass


def _close_standalone_stack(slot):
    """Tear down a non-jar session's own browser. Best-effort.

    Same shared-driver caveat as `_close_jar_stack`: the process-wide
    Playwright is stopped only when every session is gone.
    """
    try:
        if slot.browser is not None:
            slot.browser.close()
    except Exception:
        pass


def _close_session(slot):
    """Close one session, decrementing its jar (if any).

    If the session was the last one in its jar, the shared context/browser
    are closed too and the jar entry is popped. Centralised so the
    explicit `session_close` handler and the idle reaper use identical
    bookkeeping.

    Also purges the saved-state entry for this session and its storage
    file. For jar-backed sessions the jar storage file is deleted only
    when the jar itself tears down (last session in the jar closed).
    """
    # Always close the page first (best-effort).
    try:
        if slot.page is not None:
            slot.page.close()
    except Exception:
        pass

    # Saved-state bookkeeping happens in every close path so the idle
    # reaper and the explicit close_close both drop the saved entry.
    _drop_saved_session(slot.session_id, slot.jar)

    if slot.jar is None:
        # Standalone: slot owns the whole stack.
        _close_standalone_stack(slot)
        return

    entry = _JARS.get(slot.jar)
    if entry is None:
        # Shouldn't happen — but if the jar is already gone, nothing left
        # to release.
        return
    entry["session_count"] = max(0, entry.get("session_count", 0) - 1)
    if entry["session_count"] == 0:
        _close_jar_stack(entry)
        _JARS.pop(slot.jar, None)
        _drop_saved_jar(slot.jar)


# --- Persistence helpers ----------------------------------------------------


def _ensure_state_dirs():
    """Create STATE_DIR and STORAGE_DIR if missing. Tolerant of races."""
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        os.makedirs(STORAGE_DIR, exist_ok=True)
    except Exception as exc:
        print(
            "browser_daemon: failed to create state dir %s: %s" % (STATE_DIR, exc),
            file=sys.stderr,
        )


def _session_storage_path(session_id):
    return os.path.join(STORAGE_DIR, "%s.json" % session_id)


def _jar_storage_path(jar_name):
    return os.path.join(STORAGE_DIR, "jar_%s.json" % jar_name)


def _remove_file_silently(path):
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass


def _drop_saved_session(session_id, jar):
    """Remove a session's saved entry + its storage file (if jar-less)."""
    entry = _SAVED_SESSIONS.pop(session_id, None)
    if entry is None:
        # Not saved yet — nothing to clean. Still call _persist_state so
        # the file stays coherent after an idle-reap of an un-persisted
        # session.
        _persist_state()
        return
    # Storage path: jar-less sessions own their own storage file; jar-backed
    # sessions share the jar's storage file (cleaned when the jar itself
    # tears down via _drop_saved_jar).
    if jar is None:
        _remove_file_silently(entry.get("storage_state_path"))
    _persist_state()


def _drop_saved_jar(jar_name):
    """Remove a jar's saved entry + its shared storage file."""
    entry = _SAVED_JARS.pop(jar_name, None)
    if entry is None:
        _persist_state()
        return
    _remove_file_silently(entry.get("storage_state_path"))
    _persist_state()


def _capture_storage_state(context, path):
    """Best-effort dump a context's storage_state to `path`.

    Returns True on success. Never raises — a failed capture just means
    the next restore won't have cookies for this context, not that the
    session is broken.
    """
    try:
        context.storage_state(path=path)
        return True
    except Exception as exc:
        print(
            "browser_daemon: storage_state capture failed for %s: %s" % (path, exc),
            file=sys.stderr,
        )
        return False


def _now_epoch():
    return time.time()


def _monotonic_to_epoch(monotonic_ts):
    """Convert a time.monotonic() reading to a wall-clock epoch estimate.

    monotonic and time.time() both tick at roughly the same rate; the
    delta between them is (approximately) a constant per process, so
    `now_epoch - (now_monotonic - saved_monotonic)` reverses it.
    """
    return _now_epoch() - (time.monotonic() - monotonic_ts)


def _expired_at_save_time(saved_session):
    """Return True if the saved session has been idle > IDLE_TIMEOUT_SECS.

    `last_activity_at_epoch` is a wall-clock timestamp written when the
    session last mutated state. Compared to `now_epoch()` at restore time.
    """
    last_epoch = saved_session.get("last_activity_at_epoch")
    if not isinstance(last_epoch, (int, float)):
        return True
    return (_now_epoch() - float(last_epoch)) > IDLE_TIMEOUT_SECS


def _persist_state():
    """Atomic write of the full state snapshot.

    For every live session, re-capture its context's storage_state into
    the per-session or per-jar file. Then re-serialise the index using a
    write-temp → fsync → rename pattern so a crash never leaves a
    half-written file visible.

    Swallows all exceptions: persistence failures must NEVER crash the
    daemon. The last successfully written file wins.
    """
    _ensure_state_dirs()

    now_epoch = _now_epoch()
    sessions_payload = []

    # Track which jars have already had their storage captured this pass
    # so we don't re-capture once per session sharing the jar.
    jar_captured = set()

    for sid, slot in _SESSIONS.items():
        try:
            last_url = slot.page.url
        except Exception:
            last_url = ""
        last_activity_epoch = _monotonic_to_epoch(slot.last_activity_at)

        if slot.jar is None:
            storage_path = _session_storage_path(sid)
            _capture_storage_state(slot.context, storage_path)
        else:
            storage_path = _jar_storage_path(slot.jar)
            if slot.jar not in jar_captured:
                _capture_storage_state(slot.context, storage_path)
                jar_captured.add(slot.jar)

        entry = {
            "session_id": sid,
            "last_url": last_url,
            "last_activity_at_epoch": last_activity_epoch,
            "jar": slot.jar,
            "storage_state_path": storage_path,
        }
        _SAVED_SESSIONS[sid] = entry
        sessions_payload.append(entry)

    # Also refresh the in-memory jar index from the live jars.
    jars_payload = []
    for jar_name, jar_entry in _JARS.items():
        storage_path = _jar_storage_path(jar_name)
        created_rel = jar_entry.get("created_at_monotonic", 0.0)
        meta = {
            "name": jar_name,
            "created_at_monotonic_relative_secs": created_rel,
            "storage_state_path": storage_path,
        }
        _SAVED_JARS[jar_name] = meta
        jars_payload.append(meta)

    # Saved-but-not-live sessions/jars (persisted from a previous boot
    # and not yet restored) stay in the payload so a second restart
    # doesn't lose them.
    live_sids = {sid for sid in _SESSIONS.keys()}
    live_jars = {name for name in _JARS.keys()}
    for sid, meta in _SAVED_SESSIONS.items():
        if sid in live_sids:
            continue
        sessions_payload.append(meta)
    for name, meta in _SAVED_JARS.items():
        if name in live_jars:
            continue
        jars_payload.append(meta)

    # De-duplicate in case the live and saved pass both emitted an entry.
    seen = set()
    deduped_sessions = []
    for entry in sessions_payload:
        sid = entry.get("session_id")
        if sid in seen:
            continue
        seen.add(sid)
        deduped_sessions.append(entry)

    seen_jars = set()
    deduped_jars = []
    for entry in jars_payload:
        name = entry.get("name")
        if name in seen_jars:
            continue
        seen_jars.add(name)
        deduped_jars.append(entry)

    document = {
        "schema_version": _SCHEMA_VERSION,
        "saved_at": datetime.fromtimestamp(now_epoch, tz=timezone.utc).isoformat(),
        "sessions": deduped_sessions,
        "jars": deduped_jars,
    }

    tmp_path = STATE_FILE + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(document, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, STATE_FILE)
    except Exception as exc:
        print(
            "browser_daemon: persist_state failed: %s" % exc,
            file=sys.stderr,
        )
        _remove_file_silently(tmp_path)


def _load_saved_state():
    """Read sessions_state.json into _SAVED_SESSIONS / _SAVED_JARS.

    Tolerant of missing / corrupt files: on any parse error, log once to
    stderr and start with an empty index. Never raises.
    """
    global _SAVED_SESSIONS, _SAVED_JARS
    _SAVED_SESSIONS = {}
    _SAVED_JARS = {}
    if not os.path.isfile(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            document = json.load(f)
    except Exception as exc:
        print(
            "browser_daemon: sessions_state.json unreadable (%s); starting clean"
            % exc,
            file=sys.stderr,
        )
        return
    if not isinstance(document, dict):
        print(
            "browser_daemon: sessions_state.json not an object; starting clean",
            file=sys.stderr,
        )
        return
    sessions = document.get("sessions")
    if isinstance(sessions, list):
        for entry in sessions:
            if not isinstance(entry, dict):
                continue
            sid = entry.get("session_id")
            if not isinstance(sid, str) or not sid:
                continue
            _SAVED_SESSIONS[sid] = {
                "session_id": sid,
                "last_url": entry.get("last_url") or "",
                "last_activity_at_epoch": entry.get("last_activity_at_epoch") or 0.0,
                "jar": entry.get("jar"),
                "storage_state_path": entry.get("storage_state_path") or "",
            }
    jars = document.get("jars")
    if isinstance(jars, list):
        for entry in jars:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not isinstance(name, str) or not name:
                continue
            _SAVED_JARS[name] = {
                "name": name,
                "created_at_monotonic_relative_secs": entry.get(
                    "created_at_monotonic_relative_secs", 0.0
                ),
                "storage_state_path": entry.get("storage_state_path") or "",
            }


def _prune_orphan_storage_files():
    """Delete files in STORAGE_DIR that no saved entry references.

    Defensive: a crash mid-write could leak a storage file after its
    index entry was dropped. We run once at startup after _load_saved_state.
    """
    if not os.path.isdir(STORAGE_DIR):
        return
    referenced = set()
    for meta in _SAVED_SESSIONS.values():
        sp = meta.get("storage_state_path")
        if sp:
            referenced.add(os.path.abspath(sp))
    for meta in _SAVED_JARS.values():
        sp = meta.get("storage_state_path")
        if sp:
            referenced.add(os.path.abspath(sp))
    try:
        for name in os.listdir(STORAGE_DIR):
            full = os.path.abspath(os.path.join(STORAGE_DIR, name))
            if full not in referenced:
                _remove_file_silently(full)
    except Exception:
        pass


def _reap_idle_sessions():
    """Close sessions idle > IDLE_TIMEOUT_SECS. Called inline on every RPC."""
    expired = [sid for sid, slot in _SESSIONS.items()
               if slot.idle_seconds() > IDLE_TIMEOUT_SECS]
    for sid in expired:
        slot = _SESSIONS.pop(sid, None)
        if slot is not None:
            _close_session(slot)


def _new_context_with_optional_storage(browser, storage_state_path):
    """Create a BrowserContext, loading `storage_state` from disk if available.

    Playwright accepts either a dict or a path via `storage_state=`. We
    pass the path directly when the file is readable; falling back to a
    fresh context if the file is missing or malformed.
    """
    if storage_state_path and os.path.isfile(storage_state_path):
        try:
            return browser.new_context(storage_state=storage_state_path)
        except Exception as exc:
            print(
                "browser_daemon: storage_state load failed for %s: %s"
                % (storage_state_path, exc),
                file=sys.stderr,
            )
    return browser.new_context()


def _launch_standalone_session(session_id, storage_state_path=None):
    """Create a new browser+context+page for a jar-less session.

    Reuses the process-wide Playwright driver but launches a dedicated
    Chromium so cookies/storage stay isolated from every other session.
    Optionally hydrates the context from a saved `storage_state_path`
    (cookies + localStorage) when restoring a previously-saved session.
    """
    p = _ensure_playwright()
    browser = p.chromium.launch(headless=True)
    context = _new_context_with_optional_storage(browser, storage_state_path)
    page = context.new_page()
    page.set_default_timeout(NAV_TIMEOUT_MS)
    page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    return SessionSlot(session_id, p, browser, context, page, jar=None)


def _launch_jar_session(session_id, jar, storage_state_path=None):
    """Create (or reuse) a jar's shared context and allocate a page on it.

    First session in a jar bootstraps a dedicated Chromium + context on
    top of the shared Playwright driver; subsequent sessions in the same
    jar open a new page on the existing context so cookies/localStorage
    are shared. When a saved `storage_state_path` is supplied AND we're
    creating the jar (not reusing), we hydrate the context from it.
    """
    p = _ensure_playwright()

    entry = _JARS.get(jar)
    if entry is None:
        browser = p.chromium.launch(headless=True)
        context = _new_context_with_optional_storage(browser, storage_state_path)
        entry = {
            "browser": browser,
            "context": context,
            "session_count": 0,
            "created_at_monotonic": time.monotonic(),
        }
        _JARS[jar] = entry

    page = entry["context"].new_page()
    page.set_default_timeout(NAV_TIMEOUT_MS)
    page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    entry["session_count"] = entry.get("session_count", 0) + 1
    return SessionSlot(
        session_id,
        p,
        entry["browser"],
        entry["context"],
        page,
        jar=jar,
    )


def _launch_session(session_id, jar=None, storage_state_path=None):
    """Dispatch to the right launcher based on whether a jar was requested."""
    if jar is None:
        return _launch_standalone_session(session_id, storage_state_path=storage_state_path)
    return _launch_jar_session(session_id, jar, storage_state_path=storage_state_path)


# --- Recipe step handlers (shared shape with tools/browser.py) ---------------


def _step_fill(page, step):
    selector = step.get("selector")
    value = step.get("value")
    if not isinstance(selector, str) or not selector:
        raise ValueError("fill step requires string `selector`")
    if not isinstance(value, str):
        raise ValueError("fill step requires string `value`")
    page.fill(selector, value, timeout=STEP_TIMEOUT_MS)
    return None


def _step_click(page, step):
    selector = step.get("selector")
    if not isinstance(selector, str) or not selector:
        raise ValueError("click step requires string `selector`")
    page.click(selector, timeout=STEP_TIMEOUT_MS)
    return None


def _step_press(page, step):
    key = step.get("key")
    if not isinstance(key, str) or not key:
        raise ValueError("press step requires string `key`")
    selector = step.get("selector")
    if isinstance(selector, str) and selector:
        page.focus(selector, timeout=STEP_TIMEOUT_MS)
    page.keyboard.press(key)
    return None


def _step_wait_for(page, step):
    selector = step.get("selector")
    if not isinstance(selector, str) or not selector:
        raise ValueError("wait_for step requires string `selector`")
    page.wait_for_selector(selector, state="visible", timeout=STEP_TIMEOUT_MS)
    return None


def _step_screenshot(page, step):
    out_path = step.get("out_path")
    if not isinstance(out_path, str) or not out_path:
        raise ValueError("screenshot step requires string `out_path`")
    page.screenshot(path=out_path, full_page=True)
    return {"out_path": out_path}


def _step_extract_text(page, _step):
    body_text = page.inner_text("body") if page.query_selector("body") else ""
    return body_text[:EXTRACT_TEXT_CAP]


def _step_extract_links(page, _step):
    anchors = page.eval_on_selector_all(
        "a",
        """els => els.map(el => ({
            href: el.getAttribute('href') || '',
            text: (el.innerText || el.textContent || '').trim()
        })).filter(x => x.href)"""
    )
    return anchors


def _step_fetch_html(page, _step):
    html = page.content() or ""
    return html[:FETCH_HTML_CAP]


def _step_navigate(page, step):
    url = step.get("url")
    if not isinstance(url, str) or not url:
        raise ValueError("navigate step requires string `url`")
    _goto(page, url)
    return {"final_url": page.url, "title": page.title() or ""}


def _step_evaluate(page, step):
    """Run arbitrary JS via `page.evaluate(expression)`.

    Requires BOTH gates:
      - env var NYX_BROWSER_ALLOW_JS_EVAL=1 (operator consent)
      - `confirm` == "I know what this does" (per-call caller consent)

    Rationale: `page.evaluate` can exfiltrate DOM state, cookies (when
    cookies have been loaded into the context), localStorage, and mutate
    the page in ways a step-by-step recipe reader can't audit. We want
    it reachable for power-user flows (e.g. scraping JS-computed values)
    but never accidentally on. The dual-gate design makes every single
    use a deliberate choice at two layers.
    """
    # Gate 1: server-side env. Checked BEFORE confirm so the operator's
    # consent is validated first; otherwise a caller could learn the
    # confirm string from error messages and chain into a later env-flip.
    if os.environ.get(_EVALUATE_ENV_VAR) != "1":
        raise ValueError(
            'evaluate step is disabled; set %s=1 to enable' % _EVALUATE_ENV_VAR
        )
    # Gate 2: per-call confirm string (exact match required).
    if step.get("confirm") != _EVALUATE_CONFIRM_STRING:
        raise ValueError(
            'evaluate step requires explicit confirm="%s"' % _EVALUATE_CONFIRM_STRING
        )
    expression = step.get("expression")
    if not isinstance(expression, str) or not expression:
        raise ValueError("evaluate step requires string `expression`")

    # Playwright's `page.evaluate` takes an expression or function source.
    # Note: there is no per-call timeout parameter in sync_api; the page's
    # default timeout (NAV_TIMEOUT_MS, 30s) bounds navigation-related waits
    # inside the evaluated script. Synchronous returns come back near-
    # instantly for typical use. The outer RPC timeout also bounds this.
    value = page.evaluate(expression)
    encoded = json.dumps(value, default=str)
    if len(encoded) > EVALUATE_OUTPUT_CAP:
        encoded = encoded[:EVALUATE_OUTPUT_CAP]
    return encoded


_STEP_DISPATCH = {
    "fill":          _step_fill,
    "click":         _step_click,
    "press":         _step_press,
    "wait_for":      _step_wait_for,
    "screenshot":    _step_screenshot,
    "extract_text":  _step_extract_text,
    "extract_links": _step_extract_links,
    "fetch_html":    _step_fetch_html,
    "navigate":      _step_navigate,
    "evaluate":      _step_evaluate,
}

_CAPTURE_STEPS = {
    "screenshot",
    "extract_text",
    "extract_links",
    "fetch_html",
    "navigate",
    "evaluate",
}


def _run_steps(page, steps):
    """Return (records, error_message_or_None). Same shape as tools/browser.py."""
    records = []
    for idx, raw_step in enumerate(steps):
        if not isinstance(raw_step, dict):
            records.append({
                "index": idx,
                "type": None,
                "ok": False,
                "error": "step is not an object",
            })
            return records, "step %d: step is not an object" % idx

        step_type = raw_step.get("type")
        if not isinstance(step_type, str) or not step_type:
            records.append({
                "index": idx,
                "type": step_type,
                "ok": False,
                "error": "missing `type`",
            })
            return records, "step %d: missing `type`" % idx

        handler = _STEP_DISPATCH.get(step_type)
        if handler is None:
            records.append({
                "index": idx,
                "type": step_type,
                "ok": False,
                "error": "unknown step type '%s'" % step_type,
            })
            return records, "step %d (%s): unknown step type" % (idx, step_type)

        try:
            output = handler(page, raw_step)
        except Exception as exc:  # noqa: BLE001 — surface any Playwright failure
            msg = "%s: %s" % (type(exc).__name__, exc)
            records.append({
                "index": idx,
                "type": step_type,
                "ok": False,
                "error": msg,
            })
            return records, "step %d (%s): %s" % (idx, step_type, msg)

        record = {"index": idx, "type": step_type, "ok": True}
        if step_type in _CAPTURE_STEPS and output is not None:
            record["output"] = output
        records.append(record)

    return records, None


# --- RPC handlers ------------------------------------------------------------


def _handle_session_open(params):
    requested_id = params.get("session_id")
    if requested_id is not None and not isinstance(requested_id, str):
        raise ValueError("`session_id` must be a string")

    # If the caller supplies a session_id that matches a saved (but not
    # currently live) session, route through the restore path. The user
    # experience is intentionally symmetric: if you know the id, you can
    # just call session_open; you don't need to list_saved first.
    if (
        requested_id
        and requested_id not in _SESSIONS
        and requested_id in _SAVED_SESSIONS
    ):
        return _restore_saved_session(requested_id)

    if len(_SESSIONS) >= MAX_SESSIONS:
        raise RuntimeError(
            "max sessions (%d) reached — close one first" % MAX_SESSIONS
        )

    session_id = requested_id if requested_id else uuid.uuid4().hex
    if session_id in _SESSIONS:
        raise ValueError("session_id `%s` already exists" % session_id)

    url = params.get("url")
    if url is not None and not isinstance(url, str):
        raise ValueError("`url` must be a string")

    jar = params.get("jar")
    if jar is not None:
        # Defensive re-validation — Rust side enforces the same regex.
        _validate_jar_name(jar)

    # If the jar already has saved storage from a previous boot AND the
    # jar isn't currently live, seed the new jar context from that state
    # so a fresh session opened into a previously-persisted jar rejoins
    # the cookies.
    storage_state_path = None
    if jar is not None and jar not in _JARS and jar in _SAVED_JARS:
        sp = _SAVED_JARS[jar].get("storage_state_path")
        if sp and os.path.isfile(sp):
            storage_state_path = sp

    slot = _launch_session(session_id, jar=jar, storage_state_path=storage_state_path)
    try:
        if url:
            _goto(slot.page, url)
        slot.touch()
        _SESSIONS[session_id] = slot
        result = {
            "session_id": session_id,
            "final_url": slot.page.url,
            "title": slot.page.title() or "",
        }
        _persist_state()
        return result
    except Exception:
        # Launch succeeded but navigation failed — tear the slot back down
        # so we don't leak a browser process OR a jar refcount.
        _close_session(slot)
        raise


def _restore_saved_session(session_id):
    """Re-launch a previously-saved session. Raises on expiry or overflow."""
    saved = _SAVED_SESSIONS.get(session_id)
    if saved is None:
        raise ValueError("unknown saved session: %s" % session_id)
    if _expired_at_save_time(saved):
        # Treat an expired saved session the same way we'd treat an idle-
        # reaped live session: the caller has to open a fresh one. Purge
        # the saved entry + storage file so the user sees a clean slate.
        jar = saved.get("jar")
        _drop_saved_session(session_id, jar)
        raise RuntimeError(
            "saved session %s expired (idle > %ds at save time)"
            % (session_id, IDLE_TIMEOUT_SECS)
        )
    if len(_SESSIONS) >= MAX_SESSIONS:
        raise RuntimeError(
            "max sessions (%d) reached — close one first" % MAX_SESSIONS
        )

    jar = saved.get("jar")
    last_url = saved.get("last_url") or ""

    # Pick the right storage path: per-session for jar-less sessions,
    # per-jar for jar-backed sessions.
    if jar is None:
        storage_state_path = saved.get("storage_state_path") or ""
    else:
        jar_meta = _SAVED_JARS.get(jar) or {}
        storage_state_path = jar_meta.get("storage_state_path") or ""

    slot = _launch_session(
        session_id, jar=jar, storage_state_path=storage_state_path or None
    )
    try:
        if last_url:
            _goto(slot.page, last_url)
        slot.touch()
        _SESSIONS[session_id] = slot
        result = {
            "session_id": session_id,
            "final_url": slot.page.url,
            "title": slot.page.title() or "",
        }
        _persist_state()
        return result
    except Exception:
        _close_session(slot)
        raise


def _handle_session_step(params):
    session_id = params.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("`session_id` is required")
    steps = params.get("steps")
    if not isinstance(steps, list) or not steps:
        raise ValueError("`steps` must be a non-empty array")

    slot = _SESSIONS.get(session_id)
    if slot is None:
        raise ValueError("unknown session_id `%s`" % session_id)

    try:
        records, error = _run_steps(slot.page, steps)
        result = {
            "session_id": session_id,
            "final_url": slot.page.url,
            "title": slot.page.title() or "",
            "steps": records,
        }
        slot.touch()
        # Persist on every step completion — success OR partial failure —
        # so the next boot sees the latest URL + cookies regardless.
        _persist_state()
        if error is None:
            return result
        # Partial failure: mimic tools/browser.py's interact envelope —
        # report ok=false with a result field. We raise a special
        # exception that _dispatch will repackage.
        raise StepFailure(error, result)
    except StepFailure:
        raise
    except Exception as exc:
        # A lower-level Playwright/context error (e.g. page crashed). Mark
        # the session dead — caller can decide to close it.
        slot.touch()
        raise RuntimeError("%s: %s" % (type(exc).__name__, exc))


class StepFailure(Exception):
    """Mid-recipe failure inside session_step. Carries the partial result."""

    def __init__(self, error, result):
        super().__init__(error)
        self.error = error
        self.result = result


def _handle_session_close(params):
    session_id = params.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("`session_id` is required")
    slot = _SESSIONS.pop(session_id, None)
    if slot is not None:
        # _close_session drops the saved entry + storage file as part
        # of its bookkeeping.
        _close_session(slot)
    elif session_id in _SAVED_SESSIONS:
        # Saved but never restored in this boot: still drop it so a user
        # who wants to "close" a dead session without restoring it first
        # can do so directly.
        jar = _SAVED_SESSIONS[session_id].get("jar")
        _drop_saved_session(session_id, jar)
    else:
        # Nothing to close — still idempotent, no error.
        pass
    return {}


def _handle_session_list(_params):
    sessions = []
    for sid, slot in _SESSIONS.items():
        try:
            final_url = slot.page.url
        except Exception:
            final_url = ""
        sessions.append({
            "session_id": sid,
            "final_url": final_url,
            "idle_seconds": int(slot.idle_seconds()),
            "jar": slot.jar,
        })
    return {"sessions": sessions}


def _handle_jar_list(_params):
    jars = []
    for name, entry in _JARS.items():
        jars.append({
            "name": name,
            "session_count": entry.get("session_count", 0),
            "created_at_monotonic": entry.get("created_at_monotonic", 0.0),
        })
    return {"jars": jars}


def _handle_session_list_saved(_params):
    """Return saved-session metadata for sessions not currently live.

    A caller uses this after a Nyx restart to see what can be restored.
    Live sessions are excluded so the UI never shows the same session
    twice (once under `session_list` and once under `session_list_saved`).
    """
    live = set(_SESSIONS.keys())
    sessions = []
    for sid, meta in _SAVED_SESSIONS.items():
        if sid in live:
            continue
        sessions.append({
            "session_id": sid,
            "last_url": meta.get("last_url") or "",
            "last_activity_at_epoch": meta.get("last_activity_at_epoch") or 0.0,
            "jar": meta.get("jar"),
            "storage_state_path": meta.get("storage_state_path") or "",
        })
    return {"sessions": sessions}


def _handle_session_restore(params):
    """Re-launch a saved session. Explicit, not automatic."""
    session_id = params.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("`session_id` is required")
    if session_id in _SESSIONS:
        raise ValueError("session %s is already live" % session_id)
    if session_id not in _SAVED_SESSIONS:
        raise ValueError("unknown saved session: %s" % session_id)
    return _restore_saved_session(session_id)


def _close_all_sessions():
    global _SHARED_PLAYWRIGHT
    # One final persist BEFORE we rip down the contexts so the file on
    # disk reflects the latest URLs + cookies. This is the contract for
    # an orderly shutdown: the last RPC before shutdown wins.
    try:
        _persist_state()
    except Exception:
        pass
    # After persisting, DO NOT let the per-session close calls drop the
    # saved entries — a graceful shutdown should leave the file coherent
    # for a future restart. We temporarily bypass _close_session's
    # saved-state cleanup by closing pages/contexts directly.
    for sid in list(_SESSIONS.keys()):
        slot = _SESSIONS.pop(sid, None)
        if slot is None:
            continue
        # Close the page only; jar/context teardown happens below.
        try:
            if slot.page is not None:
                slot.page.close()
        except Exception:
            pass
        if slot.jar is None:
            _close_standalone_stack(slot)
    # After every session is gone, every jar should have been released.
    # Belt-and-braces: tear down any jar entries still lingering.
    for name in list(_JARS.keys()):
        entry = _JARS.pop(name, None)
        if entry is not None:
            _close_jar_stack(entry)
    # With every browser + context closed, the shared Playwright driver
    # is safe to stop. We do it here rather than on every last-session
    # close so a transient zero-session window doesn't force a costly
    # cold-start the next time.
    if _SHARED_PLAYWRIGHT is not None:
        try:
            _SHARED_PLAYWRIGHT.stop()
        except Exception:
            pass
        _SHARED_PLAYWRIGHT = None


def _handle_test_expire_session(params):
    """Test hook: backdate a session's last_activity_at so the next RPC reaps it.

    Only called from the Rust-side idle-reaper integration test. Not part of
    the public contract — do not rely on this from product code.
    """
    session_id = params.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("_test_expire_session: `session_id` required")
    offset = params.get("offset_seconds", IDLE_TIMEOUT_SECS + 1)
    if not isinstance(offset, (int, float)):
        raise ValueError("_test_expire_session: `offset_seconds` must be a number")
    slot = _SESSIONS.get(session_id)
    if slot is None:
        raise ValueError("unknown session: %s" % session_id)
    slot.last_activity_at = time.monotonic() - float(offset)
    return {"session_id": session_id, "backdated_by": offset}


def _handle_test_backdate_saved_session(params):
    """Test hook: backdate a SAVED session's last_activity_at_epoch in the on-disk
    state file so the next session_restore rejects it as expired.

    Mirrors `_test_expire_session` but targets the persistence file, not
    the live SessionSlot. Used by the integration test to assert expiry
    semantics without waiting 600 real seconds.
    """
    session_id = params.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("_test_backdate_saved_session: `session_id` required")
    offset = params.get("offset_seconds", IDLE_TIMEOUT_SECS + 1)
    if not isinstance(offset, (int, float)):
        raise ValueError(
            "_test_backdate_saved_session: `offset_seconds` must be a number"
        )
    meta = _SAVED_SESSIONS.get(session_id)
    if meta is None:
        raise ValueError("unknown saved session: %s" % session_id)
    # Move the epoch timestamp backwards by `offset` seconds.
    meta["last_activity_at_epoch"] = _now_epoch() - float(offset)
    _persist_state()
    return {"session_id": session_id, "backdated_by": offset}


_METHODS = {
    "session_open":                  _handle_session_open,
    "session_step":                  _handle_session_step,
    "session_close":                 _handle_session_close,
    "session_list":                  _handle_session_list,
    "session_list_saved":            _handle_session_list_saved,
    "session_restore":               _handle_session_restore,
    "jar_list":                      _handle_jar_list,
    "_test_expire_session":          _handle_test_expire_session,
    "_test_backdate_saved_session":  _handle_test_backdate_saved_session,
}


def _dispatch(request):
    """Route one request dict to its handler. Returns the response envelope dict."""
    req_id = request.get("id")
    method = request.get("method")
    params = request.get("params") or {}
    if not isinstance(params, dict):
        return {"id": req_id, "ok": False, "error": "ValueError: `params` must be an object"}

    if method == "shutdown":
        _close_all_sessions()
        return {"id": req_id, "ok": True, "result": {}, "__shutdown__": True}

    _reap_idle_sessions()

    handler = _METHODS.get(method)
    if handler is None:
        return {"id": req_id, "ok": False, "error": "ValueError: unknown method `%s`" % method}

    try:
        result = handler(params)
        return {"id": req_id, "ok": True, "result": result}
    except StepFailure as sf:
        return {"id": req_id, "ok": False, "error": sf.error, "result": sf.result}
    except Exception as exc:
        return {"id": req_id, "ok": False, "error": "%s: %s" % (type(exc).__name__, exc)}


def _write(envelope):
    # Strip control flag before serialising.
    shutdown = envelope.pop("__shutdown__", False)
    sys.stdout.write(json.dumps(envelope))
    sys.stdout.write("\n")
    sys.stdout.flush()
    return shutdown


def main():
    # Bootstrap: load the saved-state index into memory and sweep orphan
    # storage files before processing any RPCs. Missing/corrupt state
    # file -> start clean. This is a no-op for the 99% case where the
    # user has never used persistence before.
    _ensure_state_dirs()
    _load_saved_state()
    _prune_orphan_storage_files()
    try:
        for raw in sys.stdin:
            line = raw.strip()
            if not line:
                continue
            try:
                request = json.loads(line)
            except Exception as exc:
                _write({"id": None, "ok": False, "error": "invalid JSON: %s" % exc})
                continue
            if not isinstance(request, dict):
                _write({"id": None, "ok": False, "error": "request must be a JSON object"})
                continue
            envelope = _dispatch(request)
            shutdown = _write(envelope)
            if shutdown:
                return
    finally:
        _close_all_sessions()


if __name__ == "__main__":
    main()
