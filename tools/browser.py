#!/usr/bin/env python3
"""Nyx browser tool — Playwright-backed browsing + recipe-mode interaction.

Phase 10 shipped read-only commands. Phase 10.1 adds `interact`:
multi-step stateful-within-one-invocation scripts (click, fill, press,
wait, then capture) against a freshly spawned Chromium. Each `interact`
call still gets a brand new browser that closes in a `finally` — there
is no session persistence across invocations.

Commands:
  navigate, extract_text, extract_links, screenshot, fetch_html, interact

Protocol:
  stdin  : JSON object `{"command": "<cmd>", "args": {...}}`
  stdout : JSON object `{"ok": true, "result": {...}}`
           or          `{"ok": false, "error": "<message>"}`

For `interact` failures mid-recipe, the envelope is:
  `{"ok": false, "error": "step N (<type>): <msg>", "result": {...partial}}`

Evaluate step (opt-in, doubly-gated)
------------------------------------
`interact` supports an `evaluate` step that runs arbitrary JS via
`page.evaluate(expression)`. Because it can exfiltrate DOM + cookies +
localStorage and mutate the page invisibly to a recipe reader it is
gated on BOTH:
  1. Server-side env: `NYX_BROWSER_ALLOW_JS_EVAL=1` must be set.
  2. Per-call: the step must carry `"confirm": "I know what this does"`.
Both gates must pass; either alone is insufficient.

Intentional non-goals (still):
  - no cross-invocation sessions / cookies (session_daemon handles that)
  - no auth persistence
"""
import json
import os
import sys

# Output caps, in characters (clipped at unicode boundaries by slicing).
NAVIGATE_TEXT_CAP = 4 * 1024
EXTRACT_TEXT_CAP = 16 * 1024
FETCH_HTML_CAP = 32 * 1024
# 32KB cap on the JSON-encoded return value of an evaluate step.
EVALUATE_OUTPUT_CAP = 32 * 1024

_EVALUATE_CONFIRM_STRING = "I know what this does"
_EVALUATE_ENV_VAR = "NYX_BROWSER_ALLOW_JS_EVAL"

# Default navigation timeout. 30_000 ms matches the sandbox wall-clock timeout
# that Rust side enforces, so we'll fail from the Python side first with a
# nicer error rather than getting killed mid-Playwright shutdown.
NAV_TIMEOUT_MS = 30_000

# Per-step timeout for `interact` actions that wait for something in the DOM
# (wait_for, click). 10s is tighter than Playwright's 30s default so a bad
# selector in a recipe fails the whole invocation in a bounded time rather
# than burning most of the 30s wall-clock budget on one broken step.
STEP_TIMEOUT_MS = 10_000


def _launch(sync_playwright):
    """Launch a headless Chromium page. Caller must close the browser."""
    p = sync_playwright().start()
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_default_timeout(NAV_TIMEOUT_MS)
    page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    return p, browser, page


def _require_url(args):
    url = args.get("url")
    if not url or not isinstance(url, str):
        raise ValueError("missing or invalid `args.url`")
    return url


def _goto(page, url):
    # `domcontentloaded` is faster than `load` and captures enough DOM for
    # read-only operations. Sites that render their body late via JS will
    # still have their initial HTML at this point.
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)


def _navigate(page, args):
    url = _require_url(args)
    _goto(page, url)
    body_text = page.inner_text("body") if page.query_selector("body") else ""
    return {
        "final_url": page.url,
        "title": page.title() or "",
        "text": body_text[:NAVIGATE_TEXT_CAP],
        "truncated": len(body_text) > NAVIGATE_TEXT_CAP,
    }


def _extract_text(page, args):
    url = _require_url(args)
    _goto(page, url)
    body_text = page.inner_text("body") if page.query_selector("body") else ""
    return {
        "url": page.url,
        "text": body_text[:EXTRACT_TEXT_CAP],
        "truncated": len(body_text) > EXTRACT_TEXT_CAP,
    }


def _extract_links(page, args):
    url = _require_url(args)
    _goto(page, url)
    # Grab every anchor's href + trimmed visible text. Skip anchors without
    # an href attribute at all. Keep javascript:/mailto:/etc. — the caller
    # decides what to do with them.
    anchors = page.eval_on_selector_all(
        "a",
        """els => els.map(el => ({
            href: el.getAttribute('href') || '',
            text: (el.innerText || el.textContent || '').trim()
        })).filter(x => x.href)"""
    )
    return {
        "url": page.url,
        "links": anchors,
        "count": len(anchors),
    }


def _screenshot(page, args):
    url = _require_url(args)
    out_path = args.get("out_path")
    if not out_path or not isinstance(out_path, str):
        raise ValueError("missing or invalid `args.out_path`")
    _goto(page, url)
    page.screenshot(path=out_path, full_page=True)
    return {
        "url": page.url,
        "out_path": out_path,
    }


def _fetch_html(page, args):
    url = _require_url(args)
    _goto(page, url)
    html = page.content() or ""
    return {
        "url": page.url,
        "html": html[:FETCH_HTML_CAP],
        "truncated": len(html) > FETCH_HTML_CAP,
    }


# --- Recipe-mode step handlers ------------------------------------------------
#
# Each step handler mutates the Playwright `page` in some way and, for the
# capture-shaped steps, returns a payload to attach as the step's `output`
# field. Action-shaped steps (fill, click, press, wait_for) return None and
# the dispatcher records just `{index, type, ok: true}`.


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

    Either alone is insufficient. See the module docstring for rationale.
    """
    # Env gate first: the operator's consent is validated before we even
    # look at the per-call confirm string, so a caller can't learn the
    # confirm value from differential error messages on an un-enabled host.
    if os.environ.get(_EVALUATE_ENV_VAR) != "1":
        raise ValueError(
            'evaluate step is disabled; set %s=1 to enable' % _EVALUATE_ENV_VAR
        )
    if step.get("confirm") != _EVALUATE_CONFIRM_STRING:
        raise ValueError(
            'evaluate step requires explicit confirm="%s"' % _EVALUATE_CONFIRM_STRING
        )
    expression = step.get("expression")
    if not isinstance(expression, str) or not expression:
        raise ValueError("evaluate step requires string `expression`")

    value = page.evaluate(expression)
    encoded = json.dumps(value, default=str)
    if len(encoded) > EVALUATE_OUTPUT_CAP:
        encoded = encoded[:EVALUATE_OUTPUT_CAP]
    return encoded


# Dispatch table for interact steps. One line per step type:
_STEP_DISPATCH = {
    "fill":          _step_fill,           # fill an input
    "click":         _step_click,          # click an element
    "press":         _step_press,          # press keyboard key (optional focus selector)
    "wait_for":      _step_wait_for,       # wait until selector visible (10s cap)
    "screenshot":    _step_screenshot,     # full-page PNG to host-absolute path
    "extract_text":  _step_extract_text,   # visible body text, 16KB cap
    "extract_links": _step_extract_links,  # anchors with href+text
    "fetch_html":    _step_fetch_html,     # post-render HTML, 32KB cap
    "navigate":      _step_navigate,       # point the page at a new URL mid-recipe
    "evaluate":      _step_evaluate,       # run JS via page.evaluate (doubly-gated)
}

# Capture-shaped steps return their payload to the caller; action-shaped
# steps just record ok=true. Used to decide whether to attach `output`.
_CAPTURE_STEPS = {
    "screenshot", "extract_text", "extract_links", "fetch_html",
    "navigate", "evaluate",
}


def _run_interact_steps(page, steps):
    """Run the recipe. Returns (step_records, error_message_or_None).

    On the first exception we append a failure record and stop. The caller
    gets back the list of records up to and including the failing step and
    an error string of the form `step N (<type>): <msg>`.
    """
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
        elif step_type == "screenshot" and isinstance(output, dict):
            # screenshot returns a small dict; flatten the out_path for
            # parity with the standalone `screenshot` command's shape.
            record["output"] = output
        records.append(record)

    return records, None


def _interact(url, steps):
    """Recipe-mode handler. Returns a full envelope (dict) ready to print.

    Shape on success:
      {"ok": true,
       "result": {"final_url", "title", "steps": [...]}}

    Shape on mid-recipe failure:
      {"ok": false,
       "error": "step N (<type>): <msg>",
       "result": {"final_url", "title", "steps": [...partial]}}
    """
    from playwright.sync_api import sync_playwright  # type: ignore

    p, browser, page = _launch(sync_playwright)
    try:
        _goto(page, url)
        records, error = _run_interact_steps(page, steps)
        result = {
            "final_url": page.url,
            "title": page.title() or "",
            "steps": records,
        }
        if error is None:
            return {"ok": True, "result": result}
        return {"ok": False, "error": error, "result": result}
    finally:
        try:
            browser.close()
        finally:
            p.stop()


_DISPATCH = {
    "navigate": _navigate,
    "extract_text": _extract_text,
    "extract_links": _extract_links,
    "screenshot": _screenshot,
    "fetch_html": _fetch_html,
}


def _run(command, args):
    # `interact` has its own envelope path (it can partially succeed and
    # needs to return a `result` alongside an `error`), so handle it before
    # the simple-command dispatch table.
    if command == "interact":
        url = _require_url(args)
        steps = args.get("steps")
        if not isinstance(steps, list) or not steps:
            raise ValueError("interact requires non-empty `args.steps` array")
        return _interact(url, steps)

    if command not in _DISPATCH:
        raise ValueError(
            "unknown command '%s'; expected one of: %s"
            % (command, ", ".join(sorted(list(_DISPATCH.keys()) + ["interact"])))
        )
    # Import lazily so `--help` / dispatch checks do not require playwright
    # to be installed on the host before the Rust side probes the tool.
    from playwright.sync_api import sync_playwright  # type: ignore

    p, browser, page = _launch(sync_playwright)
    try:
        return {"ok": True, "result": _DISPATCH[command](page, args)}
    finally:
        try:
            browser.close()
        finally:
            p.stop()


def main():
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
    except Exception as e:
        print(json.dumps({"ok": False, "error": "invalid JSON stdin: %s" % e}))
        return

    command = payload.get("command")
    args = payload.get("args", {})
    if not command:
        print(json.dumps({"ok": False, "error": "missing `command`"}))
        return
    if not isinstance(args, dict):
        print(json.dumps({"ok": False, "error": "`args` must be an object"}))
        return

    try:
        envelope = _run(command, args)
        # Normalise to always emit an envelope dict. `interact` already
        # returns one; the read-only handlers now also emit the same shape
        # via `_run` so `main()` is just a writer.
        print(json.dumps(envelope))
    except Exception as e:
        print(json.dumps({"ok": False, "error": "%s: %s" % (type(e).__name__, e)}))


if __name__ == "__main__":
    main()
