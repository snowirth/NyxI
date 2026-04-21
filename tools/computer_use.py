#!/usr/bin/env python3
"""Computer-use agent — Claude Haiku with computer_20250124 tool.

Correct implementation per Anthropic docs:
- Tool type: computer_20250124
- Beta header: computer-use-2025-01-24
- Resolution: 1024x768 (recommended)
- Claude returns coordinates in the declared display space
- Scale back to macOS points for CGEvent execution
- Every action → wait → screenshot → feed back as tool_result
- Loop exits when Claude stops requesting tool calls
"""
import json
import sys
import os
import io
import base64
import time
import urllib.request

DISPLAY_W = 1024
DISPLAY_H = 768
MAX_STEPS = 20
SCREENSHOT_DELAY = 1.5

# Load API key
ANTHROPIC_KEY = ""
_env = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(_env):
    for _line in open(_env):
        if _line.strip().startswith("NYX_ANTHROPIC_API_KEY="):
            ANTHROPIC_KEY = _line.strip().split("=", 1)[1].strip()


def capture():
    """Capture screen at 1024x768. Returns (b64, scale_x, scale_y)."""
    import mss
    from PIL import Image

    with mss.mss() as sct:
        monitor = sct.monitors[1]
        shot = sct.grab(monitor)
        orig_w = monitor["width"]   # macOS points
        orig_h = monitor["height"]
        img = Image.frombytes("RGB", shot.size, shot.rgb)

    img = img.resize((DISPLAY_W, DISPLAY_H), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    b64 = base64.b64encode(buf.getvalue()).decode()

    return b64, orig_w / DISPLAY_W, orig_h / DISPLAY_H


def execute_action(action_input, scale_x, scale_y):
    """Execute a computer action. Claude coords are in 1024x768 space, scale to macOS points."""
    import Quartz

    action = action_input.get("action", "")
    coord = action_input.get("coordinate")
    text = action_input.get("text", "")

    # Scale coordinates from Claude space (1024x768) to macOS points
    if coord and len(coord) == 2:
        px = round(coord[0] * scale_x)
        py = round(coord[1] * scale_y)
    else:
        px, py = 0, 0

    point = Quartz.CGPointMake(px, py)

    if action == "screenshot":
        return "screenshot"

    elif action in ("left_click", "click"):
        for evt in [Quartz.kCGEventLeftMouseDown, Quartz.kCGEventLeftMouseUp]:
            e = Quartz.CGEventCreateMouseEvent(None, evt, point, 0)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)
            time.sleep(0.05)
        return f"clicked ({px},{py})"

    elif action == "double_click":
        for _ in range(2):
            for evt in [Quartz.kCGEventLeftMouseDown, Quartz.kCGEventLeftMouseUp]:
                e = Quartz.CGEventCreateMouseEvent(None, evt, point, 0)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)
                time.sleep(0.03)
        return f"double-clicked ({px},{py})"

    elif action == "right_click":
        for evt in [Quartz.kCGEventRightMouseDown, Quartz.kCGEventRightMouseUp]:
            e = Quartz.CGEventCreateMouseEvent(None, evt, point, 0)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)
            time.sleep(0.05)
        return f"right-clicked ({px},{py})"

    elif action == "mouse_move":
        e = Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventMouseMoved, point, 0)
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)
        return f"moved ({px},{py})"

    elif action == "type":
        for ch in text:
            ev = Quartz.CGEventCreateKeyboardEvent(None, 0, True)
            Quartz.CGEventKeyboardSetUnicodeString(ev, len(ch), ch)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, ev)
            ev_up = Quartz.CGEventCreateKeyboardEvent(None, 0, False)
            Quartz.CGEventKeyboardSetUnicodeString(ev_up, len(ch), ch)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, ev_up)
            time.sleep(0.015)
        return f"typed '{text[:30]}'"

    elif action == "key":
        KEY_MAP = {
            "Return": 36, "Tab": 48, "space": 49, "Escape": 53,
            "BackSpace": 51, "Delete": 117, "Up": 126, "Down": 125,
            "Left": 123, "Right": 124, "Home": 115, "End": 119,
        }
        MOD_MAP = {
            "super": 0x100000, "command": 0x100000, "cmd": 0x100000,
            "shift": 0x20000, "alt": 0x80000, "option": 0x80000,
            "ctrl": 0x40000, "control": 0x40000,
        }
        parts = text.split("+") if "+" in text else [text]
        mods = 0
        kc = 36
        for p in parts:
            p = p.strip()
            if p.lower() in MOD_MAP:
                mods |= MOD_MAP[p.lower()]
            elif p in KEY_MAP:
                kc = KEY_MAP[p]
        ev = Quartz.CGEventCreateKeyboardEvent(None, kc, True)
        if mods:
            Quartz.CGEventSetFlags(ev, mods)
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, ev)
        time.sleep(0.05)
        ev_up = Quartz.CGEventCreateKeyboardEvent(None, kc, False)
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, ev_up)
        return f"pressed {text}"

    elif action == "scroll":
        direction = action_input.get("scroll_direction", "down")
        amount = action_input.get("scroll_amount", 3)
        val = -amount if direction in ("down", "right") else amount
        ev = Quartz.CGEventCreateScrollWheelEvent(None, Quartz.kCGScrollEventUnitLine, 1, val)
        if px or py:
            Quartz.CGEventSetLocation(ev, point)
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, ev)
        return f"scrolled {direction}"

    elif action == "wait":
        secs = action_input.get("duration", 2)
        time.sleep(secs)
        return f"waited {secs}s"

    elif action == "triple_click":
        for _ in range(3):
            for evt in [Quartz.kCGEventLeftMouseDown, Quartz.kCGEventLeftMouseUp]:
                e = Quartz.CGEventCreateMouseEvent(None, evt, point, 0)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)
                time.sleep(0.02)
        return f"triple-clicked ({px},{py})"

    elif action == "middle_click":
        for evt in [Quartz.kCGEventOtherMouseDown, Quartz.kCGEventOtherMouseUp]:
            e = Quartz.CGEventCreateMouseEvent(None, evt, point, 2)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)
            time.sleep(0.05)
        return f"middle-clicked ({px},{py})"

    return f"unknown: {action}"


def run(task, max_steps=MAX_STEPS):
    """Main loop: Claude drives actions via computer_20250124 tool calls."""
    if not ANTHROPIC_KEY:
        return {"success": False, "error": "no NYX_ANTHROPIC_API_KEY"}

    history = []
    b64, scale_x, scale_y = capture()

    # Auto-focus: if task mentions a specific app, activate it first
    app_names = {"brave": "Brave Browser", "safari": "Safari", "chrome": "Google Chrome",
                 "finder": "Finder", "terminal": "Terminal", "notes": "Notes",
                 "calculator": "Calculator", "vscode": "Visual Studio Code", "code": "Visual Studio Code"}
    task_lower = task.lower()
    for key, app in app_names.items():
        if key in task_lower:
            import subprocess
            subprocess.run(["osascript", "-e", f'tell application "{app}" to activate'], timeout=3)
            time.sleep(1)
            break

    # System prompt to guide Claude's decision-making
    system_prompt = (
        "You are controlling a Mac computer. Be efficient — minimize steps.\n"
        "Rules:\n"
        "1. Don't take unnecessary screenshots — you already see the screen after each action.\n"
        "2. After clicking something, WAIT for it to respond before clicking again.\n"
        "3. To open an app not in the dock: press cmd+space (Spotlight), type the app name, press Return.\n"
        "4. To navigate in a browser: click the address bar, press cmd+a to select all, type the URL, press Return.\n"
        "5. Complete the task in as few steps as possible. Don't retry the same action.\n"
        "6. When the task is done, stop making tool calls and describe what you accomplished."
    )

    messages = [
        {"role": "user", "content": [
            {"type": "text", "text": f"{system_prompt}\n\nTask: {task}"},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": b64}},
        ]}
    ]

    for step in range(max_steps):
        # Call Claude with computer_20250124 tool
        try:
            data = json.dumps({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1024,
                "tools": [{
                    "type": "computer_20250124",
                    "name": "computer",
                    "display_width_px": DISPLAY_W,
                    "display_height_px": DISPLAY_H,
                }],
                "messages": messages,
            }).encode()

            req = urllib.request.Request("https://api.anthropic.com/v1/messages",
                data=data, headers={
                    "Content-Type": "application/json",
                    "x-api-key": ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "anthropic-beta": "computer-use-2025-01-24",
                })
            resp = urllib.request.urlopen(req, timeout=30)
            result = json.loads(resp.read())
        except Exception as e:
            err = ""
            if hasattr(e, "read"):
                try: err = e.read().decode()[:200]
                except: pass
            history.append(f"API error: {e} {err}")
            break

        content = result.get("content", [])
        messages.append({"role": "assistant", "content": content})

        # Process tool_use blocks
        tool_results = []
        final_text = ""

        for block in content:
            if block.get("type") == "text":
                final_text = block.get("text", "")
            elif block.get("type") == "tool_use":
                tool_input = block.get("input", {})
                tool_id = block.get("id", "")

                action_desc = execute_action(tool_input, scale_x, scale_y)
                history.append(action_desc)

                # Wait for UI, then screenshot
                if action_desc != "screenshot":
                    time.sleep(SCREENSHOT_DELAY)
                b64, scale_x, scale_y = capture()

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "content": [{"type": "image", "source": {
                        "type": "base64", "media_type": "image/png", "data": b64,
                    }}],
                })

        # No tool calls = done
        if not tool_results:
            return {
                "success": True,
                "output": final_text or "done",
                "steps": len(history),
                "history": history,
            }

        messages.append({"role": "user", "content": tool_results})

        # Prune old screenshots (keep last 5)
        prune_images(messages, keep=5)

    return {
        "success": True,
        "output": f"max steps ({max_steps}). last: {'; '.join(history[-3:])}",
        "steps": len(history),
        "history": history,
    }


def prune_images(messages, keep=5):
    """Replace old screenshots with text placeholder."""
    indices = []
    for i, msg in enumerate(messages):
        content = msg.get("content", [])
        if not isinstance(content, list): continue
        for j, block in enumerate(content):
            if not isinstance(block, dict): continue
            if block.get("type") == "image":
                indices.append((i, j))
            elif block.get("type") == "tool_result":
                for k, c in enumerate(block.get("content", [])):
                    if isinstance(c, dict) and c.get("type") == "image":
                        indices.append((i, j, k))

    for loc in indices[:max(0, len(indices) - keep)]:
        if len(loc) == 2:
            i, j = loc
            messages[i]["content"][j] = {"type": "text", "text": "[screenshot]"}
        elif len(loc) == 3:
            i, j, k = loc
            messages[i]["content"][j]["content"][k] = {"type": "text", "text": "[screenshot]"}


if __name__ == "__main__":
    try:
        raw = sys.stdin.read().strip()
        req = json.loads(raw) if raw else {}
    except:
        req = {}
    task = req.get("task", " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "")
    if not task:
        print(json.dumps({"success": False, "error": "no task"}))
    else:
        print(json.dumps(run(task, req.get("max_steps", MAX_STEPS))))
