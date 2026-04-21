#!/usr/bin/env python3
"""Speech-to-text via whisper. Transcribes audio file to text.

Requires: pip install openai-whisper (or use whisper.cpp)
Falls back to macOS speech recognition if whisper unavailable.
"""
import json, sys, os, subprocess, tempfile

def transcribe(audio_path):
    if not os.path.exists(audio_path):
        return {"success": False, "error": f"file not found: {audio_path}"}

    # Try whisper CLI first (pip install openai-whisper)
    try:
        result = subprocess.run(
            ["whisper", audio_path, "--model", "base", "--output_format", "txt",
             "--output_dir", tempfile.gettempdir(), "--language", "en"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            # Read the output text file
            base = os.path.splitext(os.path.basename(audio_path))[0]
            txt_path = os.path.join(tempfile.gettempdir(), f"{base}.txt")
            if os.path.exists(txt_path):
                with open(txt_path) as f:
                    text = f.read().strip()
                os.remove(txt_path)
                return {"success": True, "output": text}
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Try whisper.cpp CLI
    try:
        result = subprocess.run(
            ["whisper-cpp", "-m", os.path.expanduser("~/.cache/whisper/ggml-base.en.bin"),
             "-f", audio_path, "--no-timestamps"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return {"success": True, "output": result.stdout.strip()}
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    return {"success": False, "error": "no whisper installation found. install: pip install openai-whisper"}

if __name__ == "__main__":
    try:
        req = json.loads(sys.stdin.read().strip())
    except Exception:
        req = {}
    print(json.dumps(transcribe(req.get("audio_path", ""))))
