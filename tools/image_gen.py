#!/usr/bin/env python3
"""Image generation via NVIDIA NIM FLUX.1-dev API. Free with NIM API key."""
import json, sys, os, base64, time, uuid
try:
    import urllib.request
except ImportError:
    pass

IMAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "workspace", "images")
os.makedirs(IMAGE_DIR, exist_ok=True)

NIM_KEY = os.getenv("NYX_NIM_API_KEY", "")
FLUX_URL = "https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-dev"
POLLINATIONS_URL = "https://image.pollinations.ai/prompt/{prompt}?width={width}&height={height}&nologo=true"
FLUX_SIZES = [768, 832, 896, 960, 1024, 1088, 1152, 1216, 1280, 1344]

STYLES = {
    "realistic": "photorealistic, ultra detailed, 8k uhd, sharp focus, natural lighting",
    "anime": "anime style, high quality anime art, vibrant colors, clean lines",
    "artistic": "artistic, masterpiece, trending on artstation, concept art",
    "cinematic": "cinematic shot, dramatic lighting, film grain, depth of field",
    "pixel": "pixel art, retro game style, 16-bit, sprite art",
}

def generate(prompt, style=None, width=1024, height=1024, steps=20):
    if not NIM_KEY:
        return {"success": False, "error": "NYX_NIM_API_KEY not set"}
    if not prompt:
        return {"success": False, "error": "no prompt"}

    # Apply style
    if style and style in STYLES:
        prompt = f"{STYLES[style]}, {prompt}"

    # Snap to valid FLUX sizes
    width = min(FLUX_SIZES, key=lambda s: abs(s - width))
    height = min(FLUX_SIZES, key=lambda s: abs(s - height))
    steps = max(4, min(steps, 28))

    payload = json.dumps({
        "prompt": prompt,
        "width": width,
        "height": height,
        "steps": steps,
    }).encode()

    req = urllib.request.Request(FLUX_URL, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {NIM_KEY}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200] if hasattr(e, 'read') else str(e)
        return {"success": False, "error": f"FLUX API {e.code}: {body}"}
    except Exception as e:
        return {"success": False, "error": f"FLUX failed: {e}"}

    artifacts = data.get("artifacts", [])
    if not artifacts or not artifacts[0].get("base64"):
        reason = artifacts[0].get("finishReason", "unknown") if artifacts else "no artifacts"
        return {"success": False, "error": f"no image: {reason}"}

    if artifacts[0].get("finishReason") == "CONTENT_FILTERED":
        return {"success": False, "error": "blocked by content filter. try different prompt."}

    img_bytes = base64.b64decode(artifacts[0]["base64"])
    if len(img_bytes) < 10000:
        return {"success": False, "error": "image too small (likely filtered)"}

    filename = f"gen_{int(time.time())}_{uuid.uuid4().hex[:8]}.jpg"
    filepath = os.path.join(IMAGE_DIR, filename)
    with open(filepath, "wb") as f:
        f.write(img_bytes)

    return {
        "success": True,
        "output": f"image generated and saved to {filepath}",
        "file": filepath,
        "prompt": prompt[:200],
    }

def generate_hf(prompt, style=None, width=1024, height=1024):
    """Fallback: Hugging Face Inference API — free tier, rate limited."""
    if style and style in STYLES:
        prompt = f"{STYLES[style]}, {prompt}"

    # Use FLUX.1-schnell (fast) or stable-diffusion-xl
    url = "https://router.huggingface.co/hf-inference/models/black-forest-labs/FLUX.1-schnell"
    hf_token = os.getenv("HF_TOKEN", "")

    payload = json.dumps({"inputs": prompt}).encode()
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    if hf_token:
        req.add_header("Authorization", f"Bearer {hf_token}")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            content_type = resp.headers.get("Content-Type", "")
            data = resp.read()
            if "image" in content_type:
                img_bytes = data
            else:
                # Might be JSON error
                return {"success": False, "error": f"HF returned: {data[:200].decode()}"}
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200] if hasattr(e, 'read') else str(e)
        return {"success": False, "error": f"HF API {e.code}: {body}"}
    except Exception as e:
        return {"success": False, "error": f"HF failed: {e}"}

    if len(img_bytes) < 5000:
        return {"success": False, "error": "image too small"}

    filename = f"gen_{int(time.time())}_{uuid.uuid4().hex[:8]}.jpg"
    filepath = os.path.join(IMAGE_DIR, filename)
    with open(filepath, "wb") as f:
        f.write(img_bytes)

    return {
        "success": True,
        "output": f"image generated and saved to {filepath}",
        "file": filepath,
        "prompt": prompt[:200],
    }


if __name__ == "__main__":
    try:
        req = json.loads(sys.stdin.read().strip())
    except Exception:
        req = {}

    # Try FLUX first, fall back to Pollinations
    result = generate(
        req.get("prompt", ""),
        style=req.get("style"),
        width=req.get("width", 1024),
        height=req.get("height", 1024),
        steps=req.get("steps", 20),
    )
    if not result.get("success") and os.getenv("HF_TOKEN"):
        result = generate_hf(
            req.get("prompt", ""),
            style=req.get("style"),
            width=req.get("width", 1024),
            height=req.get("height", 1024),
        )
    print(json.dumps(result))
