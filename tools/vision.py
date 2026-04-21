#!/usr/bin/env python3
import json, sys, os, io, base64, urllib.request

NIM_KEY = ""
env = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(env):
    for line in open(env):
        if line.strip().startswith("NYX_NIM_API_KEY="): NIM_KEY = line.strip().split("=",1)[1]

def look(prompt=None):
    import mss
    from PIL import Image
    p = prompt or "List: 1) app names 2) visible text 3) what user is doing. Be specific."
    try:
        with mss.mss() as sct:
            shot = sct.grab(sct.monitors[1])
            img = Image.frombytes("RGB", shot.size, shot.rgb).resize((1024,768), Image.LANCZOS)
        buf = io.BytesIO(); img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode()
        if NIM_KEY:
            data = json.dumps({"model":"meta/llama-3.2-90b-vision-instruct",
                "messages":[{"role":"user","content":[{"type":"text","text":p},
                {"type":"image_url","image_url":{"url":f"data:image/png;base64,{b64}"}}]}],
                "max_tokens":300}).encode()
            req = urllib.request.Request("https://integrate.api.nvidia.com/v1/chat/completions",
                data=data, headers={"Content-Type":"application/json","Authorization":f"Bearer {NIM_KEY}"})
            resp = json.loads(urllib.request.urlopen(req, timeout=30).read())
            return {"success":True,"output":resp["choices"][0]["message"]["content"]}
        return {"success":False,"error":"no NIM key"}
    except Exception as e:
        return {"success":False,"error":str(e)}

if __name__ == "__main__":
    try: req = json.loads(sys.stdin.read().strip())
    except: req = {}
    print(json.dumps(look(req.get("prompt"))))
