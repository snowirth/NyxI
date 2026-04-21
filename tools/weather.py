#!/usr/bin/env python3
import json, sys, urllib.request

COUNTRY_TO_CITY = {"vietnam":"Ho Chi Minh City","japan":"Tokyo","korea":"Seoul",
    "usa":"New York","uk":"London","australia":"Sydney","france":"Paris",
    "germany":"Berlin","thailand":"Bangkok","india":"Mumbai","singapore":"Singapore"}

def get_weather(city="Adelaide"):
    city = COUNTRY_TO_CITY.get(city.lower(), city)
    try:
        url = f"https://wttr.in/{urllib.request.quote(city)}?format=j1"
        req = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
        data = json.loads(urllib.request.urlopen(req, timeout=10).read())
        cc = data['current_condition'][0]
        return {"success": True, "output": f"{cc['weatherDesc'][0]['value']}, {cc['temp_C']}°C (feels {cc['FeelsLikeC']}°C) in {city}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    try: req = json.loads(sys.stdin.read().strip())
    except: req = {}
    print(json.dumps(get_weather(req.get("city", "Adelaide"))))
