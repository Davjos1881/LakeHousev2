import json
import urllib.request
import urllib.error

BASE = "http://nessie:19120/api/v2"


def call(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, method=method, data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as r:
            txt = r.read().decode()
            return json.loads(txt) if txt else {}
    except urllib.error.HTTPError as e:
        print("HTTP", e.code, e.read().decode())
        raise


ref = call("GET", f"{BASE}/trees/main")
h = ref["reference"]["hash"]
print("hash de main:", h)

res = call(
    "POST",
    f"{BASE}/trees/main@{h}/history/commit",
    {
        "commitMeta": {"message": "drop smoke.t (limpieza de prueba)"},
        "operations": [{"type": "DELETE", "key": {"elements": ["smoke", "t"]}}],
    },
)
print("commit hecho:", res)