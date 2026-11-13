"""模擬 LINE 平台送 webhook 給寄書機器人（帶正確簽章）。密鑰從 Railway 變數即時讀取，不印出。"""
import base64, csv, hashlib, hmac, json, os, subprocess, sys, time, urllib.request, uuid

URL = "https://line-webhook-prod2.up.railway.app/callback"
HERE = os.path.dirname(os.path.abspath(__file__))
WL = os.path.join(HERE, "data", "wl.csv")
text = sys.argv[1]

uid = next(r["user_id"] for r in csv.DictReader(open(WL, encoding="utf-8-sig")) if r["name"] == "Vic")
out = subprocess.run(["railway", "variables", "--json", "-s", "line-webhook", "-e", "Prod2"],
                     capture_output=True, text=True, encoding="utf-8",
                     cwd=HERE, shell=True)
secret = json.loads(out.stdout)["LINE_CHANNEL_SECRET"]

body = json.dumps({
    "destination": "test",
    "events": [{
        "type": "message", "mode": "active", "timestamp": int(time.time() * 1000),
        "source": {"type": "user", "userId": uid},
        "webhookEventId": uuid.uuid4().hex.upper()[:26],
        "deliveryContext": {"isRedelivery": False},
        "replyToken": uuid.uuid4().hex,
        "message": {"id": str(int(time.time() * 1000)), "type": "text", "quoteToken": uuid.uuid4().hex, "text": text},
    }],
}, ensure_ascii=False).encode("utf-8")
sig = base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()
req = urllib.request.Request(URL, data=body, headers={"Content-Type": "application/json", "X-Line-Signature": sig})
print(urllib.request.urlopen(req, timeout=60).status)
