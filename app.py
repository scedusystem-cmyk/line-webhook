from flask import Flask, request, abort
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import os

app = Flask(__name__)

CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

if not CHANNEL_SECRET or not CHANNEL_ACCESS_TOKEN:
    print("[WARN] Missing LINE env vars.")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

# 接受 GET/HEAD（Verify 用）與 POST（正式事件）
@app.route("/callback", methods=["GET", "HEAD", "POST"])
def callback():
    # 1) Verify 會用 GET/HEAD 來探測
    if request.method in ("GET", "HEAD"):
        return "OK", 200

    # 2) 有些 Verify/健康檢查會發沒有簽章的 POST，直接回 200
    signature = request.headers.get("X-Line-Signature")
    if not signature:
        return "OK", 200

    if parser is None or line_bot_api is None:
        abort(500)

    body = request.get_data(as_text=True)

    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        # 簽章錯誤的話，為了不中斷 Verify，回 200 不處理
        print("[WARN] Invalid signature on /callback")
        return "OK", 200

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
            reply = f"你說：{event.message.text}"
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=reply)
            )

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
