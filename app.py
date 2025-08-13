from flask import Flask, request, abort
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import os

app = Flask(__name__)

CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

if not CHANNEL_SECRET or not CHANNEL_ACCESS_TOKEN:
    # 方便在 Railway Logs 看出未設定環境變數
    print("[WARN] Missing LINE_CHANNEL_SECRET or LINE_CHANNEL_ACCESS_TOKEN env vars.")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

@app.route("/callback", methods=["POST"])
def callback():
    if parser is None or line_bot_api is None:
        abort(500)

    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        abort(400)

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