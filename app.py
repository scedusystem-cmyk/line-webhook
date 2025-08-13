from flask import Flask, request, abort
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import os

app = Flask(__name__)

# 從環境變數讀取（Railway Variables）
CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

if not CHANNEL_SECRET or not CHANNEL_ACCESS_TOKEN:
    print("[WARN] Missing LINE_CHANNEL_SECRET or LINE_CHANNEL_ACCESS_TOKEN env vars.")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

# 同一路徑接受 GET（給 Verify）與 POST（正式事件）
@app.route("/callback", methods=["GET", "POST"])
def callback():
    # A) LINE Console 的 Verify 會發 GET，要回 200
    if request.method == "GET":
        return "OK", 200

    # B) LINE 事件是 POST，以下是處理訊息事件
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
