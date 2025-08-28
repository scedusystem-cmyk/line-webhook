from flask import Flask, request, abort
import gspread
from google.oauth2.service_account import Credentials
import os, re, difflib, json
from datetime import datetime
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, ImageMessage

# ====== （OCR 新增）import 區 ======
import io
try:
    from google.cloud import vision
    from google.oauth2 import service_account
    _HAS_VISION = True
except Exception:
    _HAS_VISION = False
# ==================================

app = Flask(__name__)

# =========================
# Google Sheets 連線設定區
# =========================
SHEET_ID = os.getenv("SHEET_ID", "")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def _build_gspread_client():
    json_path = "service_account.json"
    if os.path.exists(json_path):
        creds = Credentials.from_service_account_file(json_path, scopes=SCOPES)
        return gspread.authorize(creds)
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_json:
        raise RuntimeError("Missing service account credentials.")
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    return gspread.authorize(creds)

def _get_worksheet(sheet_name: str):
    gc = _build_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(sheet_name)

# =========================
# LINE Bot 設定
# =========================
CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# =========================
# 工具函式
# =========================
def normalize_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)
    if digits.startswith("09") and len(digits) == 10:
        return digits
    return ""

def lookup_zipcode(address: str) -> str:
    try:
        ws = _get_worksheet("郵遞區號參照表")
        rows = ws.get_all_values()
        for row in rows[1:]:
            if row[1] and row[1] in address:
                return row[0] + address
    except Exception as e:
        app.logger.error(f"[ZIPCODE] lookup error: {e}")
    return address

def fuzzy_match_book(book_name: str) -> str:
    ws = _get_worksheet("書目主檔")
    official_names = [row[1] for row in ws.get_all_values()[1:] if row[1]]
    matches = difflib.get_close_matches(book_name, official_names, n=1, cutoff=0.6)
    return matches[0] if matches else None

# =========================
# LINE Webhook 主入口
# =========================
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers["X-Line-Signature"]
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"

# =========================
# 文字訊息處理
# =========================
@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    user_id = getattr(event.source, "user_id", "unknown")
    user_name = "LINE使用者"

    text = event.message.text.strip()
    if text.startswith("#寄書需求") or text.startswith("#寄書"):
        # 簡單解析
        lines = text.split("\n")
        data = {}
        for line in lines:
            if "姓名" in line:
                data["name"] = line.split("：",1)[1].strip()
            elif "電話" in line:
                data["phone"] = normalize_phone(line.split("：",1)[1])
            elif "地址" in line:
                data["address"] = lookup_zipcode(line.split("：",1)[1].strip())
            elif "書籍" in line:
                book_input = line.split("：",1)[1].strip()
                match = fuzzy_match_book(book_input)
                data["book"] = match if match else book_input

        # 檢查缺失
        missing = [k for k in ["name","phone","address","book"] if not data.get(k)]
        if missing:
            reply = f"❌ 缺少欄位: {','.join(missing)}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
            return

        try:
            ws = _get_worksheet(os.getenv("MAIN_SHEET_NAME","寄書任務"))
            rows = ws.get_all_values()
            new_id = str(len(rows))  # 簡單流水號
            today = datetime.now().strftime("%Y-%m-%d")
            row = [
                new_id,
                today,
                user_name,
                data["name"],
                data["phone"],
                data["address"],
                data["book"],
                "", "", "", "", "", "", ""
            ]
            ws.append_row(row)
            reply = f"✅ 已成功建檔：\n姓名：{data['name']}\n電話：{data['phone']}\n地址：{data['address']}\n書籍：{data['book']}"
        except Exception as e:
            reply = f"❌ Google Sheet 寫入失敗: {e}"

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))

# =========================
# 圖片訊息處理器（含 OCR）
# =========================
def _download_line_image_bytes(message_id: str) -> bytes:
    content = line_bot_api.get_message_content(message_id)
    return b"".join([chunk for chunk in content.iter_content()])

def _ocr_text_from_bytes(img_bytes: bytes) -> str:
    if not _HAS_VISION:
        return ""
    creds_path = "service_account.json"
    if os.path.exists(creds_path):
        creds = service_account.Credentials.from_service_account_file(creds_path)
    else:
        sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        creds = service_account.Credentials.from_service_account_info(json.loads(sa_json))
    client = vision.ImageAnnotatorClient(credentials=creds)
    image = vision.Image(content=img_bytes)
    response = client.text_detection(image=image)
    texts = response.text_annotations
    return texts[0].description if texts else ""

@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    try:
        user_id = getattr(event.source, "user_id", "unknown")
        app.logger.info(f"[IMG] 收到圖片 user_id={user_id}, msg_id={event.message.id}")

        # 下載圖片
        img_bytes = _download_line_image_bytes(event.message.id)

        # 做 OCR
        text = _ocr_text_from_bytes(img_bytes)

        app.logger.info(f"[OCR_RAW_OUTPUT] {repr(text)}")

        if text:
            preview = (text[:200] + "…") if len(text) > 200 else text
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"✅ 已收到圖片並完成 OCR：\n{preview}")
            )
            app.logger.info(f"[OCR_TEXT]\n{text}")
        else:
            tip = "（目前 OCR 未啟用或未安裝，無法辨識文字）"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=tip))

    except Exception as e:
        app.logger.error(f"[OCR_ERROR] {e}", exc_info=True)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="❌ OCR 處理時發生錯誤，請稍後再試。")
        )

# =========================
# 主程式入口
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
