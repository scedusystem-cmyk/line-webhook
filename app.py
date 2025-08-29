# app.py
# ============================================
# 尚進《寄書＋進銷存》：
# - OCR → 解析 → 回填寄書任務
# - 查詢寄書進度 (#查寄書)
# - 申請刪除 (#申請刪除)
# ============================================

from flask import Flask, request, abort
import gspread
from google.oauth2.service_account import Credentials
import os, re, json, io, logging
from datetime import datetime, timedelta
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    ImageMessage
)

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# =========================
# ✅ Google Sheets 連線設定
# =========================
SHEET_ID = os.getenv("SHEET_ID", "")  # 你的試算表 ID
MAIN_SHEET_NAME = os.getenv("MAIN_SHEET_NAME", "寄書任務")  # 主工作表名（預設：寄書任務）

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
        raise RuntimeError("缺少 Google Service Account 憑證：請提供 service_account.json 或環境變數 GOOGLE_SERVICE_ACCOUNT_JSON")
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    return gspread.authorize(creds)

# =========================
# ✅ LINE Bot 設定
# =========================
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

if not (LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN):
    app.logger.warning("⚠️ 尚未設置 LINE_CHANNEL_SECRET 或 LINE_CHANNEL_ACCESS_TOKEN")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# =========================
# ✅ OCR 設定
# =========================
_HAS_VISION = True
try:
    from google.cloud import vision
    from google.oauth2 import service_account as gservice_account
    _vision_creds = None
    vjson = os.getenv("VISION_SERVICE_ACCOUNT_JSON", "")
    if vjson:
        _vision_creds = gservice_account.Credentials.from_service_account_info(json.loads(vjson))
    else:
        if os.path.exists("service_account.json"):
            _vision_creds = gservice_account.Credentials.from_service_account_file("service_account.json")
        else:
            sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
            if sa_json:
                _vision_creds = gservice_account.Credentials.from_service_account_info(json.loads(sa_json))
            else:
                _vision_creds = None
    if _vision_creds is None:
        _HAS_VISION = False
except Exception as _e:
    _HAS_VISION = False
    app.logger.warning(f"⚠️ Vision 初始化失敗或未安裝：{_e}")

# =========================
# LINE Webhook 入口
# =========================
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)
    app.logger.info(f"[LINE_CALLBACK] body={body[:500]}...")

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"

# =========================
# 文字訊息處理
# =========================
_pending_delete = {}  # 暫存用戶待確認刪除的紀錄ID

@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = event.message.text.strip()
    user_id = getattr(event.source, "user_id", "")

    # ✅ 查詢寄書進度
    if text.startswith("#查寄書"):
        query = text.replace("#查寄書", "").strip()
        if not query:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="❌ 請輸入學員姓名或電話號碼")
            )
            return
        result = search_ship_status(query)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=result))
        return

    # ✅ 申請刪除
    if text.startswith("#申請刪除"):
        query = text.replace("#申請刪除", "").strip()
        if not query:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 請輸入學員姓名或電話號碼"))
            return
        result = request_delete(query, user_id)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=result))
        return

    # ✅ 確認刪除 (Y/是)
    if text in ["Y", "是"]:
        if user_id in _pending_delete:
            record_id = _pending_delete.pop(user_id)
            result = confirm_delete(record_id, user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=result))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 沒有待刪除的申請"))
        return

    # 保留原本 echo
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"已收到訊息：{text}"))

# =========================
# ✅ 查詢寄書進度（30 天內）
# =========================
def search_ship_status(query: str) -> str:
    try:
        gc = _build_gspread_client()
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet(MAIN_SHEET_NAME)
        rows = ws.get_all_records()

        today = datetime.today().date()
        cutoff = today - timedelta(days=30)

        matched = []
        for row in rows:
            build_date_str = str(row.get("建單日期", "")).strip()
            if not build_date_str:
                continue
            try:
                date_part = build_date_str[:10]
                build_date = datetime.strptime(date_part, "%Y-%m-%d").date()
            except Exception:
                continue
            if build_date < cutoff:
                continue

            name = str(row.get("學員姓名", "")).strip()
            phone_raw = str(row.get("學員電話", "")).strip()
            phone_tail9 = phone_raw[-9:] if len(phone_raw) >= 9 else phone_raw
            query_tail9 = query[-9:] if query.isdigit() else query

            if query in name or query_tail9 == phone_tail9:
                matched.append(row)

        if not matched:
            return "❌ 查無 30 天內的寄書紀錄，請確認姓名或電話是否正確"

        reply_lines = []
        for row in matched:
            name = row.get("學員姓名", "")
            book = row.get("書籍名稱", "")
            status = str(row.get("寄送狀態", "")).strip()
            send_date = row.get("寄出日期", "")
            method = row.get("寄送方式", "")
            tracking = str(row.get("託運單號", "")).strip()

            corrected = False
            if tracking and status != "已託運":
                status = "已託運"
                corrected = True

            if status == "待處理" or not status:
                reply_lines.append(f"📦 {name} 的 {book} 待處理")
            elif status == "已託運":
                msg = f"📦 {name} 的 {book}\n已於 {send_date}\n由 {method} 寄出\n託運單號：{tracking}"
                if corrected:
                    msg += "\n⚠️ 自動更正：原狀態未更新，已視為【已託運】"
                reply_lines.append(msg)
            else:
                reply_lines.append(f"📦 {name} 的 {book} 狀態：{status or '未更新'}")

        return "\n\n".join(reply_lines)

    except Exception as e:
        app.logger.exception(e)
        return f"❌ 查詢時發生錯誤：{e}"

# =========================
# ✅ 申請刪除
# =========================
def request_delete(query: str, user_id: str) -> str:
    try:
        gc = _build_gspread_client()
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet(MAIN_SHEET_NAME)
        rows = ws.get_all_records()
        matched = None
        row_index = None

        for idx, row in enumerate(rows, start=2):  # 因為第1列是表頭
            name = str(row.get("學員姓名", "")).strip()
            phone = str(row.get("學員電話", "")).strip()
            status = str(row.get("寄送狀態", "")).strip()
            creator = str(row.get("LINE_USER_ID", "")).strip()

            if (query in name or query in phone) and status == "待處理" and creator == user_id:
                matched = row
                row_index = idx
                break

        if not matched:
            return "❌ 沒有符合條件的待處理紀錄（只能刪自己建立，且狀態為待處理）"

        record_id = matched.get("紀錄ID", "")
        book = matched.get("書籍名稱", "")
        student = matched.get("學員姓名", "")

        # 暫存這筆紀錄，等待確認
        _pending_delete[user_id] = row_index
        return f"⚠️ 找到紀錄 {record_id}\n學員：{student}\n書籍：{book}\n狀態：待處理\n\n請輸入 Y 或 是 確認刪除"

    except Exception as e:
        app.logger.exception(e)
        return f"❌ 申請刪除時發生錯誤：{e}"

# =========================
# ✅ 確認刪除
# =========================
def confirm_delete(row_index: int, user_id: str) -> str:
    try:
        gc = _build_gspread_client()
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet(MAIN_SHEET_NAME)
        row = ws.row_values(row_index)

        student = row[4] if len(row) > 4 else ""  # 學員姓名
        book = row[7] if len(row) > 7 else ""    # 書籍名稱

        # 更新寄送狀態 = 已刪除
        ws.update_cell(row_index, 14, "已刪除")  # 第14欄是寄送狀態

        # 加刪除線：把整列套用刪除線
        fmt = gspread.format.CellFormat(textFormat={"strikethrough": True})
        ws.format(f"A{row_index}:N{row_index}", fmt)

        return f"✅ 學員：{student} ／ 書籍：{book} 已刪除"

    except Exception as e:
        app.logger.exception(e)
        return f"❌ 確認刪除時發生錯誤：{e}"

# =========================
# 工具：下載 LINE 圖片位元組
# =========================
def _download_line_image_bytes(message_id: str) -> bytes:
    content = line_bot_api.get_message_content(message_id)
    b = io.BytesIO()
    for chunk in content.iter_content():
        b.write(chunk)
    return b.getvalue()

# =========================
# 工具：OCR → 純文字
# =========================
def _ocr_text_from_bytes(image_bytes: bytes) -> str:
    if not _HAS_VISION:
        app.logger.warning("⚠️ OCR 未啟用：未安裝或未設定 Vision 憑證")
        return ""
    try:
        client = vision.ImageAnnotatorClient(credentials=_vision_creds) if _vision_creds else vision.ImageAnnotatorClient()
        image = vision.Image(content=image_bytes)
        resp = client.text_detection(image=image)
        if resp.error.message:
            raise RuntimeError(resp.error.message)
        text = resp.full_text_annotation.text if resp.full_text_annotation else (resp.text_annotations[0].description if resp.text_annotations else "")
        return text or ""
    except Exception as e:
        app.logger.exception(e)
        return ""

# =========================
# 本地測試入口
# =========================
@app.route("/", methods=["GET"])
def index():
    return "OK"

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
