# app.py
# ============================================
# 《寄書＋進銷存 自動化機器人》— 完整版（加入白名單；移除刪除線；地址自動補郵遞區號；OCR 僅在 #出書/#出貨 後觸發一次）
# 架構：Flask + LINE Webhook + Google Sheets + Vision OCR
# ============================================

from flask import Flask, request, abort
import os, re, io, json, difflib, logging, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, ImageMessage,
    TextSendMessage,
)

# ====== （OCR：使用顯式憑證）======
_HAS_VISION = False
_vision_client = None
try:
    from google.cloud import vision
    from google.oauth2 import service_account as gcp_service_account
    _HAS_VISION = True
except Exception:
    _HAS_VISION = False
# ==================================

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# ---- 環境變數 ----
SHEET_ID = os.getenv("SHEET_ID", "").strip()

MAIN_SHEET_NAME = os.getenv("MAIN_SHEET_NAME", "寄書任務")
BOOK_MASTER_SHEET_NAME = os.getenv("BOOK_MASTER_SHEET_NAME", "書目主檔")
ZIPREF_SHEET_NAME = os.getenv("ZIPREF_SHEET_NAME", "郵遞區號參照表")
STOCK_IN_SHEET_NAME = os.getenv("STOCK_IN_SHEET_NAME", "入庫明細")
HISTORY_SHEET_NAME = os.getenv("HISTORY_SHEET_NAME", "歷史紀錄")

# === 白名單設定 ===
WHITELIST_SHEET_NAME = os.getenv("WHITELIST_SHEET_NAME", "白名單")
CANDIDATE_SHEET_NAME = os.getenv("CANDIDATE_SHEET_NAME", "候選名單")
WHITELIST_MODE = os.getenv("WHITELIST_MODE", "off").strip().lower()
ADMIN_USER_IDS = {x.strip() for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()}
_WHITELIST_CACHE = {"ts": 0.0, "set": set()}
_WHITELIST_TTL = 300  # 秒

FUZZY_THRESHOLD = float(os.getenv("FUZZY_THRESHOLD", "0.6"))
QUERY_DAYS = int(os.getenv("QUERY_DAYS", "30"))
PHONE_SUFFIX_MATCH = int(os.getenv("PHONE_SUFFIX_MATCH", "9"))
WRITE_ZIP_TO_ADDRESS = os.getenv("WRITE_ZIP_TO_ADDRESS", "true").lower() == "true"
LOG_OCR_RAW = os.getenv("LOG_OCR_RAW", "true").lower() == "true"

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

if not LINE_CHANNEL_SECRET or not LINE_CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("Missing LINE credentials.")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

TZ = ZoneInfo("Asia/Taipei")

# ============================================
# Google Sheets 連線 + 表頭對應
# ============================================
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

gc = _build_gspread_client()
ss = gc.open_by_key(SHEET_ID)

def _ws(name: str):
    return ss.worksheet(name)

# ...（中間省略：白名單、工具函式、寄書/查詢/取消/OCR helper 等都保持原樣）...

# ============================================
# 文字訊息處理
# ============================================
_ocr_pending = {}  # user_id -> True (等待下一張圖片 OCR)

@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = (event.message.text or "").strip()
    uid = getattr(event.source, "user_id", "")

    # 🔓 特例：任何人都可用「我的ID」
    if text in ("我的ID", "#我的ID"):
        try:
            profile = line_bot_api.get_profile(uid)
            name = profile.display_name or "LINE使用者"
        except Exception:
            name = "LINE使用者"
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"你的 ID：\n{uid}\n顯示名稱：{name}\n\n請提供給管理員加入白名單。")
        )
        if uid:
            _log_candidate(uid, name)
        return

    # 先處理待確認的 Y/N
    if _handle_pending_answer(event, text):
        return

    # ⛔ 白名單驗證
    if not _ensure_authorized(event, scope="text"):
        return

    # === 指令區 ===
    if text.startswith("#寄書需求") or text.startswith("#寄書"):
        _handle_new_order(event, text); return

    if text.startswith("#查詢寄書") or text.startswith("#查寄書") or text.startswith("#查出書"):
        _handle_query(event, text); return

    if text.startswith("#取消寄書") or text.startswith("#刪除寄書"):
        _handle_cancel_request(event, text); return

    if text.startswith("#出書") or text.startswith("#出貨"):
        _ocr_pending[uid] = True
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請上傳出貨單圖片，我會進行 OCR 處理。"))
        return

    # ❌ 其他文字 → 不回覆
    return

# ============================================
# 圖片訊息處理（OCR）
# ============================================
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    uid = getattr(event.source, "user_id", "")

    # ⛔ 白名單驗證
    if not _ensure_authorized(event, scope="ocr"):
        return

    # 僅當「使用者有啟動 OCR 模式」時才處理
    if not _ocr_pending.get(uid):
        return

    # 一次性處理 → 用完即刪
    _ocr_pending.pop(uid, None)

    try:
        app.logger.info(f"[IMG] 收到圖片 user_id={uid} msg_id={event.message.id}")
        img_bytes = _download_line_image_bytes(event.message.id)
        if not _vision_client:
            msg = "❌ OCR 錯誤：Vision 用戶端未初始化。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg)); return

        text = _ocr_text_from_bytes(img_bytes)
        if LOG_OCR_RAW:
            app.logger.info(f"[OCR_TEXT]\n{text}")

        pairs, leftovers = _pair_ids_with_numbers(text)
        resp = _write_ocr_results(pairs, event)
        if leftovers:
            resp += "\n\n❗以下項目需人工檢核：\n" + "\n".join(leftovers[:10])

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=resp))
    except Exception as e:
        code = datetime.now(TZ).strftime("%Y%m%d%H%M%S")
        app.logger.exception("[OCR_ERROR]")
        msg = f"❌ OCR 錯誤（代碼 {code}）：{e}"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

# ============================================
# 健康檢查
# ============================================
@app.route("/", methods=["GET"])
def index():
    try:
        names = [ws.title for ws in ss.worksheets()]
        return "OK / Worksheets: " + ", ".join(names)
    except Exception as e:
        return f"OK / (Sheets not loaded) {e}"

# 本地執行（Railway 用 gunicorn 啟動）
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
