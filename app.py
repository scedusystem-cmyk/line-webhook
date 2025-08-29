# app.py
# ============================================
# 尚進《寄書＋進銷存》：
# - #寄書需求 → 新增紀錄
# - #查寄書 → 查詢進度
# - #申請刪除 → 刪除紀錄
# - 上傳圖片 → OCR 解析 → 自動更新紀錄ID與託運單號
# ============================================

from flask import Flask, request, abort
import gspread
from google.oauth2.service_account import Credentials
import os, re, json, io, logging, difflib
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
SHEET_ID = os.getenv("SHEET_ID", "")
MAIN_SHEET_NAME = os.getenv("MAIN_SHEET_NAME", "寄書任務")

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
        raise RuntimeError("缺少 Google Service Account 憑證")
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    return gspread.authorize(creds)

def _get_worksheet(sheet_name: str):
    gc = _build_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(sheet_name)

# =========================
# ✅ 簡單工具
# =========================
def normalize_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)  # 去掉非數字
    if digits.startswith("886"):
        digits = "0" + digits[3:]
    return digits

def lookup_zipcode(addr: str) -> str:
    # TODO: 改成查郵遞區號表，目前只是原樣回傳
    return addr

def fuzzy_match_book(input_book: str) -> str:
    try:
        ws = _get_worksheet("書目主檔")
        books = [row[0] for row in ws.get_all_values()[1:]]  # B欄正式書名
        match = difflib.get_close_matches(input_book, books, n=1, cutoff=0.6)
        return match[0] if match else ""
    except Exception:
        return ""

# =========================
# ✅ LINE Bot 設定
# =========================
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

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
_pending_delete = {}

@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = event.message.text.strip()
    user_id = getattr(event.source, "user_id", "")
    user_name = "LINE使用者"  # TODO: 可改抓 LINE 顯示名稱

    # ✅ 寄書需求
    if text.startswith("#寄書需求") or text.startswith("#寄書"):
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

        missing = [k for k in ["name","phone","address","book"] if not data.get(k)]
        if missing:
            reply = f"❌ 缺少欄位: {','.join(missing)}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
            return

        try:
            ws = _get_worksheet(MAIN_SHEET_NAME)
            rows = ws.get_all_values()
            new_id = str(len(rows))  # 簡單流水號
            today = datetime.now().strftime("%Y-%m-%d")
            row = [
                new_id, today, user_name,
                data["name"], data["phone"], data["address"], data["book"],
                "", "", "", "", "", "", ""
            ]
            ws.append_row(row)
            reply = f"✅ 已成功建檔：\n姓名：{data['name']}\n電話：{data['phone']}\n地址：{data['address']}\n書籍：{data['book']}"
        except Exception as e:
            reply = f"❌ Google Sheet 寫入失敗: {e}"

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
        return

    # ✅ 查詢寄書進度
    if text.startswith("#查寄書"):
        query = text.replace("#查寄書", "").strip()
        result = search_ship_status(query)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=result))
        return

    # ✅ 申請刪除
    if text.startswith("#申請刪除"):
        query = text.replace("#申請刪除", "").strip()
        result = request_delete(query, user_id)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=result))
        return

    # ✅ 確認刪除
    if text in ["Y", "是"]:
        if user_id in _pending_delete:
            row_index = _pending_delete.pop(user_id)
            result = confirm_delete(row_index, user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=result))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 沒有待刪除的申請"))
        return

    # 預設回覆
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

        ws.update_cell(row_index, 14, "已刪除")  # 第14欄是寄送狀態
        # 加刪除線
        fmt = gspread.format.CellFormat(textFormat={"strikethrough": True})
        ws.format(f"A{row_index}:N{row_index}", fmt)

        return f"✅ 學員：{student} ／ 書籍：{book} 已刪除"

    except Exception as e:
        app.logger.exception(e)
        return f"❌ 確認刪除時發生錯誤：{e}"

# =========================
# ✅ OCR 功能：解析 & 自動更新
# =========================
def _download_line_image_bytes(message_id: str) -> bytes:
    content = line_bot_api.get_message_content(message_id)
    b = io.BytesIO()
    for chunk in content.iter_content():
        b.write(chunk)
    return b.getvalue()

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

def _extract_ship_info(text: str):
    record_ids = re.findall(r"R\d{3,}", text)  # e.g. R0024
    trackings = re.findall(r"\d{12}", text)   # 12碼數字
    return list(zip(record_ids, trackings))

def _update_tracking_in_sheet(pairs, user_id=""):
    if not pairs:
        return "❌ OCR 未找到有效的紀錄ID與託運單號，請手動檢查"

    gc = _build_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(MAIN_SHEET_NAME)
    rows = ws.get_all_records()

    updated, failed = [], []
    today = datetime.now().strftime("%Y-%m-%d")

    for rid, tno in pairs:
        found = False
        for idx, row in enumerate(rows, start=2):
            record_id = str(row.get("紀錄ID", "")).strip()
            if record_id == rid:
                ws.update_cell(idx, 9, today)       # 寄出日期
                ws.update_cell(idx, 11, tno)        # 託運單號
                ws.update_cell(idx, 12, "已託運")   # 狀態
                updated.append(f"{rid} → {tno}")
                found = True
                break
        if not found:
            failed.append(f"{rid} → {tno}")

    msg = ""
    if updated:
        msg += "✅ 已更新：\n" + "\n".join(updated) + "\n"
    if failed:
        msg += "❗未找到紀錄：\n" + "\n".join(failed)
    return msg.strip()

@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    try:
        user_id = getattr(event.source, "user_id", "unknown")
        app.logger.info(f"[IMG] 收到圖片 user_id={user_id}, msg_id={event.message.id}")

        img_bytes = _download_line_image_bytes(event.message.id)
        text = _ocr_text_from_bytes(img_bytes)
        app.logger.info(f"[OCR_TEXT]\n{text}")

        if not text:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ OCR 未讀到任何文字"))
            return

        pairs = _extract_ship_info(text)
        if pairs:
            result_msg = _update_tracking_in_sheet(pairs, user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=result_msg))
        else:
            preview = (text[:200] + "…") if len(text) > 200 else text
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"⚠️ OCR 成功，但未找到紀錄ID/託運單號\n辨識內容：\n{preview}")
            )
    except Exception as e:
        app.logger.exception(e)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ OCR 發生錯誤：{e}"))

# =========================
# 本地測試入口
# =========================
@app.route("/", methods=["GET"])
def index():
    return "OK"

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
