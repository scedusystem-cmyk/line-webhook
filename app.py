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
    """在〈郵遞區號參照表〉找出相符行政區，將郵遞區號 + 原地址回填。"""
    try:
        ws = _get_worksheet("郵遞區號參照表")
        rows = ws.get_all_values()
        for row in rows[1:]:
            # 假設 A欄=郵遞區號, B欄=行政區片段（例如 台中市西屯區）
            if len(row) >= 2 and row[1] and row[1] in address:
                return row[0] + address
    except Exception as e:
        app.logger.error(f"[ZIPCODE] lookup error: {e}")
    return address

def fuzzy_match_book(book_name: str) -> str | None:
    """在〈書目主檔〉做模糊比對（B欄正式書名），cutoff=0.6，回傳最佳匹配或 None。"""
    ws = _get_worksheet("書目主檔")
    vals = ws.get_all_values()
    official_names = [row[1] for row in vals[1:] if len(row) > 1 and row[1]]
    matches = difflib.get_close_matches(book_name, official_names, n=1, cutoff=0.6)
    return matches[0] if matches else None

# =========================
# LINE Webhook 主入口
# =========================
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
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
    user_name = "LINE使用者"  # 之後可改為從 profile 抓顯示名稱

    text = event.message.text.strip()
    if text.startswith("#寄書需求") or text.startswith("#寄書"):
        # ===== 簡單解析 =====
        lines = text.split("\n")
        data = {"name": "", "phone": "", "address": "", "book": ""}
        for line in lines:
            if "姓名" in line:
                data["name"] = line.split("：", 1)[-1].strip()
            elif "電話" in line:
                data["phone"] = normalize_phone(line.split("：", 1)[-1])
            elif "地址" in line:
                data["address"] = lookup_zipcode(line.split("：", 1)[-1].strip())
            elif "書籍" in line or "書名" in line:
                book_input = line.split("：", 1)[-1].strip()
                match = fuzzy_match_book(book_input)
                data["book"] = match if match else book_input

        # ===== 檢查缺失 =====
        missing = [k for k in ["name", "phone", "address", "book"] if not data.get(k)]
        if missing:
            reply = f"❌ 缺少欄位: {', '.join(missing)}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
            return

        # ===== 寫入試算表 =====
        try:
            ws = _get_worksheet(os.getenv("MAIN_SHEET_NAME", "寄書任務"))
            rows = ws.get_all_values()
            new_id = str(len(rows))  # 簡單流水號（依你目前規劃）
            today = datetime.now().strftime("%Y-%m-%d")
            row = [
                new_id,          # A 紀錄ID
                today,           # B 建單日期
                user_name,       # C 建單人
                data["name"],    # D 學員姓名
                data["phone"],   # E 學員電話（已淨化）
                data["address"], # F 寄送地址（已加郵遞區號）
                data["book"],    # G 書籍名稱（含模糊比對結果）
                "", "", "", "", "", "", ""  # H~N 保留空白
            ]
            ws.append_row(row)
            reply = (
                "✅ 已成功建檔：\n"
                f"姓名：{data['name']}\n"
                f"電話：{data['phone']}\n"
                f"地址：{data['address']}\n"
                f"書籍：{data['book']}"
            )
        except Exception as e:
            reply = f"❌ Google Sheet 寫入失敗: {e}"

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))

# =========================
# 圖片訊息處理器（含 OCR + 健檢 + 詳細錯誤回覆）
# =========================
def _download_line_image_bytes(message_id: str) -> bytes:
    content = line_bot_api.get_message_content(message_id)
    return b"".join([chunk for chunk in content.iter_content()])

def _ocr_text_from_bytes(img_bytes: bytes) -> str:
    """執行 Vision OCR，若 Vision API 本身回 error，直接 raise 以便外層捕捉。"""
    if not _HAS_VISION:
        raise RuntimeError("未安裝 google-cloud-vision 套件")

    # 建立憑證（檔案或環境變數其一）
    creds_path = "service_account.json"
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if os.path.exists(creds_path):
        creds = service_account.Credentials.from_service_account_file(creds_path)
    elif sa_json:
        creds = service_account.Credentials.from_service_account_info(json.loads(sa_json))
    else:
        raise RuntimeError("找不到 service_account.json，且未設定 GOOGLE_SERVICE_ACCOUNT_JSON")

    client = vision.ImageAnnotatorClient(credentials=creds)
    image = vision.Image(content=img_bytes)
    response = client.text_detection(image=image)

    # Vision API 的錯誤會放在 response.error.message
    if getattr(response, "error", None) and getattr(response.error, "message", ""):
        raise RuntimeError(f"Vision API 錯誤：{response.error.message}")

    texts = response.text_annotations
    return texts[0].description if texts else ""

@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    try:
        user_id = getattr(event.source, "user_id", "unknown")
        app.logger.info(f"[IMG] 收到圖片 user_id={user_id}, msg_id={event.message.id}")

        # === [OCR 健檢] 開始 ===
        problems = []
        if not _HAS_VISION:
            problems.append("未安裝 google-cloud-vision 套件")
        sa_path = "service_account.json"
        sa_env  = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not os.path.exists(sa_path) and not sa_env:
            problems.append("找不到 service_account.json，且未設定 GOOGLE_SERVICE_ACCOUNT_JSON")
        if not CHANNEL_ACCESS_TOKEN:
            problems.append("LINE CHANNEL_ACCESS_TOKEN 未設定")
        if problems:
            msg = "；".join(problems)
            app.logger.error(f"[OCR_CHECK] 失敗：{msg}")
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"❌ OCR 健檢未通過：{msg}")
            )
            return
        # === [OCR 健檢] 結束 ===

        # 下載圖片
        img_bytes = _download_line_image_bytes(event.message.id)
        app.logger.info(f"[IMG_BYTES] size={len(img_bytes)} bytes")

        if not img_bytes:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="❌ 圖片下載失敗（可能是 LINE 權限或 Access Token 錯誤）")
            )
            return

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
            tip = "（OCR 無辨識到文字，請確認影像清晰度/對比/角度）"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=tip))

    except Exception as e:
        # 產生簡短錯誤代碼，方便和伺服器日誌對應
        err_code = datetime.now().strftime("%Y%m%d%H%M%S")
        app.logger.error(f"[OCR_ERROR][{err_code}] {repr(e)}", exc_info=True)

        # 回覆使用者可讀的錯誤（避免洩漏私鑰）
        safe_msg = str(e)
        safe_msg = re.sub(
            r'BEGIN PRIVATE KEY.*END PRIVATE KEY',
            '[private_key]',
            safe_msg,
            flags=re.S
        )
        safe_msg = safe_msg[:400]  # 最多 400 字

        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"❌ OCR 錯誤（代碼 {err_code}）：{safe_msg}")
        )

# =========================
# 主程式入口
# =========================
if __name__ == "__main__":
    # 你若想在啟動時印出有哪些工作表，可取消下一行註解
    # app.logger.info(f"=== DEBUG: Worksheets found === {[ws.title for ws in _build_gspread_client().open_by_key(SHEET_ID).worksheets()]}")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
