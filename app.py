# app.py
# ============================================
# 尚進《寄書＋進銷存》：OCR → 解析 → 回填寄書任務
# - 只抓取：紀錄ID（R+4碼）與 下方手寫12碼託運單號
# - 只寫入長度=12的託運單號，否則回覆提醒人工檢查
# ============================================

from flask import Flask, request, abort
import gspread
from google.oauth2.service_account import Credentials
import os, re, json, io, logging
from datetime import datetime
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    ImageMessage
)

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# =========================
# ✅（錨點）Google Sheets 連線設定
# =========================
SHEET_ID = os.getenv("SHEET_ID", "")  # 你的試算表 ID
MAIN_SHEET_NAME = os.getenv("MAIN_SHEET_NAME", "寄書任務")  # 主工作表名（預設：寄書任務）

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def _build_gspread_client():
    """
    先找 service_account.json（檔案），找不到再用環境變數 GOOGLE_SERVICE_ACCOUNT_JSON。
    """
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
# ✅（錨點）LINE Bot 設定
# =========================
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

if not (LINE_CHANNEL_SECRET and LINE_CHANNEL_ACCESS_TOKEN):
    app.logger.warning("⚠️ 尚未設置 LINE_CHANNEL_SECRET 或 LINE_CHANNEL_ACCESS_TOKEN")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# =========================
# ✅（錨點）OCR 設定：Google Cloud Vision（可選）
# =========================
_HAS_VISION = True
try:
    from google.cloud import vision
    from google.oauth2 import service_account as gservice_account
    # Vision 憑證優先順序：VISION_SERVICE_ACCOUNT_JSON 環境變數 > service_account.json 檔案
    _vision_creds = None
    vjson = os.getenv("VISION_SERVICE_ACCOUNT_JSON", "")
    if vjson:
        _vision_creds = gservice_account.Credentials.from_service_account_info(json.loads(vjson))
    else:
        if os.path.exists("service_account.json"):
            _vision_creds = gservice_account.Credentials.from_service_account_file("service_account.json")
        else:
            # 若沒特別提供，嘗試沿用同一份 GOOGLE_SERVICE_ACCOUNT_JSON
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
# 文字訊息（保留簡單 Echo / 日後擴充）
# =========================
@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = event.message.text.strip()
    # 這裡暫時不動你的寄書任務既有流程，只回應簡訊；後續若要補指令可再擴充
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=f"已收到訊息：{text}")
    )

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
    """
    以 Google Cloud Vision 做 OCR；若未啟用/安裝，回傳空字串並在 log 提醒。
    """
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
# ✅ OCR → 資料解析器
# 規則：
# - 紀錄ID：R+4碼（例如 R0024），視為「定位點」
# - 往下 1~5 行，抓第一個「12 碼純數字」作為託運單號
# - 若不是 12 碼，不寫入；回覆提醒人工檢查
# =========================
ID_REGEX = re.compile(r'\bR(\d{4})\b', re.IGNORECASE)   # Rdddd
DIGIT_BLOCK_REGEX = re.compile(r'(\d[\d\-\s]{10,}\d)')  # 先抓「看起來像一大段數字」的塊，允許空白或破折號

def _normalize_digits(s: str) -> str:
    return re.sub(r'\D+', '', s or '')

def parse_ocr_for_pairs(text: str, look_ahead_lines: int = 5):
    """
    傳回：
    - valid_pairs: [(rid, tracking12), ...]  # tracking 僅保留長度=12 的
    - invalid_pairs: [(rid, found_raw, normalized, reason), ...]  # 非 12 碼的候選會放這裡
    """
    valid_pairs = []
    invalid_pairs = []

    if not text:
        return valid_pairs, invalid_pairs

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    for idx, ln in enumerate(lines):
        m = ID_REGEX.search(ln)
        if not m:
            continue
        rid = f"R{m.group(1)}".upper()

        candidate_found = False
        # 往下找 1~N 行
        for j in range(1, look_ahead_lines + 1):
            if idx + j >= len(lines):
                break
            cand_line = lines[idx + j]
            for m2 in DIGIT_BLOCK_REGEX.finditer(cand_line):
                raw = m2.group(1)
                pure = _normalize_digits(raw)
                if len(pure) == 12:
                    valid_pairs.append((rid, pure))
                    candidate_found = True
                    break
                else:
                    # 紀錄非 12 碼的候選（只收第一個）
                    invalid_pairs.append((rid, raw, pure, f"長度{len(pure)}≠12"))
                    candidate_found = True
                    break
            if candidate_found:
                break

        # 若完全找不到數字塊，也記錄為 invalid（以便提示）
        if not candidate_found:
            invalid_pairs.append((rid, "", "", "未找到候選數字"))

    # 去重：同一 Rxxxx 僅保留第一筆
    seen = set()
    vp2 = []
    for rid, t in valid_pairs:
        if rid in seen:
            continue
        seen.add(rid)
        vp2.append((rid, t))

    # invalid 也做去重（避免同 rid 重複吵）
    seen_i = set()
    ip2 = []
    for rid, raw, pure, reason in invalid_pairs:
        if rid in seen_i:
            continue
        seen_i.add(rid)
        ip2.append((rid, raw, pure, reason))

    return vp2, ip2

# =========================
# ✅ 回填「寄書任務」對應列
# 欄位名可在下方 COL_* 常數調整
# =========================
def update_sheet_with_pairs(pairs, handler_display_name: str):
    """
    依據 (record_id, tracking12) 清單，回填〈寄書任務〉對應列的欄位：
      - 託運單號 ← tracking12
      - 寄送方式 ← "便利帶"
      - 寄出日期 ← 今日 YYYY-MM-DD
      - 寄送狀態 ← "已託運"
      - 經手人   ← handler_display_name（若無此欄，附加在備註尾端）
    僅寫入 tracking12 長度=12 的資料（呼叫端已驗算）。
    """
    if not pairs:
        return {"updated": 0, "not_found": [], "details": [], "skipped": []}

    gc = _build_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(MAIN_SHEET_NAME)

    # =====（錨點）表頭名稱對應：若你表頭不同，可在這裡修改 =====
    COL_ID_NAME   = "紀錄ID"
    COL_SHIP_MTD  = "寄送方式"
    COL_SHIP_DATE = "寄出日期"
    COL_TRACKING  = "託運單號"
    COL_STATUS    = "寄送狀態"
    COL_HANDLER   = "經手人"
    COL_NOTE      = "備註"
    # ============================================================

    headers = ws.row_values(1)
    header_idx = {name.strip(): i+1 for i, name in enumerate(headers) if name.strip()}

    # 取 紀錄ID 欄位位置（找不到就用第一欄）
    id_col_idx = header_idx.get(COL_ID_NAME, 1)
    id_col = ws.col_values(id_col_idx)  # 含表頭
    rid_to_row = {}
    for r, val in enumerate(id_col, start=1):
        if r == 1:
            continue
        v = (val or "").strip().upper()
        if v:
            rid_to_row[v] = r

    # 欄位索引
    col_ship_mtd  = header_idx.get(COL_SHIP_MTD)
    col_ship_date = header_idx.get(COL_SHIP_DATE)
    col_tracking  = header_idx.get(COL_TRACKING)
    col_status    = header_idx.get(COL_STATUS)
    col_handler   = header_idx.get(COL_HANDLER)
    col_note      = header_idx.get(COL_NOTE)

    # 小工具：把 A1 座標補上工作表名稱，避免寫到別的分頁
    def rng(a1: str) -> str:
        # 工作表名若有空白或特殊字元，加單引號
        return f"'{MAIN_SHEET_NAME}'!{a1}"

    today = datetime.now().strftime("%Y-%m-%d")

    updated = 0
    not_found = []
    details = []
    skipped = []
    batch_data = []

    for rid, tracking12 in pairs:
        row = rid_to_row.get(rid)
        if not row:
            not_found.append(rid)
            continue

        row_writes = 0
        if col_tracking:
            a1 = gspread.utils.rowcol_to_a1(row, col_tracking)
            batch_data.append({"range": rng(a1), "values": [[tracking12]]})
            row_writes += 1
        if col_ship_mtd:
            a1 = gspread.utils.rowcol_to_a1(row, col_ship_mtd)
            batch_data.append({"range": rng(a1), "values": [["便利帶"]]})
            row_writes += 1
        if col_ship_date:
            a1 = gspread.utils.rowcol_to_a1(row, col_ship_date)
            batch_data.append({"range": rng(a1), "values": [[today]]})
            row_writes += 1
        if col_status:
            a1 = gspread.utils.rowcol_to_a1(row, col_status)
            batch_data.append({"range": rng(a1), "values": [["已託運"]]})
            row_writes += 1

        # 經手人欄位：若沒有就附加到備註
        if col_handler:
            a1 = gspread.utils.rowcol_to_a1(row, col_handler)
            batch_data.append({"range": rng(a1), "values": [[handler_display_name]]})
            row_writes += 1
        elif col_note:
            old_note = ws.cell(row, col_note).value or ""
            sep = "；" if old_note and not old_note.endswith(("；", ";")) else ""
            new_note = f"{old_note}{sep}經手人：{handler_display_name}".strip("；")
            a1 = gspread.utils.rowcol_to_a1(row, col_note)
            batch_data.append({"range": rng(a1), "values": [[new_note]]})
            row_writes += 1

        if row_writes > 0:
            updated += 1
            details.append(f"{rid} → {tracking12}")
        else:
            skipped.append(rid)  # 沒有任何可寫欄位

    # 批次寫入（這次都有帶上分頁名稱）
    if batch_data:
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [{"range": it["range"], "values": it["values"]} for it in batch_data]
        }
        # 多補一行 log，方便你在 Railway Logs 檢查發送範圍
        app.logger.info(f"[GSHEET_BATCH_UPDATE] {len(batch_data)} ranges")
        ws.spreadsheet.values_batch_update(body)

    return {"updated": updated, "not_found": not_found, "details": details, "skipped": skipped}


# =========================
# ✅ 圖片訊息處理：拍照 → OCR → 解析 → 驗算 → 寫回
# =========================
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    try:
        user_id = getattr(event.source, "user_id", "unknown")
        # 取 LINE 顯示名稱（作為「經手人」）
        try:
            profile = line_bot_api.get_profile(user_id)
            handler_name = getattr(profile, "display_name", "未知經手人")
        except Exception:
            handler_name = "未知經手人"

        app.logger.info(f"[IMG] 收到圖片 user_id={user_id}, msg_id={event.message.id}")

        # 1) 下載圖片
        img_bytes = _download_line_image_bytes(event.message.id)

        # 2) OCR → 文字
        text = _ocr_text_from_bytes(img_bytes)
        app.logger.info(f"[OCR_RAW]\n{text[:1000]}")

        # 3) 解析：成對 (Rdddd, 12碼?)，並區分 valid / invalid
        valid_pairs, invalid_pairs = parse_ocr_for_pairs(text)
        app.logger.info(f"[OCR_PAIRS_VALID] {valid_pairs}")
        app.logger.info(f"[OCR_PAIRS_INVALID] {invalid_pairs}")

        if not valid_pairs and not invalid_pairs:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="✅ 已收到圖片，但沒找到『紀錄ID（Rxxxx）』或其下方的『託運單號』。\n請確認取景：上方為 Rxxxx、其下方為手寫 12 碼。"
                )
            )
            return

        # 4) 寫回：只寫入長度=12 的 valid_pairs
        result = update_sheet_with_pairs(valid_pairs, handler_name)

        # 5) 組裝回覆
        lines = []
        if result["updated"] > 0:
            lines.append(f"✅ 已更新：{result['updated']} 筆")
            if result["details"]:
                lines.append("明細：\n" + "\n".join(result["details"]))
        else:
            lines.append("⚠️ 本次未寫入任何託運單號（未找到長度=12 的有效單號）。")

        # 找不到對應列的紀錄ID
        if result["not_found"]:
            lines.append("\n⚠️ 下列紀錄ID 在《寄書任務》找不到：\n" + ", ".join(result["not_found"]))

        # 非 12 碼／或未找到數字塊 → 提醒人工檢查
        if invalid_pairs:
            warn_rows = []
            for rid, raw, pure, reason in invalid_pairs:
                # 顯示：Rxxxx｜辨識="原始片段"｜淨化=純數字｜原因
                part_raw = raw if raw else "（未擷取到數字）"
                part_pure = pure if pure else "（無）"
                warn_rows.append(f"{rid}｜辨識：{part_raw}｜淨化：{part_pure}｜原因：{reason}")
            lines.append("\n❗以下項目未寫入（需手動檢查/調整為 12 碼）：\n" + "\n".join(warn_rows))

        # 經手人
        lines.append(f"\n經手人：{handler_name}")

        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="\n".join(lines))
        )

    except Exception as e:
        app.logger.exception(e)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"❌ 圖片處理發生錯誤：{e}")
        )

# =========================
# 本地測試入口（Railway 可無視）
# =========================
@app.route("/", methods=["GET"])
def index():
    return "OK"

if __name__ == "__main__":
    # Railway 會用 gunicorn 啟動；本地開發可直接跑
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
