from flask import Flask, request, abort
import gspread
from google.oauth2.service_account import Credentials
import os, re, difflib, json
from datetime import datetime
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = Flask(__name__)

# =========================
# Google Sheets 連線設定區
# （錨點：需改的分頁名/Sheet ID/金鑰請看環境變數）
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
        raise RuntimeError("Missing service account credentials. "
                           "Provide service_account.json file OR set env GOOGLE_SERVICE_ACCOUNT_JSON.")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

GC = _build_gspread_client()

# （錨點：分頁名稱若改，直接改環境變數或這裡的預設值）
MAIN_SHEET_NAME = os.getenv("MAIN_SHEET_NAME", "寄書任務")
ZIP_SHEET_NAME  = os.getenv("ZIP_SHEET_NAME", "郵遞區號參照表")
BOOK_SHEET_NAME = os.getenv("BOOK_SHEET_NAME", "書目主檔")

_spread = GC.open_by_key(SHEET_ID)
_titles = [ws.title for ws in _spread.worksheets()]
print("=== DEBUG: Worksheets found ===", _titles)

MAIN_WS = _spread.worksheet(MAIN_SHEET_NAME)
ZIP_WS  = _spread.worksheet(ZIP_SHEET_NAME)
BOOK_WS = _spread.worksheet(BOOK_SHEET_NAME)

# =========================
# LINE Bot 設定
# =========================
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# =========================
# 狀態管理
# =========================
PENDING = {}
CONFIRM_OK = {"y","yes","是","好","ok","確認","對","Y","OK","Ok"}
CONFIRM_NO = {"n","no","否","不要","取消","N","取消作業","重新輸入"}

def start_pending(user_id: str, p_type: str, data: dict, context: dict):
    PENDING[user_id] = {"type": p_type, "data": data, "context": context}

def clear_pending(user_id: str):
    if user_id in PENDING:
        del PENDING[user_id]

# =========================
# 流水號工具
# =========================
def get_next_record_id():
    """回傳新紀錄ID，格式：R0001, R0002...（取A欄目前最大號+1）"""
    col_values = MAIN_WS.col_values(1)  # A欄
    max_num = 0
    for v in col_values[1:]:  # 跳過表頭
        v = (v or "").strip()
        if v.startswith("R") and v[1:].isdigit():
            num = int(v[1:])
            if num > max_num:
                max_num = num
    next_num = max_num + 1
    return f"R{next_num:04d}"

# =========================
# 解析/驗證
# =========================
def is_trigger(text: str) -> bool:
    """僅當訊息首行為 #寄書 時觸發"""
    first_line = text.strip().splitlines()[0] if text.strip() else ""
    return first_line.strip().startswith("#寄書")

def parse_input(txt: str):
    """
    解析欄位（允許第一行是 #寄書）
    支援欄位：
    - 學員姓名：
    - 學員電話：
    - 寄送地址：
    - 書籍名稱：/書籍：/書：
    - 備註：/業務備註：（可選）
    """
    lines = txt.splitlines()
    if lines and lines[0].strip().startswith("#寄書"):
        lines = lines[1:]  # 去掉觸發行
    # 保留使用者內容（不要砍掉 #），避免誤刪內文
    txt = "\n".join(lines)

    name_pat    = r"(?:學員)?姓名[:：]?\s*([^\n]+)"
    phone_pat   = r"(?:學員)?電話[:：]?\s*([0-9\-\s\(\)]+)"
    address_pat = r"(?:寄送)?地址[:：]?\s*([^\n]+)"
    book_pat    = r"(?:書籍名稱|書籍|書)[:：]?\s*([^\n]+)"
    note_pat    = r"(?:備註|業務備註)[:：]?\s*([^\n]+)"

    name = re.search(name_pat, txt)
    phone = re.search(phone_pat, txt)
    addr = re.search(address_pat, txt)
    book = re.search(book_pat, txt)
    note = re.search(note_pat, txt)

    return {
        "name": name.group(1).strip() if name else "",
        "phone": phone.group(1).strip() if phone else "",
        "address": addr.group(1).strip() if addr else "",
        "book": book.group(1).strip() if book else "",
        "note": note.group(1).strip() if note else ""
    }

def is_full_form_message(txt: str) -> bool:
    """必須同時：#寄書 觸發 + 四大欄位齊全"""
    if not is_trigger(txt):
        return False
    data = parse_input(txt)
    return all([data["name"], data["phone"], data["address"], data["book"]])

def validate_phone(p: str):
    digits = re.sub(r"\D", "", p)  # 清除非數字
    return digits if re.fullmatch(r"09\d{8}", digits) else None

def validate_address(a: str):
    return ("縣" in a) or ("市" in a)

# =========================
# 書名比對
# =========================
def load_book_index():
    rows = BOOK_WS.get_all_values()
    titles = set()
    alias2title = {}
    if not rows:
        return titles, alias2title, []
    for r in rows[1:]:
        if not r:
            continue
        title = r[1].strip() if len(r) > 1 else ""
        if not title:
            continue
        titles.add(title)
        alias_raw = r[10].strip() if len(r) > 10 else ""
        if alias_raw:
            parts = re.split(r"[,\u3001/;| ]+", alias_raw)
            for a in parts:
                a = a.strip()
                if a:
                    alias2title[a] = title
    candidates = list(titles) + list(alias2title.keys())
    return titles, alias2title, candidates

def resolve_book(user_book: str):
    titles, alias2title, candidates = load_book_index()
    if not user_book:
        return False, "", "⚠️ 書籍名稱不可為空，請重新輸入。", False

    if user_book in titles:
        return True, user_book, "", False
    if user_book in alias2title:
        return True, alias2title[user_book], "", False

    matches = difflib.get_close_matches(user_book, candidates, n=1, cutoff=0.6)
    if matches:
        m = matches[0]
        canonical = alias2title.get(m, m)
        msg = f"找不到《{user_book}》。您是要《{canonical}》嗎？\n回覆「Y」採用，或回覆「N」取消。"
        return False, canonical, msg, True

    return False, "", f"⚠️ 書籍《{user_book}》不存在，請確認後再輸入。", False

# =========================
# 郵遞區號（僅用於回覆展示；目前不入表）
# =========================
def find_zipcode(address: str):
    records = ZIP_WS.get_all_records()
    for rec in records:
        if rec.get("地區") and rec["地區"] in address:
            return rec.get("郵遞區號", "")
    return ""

# =========================
# 寫入寄書任務表
# （錨點：這裡對應新的 A~M 欄位）
# =========================
def append_row(record_id, sender_name, name, phone, address, book, note):
    # 建單日期含時分
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    row = [
        record_id,          # A 紀錄ID
        today,              # B 建單日期（YYYY-MM-DD HH:MM）
        sender_name,        # C 建單人
        name,               # D 學員姓名
        phone,              # E 學員電話
        address,            # F 寄送地址
        book,               # G 書籍名稱
        note,               # H 業務備註（可空字串）
        "",                 # I 寄送方式（保留空白）
        "",                 # J 寄出日期（保留空白）
        "",                 # K 託運單號（保留空白）
        "",                 # L 經手人（保留空白）
        "待處理",           # M 寄送狀態（預設）
    ]
    MAIN_WS.append_row(row)

# =========================
# LINE Webhook
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

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()

    # ===== 僅允許 #寄書 作為新單觸發 =====
    # 若使用者在「模糊比對待確認」階段，仍可回覆 Y/N 繼續，不需要再 #寄書
    if user_id not in PENDING and not is_trigger(text):
        # 非觸發訊息，不處理（也可改為友善提示）
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="請以「#寄書」開頭送單，例如：\n#寄書\n學員姓名：王小明\n學員電話：0912345678\n寄送地址：台中市西屯區至善路250號\n書籍名稱：Let's Go 5\n（可加：備註：XX）")
        )
        return

    # ===== 已在等待 Y/N 的階段 =====
    if user_id in PENDING:
        # 若改貼 #寄書 重新送單，就清除等待狀態，走新單流程
        if is_trigger(text):
            clear_pending(user_id)
        else:
            if text in CONFIRM_OK and PENDING[user_id]["type"] == "confirm_suggestion":
                info = PENDING.pop(user_id)
                data = info["data"]
                canonical_book = info["context"]["suggested_book"]

                clean_phone = validate_phone(data["phone"])
                if not clean_phone:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 電話號碼格式錯誤，請重新輸入完整資料。"))
                    return
                if not validate_address(data["address"]):
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 地址格式錯誤，請重新輸入包含縣市的地址。"))
                    return

                profile = line_bot_api.get_profile(user_id)
                sender_name = profile.display_name
                record_id = get_next_record_id()

                # 入表（新版欄位＋備註）
                append_row(record_id, sender_name, data["name"], clean_phone, data["address"], canonical_book, data.get("note",""))

                zipcode = find_zipcode(data["address"])
                reply = (
                    "✅ 已採用建議書名並建檔：\n"
                    f"姓名：{data['name']}\n"
                    f"電話：{clean_phone}\n"
                    f"地址：{data['address']}\n"
                    + (f"備註：{data['note']}\n" if data.get("note") else "")
                    + (f"郵遞區號：{zipcode}\n" if zipcode else "")
                    + f"書籍：{canonical_book}\n"
                    + f"狀態：待處理"
                )

                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
                return

            if text in CONFIRM_NO:
                clear_pending(user_id)
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="已取消。若要重新申請請以「#寄書」開頭貼上完整資料。"))
                return

            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="目前等待確認中：請回覆「Y」採用、或「取消」。\n或以「#寄書」開頭貼上完整表單來重新送單。")
            )
            return

    # ===== 新單流程（#寄書） =====
    # 檢查欄位
    data = parse_input(text)
    missing = []
    if not data["name"]:    missing.append("學員姓名")
    if not data["phone"]:   missing.append("學員電話")
    if not data["address"]: missing.append("寄送地址")
    if not data["book"]:    missing.append("書籍名稱")
    if missing:
        need = "、".join(missing)
        demo = (
            "請依格式補齊：\n"
            "#寄書\n"
            "學員姓名：王小明\n"
            "學員電話：0912345678\n"
            "寄送地址：台中市西屯區至善路250號\n"
            "書籍名稱：Let's Go 5\n"
            "備註：可留空（選填）"
        )
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 缺少欄位：{need}\n\n{demo}"))
        return

    clean_phone = validate_phone(data["phone"])
    if not clean_phone:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 電話號碼格式錯誤（例：0912345678）。"))
        return
    if not validate_address(data["address"]):
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 地址需包含「縣」或「市」，請確認。"))
        return

    # 書名比對 / 模糊建議
    ok, canonical_book, msg, need_confirm = resolve_book(data["book"])
    if not ok:
        if need_confirm:
            start_pending(user_id, "confirm_suggestion", data, {"suggested_book": canonical_book})
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        return

    # 建檔
    profile = line_bot_api.get_profile(user_id)
    sender_name = profile.display_name
    record_id = get_next_record_id()

    append_row(record_id, sender_name, data["name"], clean_phone, data["address"], canonical_book, data.get("note",""))

    zipcode = find_zipcode(data["address"])
    reply = (
        "✅ 已成功建檔：\n"
        f"姓名：{data['name']}\n"
        f"電話：{clean_phone}\n"
        f"地址：{data['address']}\n"
        + (f"備註：{data['note']}\n" if data.get("note") else "")
        + (f"郵遞區號：{zipcode}\n" if zipcode else "")
        + f"書籍：{canonical_book}\n"
        + f"狀態：待處理"
    )

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))

# =========================
# App 啟動
# =========================
if __name__ == "__main__":
    # Railway 預設 PORT
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
