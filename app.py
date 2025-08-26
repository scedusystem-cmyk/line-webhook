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
        raise RuntimeError("Missing service account credentials. Provide service_account.json OR env GOOGLE_SERVICE_ACCOUNT_JSON.")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

GC = _build_gspread_client()

# （可用環境變數覆蓋）
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
    ＊規則：凡是不符合上述欄位格式的行，將自動歸入「業務備註」。
    """
    lines = txt.splitlines()
    if lines and lines[0].strip().startswith("#寄書"):
        lines = lines[1:]

    name = phone = address = book = note = ""
    extras = []

    for ln in lines:
        s = ln.strip()
        if not s:
            continue

        m = re.match(r"^(?:學員)?姓名[:：]\s*(.+)$", s)
        if m:
            name = m.group(1).strip()
            continue

        m = re.match(r"^(?:學員)?電話[:：]\s*([0-9\-\s\(\)]+)$", s)
        if m:
            phone = m.group(1).strip()
            continue

        m = re.match(r"^(?:寄送)?地址[:：]\s*(.+)$", s)
        if m:
            address = m.group(1).strip()
            continue

        m = re.match(r"^(?:書籍名稱|書籍|書)[:：]\s*(.+)$", s)
        if m:
            book = m.group(1).strip()
            continue

        m = re.match(r"^(?:備註|業務備註)[:：]\s*(.+)$", s)
        if m:
            note = (note + "\n" + m.group(1).strip()).strip() if note else m.group(1).strip()
            continue

        # 其他未匹配行 → 歸入備註
        extras.append(s)

    if extras and not note:
        note = "\n".join(extras)
    elif extras:
        note = (note + "\n" + "\n".join(extras)).strip()

    return {"name": name, "phone": phone, "address": address, "book": book, "note": note}

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
# 郵遞區號 & 地址組裝
# =========================
def find_zipcode(address: str):
    """依 〈郵遞區號參照表〉的『地區』欄進行包含比對，回傳郵遞區號或空字串。"""
    records = ZIP_WS.get_all_records()
    for rec in records:
        area = rec.get("地區")
        if area and area in address:
            return rec.get("郵遞區號", "")
    return ""

def compose_address_with_zip(address: str) -> str:
    """若地址未以數字ZIP開頭，將 ZIP 前置；找不到ZIP則維持原樣。"""
    zipc = find_zipcode(address)
    if not zipc:
        return address
    if re.match(r"^\s*\d{3,5}", address):  # 已存在郵遞區號
        return address
    return f"{zipc}{address}"

# =========================
# 寫入寄書任務表（A~M）
# =========================
def append_row(record_id, sender_name, name, phone, address, book, note):
    # 建單日期含時分
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    # 郵遞區號前置到地址
    full_address = compose_address_with_zip(address)

    row = [
        record_id,          # A 紀錄ID
        today,              # B 建單日期（YYYY-MM-DD HH:MM）
        sender_name,        # C 建單人
        name,               # D 學員姓名
        phone,              # E 學員電話
        full_address,       # F 寄送地址（已前置ZIP）
        book,               # G 書籍名稱
        note,               # H 業務備註
        "",                 # I 寄送方式
        "",                 # J 寄出日期
        "",                 # K 託運單號
        "",                 # L 經手人
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
    if user_id not in PENDING and not is_trigger(text):
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=(
                    "請以「#寄書」開頭送單，例如：\n"
                    "#寄書\n學員姓名：王小明\n學員電話：0912345678\n寄送地址：台中市西屯區至善路250號\n書籍名稱：Let's Go 5\n（可加：備註：XX）"
                )
            )
        )
        return

    # ===== 等待 Y/N 階段 =====
    if user_id in PENDING:
        if is_trigger(text):
            clear_pending(user_id)  # 使用者改貼新單，清除等待狀態
        else:
            info = PENDING[user_id]
            if text in CONFIRM_OK and info["type"] == "confirm_suggestion":
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

                append_row(record_id, sender_name, data["name"], clean_phone, data["address"], canonical_book, data.get("note",""))

                # 回覆（地址顯示帶ZIP）
                display_address = compose_address_with_zip(data["address"])
                lines = [
                    "✅ 已採用建議書名並建檔：",
                    f"姓名：{data['name']}",
                    f"電話：{clean_phone}",
                    f"地址：{display_address}",
                ]
                if data.get("note"):
                    lines.append(f"備註：{data['note']}")
                lines.append(f"書籍：{canonical_book}")
                lines.append("狀態：待處理")
                reply = "\n".join(lines)

                clear_pending(user_id)
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

    ok, canonical_book, msg, need_confirm = resolve_book(data["book"])
    if not ok:
        if need_confirm:
            start_pending(user_id, "confirm_suggestion", data, {"suggested_book": canonical_book})
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        return

    profile = line_bot_api.get_profile(user_id)
    sender_name = profile.display_name
    record_id = get_next_record_id()

    append_row(record_id, sender_name, data["name"], clean_phone, data["address"], canonical_book, data.get("note",""))

    # 回覆（地址顯示帶ZIP）
    display_address = compose_address_with_zip(data["address"])
    lines = [
        "✅ 已成功建檔：",
        f"姓名：{data['name']}",
        f"電話：{clean_phone}",
        f"地址：{display_address}",
    ]
    if data.get("note"):
        lines.append(f"備註：{data['note']}")
    lines.append(f"書籍：{canonical_book}")
    lines.append("狀態：待處理")
    reply = "\n".join(lines)

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))

# =========================
# App 啟動
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
