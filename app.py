from flask import Flask, request, abort
import gspread
from google.oauth2.service_account import Credentials
import os, re, difflib, json
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = Flask(__name__)

# ===== Google Sheets 連線（支援檔案或環境變數） =====
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

# =====★ 分頁名稱改為可設定，並給出合理預設 =====
MAIN_SHEET_NAME = os.getenv("MAIN_SHEET_NAME", "寄書任務")        # 你的主資料分頁（以前硬寫成 工作表1）
ZIP_SHEET_NAME  = os.getenv("ZIP_SHEET_NAME", "郵遞區號參照表")
BOOK_SHEET_NAME = os.getenv("BOOK_SHEET_NAME", "書目主檔")        # B=書名, K=別名

# 啟動時列出所有分頁，方便對照（看 Railway Deploy Logs）
_spread = GC.open_by_key(SHEET_ID)
_titles = [ws.title for ws in _spread.worksheets()]
print("=== DEBUG: Worksheets found ===", _titles)

# 明確打開分頁，不存在就丟出清楚錯誤（避免隱性崩潰）
try:
    MAIN_WS = _spread.worksheet(MAIN_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound as e:
    raise RuntimeError(f"找不到主分頁：{MAIN_SHEET_NAME}，目前活頁簿分頁有：{_titles}")

try:
    ZIP_WS  = _spread.worksheet(ZIP_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound as e:
    raise RuntimeError(f"找不到郵遞區號分頁：{ZIP_SHEET_NAME}，目前活頁簿分頁有：{_titles}")

try:
    BOOK_WS = _spread.worksheet(BOOK_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound as e:
    raise RuntimeError(f"找不到書目主檔分頁：{BOOK_SHEET_NAME}，目前活頁簿分頁有：{_titles}")

# ===== LINE Bot =====
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ===== 使用者暫存狀態（不綁秒數；每位使用者獨立）=====
PENDING = {}
CONFIRM_OK = {"y", "yes", "是", "好", "ok", "確認", "對", "Y", "OK", "Ok"}
CONFIRM_NO = {"n", "no", "否", "不要", "取消", "N", "取消作業", "重新輸入"}

# ===== 工具函式 =====
def parse_input(txt: str):
    lines = [ln for ln in txt.splitlines() if not ln.strip().startswith("#")]
    txt = "\n".join(lines)

    name_pat    = r"(?:學員)?姓名[:：]?\s*([^\n]+)"
    phone_pat   = r"(?:學員)?電話[:：]?\s*([0-9\-\s\(\)]+)"
    address_pat = r"(?:寄送)?地址[:：]?\s*([^\n]+)"
    book_pat    = r"(?:書籍名稱|書籍|書)[:：]?\s*([^\n]+)"

    name = re.search(name_pat, txt)
    phone = re.search(phone_pat, txt)
    addr = re.search(address_pat, txt)
    book = re.search(book_pat, txt)

    return {
        "name": name.group(1).strip() if name else "",
        "phone": phone.group(1).strip() if phone else "",
        "address": addr.group(1).strip() if addr else "",
        "book": book.group(1).strip() if book else ""
    }

def is_full_form_message(txt: str) -> bool:
    data = parse_input(txt)
    return all([data["name"], data["phone"], data["address"], data["book"]])

def validate_phone(p: str):
    digits = re.sub(r"\D", "", p)  # 允許 0912-345-678 等符號，統一抽數字
    return bool(re.fullmatch(r"09\d{8}", digits))

def validate_address(a: str):
    return ("縣" in a) or ("市" in a)

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

def find_zipcode(address: str):
    records = ZIP_WS.get_all_records()
    for rec in records:
        if rec.get("地區") and rec["地區"] in address:
            return rec.get("郵遞區號", "")
    return ""

def append_row(name, phone, address, zipcode, book):
    MAIN_WS.append_row([name, phone, address, zipcode, book])

def start_pending(user_id: str, p_type: str, data: dict, context: dict):
    PENDING[user_id] = {"type": p_type, "data": data, "context": context}

def clear_pending(user_id: str):
    if user_id in PENDING:
        del PENDING[user_id]

# ===== Webhook =====
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

    if user_id in PENDING:
        if text.startswith("#寄書需求") or is_full_form_message(text):
            clear_pending(user_id)
        else:
            if text in CONFIRM_OK and PENDING[user_id]["type"] == "confirm_suggestion":
                info = PENDING.pop(user_id)
                data = info["data"]
                canonical_book = info["context"]["suggested_book"]

                if not validate_phone(data["phone"]):
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 電話號碼格式錯誤，請重新輸入完整資料。"))
                    return
                if not validate_address(data["address"]):
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 地址格式錯誤，請重新輸入包含縣市的地址。"))
                    return

                zipcode = find_zipcode(data["address"])
                append_row(data["name"], data["phone"], data["address"], zipcode, canonical_book)
                reply = (
                    "✅ 已採用建議書名並建檔：\n"
                    f"姓名：{data['name']}\n"
                    f"電話：{data['phone']}\n"
                    f"地址：{data['address']}\n"
                    f"郵遞區號：{zipcode}\n"
                    f"書籍：{canonical_book}"
                )
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
                return

            if text in CONFIRM_NO:
                clear_pending(user_id)
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="已取消。若要重新申請請直接貼上完整資料。"))
                return

            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="目前等待確認中：請回覆「Y」採用、或「取消」。\n或直接貼上完整表單來重新送單。")
            )
            return

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
            "#寄書需求\n"
            "學員姓名：王小明\n"
            "學員電話：0912-345-678\n"
            "寄送地址：台中市西屯區至善路250號\n"
            "書籍名稱：Let's Go 5（第五版）"
        )
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 缺少欄位：{need}\n\n{demo}"))
        return

    if not validate_phone(data["phone"]):
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

    zipcode = find_zipcode(data["address"])
    append_row(data["name"], data["phone"], data["address"], zipcode, canonical_book)
    reply = (
        "✅ 已成功建檔：\n"
        f"姓名：{data['name']}\n"
        f"電話：{data['phone']}\n"
        f"地址：{data['address']}\n"
        f"郵遞區號：{zipcode}\n"
        f"書籍：{canonical_book}"
    )
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
