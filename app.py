from flask import Flask, request, abort
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os, re, time, difflib
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = Flask(__name__)

# ===== Google Sheets 連線 =====
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", SCOPE)
GC = gspread.authorize(CREDS)
SHEET_ID = os.getenv("SHEET_ID")

MAIN_WS = GC.open_by_key(SHEET_ID).worksheet("工作表1")
ZIP_WS = GC.open_by_key(SHEET_ID).worksheet("郵遞區號參照表")
BOOK_WS = GC.open_by_key(SHEET_ID).worksheet("書目主檔")  # B=書籍名稱, K=模糊比對書名(別名)

# ===== LINE Bot =====
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ===== 使用者確認暫存 (5 分鐘有效) =====
# user_id -> {"expires": ts, "data": {name, phone, address, book}, "suggested": canonical_book}
PENDING = {}
CONFIRM_OK = {"y", "yes", "是", "好", "ok", "確認", "對", "Y", "OK", "Ok"}
CONFIRM_NO = {"n", "no", "否", "不要", "取消", "N"}

# ===== 工具函式 =====
def clean_expired():
    now = time.time()
    for uid in list(PENDING.keys()):
        if PENDING[uid]["expires"] < now:
            del PENDING[uid]

def parse_input(txt: str):
    name = re.search(r"姓名[:：]?\s*([\S]+)", txt)
    phone = re.search(r"電話[:：]?\s*(\d+)", txt)
    addr = re.search(r"地址[:：]?\s*(.+?)(書|書籍|$)", txt)
    book = re.search(r"(?:書|書籍)[:：]?\s*([\S ]+)", txt)
    return {
        "name": name.group(1).strip() if name else "",
        "phone": phone.group(1).strip() if phone else "",
        "address": addr.group(1).strip() if addr else "",
        "book": book.group(1).strip() if book else ""
    }

def validate_phone(p: str):
    return bool(re.match(r"^09\d{8}$", p))

def validate_address(a: str):
    return ("縣" in a) or ("市" in a)

def load_book_index():
    """
    回傳:
      titles: set(正式書名, 來源B欄)
      alias2title: dict(別名 -> 正式書名, 來源K欄)
      candidates: list(正式+別名，用於 difflib)
    """
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
    """
    書名解析：
      1) 命中B欄 → 直接通過
      2) 命中K欄(別名) → 直接映射通過
      3) 兩者皆無 → 用 difflib 找最接近，提示並詢問 Y/N
    回傳 (ok, canonical_title, msg_for_user, need_confirm)
    """
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
        canonical = alias2title.get(m, m)  # 別名→正式
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
    clean_expired()

    # --- 確認流程 ---
    if user_id in PENDING:
        if text in CONFIRM_OK:
            info = PENDING.pop(user_id)
            data = info["data"]
            canonical_book = info["suggested"]
            if not validate_phone(data["phone"]):
                line_bot_api.reply_message(event.reply_token,
                    TextSendMessage(text="⚠️ 電話號碼格式錯誤，請重新輸入完整資料。"))
                return
            if not validate_address(data["address"]):
                line_bot_api.reply_message(event.reply_token,
                    TextSendMessage(text="⚠️ 地址格式錯誤，請重新輸入包含縣市的地址。"))
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
            PENDING.pop(user_id, None)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="已取消。請重新輸入：姓名、電話、地址、書籍。"))
            return
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請回覆「Y」採用，或回覆「N」取消。"))
        return

    # --- 一般輸入 ---
    data = parse_input(text)
    if not data["name"]:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ 請提供姓名。"))
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
            PENDING[user_id] = {
                "expires": time.time() + 300,
                "data": data,
                "suggested": canonical_book
            }
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
