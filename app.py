# app.py — LINE Bot + Google Sheets 寫入（寄書任務 + 自動補郵遞區號）
import os, json, datetime, re
from flask import Flask, request, abort, jsonify

# ----- LINE SDK -----
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

# ----- Google Sheets -----
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# ====== LINE 基本設定 ======
CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
if not CHANNEL_SECRET or not CHANNEL_ACCESS_TOKEN:
    print("[WARN] Missing LINE env vars.")
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

# ====== Google Sheets 連線設定 ======
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
service_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
creds = Credentials.from_service_account_info(service_info, scopes=SCOPES)
gc = gspread.authorize(creds)

SHEET_ID = os.environ["SHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "寄書任務")
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

# ====== 小工具 ======
def now_tpe_str():
    return (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

def make_record_id():
    ts = (datetime.datetime.utcnow() + datetime.timedelta(hours=8))
    return "SJREQ-" + ts.strftime("%Y%m%d-%H%M%S")

def infer_status(ship_date: str) -> str:
    return "已寄送完成" if ship_date and str(ship_date).strip() else "未寄出"

def normalize_phone(p: str) -> str:
    return "".join(ch for ch in (p or "") if ch.isdigit())

def safe_text(x):
    return x if x is not None else ""

def get_display_name_from_event(event) -> str:
    try:
        if event.source.type == "group":
            return line_bot_api.get_group_member_profile(event.source.group_id, event.source.user_id).display_name
        elif event.source.type == "room":
            return line_bot_api.get_room_member_profile(event.source.room_id, event.source.user_id).display_name
        else:
            return line_bot_api.get_profile(event.source.user_id).display_name
    except Exception:
        return "未知使用者"

# ====== 郵遞區號參照表 ======
def load_zip_dict():
    try:
        zip_ws = sh.worksheet("郵遞區號參照表")
        data = zip_ws.get_all_records()  # [{'地區': '中正區', '郵遞區號': 100}, ...]
        zip_dict = {}
        for row in data:
            code = str(row.get("郵遞區號", "")).strip()
            name = str(row.get("地區", "")).strip()
            if code and name:
                zip_dict[code] = name
        print(f"[INFO] 已載入 {len(zip_dict)} 筆郵遞區號")
        return zip_dict
    except Exception as e:
        print("[WARN] 無法載入郵遞區號參照表:", e)
        return {}

ZIP_DICT = load_zip_dict()

def lookup_zip(address: str) -> str:
    """從地址比對郵遞區號，找不到回傳空字串"""
    if not address:
        return ""
    for code, region in ZIP_DICT.items():
        if region and region in address:
            return code
    return ""

# ====== 將一筆訂單寫入 Google Sheets ======
def append_order_row(
    建單人, 學員姓名, 學員電話, 寄送地址,
    書籍名稱, 語別="", 寄送方式="", 寄出日期="",
    託運單號="", 備註="", 資料檢核狀態=""
):
    record_id = make_record_id()
    zip_code = lookup_zip(寄送地址)
    final_address = f"{zip_code} {寄送地址}" if zip_code else safe_text(寄送地址)
    status = "正常" if zip_code else "無法自動補郵遞區號"

    row = [
        record_id,                 # 1 紀錄ID
        now_tpe_str(),             # 2 建單日期
        safe_text(建單人),         # 3 建單人
        safe_text(學員姓名),       # 4 學員姓名
        normalize_phone(學員電話), # 5 學員電話
        final_address,             # 6 寄送地址（自動補郵遞區號）
        safe_text(書籍名稱),       # 7 書籍名稱
        safe_text(語別),           # 8 語別
        safe_text(寄送方式),       # 9 寄送方式
        safe_text(寄出日期),       # 10 寄出日期
        safe_text(託運單號),       # 11 託運單號
        infer_status(寄出日期),     # 12 寄送狀態
        safe_text(備註),           # 13 備註
        status                     # 14 資料檢核狀態
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    print(f"[Sheets] Append row success: {row}")
    return record_id

# ====== 欄位同義詞對照 ======
FIELD_ALIASES = {
    "學員姓名": ["學員姓名", "姓名", "名字"],
    "學員電話": ["學員電話", "電話", "手機"],
    "寄送地址": ["寄送地址", "地址", "住址"],
    "書籍名稱": ["書籍名稱", "書", "教材", "課本"],
    "備註": ["備註", "備考", "說明"]
}

def normalize_field_key(key: str) -> str:
    for std, aliases in FIELD_ALIASES.items():
        if key in aliases:
            return std
    return key

# ====== 解析「#寄書需求」訊息 ======
def parse_order_text(text: str):
    result = {"學員姓名": "", "學員電話": "", "寄送地址": "", "備註": "", "書籍清單": []}
    if not text:
        return result

    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]

    if not any("#寄書需求" in ln for ln in lines):
        return result
    start_idx = 0
    for i, ln in enumerate(lines):
        if "#寄書需求" in ln:
            start_idx = i + 1
            break
    lines = lines[start_idx:]

    buffer_books = []
    current_field = None
    for ln in lines:
        if re.match(r"^[-•・]\s*", ln):
            val = re.sub(r"^[-•・]\s*", "", ln)
            if val:
                buffer_books.append(val)
            continue

        m = re.match(r"^([^：:]+)\s*[：:]\s*(.*)$", ln)
        if m:
            key = normalize_field_key(m.group(1).strip())
            val = m.group(2).strip()
            current_field = None

            if key == "學員姓名":
                result["學員姓名"] = val
            elif key == "學員電話":
                result["學員電話"] = val
            elif key == "寄送地址":
                result["寄送地址"] = val
            elif key == "書籍名稱":
                if val:
                    buffer_books.append(val)
                current_field = "書籍名稱"
            elif key == "備註":
                result["備註"] = val if result["備註"] == "" else (result["備註"] + "；" + val)
            else:
                extra = f"{key}：{val}"
                result["備註"] = extra if result["備註"] == "" else (result["備註"] + "；" + extra)
            continue

        if current_field == "書籍名稱" and ln:
            buffer_books.append(ln)

    result["書籍清單"] = [bk for bk in [b.strip() for b in buffer_books] if bk]
    return result

def check_required_fields(parsed: dict):
    missing = []
    if not parsed.get("學員姓名"): missing.append("學員姓名")
    if not parsed.get("學員電話"): missing.append("學員電話")
    if not parsed.get("寄送地址"): missing.append("寄送地址")
    if not parsed.get("書籍清單"): missing.append("書籍名稱")
    return missing

# ====== 健康檢查 ======
@app.route("/", methods=["GET"])
def health():
    return "OK", 200

# ====== 測試寫入 ======
@app.get("/sheets/test")
def sheets_test():
    rid = append_order_row(
        建單人="測試用",
        學員姓名="王小明",
        學員電話="0912345678",
        寄送地址="台中市北屯區文心路四段100號",
        書籍名稱="Let's Go 5（第五版）",
        備註="這是一筆測試"
    )
    return jsonify({"ok": True, "record_id": rid})

# ====== LINE Webhook ======
@app.route("/callback", methods=["GET", "HEAD", "POST"])
def callback():
    if request.method in ("GET", "HEAD"):
        return "OK", 200

    signature = request.headers.get("X-Line-Signature")
    if not signature:
        return "OK", 200

    if parser is None or line_bot_api is None:
        abort(500)

    body = request.get_data(as_text=True)

    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        print("[WARN] Invalid signature on /callback")
        return "OK", 200

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
            text = event.message.text.strip()

            if "#寄書需求" in text:
                parsed = parse_order_text(text)
                missing = check_required_fields(parsed)
                if missing:
                    example = (
                        "請依格式補齊：\n"
                        "#寄書需求\n"
                        "學員姓名：王小明\n"
                        "學員電話：0912-345-678\n"
                        "寄送地址：台中市北屯區…\n"
                        "書籍名稱：\n- Let's Go 5（第五版）"
                    )
                    reply = f"❌ 缺少欄位：{ '、'.join(missing) }\n\n{example}"
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
                    continue

                creator = get_display_name_from_event(event)
                rids = []
                for book in parsed["書籍清單"]:
                    rid = append_order_row(
                        建單人=creator,
                        學員姓名=parsed["學員姓名"],
                        學員電話=parsed["學員電話"],
                        寄送地址=parsed["寄送地址"],
                        書籍名稱=book,
                        備註=parsed.get("備註", "")
                    )
                    rids.append(rid)

                books_preview = "\n- " + "\n- ".join(parsed["書籍清單"])
                reply = (
                    f"✅ 已建立寄書需求（{len(rids)} 筆）：\n"
                    f"學員：{parsed['學員姓名']}\n"
                    f"書名：{books_preview}\n"
                    f"紀錄ID：\n" + "\n".join(rids)
                )
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
                continue

            reply = f"你說：{text}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))

    return "OK", 200

# ====== 啟動 ======
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
