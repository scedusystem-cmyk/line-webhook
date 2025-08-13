# app.py  —  LINE Bot + Google Sheets 寫入（寄書任務）
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
    """回傳台北時區 yyyy-mm-dd HH:MM:SS"""
    return (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

def make_record_id():
    """SJREQ-YYYYMMDD-hhmmss"""
    ts = (datetime.datetime.utcnow() + datetime.timedelta(hours=8))
    return "SJREQ-" + ts.strftime("%Y%m%d-%H%M%S")

def infer_status(ship_date: str) -> str:
    return "已寄送完成" if ship_date and str(ship_date).strip() else "未寄出"

def normalize_phone(p: str) -> str:
    """保留數字（先簡化處理：移除空白/破折號/其他符號）"""
    return "".join(ch for ch in (p or "") if ch.isdigit())

def safe_text(x):
    return x if x is not None else ""

def get_display_name_from_event(event) -> str:
    """盡力取得使用者顯示名稱（群組/一對一皆可）"""
    try:
        if event.source.type == "group":
            return line_bot_api.get_group_member_profile(event.source.group_id, event.source.user_id).display_name
        elif event.source.type == "room":
            return line_bot_api.get_room_member_profile(event.source.room_id, event.source.user_id).display_name
        else:
            # 1:1
            return line_bot_api.get_profile(event.source.user_id).display_name
    except Exception:
        return "未知使用者"

# ====== 將一筆訂單寫入 Google Sheets（依你的欄位 14 欄） ======
def append_order_row(
    建單人, 學員姓名, 學員電話, 寄送地址,
    書籍名稱, 語別="", 寄送方式="", 寄出日期="",
    託運單號="", 備註="", 資料檢核狀態=""
):
    record_id = make_record_id()
    row = [
        record_id,                 # 1 紀錄ID
        now_tpe_str(),             # 2 建單日期
        safe_text(建單人),         # 3 建單人（LINE 名稱）
        safe_text(學員姓名),       # 4 學員姓名
        normalize_phone(學員電話), # 5 學員電話
        safe_text(寄送地址),       # 6 寄送地址（郵遞區號前三碼之後再自動化）
        safe_text(書籍名稱),       # 7 書籍名稱（V1 先填原文；日後再換主檔正式名）
        safe_text(語別),           # 8 語別
        safe_text(寄送方式),       # 9 寄送方式
        safe_text(寄出日期),       # 10 寄出日期
        safe_text(託運單號),       # 11 託運單號
        infer_status(寄出日期),     # 12 寄送狀態（有寄出日期=已寄送完成）
        safe_text(備註),           # 13 備註
        safe_text(資料檢核狀態)    # 14 資料檢核狀態
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    return record_id

# ====== 解析「#寄書需求」訊息 ======
# 支援格式（例）：
# #寄書需求
# 學員姓名：王小明
# 學員電話：0912-345-678
# 寄送地址：406 台中市北屯區文心路四段100號10樓
# 書籍名稱：
# - Let's Go 5（第五版）
# - Let's Go 6（第五版）
# 備註：請週五前寄出；雅思B.3
FIELD_KEYS = ["學員姓名", "學員電話", "寄送地址", "書籍名稱", "備註"]

def parse_order_text(text: str):
    """回傳 dict：{學員姓名, 學員電話, 寄送地址, 書籍清單(list[str]), 備註}；若缺欄位會標示"""
    result = {"學員姓名": "", "學員電話": "", "寄送地址": "", "備註": "", "書籍清單": []}
    if not text:
        return result

    # 統一換行
    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]

    # 找到 '#寄書需求' 之後的內容
    if not any("#寄書需求" in ln for ln in lines):
        return result
    start_idx = 0
    for i, ln in enumerate(lines):
        if "#寄書需求" in ln:
            start_idx = i + 1
            break
    lines = lines[start_idx:]

    # 先掃 key:value 形式
    buffer_books = []
    current_field = None
    for ln in lines:
        # 書名多行項目：允許以 '-' 或 '・' 或 '•' 開頭
        if re.match(r"^[-•・]\s*", ln):
            val = re.sub(r"^[-•・]\s*", "", ln)
            if val:
                buffer_books.append(val)
            continue

        # 一般 "欄位：值"
        m = re.match(r"^([^：:]+)\s*[：:]\s*(.*)$", ln)
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            current_field = None

            if key == "學員姓名":
                result["學員姓名"] = val
            elif key == "學員電話":
                result["學員電話"] = val
            elif key == "寄送地址":
                result["寄送地址"] = val
            elif key == "書籍名稱":
                # 可能下一行才是項目；這一行若也有值就先加
                if val:
                    buffer_books.append(val)
                current_field = "書籍名稱"
            elif key == "備註":
                result["備註"] = val if result["備註"] == "" else (result["備註"] + "；" + val)
            else:
                # 其他未知欄位全部塞備註
                extra = f"{key}：{val}"
                result["備註"] = extra if result["備註"] == "" else (result["備註"] + "；" + extra)
            continue

        # 若上一行是「書籍名稱：」且本行不是 - 開頭，就把整行當一本書
        if current_field == "書籍名稱" and ln:
            buffer_books.append(ln)

    # 若完全沒抓到書，buffer_books 可能仍空；允許之後回報缺漏
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

# ====== 測試寫入（手動驗證用） ======
@app.get("/sheets/test")
def sheets_test():
    rid = append_order_row(
        建單人="測試用",
        學員姓名="王小明",
        學員電話="0912345678",
        寄送地址="台中市北屯區文心路四段100號10樓",
        書籍名稱="Let's Go 5（第五版）",
        備註="這是一筆測試"
    )
    return jsonify({"ok": True, "record_id": rid})

# ====== LINE Webhook ======
# 接受 GET/HEAD（Verify 用）與 POST（正式事件）
@app.route("/callback", methods=["GET", "HEAD", "POST"])
def callback():
    # 1) Verify 會用 GET/HEAD 來探測
    if request.method in ("GET", "HEAD"):
        return "OK", 200

    # 2) 有些 Verify/健康檢查會發沒有簽章的 POST，直接回 200
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

            # ---- 寄書需求觸發 ----
            if "#寄書需求" in text:
                parsed = parse_order_text(text)
                missing = check_required_fields(parsed)
                if missing:
                    example = (
                        "請依格式補齊：\n"
                        "#寄書需求\n"
                        "學員姓名：王小明\n"
                        "學員電話：0912-345-678\n"
                        "寄送地址：406 台中市北屯區…\n"
                        "書籍名稱：\n- Let's Go 5（第五版）"
                    )
                    reply = f"❌ 缺少欄位：{ '、'.join(missing) }\n\n{example}"
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
                    continue

                # 寫入：一書一列
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

                summary = "、".join(parsed['學員姓名'] for _ in [0])  # 單一姓名
                books_preview = "\n- " + "\n- ".join(parsed["書籍清單"])
                reply = (
                    f"✅ 已建立寄書需求（{len(rids)} 筆）：\n"
                    f"學員：{parsed['學員姓名']}\n"
                    f"書名：{books_preview}\n"
                    f"紀錄ID：\n" + "\n".join(rids)
                )
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
                continue

            # ---- 其他文字：原樣回覆 ----
            reply = f"你說：{text}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))

    return "OK", 200

# ====== 啟動 ======
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
