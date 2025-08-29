# app.py
# ============================================
# 尚進《寄書＋進銷存 自動化機器人》— 18項最終版 實作
# 架構：Flask + LINE Webhook + Google Sheets +（選）Vision OCR
# 依據你提供的最新欄位（A~M）與流程撰寫。已標示功能模組標籤。
# ============================================

from flask import Flask, request, abort
import os, re, io, json, difflib, logging
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

# ====== （OCR 選用）======
try:
    from google.cloud import vision
    _HAS_VISION = True
except Exception:
    _HAS_VISION = False
# ========================

# ============================================
# 基本設定
# ============================================
app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# ---- 環境變數 ----
SHEET_ID = os.getenv("SHEET_ID", "").strip()

MAIN_SHEET_NAME = os.getenv("MAIN_SHEET_NAME", "寄書任務")
BOOK_MASTER_SHEET_NAME = os.getenv("BOOK_MASTER_SHEET_NAME", "書目主檔")
ZIPREF_SHEET_NAME = os.getenv("ZIPREF_SHEET_NAME", "郵遞區號參照表")
STOCK_IN_SHEET_NAME = os.getenv("STOCK_IN_SHEET_NAME", "入庫明細")
HISTORY_SHEET_NAME = os.getenv("HISTORY_SHEET_NAME", "歷史紀錄")

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
# 功能 D：Google Sheets 連線 + 表頭對應
# ============================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def _build_gspread_client():
    # 兩種取得 service account 憑證的方法：檔案或環境變數
    json_path = "service_account.json"
    if os.path.exists(json_path):
        creds = Credentials.from_service_account_file(json_path, scopes=SCOPES)
        return gspread.authorize(creds)
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_json:
        raise RuntimeError("Missing service account credentials. Provide service_account.json OR env GOOGLE_SERVICE_ACCOUNT_JSON.")
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    return gspread.authorize(creds)

gc = _build_gspread_client()
ss = gc.open_by_key(SHEET_ID)

def _ws(name: str):
    return ss.worksheet(name)

def _get_header_map(ws):
    """回傳 {欄名: index(1-based)}；自動對照目前表頭，避免欄位順序差異。"""
    header = ws.row_values(1)
    hmap = {}
    for idx, title in enumerate(header, start=1):
        t = str(title).strip()
        if t:
            hmap[t] = idx
    return hmap

def _col_idx(hmap, key, default_idx):
    """依表頭名稱找欄位，找不到就用預設序（以目前規格為準）。"""
    return hmap.get(key, default_idx)

# ============================================
# 功能 F：工具與格式化
# ============================================
def now_str_min():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M")

def today_str():
    return datetime.now(TZ).strftime("%Y-%m-%d")

def normalize_phone(s: str) -> str | None:
    digits = re.sub(r"\D+", "", s or "")
    if len(digits) == 10 and digits.startswith("09"):
        return digits
    return None

def parse_kv_lines(text: str):
    """
    將多行文字用「：」、「:」等切出 key/value，回傳 dict（不做強制鍵名）。
    """
    data = {}
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        if ln.startswith("#"):  # 指令行略過
            continue
        if "：" in ln:
            k, v = ln.split("：", 1)
        elif ":" in ln:
            k, v = ln.split(":", 1)
        else:
            # 無法辨識的行保留，後面當作「業務備註」候選
            k, v = "_free_", ln
        k = k.strip()
        v = v.strip()
        data.setdefault(k, []).append(v)
    return data

def detect_delivery_method(text: str) -> str | None:
    s = (text or "").lower().replace("—", "-").replace("／", "/")
    # 支援模糊關鍵字
    if any(k in s for k in ["7-11", "7/11", "7／11", "7–11", "711", "小七"]):
        return "7-11"
    if "全家" in s or "family" in s:
        return "全家"
    if "萊爾富" in s or "hi-life" in s or "hilife" in s:
        return "萊爾富"
    if "ok" in s or "ok超商" in s:
        return "OK"
    if "宅配" in s or "黑貓" in s or "宅急便" in s:
        return "宅配"
    return None  # 未偵測→留白

# ============================================
# 功能 E：郵遞區號查找（前置）
# ============================================
_zip_cache = None
def _load_zipref():
    """讀取郵遞區號表，做簡單的「最長前綴」匹配。容錯多種欄位命名。"""
    global _zip_cache
    if _zip_cache is not None:
        return _zip_cache
    try:
        ws = _ws(ZIPREF_SHEET_NAME)
        rows = ws.get_all_values()
        header = rows[0] if rows else []
        # 嘗試找出「郵遞區號」欄與「地址/區域」欄
        # 盡量容錯：例如 ["郵遞區號","縣市區…"] 或倒過來
        zi, ai = None, None
        for i, name in enumerate(header):
            n = str(name).strip()
            if zi is None and re.search(r"郵遞區號|郵遞|zip|ZIP", n, re.I):
                zi = i
            if ai is None and re.search(r"地址|路|區|鄉|鎮|村|里|段|巷|市|縣", n):
                ai = i
        # 若無表頭線索，嘗試以第一欄為地址、第二欄為郵遞區號
        if zi is None or ai is None:
            zi = 1
            ai = 0
        pairs = []
        for r in rows[1:]:
            try:
                prefix = (r[ai] if ai < len(r) else "").strip()
                z = (r[zi] if zi < len(r) else "").strip()
                if prefix and re.fullmatch(r"\d{3}(\d{2})?", z):
                    pairs.append((prefix, z))
            except Exception:
                continue
        # 依 prefix 長度排序（長的優先）
        pairs.sort(key=lambda x: len(x[0]), reverse=True)
        _zip_cache = pairs
        return _zip_cache
    except Exception as e:
        app.logger.info(f"[ZIPREF] Load failed: {e}")
        _zip_cache = []
        return _zip_cache

def lookup_zip(address: str) -> str | None:
    if not address:
        return None
    pairs = _load_zipref()
    a = address.strip()
    for prefix, z in pairs:
        if a.startswith(prefix):
            return z
    return None

# ============================================
# 功能 B：書名比對（正式名 / 別名 / 模糊）
# ============================================
def load_book_master():
    ws = _ws(BOOK_MASTER_SHEET_NAME)
    rows = ws.get_all_values()
    if not rows:
        return []
    header = rows[0]
    # 主要欄位推測：A=是否啟用、B=書籍名稱、K=模糊比對書名
    use_idx = 0
    name_idx = 1
    alias_idx = None
    for i, col in enumerate(header):
        t = str(col).strip()
        if re.search(r"模糊|別名|比對", t):
            alias_idx = i
    data = []
    for r in rows[1:]:
        try:
            enabled = str(r[use_idx]).strip()
            if enabled != "使用中":
                continue
            name = (r[name_idx] if name_idx < len(r) else "").strip()
            alias_raw = (r[alias_idx] if alias_idx is not None and alias_idx < len(r) else "").strip()
            aliases = []
            if alias_raw:
                aliases = re.split(r"[、,\s\|／/]+", alias_raw)
                aliases = [a.strip() for a in aliases if a.strip()]
            data.append({
                "name": name,
                "aliases": aliases
            })
        except Exception:
            continue
    return data

def resolve_book_name(user_input: str):
    """
    回傳 (正式書名, 來源型態)；來源型態：exact/alias/fuzzy
    若無法唯一決定，回傳 (None, 'ambiguous' or 'notfound', 候選清單)
    """
    src = (user_input or "").strip()
    if not src:
        return (None, "notfound", [])
    books = load_book_master()
    # 1) 完全比對（正式名 or 別名）
    exact = [b for b in books if src.lower() == b["name"].lower()]
    if exact:
        return (exact[0]["name"], "exact", None)
    for b in books:
        if any(src.lower() == a.lower() for a in b["aliases"]):
            return (b["name"], "alias", None)
    # 2) 模糊比對（對 正式名 + 別名）
    cand = []
    universe = []
    reverse_map = {}
    for b in books:
        universe.append(b["name"])
        reverse_map[b["name"]] = b["name"]
        for a in b["aliases"]:
            universe.append(a)
            reverse_map[a] = b["name"]
    matches = difflib.get_close_matches(src, universe, n=5, cutoff=FUZZY_THRESHOLD)
    if not matches:
        return (None, "notfound", [])
    # 映射回正式名並去重
    formal = []
    for m in matches:
        fm = reverse_map.get(m)
        if fm and fm not in formal:
            formal.append(fm)
    if len(formal) == 1:
        return (formal[0], "fuzzy", None)
    # 多筆候選，請使用者更明確
    return (None, "ambiguous", formal)

# ============================================
# 功能 A：文字訊息處理（#寄書 / #寄書需求、#查詢寄書 / #查寄書）
# ============================================
def _gen_next_record_id(ws, header_map):
    colA = _col_idx(header_map, "紀錄ID", 1)
    values = ws.col_values(colA)[1:]  # 跳過表頭
    max_no = 0
    for v in values:
        m = re.fullmatch(r"R(\d{4})", str(v).strip())
        if m:
            n = int(m.group(1))
            if n > max_no:
                max_no = n
    nxt = max_no + 1
    return f"R{nxt:04d}"

def _build_insert_row(ws, data, who_display_name):
    """
    data 需包含：name, phone, address, book_formal, raw_text, delivery
    依照目前表頭（A~M）回傳可 append 的列表（USER_ENTERED）。
    欄位定義（你最新規格）：
    A紀錄ID B建單日期 C建單人 D學員姓名 E學員電話 F寄送地址
    G書籍名稱 H業務備註 I寄送方式 J寄出日期 K託運單號 L經手人 M寄送狀態
    """
    hmap = _get_header_map(ws)
    # 預設索引
    idxA = _col_idx(hmap, "紀錄ID", 1)
    idxB = _col_idx(hmap, "建單日期", 2)
    idxC = _col_idx(hmap, "建單人", 3)
    idxD = _col_idx(hmap, "學員姓名", 4)
    idxE = _col_idx(hmap, "學員電話", 5)
    idxF = _col_idx(hmap, "寄送地址", 6)
    idxG = _col_idx(hmap, "書籍名稱", 7)
    idxH = _col_idx(hmap, "業務備註", 8)
    idxI = _col_idx(hmap, "寄送方式", 9)
    idxJ = _col_idx(hmap, "寄出日期", 10)
    idxK = _col_idx(hmap, "託運單號", 11)
    idxL = _col_idx(hmap, "經手人", 12)
    idxM = _col_idx(hmap, "寄送狀態", 13)

    total_cols = max(idxA,idxB,idxC,idxD,idxE,idxF,idxG,idxH,idxI,idxJ,idxK,idxL,idxM)
    row = [""] * total_cols

    rid = _gen_next_record_id(ws, hmap)
    row[idxA-1] = rid
    row[idxB-1] = now_str_min()
    row[idxC-1] = who_display_name or "LINE使用者"
    row[idxD-1] = data.get("name","")
    row[idxE-1] = data.get("phone","")

    address = data.get("address","")
    # 宅配才補郵遞區號；超商可不強制門牌
    if WRITE_ZIP_TO_ADDRESS and (data.get("delivery") in (None, "", "宅配")) and address:
        z = lookup_zip(address)
        if z and not re.match(r"^\d{3}", address):
            address = f"{z}{address}"
    row[idxF-1] = address

    row[idxG-1] = data.get("book_formal","")
    row[idxH-1] = data.get("biz_note","")
    row[idxI-1] = data.get("delivery") or ""  # 未偵測→留白
    row[idxJ-1] = ""  # 出貨時填
    row[idxK-1] = ""  # 單號
    row[idxL-1] = ""  # 經手人（OCR 時填）
    row[idxM-1] = "待處理"

    return row, {"rid": rid}

def _parse_new_order_text(raw_text: str):
    """
    解析 #寄書 / #寄書需求 文字，輸出 dict 與錯誤清單
    必填：姓名、電話、書名；地址（若非超商）
    非姓名/電話/地址/書名/寄送方式 → 業務備註
    """
    data = parse_kv_lines(raw_text)
    merged_text = "\n".join(sum(data.values(), []))  # 用於寄送方式偵測

    # 1) 姓名（常見鍵包含：姓名/學員姓名/收件人）
    name = None
    for k in list(data.keys()):
        if any(x in k for x in ["姓名","學員","收件人","名字","貴姓"]):
            name = "、".join(data.pop(k))
            break

    # 2) 電話
    phone = None
    for k in list(data.keys()):
        if "電話" in k:
            for v in data.pop(k):
                p = normalize_phone(v)
                if p:
                    phone = p
                    break
            break

    # 3) 地址
    address = None
    for k in list(data.keys()):
        if any(x in k for x in ["寄送地址","地址","收件地址","配送地址"]):
            address = " ".join(data.pop(k))
            address = address.replace(" ", "")
            break

    # 4) 書名
    book_raw = None
    for k in list(data.keys()):
        if any(x in k for x in ["書","書名","教材","書籍名稱"]):
            book_raw = " ".join(data.pop(k)).strip()
            break

    # 5) 寄送方式（偵測）
    delivery = detect_delivery_method(merged_text)

    # 6) 其餘 → 業務備註
    others = []
    for k, arr in data.items():
        for v in arr:
            if k != "_free_":
                others.append(f"{k}：{v}")
            else:
                others.append(v)
    biz_note = " / ".join([x for x in others if x.strip()])

    # 驗證必填（若非超商需地址）
    errors = []
    if not name: errors.append("缺少【姓名】")
    if not phone: errors.append("電話格式錯誤（需 09 開頭 10 碼）")
    if not book_raw: errors.append("缺少【書名】")
    if delivery not in ["7-11","全家","OK","萊爾富"] and not address:
        errors.append("缺少【寄送地址】（非超商必填）")

    return {
        "name": name,
        "phone": phone,
        "address": address,
        "book_raw": book_raw,
        "biz_note": biz_note,
        "delivery": delivery,  # 未偵測→None
        "raw_text": raw_text
    }, errors

def _handle_new_order(event, text):
    user_name = getattr(event.source, "user_id", "LINE使用者")
    # 以 profile 取顯示名稱（若取用者資訊權限已開）
    try:
        profile = line_bot_api.get_profile(event.source.user_id)
        display_name = profile.display_name
    except Exception:
        display_name = "LINE使用者"

    parsed, errs = _parse_new_order_text(text)
    if errs:
        msg = "❌ 建檔失敗：\n- " + "\n- ".join(errs)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        return

    # 書名解析 → 正式名
    book_formal, kind, extra = resolve_book_name(parsed["book_raw"])
    if not book_formal:
        if kind == "ambiguous" and extra:
            msg = "❗ 書名有多個可能，請更明確：\n" + "、".join(extra[:10])
        else:
            msg = "❌ 找不到對應的書名，請確認或補充關鍵字。"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        return

    parsed["book_formal"] = book_formal

    ws = _ws(MAIN_SHEET_NAME)
    row, meta = _build_insert_row(ws, parsed, display_name)
    ws.append_row(row, value_input_option="USER_ENTERED")

    # 成功回覆
    resp = (
        "✅ 已成功建檔\n"
        f"紀錄ID：{meta['rid']}\n"
        f"建單日期：{now_str_min()}\n"
        f"姓名：{parsed['name']}｜電話：{parsed['phone']}\n"
        f"地址：{(row[_get_header_map(ws).get('寄送地址',6)-1])}\n"
        f"書籍：{book_formal}\n"
        f"狀態：待處理"
    )
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=resp))

def _handle_query(event, text):
    # 取得查詢字串
    q = text.strip()
    # 去掉指令字頭
    q = re.sub(r"^#(查詢寄書|查寄書)\s*", "", q)

    ws = _ws(MAIN_SHEET_NAME)
    h = _get_header_map(ws)
    idxB = _col_idx(h, "建單日期", 2)
    idxD = _col_idx(h, "學員姓名", 4)
    idxE = _col_idx(h, "學員電話", 5)
    idxG = _col_idx(h, "書籍名稱", 7)
    idxI = _col_idx(h, "寄送方式", 9)
    idxJ = _col_idx(h, "寄出日期", 10)
    idxK = _col_idx(h, "託運單號", 11)
    idxM = _col_idx(h, "寄送狀態", 13)

    rows = ws.get_all_values()[1:]
    since = datetime.now(TZ) - timedelta(days=QUERY_DAYS)

    # 判斷姓名 or 電話
    phone_digits = re.sub(r"\D+","", q)
    is_phone = len(phone_digits) >= 7

    results = []
    for r in rows:
        try:
            # 時間窗
            dt_str = r[idxB-1].strip()
            dt = None
            if dt_str:
                # 兼容「YYYY-MM-DD HH:mm」或其他格式
                try:
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
                except Exception:
                    dt = None
            if dt and dt < since:
                continue
            # 篩選
            if is_phone:
                cand = re.sub(r"\D+","", r[idxE-1])
                if len(cand) >= PHONE_SUFFIX_MATCH and phone_digits[-PHONE_SUFFIX_MATCH:] == cand[-PHONE_SUFFIX_MATCH:]:
                    results.append(r)
            else:
                if q and q in r[idxD-1]:
                    results.append(r)
        except Exception:
            continue

    if not results:
        msg = "❌ 查無近 30 天內的寄書紀錄，請確認姓名或電話是否正確。"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        return

    # 新→舊排序（以建單日期字串排序）
    def sort_key(r):
        s = r[idxB-1]
        return s
    results.sort(key=sort_key, reverse=True)
    results = results[:5]

    lines = []
    for r in results:
        bd = r[idxB-1]
        book = r[idxG-1]
        ship = r[idxI-1]
        outd = r[idxJ-1]
        no = r[idxK-1]
        st = r[idxM-1]
        lines.append(f"{bd}｜{book}｜{ship or '-'}｜{outd or '-'}｜{no or '-'}｜{st or '-'}")

    msg = "查詢結果（新→舊，最多 5 筆）：\n建單日｜書籍｜寄送方式｜寄出日｜單號｜狀態\n" + "\n".join(lines)
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

# ============================================
# 功能 C：OCR 圖片處理（出貨單→單號寫回/狀態更新）
# ============================================
def _download_line_image_bytes(message_id: str) -> bytes:
    content = line_bot_api.get_message_content(message_id)
    return b"".join(chunk for chunk in content.iter_content())

def _ocr_text_from_bytes(img_bytes: bytes) -> str:
    if not _HAS_VISION:
        return ""
    try:
        client = vision.ImageAnnotatorClient()
        image = vision.Image(content=img_bytes)
        resp = client.text_detection(image=image)
        if resp.error.message:
            raise RuntimeError(resp.error.message)
        text = resp.full_text_annotation.text if resp.full_text_annotation else ""
        return text or ""
    except Exception as e:
        raise

def _pair_ids_with_numbers(text: str):
    """
    從 OCR 文本擷取 Rxxxx 與 12碼單號，嘗試「就近配對」；不複雜化。
    回傳：(pairs, leftovers)
    pairs: [(rid, no12)]
    leftovers: 訊息列表（需人工）
    """
    if not text:
        return [], ["未讀取到文字"]
    # 行切分
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if LOG_OCR_RAW:
        app.logger.info(f"[OCR_RAW_OUTPUT] {repr(text[:500])}")

    rids = []
    nums = []
    for i, ln in enumerate(lines):
        for m in re.finditer(r"R\d{4}", ln):
            rids.append((m.group(), i))
        for m in re.finditer(r"\d{12}", ln):
            nums.append((m.group(), i))

    pairs = []
    used_num = set()
    leftovers = []

    # 簡單就近：每個 rid 找同一行或下一行最近的 12 碼
    for rid, li in rids:
        chosen = None
        best_dist = 999
        for no, lj in nums:
            if (no, lj) in used_num:
                continue
            d = abs(lj - li)
            if d < best_dist:
                best_dist = d
                chosen = (no, lj)
        if chosen:
            pairs.append((rid, chosen[0]))
            used_num.add(chosen)
        else:
            leftovers.append(f"{rid}｜未找到 12 碼單號")

    # 多出的號碼
    for no, lj in nums:
        if (no, lj) not in used_num:
            leftovers.append(f"未配對單號：{no}")

    return pairs, leftovers

def _write_ocr_results(pairs, event):
    if not pairs:
        return "❗ 未寫入任何資料（未找到配對）"
    ws = _ws(MAIN_SHEET_NAME)
    h = _get_header_map(ws)
    idxA = _col_idx(h, "紀錄ID", 1)
    idxJ = _col_idx(h, "寄出日期", 10)
    idxK = _col_idx(h, "託運單號", 11)
    idxL = _col_idx(h, "經手人", 12)
    idxM = _col_idx(h, "寄送狀態", 13)

    # 取得上傳者名稱
    try:
        profile = line_bot_api.get_profile(event.source.user_id)
        uploader = profile.display_name or "LINE使用者"
    except Exception:
        uploader = "LINE使用者"

    # 建立索引：紀錄ID → row
    all_vals = ws.get_all_values()
    rows = all_vals[1:]
    id2row = {}
    for ridx, r in enumerate(rows, start=2):  # 2 = row index in sheet (含表頭)
        try:
            rid = r[idxA-1].strip()
            if re.fullmatch(r"R\d{4}", rid):
                id2row[rid] = ridx
        except Exception:
            continue

    updated = []
    for rid, no in pairs:
        row_i = id2row.get(rid)
        if not row_i:
            continue
        # 寫入：K單號、J出貨日=今日、L經手人、M狀態=已託運
        ws.update_cell(row_i, idxK, no)
        ws.update_cell(row_i, idxJ, today_str())
        ws.update_cell(row_i, idxL, uploader)
        ws.update_cell(row_i, idxM, "已託運")
        updated.append((rid, no))

    if not updated:
        return "❗ 未寫入（找不到對應的紀錄ID）"

    # 成功訊息
    lines = [f"{rid} → {no}" for rid, no in updated]
    return "✅ 已更新：{} 筆\n{}".format(len(updated), "\n".join(lines))

# ============================================
# LINE Webhook
# ============================================
@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)
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
    text = (event.message.text or "").strip()

    # 指令：#寄書 / #寄書需求
    if text.startswith("#寄書需求") or text.startswith("#寄書"):
        _handle_new_order(event, text)
        return

    # 指令：#查詢寄書 / #查寄書
    if text.startswith("#查詢寄書") or text.startswith("#查寄書"):
        _handle_query(event, text)
        return

    # 其他文字
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="請使用：\n#寄書（建立寄書任務）\n#查詢寄書（姓名或電話）")
    )

# =========================
# 圖片訊息處理（OCR）
# =========================
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    try:
        app.logger.info(f"[IMG] 收到圖片 user_id={getattr(event.source,'user_id','unknown')} msg_id={event.message.id}")
        img_bytes = _download_line_image_bytes(event.message.id)
        if not _HAS_VISION:
            # Vision 未啟用
            msg = "❌ OCR 處理時發生錯誤：Vision API 未啟用（請確認服務已開通與金鑰設定）。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
            return

        text = _ocr_text_from_bytes(img_bytes)
        if LOG_OCR_RAW:
            app.logger.info(f"[OCR_TEXT]\n{text}")

        pairs, leftovers = _pair_ids_with_numbers(text)
        resp = _write_ocr_results(pairs, event)

        # 附帶需人工檢核
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

# ============================================
# 本地執行（Railway 用 gunicorn 啟動）
# ============================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
