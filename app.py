# app.py
# ============================================
# 尚進《寄書＋進銷存 自動化機器人》— 18項最終版 實作
# 架構：Flask + LINE Webhook + Google Sheets +（選）Vision OCR
# 重點：建檔「上新下舊」、查詢回覆樣式、Vision用Service Account顯式建立
# 並依你要求：
# ① detect_delivery_method 只偵測便利商店；② 無便利商店但有地址 → 寄送方式=「便利帶」
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

# ====== （OCR：使用顯式憑證）======
_HAS_VISION = False
_vision_client = None
try:
    from google.cloud import vision
    from google.oauth2 import service_account as gcp_service_account
    _HAS_VISION = True
except Exception:
    _HAS_VISION = False
# ==================================

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
# Google Sheets 連線 + 表頭對應
# ============================================
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
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    return gspread.authorize(creds)

gc = _build_gspread_client()
ss = gc.open_by_key(SHEET_ID)

def _ws(name: str):
    return ss.worksheet(name)

def _get_header_map(ws):
    header = ws.row_values(1)
    hmap = {}
    for idx, title in enumerate(header, start=1):
        t = str(title).strip()
        if t:
            hmap[t] = idx
    return hmap

def _col_idx(hmap, key, default_idx):
    return hmap.get(key, default_idx)

# ============================================
# 工具與格式化
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
    data = {}
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        if ln.startswith("#"):
            continue
        if "：" in ln:
            k, v = ln.split("：", 1)
        elif ":" in ln:
            k, v = ln.split(":", 1)
        else:
            k, v = "_free_", ln
        k = k.strip()
        v = v.strip()
        data.setdefault(k, []).append(v)
    return data

# ============================================
# 寄送方式偵測（只偵測便利商店；其餘交由後續規則）
# ============================================
def detect_delivery_method(text: str) -> str | None:
    s = (text or "").lower().replace("—", "-").replace("／", "/")
    if any(k in s for k in ["7-11","7/11","7／11","7–11","711","小七"]): return "7-11"
    if "全家" in s or "family" in s: return "全家"
    if "萊爾富" in s or "hi-life" in s or "hilife" in s: return "萊爾富"
    if "ok" in s or "ok超商" in s: return "OK"
    return None   # 不偵測宅配；未命中便利商店則交由下一步處理

# ============================================
# 郵遞區號查找（前置）
# ============================================
_zip_cache = None
def _load_zipref():
    global _zip_cache
    if _zip_cache is not None:
        return _zip_cache
    try:
        ws = _ws(ZIPREF_SHEET_NAME)
        rows = ws.get_all_values()
        header = rows[0] if rows else []
        zi, ai = None, None
        for i, name in enumerate(header):
            n = str(name).strip()
            if zi is None and re.search(r"郵遞區號|郵遞|zip|ZIP", n, re.I): zi = i
            if ai is None and re.search(r"地址|路|區|鄉|鎮|村|里|段|巷|市|縣", n): ai = i
        if zi is None or ai is None:
            zi, ai = 1, 0
        pairs = []
        for r in rows[1:]:
            try:
                prefix = (r[ai] if ai < len(r) else "").strip()
                z = (r[zi] if zi < len(r) else "").strip()
                if prefix and re.fullmatch(r"\d{3}(\d{2})?", z):
                    pairs.append((prefix, z))
            except Exception:
                continue
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
# 書名比對
# ============================================
def load_book_master():
    ws = _ws(BOOK_MASTER_SHEET_NAME)
    rows = ws.get_all_values()
    if not rows:
        return []
    header = rows[0]
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
            data.append({"name": name, "aliases": aliases})
        except Exception:
            continue
    return data

def resolve_book_name(user_input: str):
    src = (user_input or "").strip()
    if not src:
        return (None, "notfound", [])
    books = load_book_master()
    exact = [b for b in books if src.lower() == b["name"].lower()]
    if exact:
        return (exact[0]["name"], "exact", None)
    for b in books:
        if any(src.lower() == a.lower() for a in b["aliases"]):
            return (b["name"], "alias", None)
    universe, reverse_map = [], {}
    for b in books:
        universe.append(b["name"]); reverse_map[b["name"]] = b["name"]
        for a in b["aliases"]:
            universe.append(a); reverse_map[a] = b["name"]
    matches = difflib.get_close_matches(src, universe, n=5, cutoff=FUZZY_THRESHOLD)
    if not matches:
        return (None, "notfound", [])
    formal = []
    for m in matches:
        fm = reverse_map.get(m)
        if fm and fm not in formal:
            formal.append(fm)
    if len(formal) == 1:
        return (formal[0], "fuzzy", None)
    return (None, "ambiguous", formal)

# ============================================
# Vision Client（顯式憑證建立，避免 ADC 錯誤）
# ============================================
def _build_vision_client():
    global _HAS_VISION, _vision_client
    if not _HAS_VISION:
        return None
    try:
        json_path = "service_account.json"
        creds = None
        if os.path.exists(json_path):
            creds = gcp_service_account.Credentials.from_service_account_file(json_path)
        else:
            sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
            if not sa_json:
                raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON for Vision client.")
            creds = gcp_service_account.Credentials.from_service_account_info(json.loads(sa_json))
        _vision_client = vision.ImageAnnotatorClient(credentials=creds)
        return _vision_client
    except Exception as e:
        app.logger.info(f"[VISION] init failed: {e}")
        _HAS_VISION = False
        return None

_vision_client = _build_vision_client()

# ============================================
# 建檔輔助
# ============================================
def _gen_next_record_id(ws, header_map):
    colA = _col_idx(header_map, "紀錄ID", 1)
    values = ws.col_values(colA)[1:]
    max_no = 0
    for v in values:
        m = re.fullmatch(r"R(\d{4})", str(v).strip())
        if m:
            n = int(m.group(1))
            if n > max_no:
                max_no = n
    return f"R{max_no+1:04d}"

def _build_insert_row(ws, data, who_display_name):
    """
    欄位：A紀錄ID B建單日期 C建單人 D學員姓名 E學員電話 F寄送地址
          G書籍名稱 H業務備註 I寄送方式 J寄出日期 K託運單號 L經手人 M寄送狀態
    """
    hmap = _get_header_map(ws)
    header_len = len(ws.row_values(1))
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

    total_cols = max(header_len, idxM)
    row = [""] * total_cols

    rid = _gen_next_record_id(ws, hmap)
    row[idxA-1] = rid
    row[idxB-1] = now_str_min()
    row[idxC-1] = who_display_name or "LINE使用者"
    row[idxD-1] = data.get("name","")
    phone = data.get("phone","")
    row[idxE-1] = f"'{phone}" if phone else ""
    address = data.get("address","")

    # 郵遞區號：僅在（未指定 or 空 or 宅配）才補；「便利帶」不補（依你目前規則）
    if WRITE_ZIP_TO_ADDRESS and (data.get("delivery") in (None, "", "宅配")) and address:
        z = lookup_zip(address)
        if z and not re.match(r"^\d{3}", address):
            address = f"{z}{address}"
    row[idxF-1] = address

    row[idxG-1] = data.get("book_formal","")
    row[idxH-1] = data.get("biz_note","")
    row[idxI-1] = data.get("delivery") or ""  # 未偵測→留白（之後可能成為「便利帶」，見解析步驟）
    row[idxJ-1] = ""
    row[idxK-1] = ""
    row[idxL-1] = ""
    row[idxM-1] = "待處理"

    return row, {"rid": rid}

# ============================================
# 解析＋指令處理
# ============================================
def _parse_new_order_text(raw_text: str):
    """
    解析 #寄書 / #寄書需求
    必填：姓名、電話、書名；地址（若非便利商店寄送）
    """
    data = parse_kv_lines(raw_text)

    # 先抓核心欄位
    name = None
    for k in list(data.keys()):
        if any(x in k for x in ["姓名","學員","收件人","名字","貴姓"]):
            name = "、".join(data.pop(k))
            break

    phone = None
    for k in list(data.keys()):
        if "電話" in k:
            for v in data.pop(k):
                p = normalize_phone(v)
                if p:
                    phone = p
                    break
            break

    address = None
    for k in list(data.keys()):
        if any(x in k for x in ["寄送地址","地址","收件地址","配送地址"]):
            address = " ".join(data.pop(k))
            address = address.replace(" ", "")
            break

    book_raw = None
    for k in list(data.keys()):
        if any(x in k for x in ["書","書名","教材","書籍名稱"]):
            book_raw = " ".join(data.pop(k)).strip()
            break

    # 合併剩餘文字以利偵測便利商店
    merged_text = "\n".join(sum(data.values(), []))
    delivery = detect_delivery_method(merged_text)

    # ② 若沒偵測到便利商店、但有地址 → 寄送方式=「便利帶」
    if not delivery and address:
        delivery = "便利帶"

    # 其他文字 → 業務備註
    others = []
    for k, arr in data.items():
        for v in arr:
            if k != "_free_":
                others.append(f"{k}：{v}")
            else:
                others.append(v)
    biz_note = " / ".join([x for x in others if x.strip()])

    # 驗證必填（若非便利商店需地址）
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
        "delivery": delivery,      # 可能是 7-11/全家/OK/萊爾富 或 便利帶 或 None
        "raw_text": raw_text
    }, errors

def _handle_new_order(event, text):
    # 取得使用者顯示名稱
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

    # 上新下舊：插入第 2 列
    ws.insert_row(row, index=2, value_input_option="USER_ENTERED")

    # 成功回覆
    resp = (
        "✅ 已成功建檔\n"
        f"紀錄ID：{meta['rid']}\n"
        f"建單日期：{now_str_min()}\n"
        f"姓名：{parsed['name']}｜電話：{parsed['phone']}\n"
        f"地址：{row[_get_header_map(ws).get('寄送地址',6)-1]}\n"
        f"書籍：{book_formal}\n"
        f"狀態：待處理"
    )
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=resp))

def _handle_query(event, text):
    q = re.sub(r"^#(查詢寄書|查寄書)\s*", "", text.strip())

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

    phone_digits = re.sub(r"\D+","", q)
    is_phone = len(phone_digits) >= 7

    results = []
    for r in rows:
        try:
            dt_str = r[idxB-1].strip()
            dt = None
            if dt_str:
                try:
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
                except Exception:
                    dt = None
            if dt and dt < since:
                continue

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

    # 新→舊排序
    results.sort(key=lambda r: r[idxB-1], reverse=True)
    results = results[:5]

    # 回覆樣式
    blocks = []
    for r in results:
        name = r[idxD-1]
        book = r[idxG-1]
        status = (r[idxM-1] or "").strip()
        outd = (r[idxJ-1] or "").strip()
        ship = (r[idxI-1] or "").strip()
        no = (r[idxK-1] or "").strip()

        if status == "已託運":
            lines = [f"📦 {name} 的 {book}"]
            if outd:
                lines.append(f"已於 {outd}")
            if ship:
                lines.append(f"由 {ship} 寄出")
            if no:
                lines.append(f"託運單號：{no}")
            blocks.append("\n".join(lines))
        else:
            blocks.append(f"📦 {name} 的 {book} {status or '待處理'}")

    msg = "\n\n".join(blocks)
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

# ============================================
# 圖片（OCR）處理：寫回單號/出貨日/經手人/狀態
# ============================================
def _download_line_image_bytes(message_id: str) -> bytes:
    content = line_bot_api.get_message_content(message_id)
    return b"".join(chunk for chunk in content.iter_content())

def _ocr_text_from_bytes(img_bytes: bytes) -> str:
    if not _vision_client:
        raise RuntimeError("Vision 用戶端未初始化（請確認 GOOGLE_SERVICE_ACCOUNT_JSON 已設定，且專案已啟用 Vision API）。")
    image = vision.Image(content=img_bytes)
    resp = _vision_client.text_detection(image=image)
    if resp.error.message:
        raise RuntimeError(resp.error.message)
    text = resp.full_text_annotation.text if resp.full_text_annotation else ""
    return text or ""

def _pair_ids_with_numbers(text: str):
    if not text:
        return [], ["未讀取到文字"]
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if LOG_OCR_RAW:
        app.logger.info(f"[OCR_RAW_OUTPUT] {repr(text[:1000])}")

    rids, nums = [], []
    for i, ln in enumerate(lines):
        for m in re.finditer(r"R\d{4}", ln):
            rids.append((m.group(), i))
        for m in re.finditer(r"\d{12}", ln):
            nums.append((m.group(), i))

    pairs, used_num, leftovers = [], set(), []
    for rid, li in rids:
        chosen, best_dist = None, 999
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

    # 上傳者名稱
    try:
        profile = line_bot_api.get_profile(event.source.user_id)
        uploader = profile.display_name or "LINE使用者"
    except Exception:
        uploader = "LINE使用者"

    # 索引：紀錄ID → row
    all_vals = ws.get_all_values()
    rows = all_vals[1:]
    id2row = {}
    for ridx, r in enumerate(rows, start=2):
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
        ws.update_cell(row_i, idxK, f"'{no}")
        ws.update_cell(row_i, idxJ, today_str())
        ws.update_cell(row_i, idxL, uploader)
        ws.update_cell(row_i, idxM, "已託運")
        updated.append((rid, no))

    if not updated:
        return "❗ 未寫入（找不到對應的紀錄ID）"

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

# 文字訊息處理
@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = (event.message.text or "").strip()
    if text.startswith("#寄書需求") or text.startswith("#寄書"):
        _handle_new_order(event, text); return
    if text.startswith("#查詢寄書") or text.startswith("#查寄書"):
        _handle_query(event, text); return
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="請使用：\n#寄書（建立寄書任務）\n#查寄書（姓名或電話）")
    )

# 圖片訊息處理（OCR）
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    try:
        app.logger.info(f"[IMG] 收到圖片 user_id={getattr(event.source,'user_id','unknown')} msg_id={event.message.id}")
        img_bytes = _download_line_image_bytes(event.message.id)
        if not _vision_client:
            msg = "❌ OCR 處理時發生錯誤：Vision 用戶端未初始化（請確認 GOOGLE_SERVICE_ACCOUNT_JSON 已設定，且專案已啟用 Vision API）。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg)); return

        text = _ocr_text_from_bytes(img_bytes)
        if LOG_OCR_RAW:
            app.logger.info(f"[OCR_TEXT]\n{text}")

        pairs, leftovers = _pair_ids_with_numbers(text)
        resp = _write_ocr_results(pairs, event)
        if leftovers:
            resp += "\n\n❗以下項目需人工檢核：\n" + "\n".join(leftovers[:10])

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=resp))
    except Exception as e:
        code = datetime.now(TZ).strftime("%Y%m%d%H%M%S")
        app.logger.exception("[OCR_ERROR]")
        msg = f"❌ OCR 錯誤（代碼 {code}）：{e}"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

# 健康檢查
@app.route("/", methods=["GET"])
def index():
    try:
        names = [ws.title for ws in ss.worksheets()]
        return "OK / Worksheets: " + ", ".join(names)
    except Exception as e:
        return f"OK / (Sheets not loaded) {e}"

# 本地執行（Railway 用 gunicorn 啟動）
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
