# app.py
# ============================================
# 《寄書＋進銷存 自動化機器人》— 完整版（白名單／只回覆指令／OCR門檻／入庫支援負數／多書展開／取消寄書修補）
# 架構：Flask + LINE Webhook + Google Sheets +（選）Vision OCR
#
# 指令只處理以下：
#   #我的ID、#寄書、#查寄書、#取消寄書、#刪除寄書、#刪除出書、#取消出書、#出書、#買書、#入庫
# 其他文字／圖片：一律不處理、不回覆。
#
# 模組索引（完整 PY 原則）：
# - 功能 A：白名單驗證 + 候選名單記錄
# - 功能 B：寄書建立（#寄書；地址自動補3碼郵遞區號；★★多本書同ID展開多列）
# - 功能 C：查寄書（電話後碼模糊比對；★★同一ID合併顯示多本）
# - 功能 D：取消/刪除寄書（★★同一ID全部刪除；需建單人；※排序與已刪除修補）
# - 功能 E：出書 OCR 啟用（#出書 開啟10分鐘會話，未開啟不回覆圖片）
# - 功能 F：OCR 解析 + 寫回（R#### ↔ 12碼單號、寄出日期、經手人、狀態）
# - 功能 G：刪除/取消出書（撤銷已託運欄位，狀態回待處理）
# - 功能 H：入庫（#買書/#入庫：書名辨識→OK確認→寫入；支援負數＝盤點調整）
# - 功能 I：#我的ID（不受白名單限制）
# - 功能 J：只回覆指定指令／其他一律不回覆
# ============================================

from flask import Flask, request, abort
import os, re, io, json, difflib, logging, time
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

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# ---- 環境變數 ----
SHEET_ID = os.getenv("SHEET_ID", "").strip()

MAIN_SHEET_NAME = os.getenv("MAIN_SHEET_NAME", "寄書任務")
BOOK_MASTER_SHEET_NAME = os.getenv("BOOK_MASTER_SHEET_NAME", "書目主檔")
ZIPREF_SHEET_NAME = os.getenv("ZIPREF_SHEET_NAME", "郵遞區號參照表")
STOCK_IN_SHEET_NAME = os.getenv("STOCK_IN_SHEET_NAME", "入庫明細")
HISTORY_SHEET_NAME = os.getenv("HISTORY_SHEET_NAME", "歷史紀錄")

# === 白名單設定（功能 A）===
WHITELIST_SHEET_NAME = os.getenv("WHITELIST_SHEET_NAME", "白名單")
CANDIDATE_SHEET_NAME = os.getenv("CANDIDATE_SHEET_NAME", "候選名單")
# WHITELIST_MODE: off | log | enforce
WHITELIST_MODE = os.getenv("WHITELIST_MODE", "enforce").strip().lower()
ADMIN_USER_IDS = {x.strip() for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()}
_WHITELIST_CACHE = {"ts": 0.0, "set": set()}
_WHITELIST_TTL = 300  # 秒

FUZZY_THRESHOLD = float(os.getenv("FUZZY_THRESHOLD", "0.6"))
QUERY_DAYS = int(os.getenv("QUERY_DAYS", "30"))
PHONE_SUFFIX_MATCH = int(os.getenv("PHONE_SUFFIX_MATCH", "9"))
WRITE_ZIP_TO_ADDRESS = os.getenv("WRITE_ZIP_TO_ADDRESS", "true").lower() == "true"
LOG_OCR_RAW = os.getenv("LOG_OCR_RAW", "true").lower() == "true"
OCR_SESSION_TTL_MIN = int(os.getenv("OCR_SESSION_TTL_MIN", "10"))

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

def _get_or_create_ws(name: str, headers: list[str]):
    """若工作表不存在則建立，並補上表頭"""
    try:
        ws = ss.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=name, rows=200, cols=max(10, len(headers)))
        if headers:
            ws.update(f"A1:{chr(64+len(headers))}1", [headers])
    return ws

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
# 功能 A：白名單（user_id 驗證 + 候選名單記錄）
# ============================================
def _truthy(v) -> bool:
    s = str(v).strip().lower()
    return s in ("1","true","yes","y","t","啟用","是","enabled","on")

def _load_whitelist(force: bool = False) -> set[str]:
    """回傳 enabled 的 user_id set，快取 5 分鐘"""
    now = time.time()
    if (not force) and _WHITELIST_CACHE["set"] and (now - _WHITELIST_CACHE["ts"] < _WHITELIST_TTL):
        return _WHITELIST_CACHE["set"]
    ws = _get_or_create_ws(WHITELIST_SHEET_NAME, ["user_id","name","enabled"])
    rows = ws.get_all_records()
    enabled = {str(r.get("user_id","")).strip() for r in rows if str(r.get("user_id","")).strip() and _truthy(r.get("enabled", "1"))}
    _WHITELIST_CACHE["set"] = enabled
    _WHITELIST_CACHE["ts"] = now
    return enabled

def _log_candidate(user_id: str, name: str):
    """自動記錄到候選名單（若已存在只更新 last_seen）"""
    try:
        ws = _get_or_create_ws(CANDIDATE_SHEET_NAME, ["user_id","name","first_seen","last_seen"])
        all_vals = ws.get_all_values()
        h = _get_header_map(ws)
        idx_uid = _col_idx(h, "user_id", 1)
        idx_name = _col_idx(h, "name", 2)
        idx_first = _col_idx(h, "first_seen", 3)
        idx_last = _col_idx(h, "last_seen", 4)

        exists_row = None
        for i, r in enumerate(all_vals[1:], start=2):
            if (len(r) >= idx_uid) and r[idx_uid-1] == user_id:
                exists_row = i
                break

        now_s = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
        if exists_row:
            if name:
                try:
                    ws.update_cell(exists_row, idx_name, name)
                except Exception:
                    pass
            ws.update_cell(exists_row, idx_last, now_s)
        else:
            ws.append_row([user_id, name, now_s, now_s], value_input_option="USER_ENTERED")
    except Exception as e:
        app.logger.info(f"[CANDIDATE] log failed: {e}")

def _ensure_authorized(event, scope: str = "*") -> bool:
    """根據 WHITELIST_MODE 判斷是否放行；未授權時回覆說明並附使用者ID"""
    try:
        uid = getattr(event.source, "user_id", "")
        profile = line_bot_api.get_profile(uid)
        display_name = profile.display_name or "LINE使用者"
    except Exception:
        uid = getattr(event.source, "user_id", "")
        display_name = "LINE使用者"

    # 候選名單永遠記錄
    if uid:
        _log_candidate(uid, display_name)

    if uid in ADMIN_USER_IDS:
        return True
    if WHITELIST_MODE in ("off", "log"):
        return True  # 允許

    allowed = _load_whitelist()
    if uid in allowed:
        return True

    # 未授權 → 文字才提示；圖片不回覆
    if scope == "text":
        msg = f"❌ 尚未授權使用。\n請將此 ID 提供給管理員開通：\n{uid}\n\n（提示：傳「#我的ID」也能取得這串 ID）"
        try:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        except Exception:
            pass
    return False

# ============================================
# 工具與格式化
# ============================================
def now_str_min():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M")

def today_str():
    return datetime.now(TZ).strftime("%Y-%m-%d")

def normalize_phone(s: str):
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

def col_to_letter(col: int) -> str:
    s = ""
    while col > 0:
        col, r = divmod(col - 1, 26)
        s = chr(r + 65) + s
    return s

# ============================================
# 寄送方式偵測（只偵測便利商店；其餘交由後續規則）
# ============================================
def detect_delivery_method(text: str):
    s = (text or "").lower().replace("—", "-").replace("／", "/")
    if any(k in s for k in ["7-11","7/11","7／11","7–11","711","小七"]): return "7-11"
    if "全家" in s or "family" in s: return "全家"
    if "萊爾富" in s or "hi-life" in s or "hilife" in s: return "萊爾富"
    if "ok" in s or "ok超商" in s: return "OK"
    return None

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

def lookup_zip(address: str):
    if not address:
        return None
    pairs = _load_zipref()  # [("中正區","100"), ...]
    a = address.strip()
    for prefix, z in pairs:
        if prefix in a:
            return z
    return None

# ============================================
# 書名比對（共用）
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
    # exact
    exact = [b for b in books if src.lower() == b["name"].lower()]
    if exact:
        return (exact[0]["name"], "exact", None)
    # alias match
    for b in books:
        if any(src.lower() == a.lower() for a in b["aliases"]):
            return (b["name"], "alias", None)
    # fuzzy universe
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
# Vision Client（顯式憑證建立）
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
# 建檔輔助（寄書）
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

def _build_insert_row(ws, data, who_display_name, *, force_rid=None, force_book=None):
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

    if force_rid:
        rid = force_rid
    else:
        rid = _gen_next_record_id(ws, hmap)

    row[idxA-1] = rid
    row[idxB-1] = now_str_min()
    row[idxC-1] = who_display_name or "LINE使用者"
    row[idxD-1] = data.get("name","")
    phone = data.get("phone","")
    row[idxE-1] = f"'{phone}" if phone else ""
    address = data.get("address","")

    # ✅ 一律嘗試補郵遞區號（若能對上且目前沒3碼開頭）
    if WRITE_ZIP_TO_ADDRESS and address:
        z = lookup_zip(address)
        if z and not re.match(r"^\d{3}", address):
            address = f"{z}{address}"
    row[idxF-1] = address

    # ★★ 允許覆寫書名（多本展開）
    row[idxG-1] = force_book if force_book else data.get("book_formal","")
    row[idxH-1] = data.get("biz_note","")
    row[idxI-1] = data.get("delivery") or ""
    row[idxJ-1] = ""
    row[idxK-1] = ""
    row[idxL-1] = ""
    row[idxM-1] = "待處理"

    return row, {"rid": rid}

# ★ 插入第2列不繼承格式
def _insert_row_values_no_inherit(ws, row_values, index=2):
    ss.batch_update({
        "requests": [{
            "insertDimension": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "ROWS",
                    "startIndex": index - 1,
                    "endIndex": index
                },
                "inheritFromBefore": False
            }
        }]
    })
    header_len = len(ws.row_values(1)) or 13
    last_col = max(header_len, len(row_values))
    if len(row_values) < last_col:
        row_values = row_values + [""] * (last_col - len(row_values))
    rng = f"A{index}:{col_to_letter(last_col)}{index}"
    ws.update(rng, [row_values], value_input_option="USER_ENTERED")

# ============================================
# 功能 B：解析＋建立寄書（#寄書）
# （★★ 多本書支援：同一ID展開多列；寄送方式也檢查地址）
# ============================================
_BOOK_SPLIT_RE = re.compile(r"[、，,／/\s\t]+")

def _parse_new_order_text(raw_text: str):
    data = parse_kv_lines(raw_text)

    # 1) 姓名
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

    # 4) 書名（先整串）
    book_raw = None
    for k in list(data.keys()):
        if any(x in k for x in ["書","書名","教材","書籍名稱"]):
            book_raw = " ".join(data.pop(k)).strip()
            break

    # 合併剩餘文字以利偵測便利商店
    merged_text = "\n".join(sum(data.values(), []))

    # ★★ 寄送方式檢測擴大到「剩餘文字 + 地址」
    delivery = detect_delivery_method(merged_text + " " + (address or ""))

    # 若未偵測到便利商店、但有地址 → 寄送方式=「便利帶」
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

    # 驗證（若非便利商店需地址）
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
        "delivery": delivery,
        "raw_text": raw_text
    }, errors

def _handle_new_order(event, text):
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

    # ★★ 多本書切割 + 個別解析
    raw_list = [s for s in _BOOK_SPLIT_RE.split(parsed["book_raw"] or "") if s.strip()]
    if not raw_list:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 建檔失敗：未讀到有效書名"))
        return

    formal_list = []
    amb_msgs, nf_msgs = [], []
    for token in raw_list:
        bk, kind, extra = resolve_book_name(token)
        if not bk:
            if kind == "ambiguous" and extra:
                amb_msgs.append(f"「{token}」可能是：{ '、'.join(extra[:10]) }")
            else:
                nf_msgs.append(f"找不到書名：{token}")
        else:
            formal_list.append(bk)

    if amb_msgs:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❗ 書名不夠明確：\n" + "\n".join(amb_msgs)))
        return
    if nf_msgs:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 找不到書名：\n" + "\n".join(nf_msgs)))
        return

    # 取得表／產生同一組 RID
    ws = _ws(MAIN_SHEET_NAME)
    rid = _gen_next_record_id(ws, _get_header_map(ws))

    # 逐本書展開為多列（同一 RID）
    # 為維持輸入順序，連續插入第2列會造成反序；因此倒序插入即可保持最終自上而下=原順序。
    for bk in reversed(formal_list):
        row, _ = _build_insert_row(ws, parsed, display_name, force_rid=rid, force_book=bk)
        _insert_row_values_no_inherit(ws, row, index=2)

    resp = (
        "✅ 已成功建檔\n"
        f"紀錄ID：{rid}\n"
        f"建單日期：{now_str_min()}\n"
        f"姓名：{parsed['name']}｜電話：{parsed['phone']}\n"
        f"地址：{row[_get_header_map(ws).get('寄送地址',6)-1]}\n"
        f"書籍：{'、'.join(formal_list)}\n"
        f"狀態：待處理"
    )
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=resp))

# ============================================
# 功能 C：查詢寄書（預設不顯示「已刪除」）
# （★★ 同一ID合併多本顯示）
# ============================================
def _handle_query(event, text):
    q = re.sub(r"^#(查詢寄書|查寄書)\s*", "", text.strip())

    ws = _ws(MAIN_SHEET_NAME)
    h = _get_header_map(ws)
    idxA = _col_idx(h, "紀錄ID", 1)
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

    # 先篩近 30 天且非已刪除；依查詢條件過濾
    filtered = []
    for r in rows:
        try:
            st = (r[idxM-1] or "").strip()
            if st == "已刪除":
                continue
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
                    filtered.append(r)
            else:
                if q and q in r[idxD-1]:
                    filtered.append(r)
        except Exception:
            continue

    if not filtered:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 查無近 30 天內的寄書紀錄，請確認姓名或電話是否正確。"))
        return

    # ★★ 依 RID 合併
    groups = {}
    for r in filtered:
        rid = (r[idxA-1] or "").strip()
        if not rid:
            continue
        g = groups.setdefault(rid, {
            "dt": r[idxB-1],
            "name": r[idxD-1],
            "books": [],
            "status_any": set(),
            "ship_out": "",
            "ship_via": "",
            "ship_no": ""
        })
        # 以最大時間為該群主時間
        if r[idxB-1] > g["dt"]:
            g["dt"] = r[idxB-1]
        g["books"].append((r[idxG-1] or "").strip())
        st = (r[idxM-1] or "").strip()
        if st:
            g["status_any"].add(st)
        # 取較新的出貨欄位
        j, i, k = (r[idxJ-1] or "").strip(), (r[idxI-1] or "").strip(), (r[idxK-1] or "").strip()
        if j or i or k:
            # 粗略策略：若尚未有資料，或這列時間較新，就覆蓋
            if not (g["ship_out"] or g["ship_via"] or g["ship_no"]) or r[idxB-1] >= g["dt"]:
                g["ship_out"], g["ship_via"], g["ship_no"] = j, i, k

    # 依建單時間倒序；取前 5 組
    ordered = sorted(groups.items(), key=lambda kv: kv[1]["dt"], reverse=True)[:5]

    blocks = []
    for rid, g in ordered:
        books = "、".join(sorted(set([b for b in g["books"] if b])))
        name = g["name"]
        statuses = g["status_any"]
        if "已託運" in statuses:
            lines = [f"📦 {name}（{rid}）：{books}"]
            if g["ship_out"]: lines.append(f"已於 {g['ship_out']}")
            if g["ship_via"]: lines.append(f"由 {g['ship_via']} 寄出")
            if g["ship_no"]:  lines.append(f"託運單號：{g['ship_no']}")
            blocks.append("\n".join(lines))
        else:
            # 取一個代表狀態（沒有就顯示待處理）
            st = next(iter(statuses)) if statuses else "待處理"
            blocks.append(f"📦 {name}（{rid}）：{books} {st}")

    msg = "\n\n".join(blocks)
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

# ============================================
# 功能 D：取消寄書（★★同一ID全部刪除；無刪除線；已刪除排除＋正確排序 修補）
# 指令：#取消寄書 / #刪除寄書  +（姓名/電話）
# 權限：建單人（C欄）須等於操作者的 LINE 顯示名稱
# 確認：回覆 Y/N
# ============================================
_PENDING = {}  # user_id -> pending dict
_OCR_SESSION = {}  # user_id -> {"type":"ship","expire_ts": epoch}

def _extract_cancel_target(text: str):
    body = re.sub(r"^#(取消寄書|刪除寄書)\s*", "", text.strip())
    name, phone = None, None

    data = parse_kv_lines(body)
    for k in list(data.keys()):
        if any(x in k for x in ["姓名","學員","收件人","名字","貴姓"]):
            name = "、".join(data.pop(k)); break
    for k in list(data.keys()):
        if "電話" in k:
            for v in data.pop(k):
                p = normalize_phone(v)
                if p: phone = p; break
            break

    if not name or not phone:
        tokens = re.split(r"\s+", body)
        for t in tokens:
            tt = t.strip()
            if not tt: continue
            p = normalize_phone(tt)
            if (not phone) and p: phone = p; continue
            if not name and not re.search(r"\d", tt): name = tt
    return (name, phone)

# 🔧 修補版：排除「已刪除」，用可解析的建單時間做排序（取最近一筆）
def _find_latest_order(ws, name, phone):
    h = _get_header_map(ws)
    idxA = _col_idx(h, "紀錄ID", 1)
    idxB = _col_idx(h, "建單日期", 2)
    idxC = _col_idx(h, "建單人", 3)
    idxD = _col_idx(h, "學員姓名", 4)
    idxE = _col_idx(h, "學員電話", 5)
    idxM = _col_idx(h, "寄送狀態", 13)

    all_vals = ws.get_all_values()
    rows = all_vals[1:]
    since = datetime.now(TZ) - timedelta(days=QUERY_DAYS)

    phone_suffix = None
    if phone:
        pd = re.sub(r"\D+","", phone)
        if len(pd) >= PHONE_SUFFIX_MATCH:
            phone_suffix = pd[-PHONE_SUFFIX_MATCH:]

    candidates = []
    for ridx, r in enumerate(rows, start=2):
        try:
            # 1) 排除「已刪除」
            if (r[idxM-1] or "").strip() == "已刪除":
                continue

            # 2) 解析建單日期，超過查詢窗範圍則跳過
            dt_str = (r[idxB-1] or "").strip()
            dt = None
            if dt_str:
                try:
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
                except Exception:
                    dt = None
            if dt and dt < since:
                continue

            # 3) 條件比對（姓名包含；電話後 N 碼）
            ok = True
            if name and name not in (r[idxD-1] or ""):
                ok = False
            if phone_suffix:
                cand = re.sub(r"\D+","", r[idxE-1] or "")
                if not (len(cand) >= PHONE_SUFFIX_MATCH and cand[-PHONE_SUFFIX_MATCH:] == phone_suffix):
                    ok = False

            if ok:
                # 用「可比較的時間」當排序 key；若無法解析時間，用 datetime.min 墊底
                key_dt = dt or datetime.min.replace(tzinfo=TZ)
                candidates.append((key_dt, ridx, r))
        except Exception:
            continue

    if not candidates:
        return (None, None)
    # 取建單時間最新的一筆
    candidates.sort(key=lambda x: x[0], reverse=True)
    _, row_i, row = candidates[0]
    return row_i, row

def _collect_rows_by_rid(ws, rid: str):
    """回傳該 RID 的所有 (row_index, row_values)"""
    h = _get_header_map(ws)
    idxA = _col_idx(h, "紀錄ID", 1)
    all_vals = ws.get_all_values()[1:]
    out = []
    for ridx, r in enumerate(all_vals, start=2):
        try:
            if (r[idxA-1] or "").strip() == rid:
                out.append((ridx, r))
        except Exception:
            continue
    return out

def _handle_cancel_request(event, text):
    try:
        profile = line_bot_api.get_profile(event.source.user_id)
        display_name = profile.display_name or "LINE使用者"
    except Exception:
        display_name = "LINE使用者"

    name, phone = _extract_cancel_target(text)
    ws = _ws(MAIN_SHEET_NAME)
    h = _get_header_map(ws)
    idxA = _col_idx(h, "紀錄ID", 1)
    idxB = _col_idx(h, "建單日期", 2)
    idxC = _col_idx(h, "建單人", 3)
    idxD = _col_idx(h, "學員姓名", 4)
    idxG = _col_idx(h, "書籍名稱", 7)
    idxJ = _col_idx(h, "寄出日期", 10)
    idxK = _col_idx(h, "託運單號", 11)
    idxL = _col_idx(h, "經手人", 12)
    idxM = _col_idx(h, "寄送狀態", 13)
    idxH = _col_idx(h, "業務備註", 8)

    row_i, r = _find_latest_order(ws, name, phone)
    if not row_i:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 找不到紀錄"))
        return

    creator = (r[idxC-1] or "").strip() or "LINE使用者"
    if creator != display_name:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 你沒有刪除權限（請聯繫管理者）"))
        return

    rid = (r[idxA-1] or "").strip()
    all_rows = _collect_rows_by_rid(ws, rid)

    # ★★ 若任一列為已託運或有出書欄位 → 禁止刪除
    for _, rr in all_rows:
        status = (rr[idxM-1] or "").strip()
        if status == "已託運":
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 已託運，無法刪除"))
            return
        if (rr[idxJ-1] or rr[idxK-1]) and status != "已託運":
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❗ 無法處理，請私訊客服。"))
            return

    stu = (r[idxD-1] or "").strip()
    books = "、".join([rr[idxG-1] for _, rr in all_rows if (rr[idxG-1] or "").strip()])
    _PENDING[event.source.user_id] = {
        "type": "cancel_order",
        "sheet": MAIN_SHEET_NAME,
        "rid": rid,
        "rows": [ri for ri, _ in all_rows],
        "stu": stu,
        "book_list": books,
        "operator": display_name,
        "idx": {"H": idxH, "L": idxL, "M": idxM}
    }
    prompt = f"請確認是否刪除整筆寄書（同一ID {rid} 共 {len(all_rows)} 列）：\n學員：{stu}\n書名：{books}\n[Y/N]"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=prompt))

# ============================================
# 功能 G：刪除／取消「出書」（撤銷已託運欄位）
# 指令：#刪除出書 / #取消出書 +（姓名/電話）
# 動作：清空 寄出日期/託運單號/經手人，狀態改為「待處理」，備註附上時間戳
# ============================================
def _extract_ship_delete_target(text: str):
    body = re.sub(r"^#(刪除出書|取消出書)\s*", "", text.strip())
    name, phone = None, None

    data = parse_kv_lines(body)
    for k in list(data.keys()):
        if any(x in k for x in ["姓名","學員","收件人","名字","貴姓"]):
            name = "、".join(data.pop(k)); break
    for k in list(data.keys()):
        if "電話" in k:
            for v in data.pop(k):
                p = normalize_phone(v)
                if p: phone = p; break
            break

    if not name or not phone:
        tokens = re.split(r"\s+", body)
        for t in tokens:
            tt = t.strip()
            if not tt: continue
            p = normalize_phone(tt)
            if (not phone) and p: phone = p; continue
            if not name and not re.search(r"\d", tt): name = tt
    return (name, phone)

def _handle_delete_ship(event, text):
    try:
        profile = line_bot_api.get_profile(event.source.user_id)
        operator = profile.display_name or "LINE使用者"
    except Exception:
        operator = "LINE使用者"

    name, phone = _extract_ship_delete_target(text)
    ws = _ws(MAIN_SHEET_NAME)
    h = _get_header_map(ws)
    idxB = _col_idx(h, "建單日期", 2)
    idxD = _col_idx(h, "學員姓名", 4)
    idxE = _col_idx(h, "學員電話", 5)
    idxH = _col_idx(h, "業務備註", 8)
    idxJ = _col_idx(h, "寄出日期", 10)
    idxK = _col_idx(h, "託運單號", 11)
    idxL = _col_idx(h, "經手人", 12)
    idxM = _col_idx(h, "寄送狀態", 13)

    rows = ws.get_all_values()[1:]
    since = datetime.now(TZ) - timedelta(days=QUERY_DAYS)

    phone_suffix = None
    if phone:
        pd = re.sub(r"\D+","", phone)
        if len(pd) >= PHONE_SUFFIX_MATCH:
            phone_suffix = pd[-PHONE_SUFFIX_MATCH:]

    candidates = []
    for ridx, r in enumerate(rows, start=2):
        try:
            dt_str = (r[idxB-1] or "").strip()
            dt = None
            if dt_str:
                try:
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
                except Exception:
                    dt = None
            if dt and dt < since: continue

            ok = True
            if name and name not in (r[idxD-1] or ""): ok = False
            if phone_suffix:
                cand = re.sub(r"\D+","", r[idxE-1] or "")
                if not (len(cand) >= PHONE_SUFFIX_MATCH and cand[-PHONE_SUFFIX_MATCH:] == phone_suffix):
                    ok = False
            # 僅鎖定「已託運」或具出書欄位者
            shipped = ((r[idxM-1] or "").strip() == "已託運") or (r[idxJ-1] or r[idxK-1])
            if ok and shipped:
                candidates.append((ridx, r))
        except Exception:
            continue

    if not candidates:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 找不到可撤銷的出書紀錄（近30天）"))
        return

    # 取最近一筆
    row_i, r = sorted(candidates, key=lambda x: x[1][idxB-1], reverse=True)[0]
    # 清空欄位，狀態回「待處理」
    note = f"[撤銷出書 {now_str_min()}]"
    try:
        curr_h = ws.cell(row_i, idxH).value or ""
    except Exception:
        curr_h = ""
    ws.update_cell(row_i, idxH, (curr_h + " " + note).strip() if curr_h else note)
    ws.update_cell(row_i, idxJ, "")
    ws.update_cell(row_i, idxK, "")
    ws.update_cell(row_i, idxL, operator)
    ws.update_cell(row_i, idxM, "待處理")

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="✅ 已撤銷最近一筆出書：欄位已清空並恢復為待處理"))

# ============================================
# 功能 E：出書 OCR 啟用（#出書 開啟會話）
# ============================================
def _start_ocr_session(user_id: str):
    _OCR_SESSION[user_id] = {
        "type": "ship",
        "expire_ts": time.time() + OCR_SESSION_TTL_MIN * 60
    }

def _has_ocr_session(user_id: str) -> bool:
    info = _OCR_SESSION.get(user_id)
    if not info: return False
    if time.time() > info["expire_ts"]:
        _OCR_SESSION.pop(user_id, None)
        return False
    return True

def _clear_ocr_session(user_id: str):
    _OCR_SESSION.pop(user_id, None)

# ============================================
# 功能 F：OCR 解析 + 寫回託運單資訊（加強版除錯）
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

# ★★ 加強版 OCR 寫回函數（詳細除錯日誌）
def _write_ocr_results(pairs, event):
    if not pairs:
        return "❗ 未寫入任何資料（未找到配對）"
    
    ws = _ws(MAIN_SHEET_NAME)
    h = _get_header_map(ws)
    
    # 記錄表頭資訊
    app.logger.info(f"[OCR_DEBUG] Header map: {h}")
    
    idxA = _col_idx(h, "紀錄ID", 1)
    idxJ = _col_idx(h, "寄出日期", 10)
    idxK = _col_idx(h, "託運單號", 11)
    idxL = _col_idx(h, "經手人", 12)
    idxM = _col_idx(h, "寄送狀態", 13)
    idxD = _col_idx(h, "學員姓名", 4)  # 用於除錯

    try:
        profile = line_bot_api.get_profile(event.source.user_id)
        uploader = profile.display_name or "LINE使用者"
    except Exception:
        uploader = "LINE使用者"

    all_vals = ws.get_all_values()
    app.logger.info(f"[OCR_DEBUG] Total rows in sheet: {len(all_vals)}")
    
    rows = all_vals[1:]  # 跳過表頭

    # ★★ 改用更安全的方式建立 RID 映射
    id2rows = {}
    for ridx, r in enumerate(rows, start=2):
        try:
            # 確保有足夠的欄位
            if len(r) < idxA:
                app.logger.warning(f"[OCR_DEBUG] Row {ridx} has insufficient columns: {len(r)}")
                continue
                
            rid = (r[idxA-1] or "").strip()
            if re.fullmatch(r"R\d{4}", rid):
                # 記錄找到的RID和對應行號、姓名（用於除錯）
                student_name = r[idxD-1] if len(r) >= idxD else "N/A"
                app.logger.info(f"[OCR_DEBUG] Found RID {rid} at row {ridx}, student: {student_name}")
                id2rows.setdefault(rid, []).append(ridx)
        except Exception as e:
            app.logger.error(f"[OCR_DEBUG] Error processing row {ridx}: {e}")
            continue

    app.logger.info(f"[OCR_DEBUG] RID mapping: {id2rows}")

    updated = []
    for rid, no in pairs:
        row_is = id2rows.get(rid, [])
        app.logger.info(f"[OCR_DEBUG] Updating RID {rid} with number {no} at rows: {row_is}")
        
        if not row_is:
            app.logger.warning(f"[OCR_DEBUG] RID {rid} not found in sheets")
            continue
            
        # 對同一RID的所有列都寫入相同的出貨資訊
        for row_i in row_is:
            try:
                # 逐一更新每個欄位並記錄
                app.logger.info(f"[OCR_DEBUG] Updating row {row_i}: setting tracking number to {no}")
                ws.update_cell(row_i, idxK, f"'{no}")
                
                app.logger.info(f"[OCR_DEBUG] Updating row {row_i}: setting ship date to {today_str()}")
                ws.update_cell(row_i, idxJ, today_str())
                
                app.logger.info(f"[OCR_DEBUG] Updating row {row_i}: setting handler to {uploader}")
                ws.update_cell(row_i, idxL, uploader)
                
                app.logger.info(f"[OCR_DEBUG] Updating row {row_i}: setting status to 已託運")
                ws.update_cell(row_i, idxM, "已託運")
                
            except Exception as e:
                app.logger.error(f"[OCR_DEBUG] Error updating row {row_i}: {e}")
                continue
                
        updated.append((rid, no))

    if not updated:
        return "❗ 未寫入（找不到對應的紀錄ID）"

    lines = [f"{rid} → {no}" for rid, no in updated]
    result_msg = "✅ 已更新：{} 筆\n{}".format(len(updated), "\n".join(lines))
    app.logger.info(f"[OCR_DEBUG] Final result: {result_msg}")
    return result_msg

# ============================================
# 功能 H：入庫（#買書 / #入庫；支援負數＝盤點調整）
# 《入庫明細》表頭：日期/經手人/書籍名稱/數量/來源/備註
# ============================================
def _ensure_stockin_sheet():
    return _get_or_create_ws(STOCK_IN_SHEET_NAME, ["日期","經手人","書籍名稱","數量","來源","備註"])

def _parse_stockin_text(body: str):
    """
    逐行解析。每行抓最後一個整數(可含 +/-)作為數量；找不到則預設 1。
    書名：去除數量後送 resolve_book_name。
    回傳：items=[{"name":書名,"qty":數量}], errors=[str], ambiguous=[(raw,[候選])]
    """
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    items, errors, ambiguous = [], [], []
    for ln in lines:
        # 支援 +/-：x -3 / × -3 / * -3 / 結尾 -3 / 數量：-3
        m = re.search(r"(?:x|×|\*)\s*([+-]?\d+)$", ln, re.I)
        if not m:
            m = re.search(r"([+-]?\d+)\s*(本|套|冊)?$", ln)
        if not m:
            m = re.search(r"數量[:：]\s*([+-]?\d+)", ln)

        qty = int(m.group(1)) if m else 1

        # 去掉尾端數量片段
        title = ln
        if m:
            title = ln[:m.start()].strip()

        # 清理連接符
        title = re.sub(r"[：:\-–—]+$", "", title).strip()

        book, kind, extra = resolve_book_name(title)
        if not book:
            if kind == "ambiguous" and extra:
                ambiguous.append((ln, extra[:10]))
            else:
                errors.append(f"找不到書名：{ln}")
            continue
        items.append({"name": book, "qty": qty})
    if not lines:
        errors.append("沒有讀到任何內容")
    return items, errors, ambiguous

def _handle_stockin(event, text):
    try:
        profile = line_bot_api.get_profile(event.source.user_id)
        operator = profile.display_name or "LINE使用者"
    except Exception:
        operator = "LINE使用者"

    body = re.sub(r"^#(買書|入庫)\s*", "", text.strip())
    items, errs, amb = _parse_stockin_text(body)

    if amb:
        tips = []
        for raw, choices in amb:
            tips.append(f"「{raw}」可能是：{ '、'.join(choices) }")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❗ 書名不夠明確：\n" + "\n".join(tips)))
        return
    if errs:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 入庫資料有誤：\n- " + "\n- ".join(errs)))
        return
    if not items:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 沒有可入庫的項目"))
        return

    # 合併相同書名
    merged = {}
    for it in items:
        merged[it["name"]] = merged.get(it["name"], 0) + int(it["qty"])
    items = [{"name": k, "qty": v} for k, v in merged.items()]

    has_negative = any(it["qty"] < 0 for it in items)

    # 存入 pending 等確認
    _PENDING[event.source.user_id] = {
        "type": "stock_in_confirm",
        "operator": operator,
        "items": items
    }
    lines = [f"• {it['name']} × {it['qty']}" for it in items]
    suffix = "\n\n※ 含負數（自動標示來源：盤點調整）" if has_negative else ""
    msg = "請確認入庫項目：\n" + "\n".join(lines) + suffix + "\n\n回覆「OK / YES / Y」確認；或回覆「N」取消。"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

def _write_stockin_rows(operator: str, items: list[dict]):
    ws = _ensure_stockin_sheet()
    rows = []
    for it in items:
        qty = int(it["qty"])
        source = "購買" if qty >= 0 else "盤點調整"
        rows.append([today_str(), operator, it["name"], qty, source, ""])
    ws.append_rows(rows, value_input_option="USER_ENTERED")

# ============================================
# 功能 I＋共用：處理待確認回答（Y/N/YES/OK）
# （含：取消寄書多列同ID一次刪）
# ============================================
def _handle_pending_answer(event, text):
    pend = _PENDING.get(event.source.user_id)
    if not pend: return False
    ans = text.strip().upper()
    if ans not in ("Y","N","YES","OK"):
        return False
    if ans in ("N",):
        _PENDING.pop(event.source.user_id, None)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="已取消。"))
        return True

    # YES / OK / Y
    if pend["type"] == "cancel_order":
        ws = _ws(pend["sheet"])
        idxH = pend["idx"]["H"]
        idxL = pend["idx"]["L"]
        idxM = pend["idx"]["M"]

        append_note = f"[已刪除 {now_str_min()}]"
        for row_i in sorted(pend["rows"], reverse=False):
            try:
                curr_h = ws.cell(row_i, idxH).value or ""
            except Exception:
                curr_h = ""
            new_h = (curr_h + " " + append_note).strip() if curr_h else append_note
            ws.update_cell(row_i, idxH, new_h)
            ws.update_cell(row_i, idxL, pend["operator"])
            ws.update_cell(row_i, idxM, "已刪除")

        msg = f"✅ 已刪除整筆寄書（{pend['rid']}）：{pend['stu']} 的 {pend['book_list']}"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        _PENDING.pop(event.source.user_id, None)
        return True

    if pend["type"] == "stock_in_confirm":
        _write_stockin_rows(pend["operator"], pend["items"])
        lines = [f"{it['name']} × {it['qty']}" for it in pend["items"]]
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="✅ 入庫完成：\n" + "\n".join(lines)))
        _PENDING.pop(event.source.user_id, None)
        return True

    return False

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

# 文字訊息處理（功能 J：只處理指定指令）
@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = (event.message.text or "").strip()

    # 功能 I：#我的ID（不受白名單限制）
    if text.startswith("#我的ID"):
        uid = getattr(event.source, "user_id", "")
        try:
            profile = line_bot_api.get_profile(uid)
            name = profile.display_name or "LINE使用者"
        except Exception:
            name = "LINE使用者"
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"你的 ID：\n{uid}\n顯示名稱：{name}\n\n請提供給管理員加入白名單。")
            )
        except Exception:
            pass
        if uid:
            _log_candidate(uid, name)
        return

    # 共用：待確認流程（Y/N/YES/OK）
    if _handle_pending_answer(event, text):
        return

    # 白名單檢查（其餘指令都要）
    if not _ensure_authorized(event, scope="text"):
        return

    # 僅處理以下指令；其餘直接不回覆
    if text.startswith("#寄書"):
        _handle_new_order(event, text); return

    if text.startswith("#查詢寄書") or text.startswith("#查寄書"):
        _handle_query(event, text); return

    if text.startswith("#取消寄書") or text.startswith("#刪除寄書"):
        _handle_cancel_request(event, text); return

    if text.startswith("#刪除出書") or text.startswith("#取消出書"):
        _handle_delete_ship(event, text); return

    if text.startswith("#出書"):
        # 開啟 OCR 視窗
        _start_ocr_session(getattr(event.source, "user_id", ""))
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"已啟用出書OCR（{OCR_SESSION_TTL_MIN} 分鐘）。請上傳出貨單照片。"))
        return

    if text.startswith("#買書") or text.startswith("#入庫"):
        _handle_stockin(event, text); return

    # 其他文字：不處理、不回覆
    return

# 圖片訊息處理（功能 E：僅在 #出書 後 N 分鐘內才啟用）
@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    # 白名單：未授權直接擋（圖片不回覆）
    if not _ensure_authorized(event, scope="image"):
        return

    uid = getattr(event.source, "user_id", "")
    if not _has_ocr_session(uid):
        # 未先 #出書 或已逾時：不處理、不回覆
        return

    try:
        app.logger.info(f"[IMG] 收到圖片 user_id={uid} msg_id={event.message.id}")
        img_bytes = _download_line_image_bytes(event.message.id)
        if not _vision_client:
            # 有啟動OCR會話，但 Vision 未設定 → 回覆錯誤
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="❌ OCR 錯誤：Vision 未初始化（請設定 GOOGLE_SERVICE_ACCOUNT_JSON 並啟用 Vision API）。")
            )
            return

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
        try:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ OCR 錯誤（代碼 {code}）：{e}"))
        except Exception:
            pass
    finally:
        # 單次處理後關閉會話；如需多張，再輸入 #出書
        _clear_ocr_session(uid)

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
