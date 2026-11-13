# app.py - 優化版
# ============================================
# 修復項目：
# S1: Vision OCR 正確初始化
# S2: 移除重複程式碼
# S3: Sheets 連線錯誤處理
# H1: 多使用者隔離（user_id 作為 key）
# H2: 所有 Sheets 操作加入錯誤處理
# H3: 白名單即時刷新機制
# M1: 優化 Sheets 讀取效能
# M2: 統一函式命名
# M3: 增加關鍵操作日誌
# 新功能：#查書名（分類查詢）+ 引導修正流程
# ============================================

import os
import re
import io
import json
import difflib
import logging
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, List, Tuple, Any

from flask import Flask, request, abort
import gspread
from google.oauth2.service_account import Credentials

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, ImageMessage,
    TextSendMessage,
)

# ====== Vision OCR 初始化（修復 S1）======
_HAS_VISION = False
_vision_client = None

def _init_vision_client():
    """正確初始化 Vision API 客戶端"""
    global _vision_client, _HAS_VISION
    try:
        from google.cloud import vision
        from google.oauth2 import service_account as gcp_service_account
        
        json_path = "service_account.json"
        if os.path.exists(json_path):
            creds = gcp_service_account.Credentials.from_service_account_file(json_path)
            _vision_client = vision.ImageAnnotatorClient(credentials=creds)
            _HAS_VISION = True
            return
        
        sa_json = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_NEW", "") or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", ""))
        if sa_json:
            info = json.loads(sa_json)
            creds = gcp_service_account.Credentials.from_service_account_info(info)
            _vision_client = vision.ImageAnnotatorClient(credentials=creds)
            _HAS_VISION = True
    except Exception as e:
        _HAS_VISION = False
        _vision_client = None
        print(f"[VISION] 初始化失敗: {e}")

# 啟動時初始化 Vision
_init_vision_client()
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

# === 白名單設定 ===
WHITELIST_SHEET_NAME = os.getenv("WHITELIST_SHEET_NAME", "白名單")
CANDIDATE_SHEET_NAME = os.getenv("CANDIDATE_SHEET_NAME", "候選名單")
WHITELIST_MODE = os.getenv("WHITELIST_MODE", "enforce").strip().lower()
ADMIN_USER_IDS = {x.strip() for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()}
_WHITELIST_CACHE = {"ts": 0.0, "set": set()}
_WHITELIST_TTL = 300

# === 新增常數定義（修復 L1）===
FUZZY_THRESHOLD = float(os.getenv("FUZZY_THRESHOLD", "0.7"))
QUERY_DAYS = int(os.getenv("QUERY_DAYS", "30"))
PHONE_SUFFIX_MATCH = int(os.getenv("PHONE_SUFFIX_MATCH", "9"))
WRITE_ZIP_TO_ADDRESS = os.getenv("WRITE_ZIP_TO_ADDRESS", "true").lower() == "true"
LOG_OCR_RAW = os.getenv("LOG_OCR_RAW", "true").lower() == "true"
OCR_SESSION_TTL_MIN = int(os.getenv("OCR_SESSION_TTL_MIN", "10"))
MAX_BOOK_SUGGESTIONS = 3  # 最多建議書籍數量
MAX_LEFTOVER_ITEMS = 10   # OCR 未配對項目最多顯示數量
INSERT_AT_TOP = True  # 固定在第二列插入新資料

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

if not LINE_CHANNEL_SECRET or not LINE_CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("Missing LINE credentials.")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

TZ = ZoneInfo("Asia/Taipei")

# ============================================
# 全域狀態管理（修復 H1：使用 user_id 隔離）
# ============================================
_PENDING: Dict[str, Dict[str, Any]] = {}  # user_id -> pending_data
_OCR_SESSIONS: Dict[str, float] = {}  # user_id -> expire_timestamp

# ============================================
# Google Sheets 連線（修復 S3：加入錯誤處理）
# ============================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def _build_gspread_client():
    """建立 gspread 客戶端"""
    json_path = "service_account.json"
    if os.path.exists(json_path):
        creds = Credentials.from_service_account_file(json_path, scopes=SCOPES)
        return gspread.authorize(creds)
    
    sa_json = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_NEW", "") or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", ""))
    if not sa_json:
        raise RuntimeError("Missing service account credentials.")
    
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    return gspread.authorize(creds)

def _safe_open_spreadsheet(sheet_id: str):
    """安全開啟試算表（修復 S3）"""
    try:
        gc = _build_gspread_client()
        return gc.open_by_key(sheet_id)
    except Exception as e:
        app.logger.error(f"[SHEETS] 無法開啟試算表 {sheet_id}: {e}")
        raise RuntimeError(f"無法連線至 Google Sheets: {e}")

# 初始化試算表
try:
    ss = _safe_open_spreadsheet(SHEET_ID)
    app.logger.info(f"[SHEETS] 成功連線至試算表")
except Exception as e:
    app.logger.error(f"[SHEETS] 啟動失敗: {e}")
    ss = None  # 允許服務啟動，但會在操作時報錯

def _ws(name: str):
    """取得工作表（修復 H2：加入錯誤處理）"""
    try:
        return ss.worksheet(name)
    except gspread.WorksheetNotFound:
        app.logger.error(f"[SHEETS] 工作表不存在: {name}")
        raise ValueError(f"找不到工作表：{name}")
    except Exception as e:
        app.logger.error(f"[SHEETS] 取得工作表失敗 {name}: {e}")
        raise

def _get_or_create_ws(name: str, headers: list):
    """取得或建立工作表（修復 H2）"""
    try:
        ws = ss.worksheet(name)
        return ws
    except gspread.WorksheetNotFound:
        try:
            ws = ss.add_worksheet(title=name, rows=200, cols=max(10, len(headers)))
            if headers:
                ws.update(f"A1:{chr(64+len(headers))}1", [headers])
            app.logger.info(f"[SHEETS] 已建立工作表: {name}")
            return ws
        except Exception as e:
            app.logger.error(f"[SHEETS] 建立工作表失敗 {name}: {e}")
            raise
    except Exception as e:
        app.logger.error(f"[SHEETS] 取得工作表失敗 {name}: {e}")
        raise

def _get_header_map(ws):
    """取得表頭對應（修復 H2）"""
    try:
        header = ws.row_values(1)
        hmap = {}
        for idx, title in enumerate(header, start=1):
            t = str(title).strip()
            if t:
                hmap[t] = idx
        return hmap
    except Exception as e:
        app.logger.error(f"[SHEETS] 取得表頭失敗: {e}")
        return {}

def _col_idx(hmap, key, default_idx):
    """取得欄位索引"""
    return hmap.get(key, default_idx)

def _safe_update_cell(ws, row: int, col: int, value: Any):
    """安全更新儲存格（修復 H2 + M3）"""
    try:
        ws.update_cell(row, col, value)
        app.logger.info(f"[SHEETS] 更新 {ws.title} R{row}C{col} = {value}")
    except Exception as e:
        app.logger.error(f"[SHEETS] 更新失敗 R{row}C{col}: {e}")
        raise

def _safe_append_row(ws, row_data: list):
    """安全新增列（固定插入第二列，不繼承格式）"""
    try:
        if INSERT_AT_TOP:
            # 在第 2 列（表頭下方）插入新資料
            # inheritFromBefore=False 確保不繼承上方格式
            ws.insert_row(row_data, index=2, value_input_option="USER_ENTERED", inherit_from_before=False)
            app.logger.info(f"[SHEETS] 插入列至 {ws.title} 第2列: {row_data[:3]}...")
        else:
            # 在最下面新增（保留原邏輯，但目前不使用）
            ws.append_row(row_data, value_input_option="USER_ENTERED")
            app.logger.info(f"[SHEETS] 新增列至 {ws.title}: {row_data[:3]}...")
    except Exception as e:
        app.logger.error(f"[SHEETS] 新增列失敗: {e}")
        raise

def _safe_append_rows(ws, rows_data: list):
    """安全批次新增列（修復 H2 + M3）"""
    try:
        ws.append_rows(rows_data, value_input_option="USER_ENTERED")
        app.logger.info(f"[SHEETS] 批次新增 {len(rows_data)} 列至 {ws.title}")
    except Exception as e:
        app.logger.error(f"[SHEETS] 批次新增失敗: {e}")
        raise

# ============================================
# 白名單功能（修復 H3：即時刷新）
# ============================================
def _truthy(v) -> bool:
    """判斷值是否為真"""
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "t", "啟用", "是", "enabled", "on")

def _load_whitelist(force: bool = False) -> set:
    """載入白名單（修復 H3：支援強制刷新）"""
    now = time.time()
    if (not force) and _WHITELIST_CACHE["set"] and (now - _WHITELIST_CACHE["ts"] < _WHITELIST_TTL):
        return _WHITELIST_CACHE["set"]
    
    try:
        ws = _get_or_create_ws(WHITELIST_SHEET_NAME, ["user_id", "name", "enabled"])
        rows = ws.get_all_records()
        enabled = {
            str(r.get("user_id", "")).strip() 
            for r in rows 
            if str(r.get("user_id", "")).strip() and _truthy(r.get("enabled", "1"))
        }
        _WHITELIST_CACHE["set"] = enabled
        _WHITELIST_CACHE["ts"] = now
        app.logger.info(f"[WHITELIST] 已載入 {len(enabled)} 個授權使用者")
        return enabled
    except Exception as e:
        app.logger.error(f"[WHITELIST] 載入失敗: {e}")
        return _WHITELIST_CACHE["set"]  # 回傳舊快取

def _log_candidate(user_id: str, name: str):
    """記錄候選名單（修復 H2）"""
    try:
        ws = _get_or_create_ws(CANDIDATE_SHEET_NAME, ["user_id", "name", "first_seen", "last_seen"])
        all_vals = ws.get_all_values()
        h = _get_header_map(ws)
        idx_uid = _col_idx(h, "user_id", 1)
        idx_name = _col_idx(h, "name", 2)
        idx_first = _col_idx(h, "first_seen", 3)
        idx_last = _col_idx(h, "last_seen", 4)

        exists_row = None
        for i, r in enumerate(all_vals[1:], start=2):
            if (len(r) >= idx_uid) and r[idx_uid - 1] == user_id:
                exists_row = i
                break

        now_s = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
        if exists_row:
            if name:
                _safe_update_cell(ws, exists_row, idx_name, name)
            _safe_update_cell(ws, exists_row, idx_last, now_s)
        else:
            _safe_append_row(ws, [user_id, name, now_s, now_s])
    except Exception as e:
        app.logger.warning(f"[CANDIDATE] 記錄失敗: {e}")

def _ensure_authorized(event, scope: str = "*") -> bool:
    """驗證授權（修復 M3：增加日誌）"""
    try:
        uid = getattr(event.source, "user_id", "")
        profile = line_bot_api.get_profile(uid)
        display_name = profile.display_name or "LINE使用者"
    except Exception:
        uid = getattr(event.source, "user_id", "")
        display_name = "LINE使用者"

    if uid:
        _log_candidate(uid, display_name)

    if uid in ADMIN_USER_IDS:
        app.logger.info(f"[AUTH] 管理員通過: {uid}")
        return True
    
    if WHITELIST_MODE in ("off", "log"):
        app.logger.info(f"[AUTH] 白名單模式 {WHITELIST_MODE}，允許: {uid}")
        return True

    allowed = _load_whitelist()
    if uid in allowed:
        app.logger.info(f"[AUTH] 白名單通過: {uid}")
        return True

    app.logger.warning(f"[AUTH] 未授權: {uid}")
    if scope == "text":
        msg = f"❌ 尚未授權使用。\n請將此 ID 提供給管理員開通：\n{uid}\n\n（提示：傳「#我的ID」也能取得這串 ID）"
        try:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        except Exception:
            pass
    return False

# ============================================
# 工具函式
# ============================================
def now_str_min():
    """目前時間字串（分鐘）"""
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M")

def today_str():
    """今日日期字串"""
    return datetime.now(TZ).strftime("%Y-%m-%d")

def normalize_phone(s: str) -> Optional[str]:
    """正規化電話號碼（放寬規則：09 開頭 + 10 位數）"""
    digits = re.sub(r"\D+", "", s or "")
    # 檢查：第一碼是 0，第二碼是 9，總共 10 位數
    if len(digits) == 10 and digits[0] == "0" and digits[1] == "9":
        return digits
    return None

_ADDR_KEYS = ("寄送地址", "地址", "address", "Address")
_BOOK_KEYS = ("書籍名稱", "書名", "book", "Book", "書籍")
_ALL_KEYS = ("學員姓名", "姓名", "name", "Name", "學員電話", "電話", "phone", "Phone", "手機",
             "業務備註", "備註", "note", "Note") + _ADDR_KEYS + _BOOK_KEYS
_KNOWN_KEY_RE = re.compile(
    r"^(" + "|".join(sorted((re.escape(k) for k in _ALL_KEYS), key=len, reverse=True)) + r")\s*[：:；;]\s*"
)

def parse_kv_lines(text: str) -> Dict[str, str]:
    """解析 key:value 格式文字，支援多種欄位名稱；
    沒有冒號的行視為上一個欄位的續行（地址中間換行不會被截斷）"""
    data = {}
    cont = {}  # key -> 續行
    last_key = None
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        # 已知欄位名開頭：冒號打成分號也認（例：「備註；點數商城購買」）
        m = _KNOWN_KEY_RE.match(line)
        if m:
            last_key = m.group(1)
            data[last_key] = line[m.end():].strip()
            continue
        m = re.match(r"^([^：:]{1,8})[：:](.*)$", line)
        if m and not re.search(r"[\d/]", m.group(1)):
            last_key = m.group(1).strip()
            data[last_key] = m.group(2).strip()
        elif last_key is not None:
            # 沒有冒號、或冒號是書名的一部分（「…4/e: Facts & Figures」）＝上一欄的續行
            cont.setdefault(last_key, []).append(line)

    for k, extra in cont.items():
        if k in _BOOK_KEYS:
            sep = "\n"  # 書名維持換行，交給原本的多本切分
        else:
            sep = " "   # 地址等欄位用空格接，地址檢查靠這個空格抓「144 10號」
        data[k] = sep.join([data[k]] + extra) if data[k] else sep.join(extra)

    # 欄位名稱正規化（支援多種寫法）
    normalized = {}
    
    # 姓名欄位
    for key in ["姓名", "學員姓名", "name", "Name"]:
        if key in data:
            normalized["姓名"] = data[key]
            break
    
    # 電話欄位
    for key in ["電話", "學員電話", "phone", "Phone", "手機"]:
        if key in data:
            normalized["電話"] = data[key]
            break
    
    # 地址欄位
    for key in _ADDR_KEYS:
        if key in data:
            normalized["寄送地址"] = data[key]
            break
    
    # 書籍欄位
    for key in _BOOK_KEYS:
        if key in data:
            normalized["書籍名稱"] = data[key]
            break
    
    # 備註欄位
    for key in ["業務備註", "備註", "note", "Note"]:
        if key in data:
            normalized["業務備註"] = data[key]
            break
    
    return normalized

# ============================================
# 書目主檔快取（修復 M1：優化讀取效能）
# ============================================
_BOOK_CACHE = {"ts": 0.0, "books": []}
_BOOK_CACHE_TTL = 600  # 10 分鐘

def _load_books(force: bool = False) -> List[Dict[str, Any]]:
    """載入書目主檔（含快取機制，修復 M1）"""
    now = time.time()
    if (not force) and _BOOK_CACHE["books"] and (now - _BOOK_CACHE["ts"] < _BOOK_CACHE_TTL):
        return _BOOK_CACHE["books"]
    
    try:
        ws = _ws(BOOK_MASTER_SHEET_NAME)
        rows = ws.get_all_records()
        books = []
        for r in rows:
            if str(r.get("是否啟用", "")).strip() == "使用中":
                name = str(r.get("書籍名稱", "")).strip()
                lang = str(r.get("語別", "")).strip()
                fuzzy = str(r.get("模糊比對書名", "")).strip()
                stock = r.get("現有庫存", 0)
                if name:
                    books.append({
                        "name": name,
                        "lang": lang,
                        "fuzzy": fuzzy,
                        "stock": stock
                    })
        _BOOK_CACHE["books"] = books
        _BOOK_CACHE["ts"] = now
        app.logger.info(f"[BOOK] 已載入 {len(books)} 本書籍")
        return books
    except Exception as e:
        app.logger.error(f"[BOOK] 載入失敗: {e}")
        return _BOOK_CACHE["books"]  # 回傳舊快取

def _search_books_by_keyword(keyword: str) -> List[Dict[str, Any]]:
    """根據關鍵字搜尋書籍（處理全形/半形差異）"""
    books = _load_books()
    keyword_normalized = _normalize_text_for_search(keyword).lower()
    results = []
    
    for book in books:
        # 搜尋書名、語別、模糊比對欄位
        search_text = _normalize_text_for_search(f"{book['name']} {book['lang']} {book['fuzzy']}").lower()
        if keyword_normalized in search_text:
            results.append(book)
    
    app.logger.info(f"[BOOK] 搜尋「{keyword}」找到 {len(results)} 本")
    return results

# ============================================
# 書名比對（2026-11-13 改寫）
# 依 LINE 群組 14 個月實際輸入調整：Let’s／Let's、空白、大小寫、
# 【】與 - 寫法、韓文／韓語、初階／初級、五十音／50音、顏色、級數
# ============================================
import unicodedata

_BOOK_SYNONYMS = [
    ("韓文", "韓語"), ("日文", "日語"), ("初階", "初級"), ("基礎", "初級"),
    ("五十音", "50音"), ("四十音", "40音"), ("聽力", "listening"),
]
_BOOK_PUNCT_RE = re.compile(
    r"[\s'\"`´‘’“”\[\]【】()（）〔〕{}「」『』<>《》\-_—–:：.,，、;；!！?？|｜/／+＋&*~～]"
)
_BOOK_EDITION_RE = re.compile(
    r"\d+\s*/\s*e\b|\d+(st|nd|rd|th)\b|\(with[^)]*\)|new\s*ed|n/e|[\(（]?(整套)?\d+本[\)）]?",
    re.I,
)
_BOOK_LEVEL_WORDS = ["初級", "進階", "發音", "習作", "listening", "reading"]
_BOOK_COLORS = "紅綠藍金"
# 「現在上的課本」各種寫法（現在上課的課本、目前上的、縣在上的課本…）
_CURRENT_BOOK_NAME = "現在上的課本"
_CURRENT_BOOK_RE = re.compile(r"(現在|目前|正在|縣在|現再).{0,6}(課本|書|教材|上的|用的)|^(現在|目前|正在)上的")


def _book_prep(s: str) -> str:
    t = unicodedata.normalize("NFKC", s or "").lower()
    for a, b in _BOOK_SYNONYMS:
        t = t.replace(a, b)
    return t


def _norm_book(s: str) -> str:
    """去掉空白與符號、統一全半形與同義字，用來比對"""
    return _BOOK_PUNCT_RE.sub("", _book_prep(s))


def _book_markers(s: str) -> set:
    """抓出會區分不同書的記號：數字、級數（1A／A1）、顏色、初級／進階／發音…"""
    t = _BOOK_EDITION_RE.sub(" ", _book_prep(s))
    m = set()
    for d in re.findall(r"\d+(?:\.\d+)?", t):
        m.add("#" + d)
    for x in re.findall(r"(?<![a-z])(\d[ab]|[ab]\d)(?![a-z0-9])", t):
        m.add("L" + "".join(sorted(x)))
    for x in re.findall(r"(?<=\s)([a-z])(?=\s*-|\s*$)", t):
        m.add("S" + x)
    for c in _BOOK_COLORS:
        if c in t:
            m.add(c)
    for w in _BOOK_LEVEL_WORDS:
        if w in t:
            m.add(w)
    return m


_BOOK_INDEX = {"ts": -1.0, "books": None}


def _book_index() -> List[Dict[str, Any]]:
    """書目索引：每本書的正規化書名、別名、記號"""
    books = _load_books()
    if _BOOK_INDEX["books"] is not None and _BOOK_INDEX["ts"] == _BOOK_CACHE["ts"]:
        return _BOOK_INDEX["books"]
    idx = []
    token_owner: Dict[str, set] = {}
    for b in books:
        aliases = []
        for part in re.split(r"[,，]", b.get("fuzzy", "") or ""):
            part = part.strip()
            if not part:
                continue
            aliases.append(part)
            toks = part.split()
            if len(toks) > 1:
                for tk in toks:
                    token_owner.setdefault(_norm_book(tk), set()).add(b["name"])
        keys = {_norm_book(b["name"])} | {_norm_book(a) for a in aliases}
        keys.discard("")
        markers = set(_book_markers(b["name"]))
        for a in aliases:
            markers |= _book_markers(a)
        idx.append({"name": b["name"], "keys": keys, "markers": markers, "tokens": set()})
    # 別名欄用空白分隔的單字：只有「全書目只有這本有」才當別名（避免 go、1、the 這種字亂配）
    by_name = {x["name"]: x for x in idx}
    for tk, owners in token_owner.items():
        if tk and len(owners) == 1 and len(tk) >= 2:
            by_name[next(iter(owners))]["tokens"].add(tk)
    _BOOK_INDEX["books"] = idx
    _BOOK_INDEX["ts"] = _BOOK_CACHE["ts"]
    return idx


def _markers_compatible(inp: set, book: set) -> bool:
    return inp <= book


def _score_key(q: str, key: str) -> float:
    if not q or not key:
        return 0.0
    if q == key:
        return 1.0
    r = difflib.SequenceMatcher(None, q, key).ratio()
    short, long_ = (q, key) if len(q) <= len(key) else (key, q)
    if len(short) >= 4 and short in long_:
        r = max(r, 0.9)
    elif len(q) >= 2 and q in key:
        r = max(r, 0.75)  # 「多益」「發音」「1B」這種短詞：列進候選，不自動選
    return r


def _resolve_book(raw: str) -> Dict[str, Any]:
    """
    回傳 {"status": "ok", "book": 書名, "changed": bool}
       或 {"status": "ask", "cands": [書名...]}（有候選，要問）
       或 {"status": "none", "cands": []}
    """
    q = _norm_book(raw)
    if not q:
        return {"status": "none", "cands": []}
    idx = _book_index()

    # 1. 完全相同（書名或別名）
    for b in idx:
        if q in b["keys"] or q in b["tokens"]:
            return {"status": "ok", "book": b["name"], "changed": False}

    # 2.「現在上的課本」家族
    names = {b["name"] for b in idx}
    if _CURRENT_BOOK_NAME in names and _CURRENT_BOOK_RE.search(unicodedata.normalize("NFKC", raw)):
        return {"status": "ok", "book": _CURRENT_BOOK_NAME, "changed": True, "guess_current": True}

    # 3. 打分：記號（數字、級數、顏色、初級／進階）要對得上
    qm = _book_markers(raw)
    scored = []
    for b in idx:
        s = max(_score_key(q, k) for k in b["keys"]) if b["keys"] else 0.0
        compat = _markers_compatible(qm, b["markers"])
        scored.append({"s": s, "compat": compat, "name": b["name"], "markers": b["markers"]})

    plausible = sorted([x for x in scored if x["compat"] and x["s"] >= 0.6], key=lambda x: -x["s"])
    if plausible:
        top = plausible[0]["s"]
        second = plausible[1]["s"] if len(plausible) > 1 else 0.0
        unique = len(plausible) == 1
        if (unique and (top >= 0.7 or (qm and top >= 0.6))) or (top >= 0.85 and top - second >= 0.15):
            return {"status": "ok", "book": plausible[0]["name"], "changed": True}

    # 4. 要問：記號對得上的排前面，只列跟最高分差不多的
    ranked = sorted(scored, key=lambda x: x["s"] + (0.25 if x["compat"] else 0), reverse=True)
    best = max((x["s"] for x in scored), default=0.0)
    floor = max(0.45, best - (0.1 if best >= 0.9 else 0.25))
    cands = [x["name"] for x in ranked if x["s"] >= floor]
    # 顏色／初級進階／習作這種記號對得上的書排最前面（例：藍色多益、Let's Go 6 習作本）
    strong = {m for m in qm if m[0] not in "#LS"}
    if strong:
        hit = [x["name"] for x in sorted(scored, key=lambda x: -x["s"]) if x["compat"]]
        cands = hit[:5] + [c for c in cands if c not in hit[:5]]
    cands = cands[:8]
    if cands:
        return {"status": "ask", "cands": cands}
    return {"status": "none", "cands": []}


def _split_book_segment(seg: str) -> List[str]:
    """一段裡塞了好幾本書（「Try N5 + Try N4」「N4 N3 N2 N1」「【…進階篇】NEW TOEIC…」）時拆開；拆不了就原樣"""
    if _resolve_book(seg)["status"] == "ok":
        return [seg]
    for sep in (r"\s*[+＋]\s*", r"\s+"):
        parts = [p for p in re.split(sep, seg.strip()) if p.strip()]
        if len(parts) > 1 and all(_resolve_book(p)["status"] == "ok" for p in parts):
            return parts
    # 整段裡找得到兩本以上完整書名
    q = _norm_book(seg)
    found = []
    for b in sorted(_book_index(), key=lambda x: -max((len(k) for k in x["keys"]), default=0)):
        for k in sorted(b["keys"], key=len, reverse=True):
            if len(k) >= 5 and k in q:
                found.append(b["name"])
                q = q.replace(k, "", 1)
                break
    if len(found) >= 2 and len(q) <= 2:
        return found
    return [seg]


def _split_books(book_raw: str) -> List[str]:
    segs = [x.strip() for x in re.split(r"[,，、;；\n]+", book_raw or "") if x.strip()]
    out = []
    for s in segs:
        out.extend(_split_book_segment(s))
    return out


def _find_book_exact(name: str) -> Optional[str]:
    """有把握就回書名（含自動校正），沒把握回 None"""
    r = _resolve_book(name)
    return r["book"] if r["status"] == "ok" else None


def _suggest_books(wrong_name: str, max_results: int = MAX_BOOK_SUGGESTIONS) -> List[str]:
    r = _resolve_book(wrong_name)
    if r["status"] == "ok":
        return [r["book"]]
    return r["cands"][:max_results]

# ============================================
# 郵遞區號查詢（修復 H2）
# ============================================
def _normalize_text_for_search(text: str) -> str:
    """正規化文字用於搜尋（處理全形/半形差異）"""
    if not text:
        return ""
    
    # 全形轉半形對照表
    # 全形數字：０-９ (U+FF10 - U+FF19)
    # 全形英文：Ａ-Ｚ、ａ-ｚ (U+FF21-U+FF3A, U+FF41-U+FF5A)
    result = []
    for char in text:
        code = ord(char)
        # 全形英文和數字轉半形 (0xFF01-0xFF5E → 0x0021-0x007E)
        if 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        else:
            result.append(char)
    
    return ''.join(result)

def detect_delivery_method(text: str) -> Optional[str]:
    """偵測寄送方式（超商辨識 + 自取）"""
    if not text:
        return None
    s = unicodedata.normalize("NFKC", text).lower().replace("—", "-").replace("–", "-")
    has_no = "號" in s and "店號" not in s

    # 檢查自取
    if "自取" in s or "self" in s or "pickup" in s:
        return "自取"

    # 檢查超商（有寫品牌就算，括號裡附門市地址也一樣）
    if re.search(r"7\s*[-/]\s*11|7\s*-?\s*eleven|小七|統一超商", s):
        return "7-11"
    if "全家" in s or "family" in s:
        return "全家"
    if "萊爾富" in s or "hi-life" in s or "hilife" in s:
        return "萊爾富"
    # 只寫 711：沒有門牌號才算（避免郵遞區號 711 被誤判）；OK 超商已退出台灣，不再判斷
    if not has_no and re.search(r"(?<!\d)7\s?11(?!\d)", s):
        return "7-11"
    return None


def _looks_like_unbranded_store(text: str) -> bool:
    """「汐止力行店」「台中惠安門市」這種沒寫是哪家超商的店名"""
    s = unicodedata.normalize("NFKC", text or "").strip()
    return "號" not in s and bool(re.search(r"(門市|店)\s*$", s)) and len(s) <= 20


# ============================================
# 郵遞區號（2026-11-13 改寫：只打區名也能查、重名區要寫縣市、縣市與區對不上會擋）
# ============================================
_ZIP_CACHE = {"ts": 0.0, "rows": None}
_ZIP_CACHE_TTL = 600

_CITY_SHORT = ["台北", "新北", "桃園", "台中", "台南", "高雄", "基隆", "新竹", "嘉義", "苗栗",
               "彰化", "南投", "雲林", "屏東", "宜蘭", "花蓮", "台東", "澎湖", "金門", "連江"]


def _normalize_address_for_compare(text: str) -> str:
    """正規化地址用於比對（臺→台、全形→半形、巿→市、去空白）"""
    t = unicodedata.normalize("NFKC", text or "")
    t = t.replace("臺", "台").replace("巿", "市")
    return re.sub(r"\s+", "", t)


def _load_zip_rows() -> List[Dict[str, str]]:
    now = time.time()
    if _ZIP_CACHE["rows"] is not None and now - _ZIP_CACHE["ts"] < _ZIP_CACHE_TTL:
        return _ZIP_CACHE["rows"]
    try:
        rows = _ws(ZIPREF_SHEET_NAME).get_all_records()
        out = []
        for r in rows:
            city = _normalize_address_for_compare(str(r.get("縣市", "")))
            dist = _normalize_address_for_compare(str(r.get("區域", "")))
            z = str(r.get("郵遞區號", "")).strip()
            if z and (city or dist):
                out.append({"city": city, "dist": dist, "zip": z})
        _ZIP_CACHE["rows"] = out
        _ZIP_CACHE["ts"] = now
        return out
    except Exception as e:
        app.logger.error(f"[ZIP] 載入參照表失敗: {e}")
        return _ZIP_CACHE["rows"] or []


def _city_short(city: str) -> str:
    return city[:-1] if city and city[-1] in "市縣" else city


def _resolve_zip(address: str) -> Dict[str, Any]:
    """
    回傳 {"zip": "950", "city": "台東縣", "district": "台東市", "fill": True/False, "error": ""}
    fill=True 代表地址沒寫縣市，建單時要補上
    """
    addr = _normalize_address_for_compare(address)
    body = re.sub(r"^\d{3,6}", "", addr)  # 去掉業務自己打的郵遞區號
    rows = _load_zip_rows()
    if not rows:
        return {"zip": None, "error": "郵遞區號參照表讀取失敗，請稍後再試"}

    # 1. 縣市＋區都寫了（最長優先）
    full = []
    for r in rows:
        if r["city"] and r["dist"]:
            if r["city"] + r["dist"] in body:
                full.append((len(r["city"] + r["dist"]), r))
        elif r["dist"] and r["dist"] in body:
            full.append((len(r["dist"]), r))
    if full:
        full.sort(key=lambda x: -x[0])
        r = full[0][1]
        return {"zip": r["zip"], "city": r["city"], "district": r["dist"], "fill": False, "error": ""}

    # 2. 只寫區名（區名要帶「區鄉鎮市」，避免「光復路」被當成光復鄉）
    hits = []
    for r in rows:
        d = r["dist"]
        if not d or d == r["city"]:
            continue
        base = d[:-1] if d[-1] in "區鄉鎮市" and len(d) > 2 else d
        pat = re.escape(d) if len(d) <= 2 and d[-1] in "區鄉鎮市" else re.escape(base) + "[區鄉鎮市]"
        m = re.search(pat, body)
        if m:
            hits.append((m.start(), m.group(0), r))
    if hits:
        pos = min(h[0] for h in hits)
        hits = [h for h in hits if h[0] == pos]
        prefix = body[:pos]
        named = [c for c in _CITY_SHORT if c in prefix]
        same_city = [h for h in hits if _city_short(h[2]["city"]) in named] if named else hits
        if named and not same_city:
            d = hits[0][1]
            owners = "／".join(sorted({h[2]["city"] for h in hits}))
            return {"zip": None, "error": f"「{d}」不在{named[-1]}，是{owners}的區，請確認縣市"}
        cities = sorted({h[2]["city"] for h in same_city})
        if len(cities) > 1:
            d = same_city[0][1]
            return {"zip": None, "error": f"「{d}」有好幾個：{'／'.join(cities)}，請在前面寫縣市（例：{_city_short(cities[0])}{d}）"}
        h = same_city[0]
        return {"zip": h[2]["zip"], "city": h[2]["city"], "district": h[1], "fill": True, "pos": pos, "error": ""}

    # 3. 只寫縣市：整個縣市只有一個郵遞區號（新竹市 300、嘉義市 600）就收
    city_zips: Dict[str, set] = {}
    for r in rows:
        if r["city"]:
            city_zips.setdefault(r["city"], set()).add(r["zip"])
    head = body[:8]
    found = [c for c in sorted(city_zips, key=len, reverse=True) if c in head]
    for c in found:
        if len(city_zips[c]) == 1:
            return {"zip": next(iter(city_zips[c])), "city": c, "district": "", "fill": False, "error": ""}
    if found:
        c = found[0]
        return {"zip": None, "error": f"{c}要寫到區（例：{c}{_example_district(rows, c)}）"}
    for c in _CITY_SHORT:
        if body.startswith(c):
            full_city = next((x for x in city_zips if _city_short(x) == c), c)
            return {"zip": None, "error": f"{full_city}要寫到區（例：{full_city}{_example_district(rows, full_city)}）"}

    return {"zip": None, "error": "找不到縣市或鄉鎮區（例：台南市北區○○路1號）"}


def _example_district(rows, city: str) -> str:
    for r in rows:
        if r["city"] == city and r["dist"] and r["dist"] != city and r["dist"][-1] in "區鄉鎮市":
            return r["dist"]
    return "○○區" if city.endswith("市") else "○○鄉"


def _complete_address(address: str, res: Dict[str, Any]) -> str:
    """只打區名時，把縣市補到地址前面（區名前面的雜字一起換掉，例：北市文山區→台北市文山區）"""
    if not res.get("fill"):
        return address
    addr = _normalize_address_for_compare(address)
    body = re.sub(r"^\d{3,6}", "", addr)
    return res["city"] + body[res.get("pos", 0):]


def _find_zip_code(address: str) -> Optional[str]:
    """查詢郵遞區號（相容舊呼叫）"""
    return _resolve_zip(address).get("zip")


# ============================================
# OCR 會話管理
# ============================================
def _start_ocr_session(user_id: str):
    """開啟 OCR 會話"""
    expire = time.time() + (OCR_SESSION_TTL_MIN * 60)
    _OCR_SESSIONS[user_id] = expire
    app.logger.info(f"[OCR] 開啟會話: {user_id}")

def _has_ocr_session(user_id: str) -> bool:
    """檢查是否有有效的 OCR 會話"""
    if user_id not in _OCR_SESSIONS:
        return False
    if time.time() > _OCR_SESSIONS[user_id]:
        _OCR_SESSIONS.pop(user_id, None)
        return False
    return True

def _clear_ocr_session(user_id: str):
    """清除 OCR 會話"""
    _OCR_SESSIONS.pop(user_id, None)
    app.logger.info(f"[OCR] 關閉會話: {user_id}")

def _download_line_image_bytes(message_id: str) -> bytes:
    """下載 LINE 圖片"""
    try:
        content = line_bot_api.get_message_content(message_id)
        return b"".join(content.iter_content())
    except Exception as e:
        app.logger.error(f"[OCR] 下載圖片失敗: {e}")
        raise

def _ocr_text_from_bytes(img_bytes: bytes) -> str:
    """從圖片提取文字（修復 S1）"""
    if not _vision_client:
        raise RuntimeError("Vision API 未初始化")
    
    try:
        from google.cloud import vision
        image = vision.Image(content=img_bytes)
        response = _vision_client.text_detection(image=image)
        
        if response.error.message:
            raise RuntimeError(f"Vision API 錯誤: {response.error.message}")
        
        texts = response.text_annotations
        if texts:
            return texts[0].description
        return ""
    except Exception as e:
        app.logger.error(f"[OCR] 辨識失敗: {e}")
        raise

# ============================================
# OCR 配對邏輯
# ============================================
def _pair_ids_with_numbers(text: str) -> Tuple[List[Tuple[str, str]], List[str]]:
    """配對寄書ID與12碼單號"""
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    
    rid_pattern = re.compile(r"R\d{4}", re.IGNORECASE)
    num_pattern = re.compile(r"\d{12}")
    
    rids = []
    nums = []
    leftovers = []
    
    for line in lines:
        r_match = rid_pattern.search(line)
        n_match = num_pattern.search(line)
        
        if r_match:
            rids.append(r_match.group(0).upper())
        if n_match:
            nums.append(n_match.group(0))
        
        if not r_match and not n_match:
            leftovers.append(line)
    
    pairs = list(zip(rids, nums))
    return pairs, leftovers

def _write_ocr_results(pairs: List[Tuple[str, str]], event) -> str:
    """寫入 OCR 結果"""
    try:
        uid = getattr(event.source, "user_id", "")
        profile = line_bot_api.get_profile(uid)
        operator = profile.display_name or "系統"
    except Exception:
        operator = "系統"
    
    try:
        ws = _ws(MAIN_SHEET_NAME)
        h = _get_header_map(ws)
        all_vals = ws.get_all_values()
        
        # 支援多種表頭名稱
        idx_rid = _col_idx(h, "紀錄ID", _col_idx(h, "寄書ID", 1))
        idx_tracking = _col_idx(h, "託運單號", _col_idx(h, "已託運-12碼單號", 11))
        idx_date = _col_idx(h, "寄出日期", _col_idx(h, "已託運-寄出日期", 10))
        idx_person = _col_idx(h, "經手人", _col_idx(h, "已託運-經手人", 12))
        idx_status = _col_idx(h, "寄送狀態", _col_idx(h, "狀態", 13))
        
        success_count = 0
        not_found = []
        
        for rid, tracking_num in pairs:
            found_row = None
            for i, r in enumerate(all_vals[1:], start=2):
                if len(r) >= idx_rid and r[idx_rid - 1] == rid:
                    found_row = i
                    break
            
            if found_row:
                _safe_update_cell(ws, found_row, idx_tracking, tracking_num)
                _safe_update_cell(ws, found_row, idx_date, today_str())
                _safe_update_cell(ws, found_row, idx_person, operator)
                _safe_update_cell(ws, found_row, idx_status, "已託運")
                success_count += 1
                app.logger.info(f"[OCR] 更新 {rid} -> {tracking_num}")
            else:
                not_found.append(rid)
        
        msg = f"✅ 已更新 {success_count} 筆出貨單"
        if not_found:
            msg += f"\n\n⚠️ 找不到以下 ID：\n" + "\n".join(not_found)
        
        return msg
    except Exception as e:
        app.logger.error(f"[OCR] 寫入失敗: {e}")
        raise

# ============================================
# 寄書功能（含驗證與引導修正）
# ============================================
# 寄書建單（2026-11-13 改寫）
# 一次檢查完姓名／電話／地址／書名，能自動校正的直接校正並在回覆裡說明；
# 只有真的拿不準的書才問，可一次選多本、可直接打書名、可略過
# ============================================
PENDING_TTL_SEC = 30 * 60


def _reply(event, text: str):
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=text))


def _extract_mobile(raw: str) -> Tuple[Optional[str], str, str]:
    """從電話欄抓出第一支手機；回傳 (手機, 其他附註, 錯誤訊息)"""
    s = unicodedata.normalize("NFKC", raw or "").strip()
    if not s:
        return None, "", "電話為必填"
    m = re.search(r"09\d{2}[-\s]?\d{3}[-\s]?\d{3}(?!\d)", s)
    if m:
        phone = re.sub(r"\D", "", m.group(0))
        extra = (s[:m.start()] + " " + s[m.end():]).strip(" /,、，()（）-")
        return phone, extra, ""
    digits = re.sub(r"\D", "", s)
    if digits.startswith("09") and len(digits) != 10:
        return None, "", f"電話「{raw}」是 {len(digits)} 碼，手機應該是 10 碼"
    return None, "", f"電話格式錯誤：「{raw}」（需為 09 開頭的 10 碼手機號碼）"


def _check_address_suspicious(address: str) -> List[str]:
    """建單前地址檢查，回傳問題清單（空＝正常）"""
    issues = []
    if re.search(r"\d\s+\d", address):
        issues.append("數字中間被斷開，可能少了「巷／弄／之」")
    if "號" not in address:
        issues.append("缺少門牌「號」")
    return issues


def _prepare_order(name: str, phone_raw: str, address_raw: str, book_raw: str, biz_note: str) -> Dict[str, Any]:
    """檢查並整理一張寄書單；errors 為空才可以建單"""
    errors: List[str] = []
    notes: List[str] = [biz_note] if biz_note else []
    order: Dict[str, Any] = {"name": name, "biz_note": biz_note}

    if not name:
        errors.append("姓名為必填")

    phone, extra, perr = _extract_mobile(phone_raw)
    if perr:
        errors.append(perr)
    elif extra:
        notes.append(f"電話備註：{extra}")
    order["phone"] = phone

    # 地址
    order["address"] = address_raw
    order["method"] = ""
    order["addr_fixed"] = False
    if not address_raw:
        errors.append("地址為必填")
    else:
        method = detect_delivery_method(address_raw)
        if method:
            order["method"] = method
            # 跟舊版一樣：超商地址裡查得到郵遞區號就加在前面（不重複加）
            res = _resolve_zip(address_raw) if method != "自取" else {}
            if res.get("zip") and not res.get("fill") and WRITE_ZIP_TO_ADDRESS:
                order["address"] = f"{res['zip']} " + re.sub(r"^\d{3,6}\s*", "", address_raw.strip())
        else:
            res = _resolve_zip(address_raw)
            if not res.get("zip"):
                if _looks_like_unbranded_store(address_raw):
                    errors.append(f"「{address_raw}」看起來是超商門市，請寫是哪一家（例：全家汐止力行店、7-11 湖西門市）")
                else:
                    errors.append(f"地址「{address_raw}」：{res['error']}")
            else:
                if res.get("fill"):
                    full = _complete_address(address_raw, res)
                else:
                    full = re.sub(r"^\d{3,6}\s*", "", address_raw.strip())
                issues = _check_address_suspicious(full)
                if issues:
                    issue_str = "、".join(issues)
                    app.logger.warning(f"[ADDR_CHECK] 可疑地址: {full} | {issue_str}")
                    errors.append(f"地址可能有誤：「{full}」（{issue_str}）")
                order["method"] = "便利帶"
                order["addr_fixed"] = bool(res.get("fill"))
                order["address"] = f"{res['zip']} {full}" if WRITE_ZIP_TO_ADDRESS else full

    # 書名
    items = []
    if not book_raw:
        errors.append("書籍名稱為必填")
    else:
        for seg in _split_books(book_raw):
            r = _resolve_book(seg)
            item = {"raw": seg, "status": r["status"], "book": r.get("book"),
                    "cands": r.get("cands", []), "changed": r.get("changed", False)}
            if r.get("guess_current") and re.search(r"英|日|韓|德|法|西|多益|兒|美|文|語", seg):
                notes.append(f"書名原文：{seg}")
            items.append(item)
    order["items"] = items
    order["note"] = "；".join(n for n in notes if n)
    order["errors"] = errors
    return order


def _book_problem_hint(items: List[Dict[str, Any]]) -> List[str]:
    out = []
    for it in items:
        if it["status"] == "ask":
            out.append(f"• 書名「{it['raw']}」可能是：" + "／".join(it["cands"][:4]))
        elif it["status"] == "none":
            out.append(f"• 書名「{it['raw']}」找不到，可用「#查書名 關鍵字」查")
    return out


def _pending_book_items(order) -> List[int]:
    return [i for i, it in enumerate(order["items"]) if it["status"] in ("ask", "none")]


def _start_book_selection(event, order: Dict[str, Any]):
    _PENDING[event.source.user_id] = {
        "type": "book_selection_step",
        "expire_at": time.time() + PENDING_TTL_SEC,
        "order": order,
    }
    _show_book_selection_prompt(event, order)


def _show_book_selection_prompt(event, order: Dict[str, Any], prefix: str = ""):
    todo = _pending_book_items(order)
    it = order["items"][todo[0]]
    total = len(todo)
    more = f"（共 {total} 本要確認）" if total > 1 else ""
    lines = [prefix] if prefix else []
    if it["status"] == "ask":
        lines.append(f"📚 「{it['raw']}」要寄哪一本？{more}")
        for i, c in enumerate(it["cands"], start=1):
            lines.append(f"[{i}] {c}")
        lines.append("")
        lines.append("回覆編號；要多本可回「1 2」或「全部」")
    else:
        lines.append(f"📚 找不到「{it['raw']}」{more}")
        lines.append("可先用「#查書名 關鍵字」查")
    lines.append("也可以直接打書名、回「略過」不寄這本，或「取消」")
    _reply(event, "\n".join(lines))


def _handle_book_selection_step(event, text: str) -> bool:
    """處理選書回覆"""
    user_id = event.source.user_id
    pend = _PENDING.get(user_id)
    if not pend or pend.get("type") != "book_selection_step":
        return False
    if time.time() > pend.get("expire_at", 0):
        _PENDING.pop(user_id, None)
        _reply(event, "⏱️ 選書已超過 30 分鐘，請重新送出 #寄書")
        return True
    pend["expire_at"] = time.time() + PENDING_TTL_SEC

    order = pend["order"]
    todo = _pending_book_items(order)
    it = order["items"][todo[0]]
    ans = unicodedata.normalize("NFKC", text).strip()
    ans_clean = re.sub(r"[\[\]【】]", "", ans).strip()

    if ans_clean.upper() in ("取消", "CANCEL", "N", "NO"):
        _PENDING.pop(user_id, None)
        _reply(event, "已取消，這張寄書沒有建立")
        return True

    if ans_clean in ("略過", "跳過", "不要", "不寄"):
        it["status"] = "skip"
    elif ans_clean in ("全部", "都要", "全選") and it["status"] == "ask":
        it["status"] = "multi"
        it["books"] = list(it["cands"])
    elif re.fullmatch(r"\d+([\s,、，]+\d+)*", ans_clean) and it["status"] == "ask":
        nums = [int(x) for x in re.split(r"[\s,、，]+", ans_clean)]
        if any(n < 1 or n > len(it["cands"]) for n in nums):
            _reply(event, f"❌ 請回 1～{len(it['cands'])} 的編號")
            return True
        picked = []
        for n in nums:
            if it["cands"][n - 1] not in picked:
                picked.append(it["cands"][n - 1])
        it["status"] = "multi"
        it["books"] = picked
    else:
        r = _resolve_book(ans)
        if r["status"] == "ok":
            it["status"] = "multi"
            it["books"] = [r["book"]]
        elif r["status"] == "ask":
            it["status"] = "ask"
            it["raw"] = ans
            it["cands"] = r["cands"]
            _show_book_selection_prompt(event, order)
            return True
        else:
            _show_book_selection_prompt(event, order, prefix=f"❌ 還是找不到「{ans}」")
            return True

    if _pending_book_items(order):
        _show_book_selection_prompt(event, order)
        return True

    _PENDING.pop(user_id, None)
    if not _final_books(order):
        _reply(event, "所有書都略過了，這張寄書沒有建立")
        return True
    _create_order_confirmed(event, order)
    return True


def _final_books(order: Dict[str, Any]) -> List[str]:
    out = []
    for it in order["items"]:
        if it["status"] == "ok":
            out.append(it["book"])
        elif it["status"] == "multi":
            out.extend(it["books"])
    return out


def _handle_new_order(event, text: str):
    """處理新寄書"""
    lines_after = re.sub(r"^[#＃]\s*寄書", "", text.strip()).strip()
    data = parse_kv_lines(lines_after)

    order = _prepare_order(
        data.get("姓名", "").strip(),
        data.get("電話", "").strip(),
        data.get("寄送地址", "").strip(),
        data.get("書籍名稱", "").strip(),
        data.get("業務備註", "").strip(),
    )

    if order["errors"]:
        lines = ["⚠️ 尚未建單，請修正後重新送出 #寄書", ""]
        for i, e in enumerate(order["errors"], start=1):
            lines.append(f"{i}. {e}")
        hint = _book_problem_hint(order["items"])
        if hint:
            lines.append("")
            lines.append("另外書名也要確認：")
            lines.extend(hint)
        _reply(event, "\n".join(lines))
        return

    if _pending_book_items(order):
        _start_book_selection(event, order)
        return

    _create_order_confirmed(event, order)


def _create_order_confirmed(event, order: Dict[str, Any]):
    """寫入寄書任務（一次插入整張單的所有書）"""
    try:
        uid = getattr(event.source, "user_id", "")
        profile = line_bot_api.get_profile(uid)
        operator = profile.display_name or "系統"
    except Exception:
        operator = "系統"

    name = order["name"]
    phone = order["phone"]
    address = order["address"]
    final_books = _final_books(order)
    app.logger.info(f"[ORDER] 建立訂單 - 姓名:{name}, 電話:{phone}, 地址:{address}, 書籍:{final_books}")

    try:
        ws = _ws(MAIN_SHEET_NAME)
        h = _get_header_map(ws)
        all_vals = ws.get_all_values()
        header = all_vals[0] if all_vals else []

        idx_rid = _col_idx(h, "寄書ID", _col_idx(h, "紀錄ID", 1))
        max_num = 0
        for r in all_vals[1:]:
            if len(r) >= idx_rid:
                m = re.match(r"R(\d+)", r[idx_rid - 1] or "")
                if m:
                    max_num = max(max_num, int(m.group(1)))
        new_rid = f"R{max_num + 1:04d}"
        now_s = now_str_min()

        def put(row, keys, value):
            for k in keys:
                if k in h:
                    row[h[k] - 1] = value
                    return

        rows = []
        for book in final_books:
            row = [""] * len(header)
            put(row, ["寄書ID", "紀錄ID"], new_rid)
            put(row, ["建單日期", "建單時間"], now_s)
            put(row, ["建單人"], operator)
            put(row, ["學員姓名", "姓名"], name)
            put(row, ["學員電話", "電話"], f"'{phone}" if phone else "")
            put(row, ["寄送地址", "地址"], address)
            put(row, ["書籍名稱"], book)
            put(row, ["業務備註"], order.get("note", ""))
            put(row, ["寄送方式"], order.get("method", ""))
            put(row, ["寄送狀態", "狀態"], "待處理")
            rows.append(row)

        ws.insert_rows(rows, row=2, value_input_option="USER_ENTERED", inherit_from_before=False)
        app.logger.info(f"[ORDER] 成功建立寄書 {new_rid}: {name} / {final_books}")

        msg_lines = ["✅ 寄書建立完成"]
        msg_lines.append(f"建單日期：{now_s}")
        msg_lines.append(f"姓名：{name}  |  電話：{phone}")
        msg_lines.append(f"地址：{address}")
        msg_lines.append(f"書籍：{', '.join(final_books)}")
        msg_lines.append("狀態：待處理")
        fixes = []
        if order.get("addr_fixed"):
            fixes.append("地址已補上縣市")
        for it in order["items"]:
            if it["status"] == "ok" and it.get("changed") and _norm_book(it["raw"]) != _norm_book(it["book"]):
                fixes.append(f"「{it['raw']}」→ {it['book']}")
        if fixes:
            msg_lines.append("")
            msg_lines.append("🔎 自動對應：")
            msg_lines.extend(f"• {f}" for f in fixes)
        if order.get("note") and order.get("note") != order.get("biz_note"):
            msg_lines.append(f"備註：{order['note']}")
        _reply(event, "\n".join(msg_lines))
    except Exception as e:
        app.logger.error(f"[ORDER] 建立失敗: {e}", exc_info=True)
        _reply(event, "❌ 寄書建立失敗，Google 表可能暫時連不上，請過一分鐘再送一次")


# ============================================
# 查詢寄書
# ============================================
def _handle_query(event, text: str):
    """查詢寄書（支援多種表頭名稱）"""
    query = text.replace("#查詢寄書", "").replace("#查寄書", "").strip()
    
    if not query:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請輸入查詢關鍵字（姓名或電話後9碼）"))
        return
    
    try:
        ws = _ws(MAIN_SHEET_NAME)
        h = _get_header_map(ws)
        all_vals = ws.get_all_values()
        
        # 支援多種欄位名稱
        idx_rid = _col_idx(h, "紀錄ID", _col_idx(h, "寄書ID", 1))
        idx_name = _col_idx(h, "學員姓名", _col_idx(h, "姓名", 4))
        idx_phone = _col_idx(h, "學員電話", _col_idx(h, "電話", 5))
        idx_book = _col_idx(h, "書籍名稱", 7)
        idx_status = _col_idx(h, "寄送狀態", _col_idx(h, "狀態", 13))
        
        app.logger.info(f"[QUERY] 欄位索引 - ID:{idx_rid}, 姓名:{idx_name}, 電話:{idx_phone}, 書籍:{idx_book}, 狀態:{idx_status}")
        
        # 查詢邏輯
        query_digits = re.sub(r"\D", "", query)
        matches = []
        
        for i, r in enumerate(all_vals[1:], start=2):
            if len(r) < max(idx_rid, idx_name, idx_phone, idx_book, idx_status):
                continue
            
            name = r[idx_name - 1] if len(r) >= idx_name else ""
            phone = r[idx_phone - 1] if len(r) >= idx_phone else ""
            
            # 姓名比對
            if query in name:
                matches.append((i, r))
                continue
            
            # 電話後9碼比對
            if query_digits and len(query_digits) >= PHONE_SUFFIX_MATCH:
                phone_digits = re.sub(r"\D", "", phone)
                if phone_digits.endswith(query_digits[-PHONE_SUFFIX_MATCH:]):
                    matches.append((i, r))
                    continue
        
        if not matches:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"查無資料：{query}"))
            return
        
        # 合併同 ID
        grouped = {}
        for row_i, r in matches:
            rid = r[idx_rid - 1] if len(r) >= idx_rid else ""
            if rid not in grouped:
                grouped[rid] = {
                    "name": r[idx_name - 1] if len(r) >= idx_name else "",
                    "phone": r[idx_phone - 1] if len(r) >= idx_phone else "",
                    "status": r[idx_status - 1] if len(r) >= idx_status else "",
                    "books": []
                }
            if len(r) >= idx_book:
                grouped[rid]["books"].append(r[idx_book - 1])
        
        # 格式化輸出
        lines = [f"查詢結果（共 {len(grouped)} 筆）：\n"]
        for rid, info in list(grouped.items())[:10]:  # 最多10筆
            books_str = "、".join(info["books"])
            lines.append(f"{rid}: {info['name']}")
            lines.append(f"  電話: {info['phone']}")
            lines.append(f"  書籍: {books_str}")
            lines.append(f"  狀態: {info['status']}\n")
        
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(lines)))
    except Exception as e:
        app.logger.error(f"[QUERY] 查詢失敗: {e}", exc_info=True)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 查詢失敗: {e}"))

# ============================================
# 取消/刪除寄書（支援 ID、姓名、電話）
# ============================================
def _extract_cancel_target(text: str):
    """從取消寄書指令中提取查詢條件（姓名、電話、或 ID）"""
    body = re.sub(r"^#(取消寄書|刪除寄書)\s*", "", text.strip())
    
    # 如果直接是 R 開頭，視為 ID
    if body.startswith("R"):
        return {"type": "id", "value": body}
    
    name, phone = None, None
    
    # 嘗試解析 key:value 格式
    data = parse_kv_lines(body)
    for k in list(data.keys()):
        if any(x in k for x in ["姓名", "學員", "收件人", "名字", "貴姓"]):
            name = data.pop(k)
            break
    for k in list(data.keys()):
        if "電話" in k:
            phone = normalize_phone(data.pop(k))
            break
    
    # 如果沒有 key:value，嘗試直接解析
    if not name and not phone:
        tokens = re.split(r"\s+", body)
        for t in tokens:
            tt = t.strip()
            if not tt:
                continue
            p = normalize_phone(tt)
            if (not phone) and p:
                phone = p
                continue
            if not name and not re.search(r"\d", tt):
                name = tt
    
    if name or phone:
        return {"type": "search", "name": name, "phone": phone}
    
    return None

def _find_latest_order(ws, name: str, phone: str):
    """根據姓名或電話查找最近一筆「待處理」的訂單"""
    h = _get_header_map(ws)
    idx_rid = _col_idx(h, "紀錄ID", _col_idx(h, "寄書ID", 1))
    idx_date = _col_idx(h, "建單日期", 2)
    idx_name = _col_idx(h, "學員姓名", _col_idx(h, "姓名", 4))
    idx_phone = _col_idx(h, "學員電話", _col_idx(h, "電話", 5))
    idx_status = _col_idx(h, "寄送狀態", _col_idx(h, "狀態", 13))
    
    all_vals = ws.get_all_values()
    rows = all_vals[1:]
    
    # 電話後 N 碼比對
    phone_suffix = None
    if phone:
        pd = re.sub(r"\D+", "", phone)
        if len(pd) >= PHONE_SUFFIX_MATCH:
            phone_suffix = pd[-PHONE_SUFFIX_MATCH:]
    
    candidates = []
    for ridx, r in enumerate(rows, start=2):
        try:
            # 排除「已刪除」
            status = (r[idx_status - 1] if len(r) >= idx_status else "").strip()
            if status == "已刪除":
                continue
            
            # 只查詢「待處理」
            if status != "待處理":
                continue
            
            # 姓名比對
            if name:
                row_name = (r[idx_name - 1] if len(r) >= idx_name else "")
                if name not in row_name:
                    continue
            
            # 電話比對
            if phone_suffix:
                row_phone = re.sub(r"\D+", "", r[idx_phone - 1] if len(r) >= idx_phone else "")
                if not (len(row_phone) >= PHONE_SUFFIX_MATCH and row_phone[-PHONE_SUFFIX_MATCH:] == phone_suffix):
                    continue
            
            # 解析建單時間
            dt_str = (r[idx_date - 1] if len(r) >= idx_date else "").strip()
            dt = None
            if dt_str:
                try:
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%d %H:%M")
                except Exception:
                    dt = None
            
            key_dt = dt or datetime.min
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
    idx_rid = _col_idx(h, "紀錄ID", _col_idx(h, "寄書ID", 1))
    all_vals = ws.get_all_values()[1:]
    out = []
    for ridx, r in enumerate(all_vals, start=2):
        try:
            if len(r) >= idx_rid and (r[idx_rid - 1] or "").strip() == rid:
                out.append((ridx, r))
        except Exception:
            continue
    return out

def _handle_cancel_request(event, text: str):
    """處理取消寄書請求（支援 ID、姓名、電話）"""
    try:
        uid = getattr(event.source, "user_id", "")
        profile = line_bot_api.get_profile(uid)
        operator = profile.display_name or "系統"
    except Exception:
        operator = "系統"
    
    # 提取查詢條件
    target = _extract_cancel_target(text)
    
    if not target:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(
            text="請輸入查詢條件：\n• #取消寄書 R0001\n• #取消寄書 測試\n• #取消寄書 0930125812"
        ))
        return
    
    try:
        ws = _ws(MAIN_SHEET_NAME)
        h = _get_header_map(ws)
        
        # 支援多種表頭名稱
        idx_rid = _col_idx(h, "紀錄ID", _col_idx(h, "寄書ID", 1))
        idx_name = _col_idx(h, "學員姓名", _col_idx(h, "姓名", 4))
        idx_book = _col_idx(h, "書籍名稱", 7)
        idx_status = _col_idx(h, "寄送狀態", _col_idx(h, "狀態", 13))
        
        # 根據查詢類型處理
        if target["type"] == "id":
            # 直接用 ID 查詢
            rid = target["value"]
            all_rows = _collect_rows_by_rid(ws, rid)
            
            if not all_rows:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"找不到寄書ID：{rid}"))
                return
            
            # 檢查是否為待處理
            for row_i, r in all_rows:
                status = (r[idx_status - 1] if len(r) >= idx_status else "").strip()
                if status != "待處理":
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ {rid} 狀態為「{status}」，只能取消「待處理」的訂單"))
                    return
            
            # 取第一列的姓名
            stu_name = all_rows[0][1][idx_name - 1] if len(all_rows[0][1]) >= idx_name else ""
            
        elif target["type"] == "search":
            # 用姓名或電話查詢
            name = target.get("name")
            phone = target.get("phone")
            
            row_i, r = _find_latest_order(ws, name, phone)
            
            if not row_i:
                query_str = name or phone or "?"
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 找不到「{query_str}」的待處理訂單"))
                return
            
            rid = (r[idx_rid - 1] if len(r) >= idx_rid else "").strip()
            stu_name = (r[idx_name - 1] if len(r) >= idx_name else "").strip()
            all_rows = _collect_rows_by_rid(ws, rid)
        
        # 收集書籍列表
        book_list = "、".join([r[idx_book - 1] for _, r in all_rows if len(r) >= idx_book and r[idx_book - 1]])
        
        # 儲存待確認
        _PENDING[event.source.user_id] = {
            "type": "cancel_order",
            "expire_at": time.time() + CONFIRM_TTL_SEC,
            "sheet": MAIN_SHEET_NAME,
            "rid": rid,
            "stu": stu_name,
            "book_list": book_list,
            "rows": [row_i for row_i, _ in all_rows],
            "operator": operator,
            "idx": {
                "H": _col_idx(h, "業務備註", _col_idx(h, "備註", 8)),
                "L": _col_idx(h, "經手人", _col_idx(h, "已託運-經手人", 12)),
                "M": _col_idx(h, "寄送狀態", _col_idx(h, "狀態", 13))
            }
        }
        
        msg = f"確認刪除寄書？\n{rid}: {stu_name}\n書籍：{book_list}\n\n回覆「Y / YES / OK」確認；或回覆「N」取消。"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
    except Exception as e:
        app.logger.error(f"[CANCEL] 處理失敗: {e}", exc_info=True)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 處理失敗: {e}"))

# ============================================
# 刪除/取消出書
# ============================================
def _handle_delete_ship(event, text: str):
    """刪除出書記錄"""
    rid = text.replace("#刪除出書", "").replace("#取消出書", "").strip()
    
    if not rid:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請輸入寄書ID（例：#刪除出書 R0001）"))
        return
    
    try:
        ws = _ws(MAIN_SHEET_NAME)
        h = _get_header_map(ws)
        all_vals = ws.get_all_values()
        
        # 支援多種表頭名稱
        idx_rid = _col_idx(h, "紀錄ID", _col_idx(h, "寄書ID", 1))
        idx_tracking = _col_idx(h, "託運單號", _col_idx(h, "已託運-12碼單號", 11))
        idx_date = _col_idx(h, "寄出日期", _col_idx(h, "已託運-寄出日期", 10))
        idx_person = _col_idx(h, "經手人", _col_idx(h, "已託運-經手人", 12))
        idx_status = _col_idx(h, "寄送狀態", _col_idx(h, "狀態", 13))
        
        found = False
        changed = 0
        other_status = set()
        for i, r in enumerate(all_vals[1:], start=2):
            if len(r) >= idx_rid and r[idx_rid - 1] == rid:
                found = True
                status = (r[idx_status - 1] if len(r) >= idx_status else "").strip()
                # 只撤銷「已託運」的列；已刪除、待處理的不動（避免刪掉的單又活回來）
                if status != "已託運":
                    other_status.add(status or "空白")
                    continue
                _safe_update_cell(ws, i, idx_tracking, "")
                _safe_update_cell(ws, i, idx_date, "")
                _safe_update_cell(ws, i, idx_person, "")
                _safe_update_cell(ws, i, idx_status, "待處理")
                changed += 1

        if changed:
            app.logger.info(f"[DELETE_SHIP] 已撤銷出書: {rid} {changed} 列")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"✅ 已撤銷 {rid} 的出貨記錄（{changed} 本）"))
        elif found:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ {rid} 狀態是「{'、'.join(sorted(other_status))}」，不是已託運，沒有更動"))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"找不到寄書ID：{rid}"))
    except Exception as e:
        app.logger.error(f"[DELETE_SHIP] 失敗: {e}")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 處理失敗: {e}"))

# ============================================
# 入庫功能
# ============================================
def _ensure_stockin_sheet():
    """確保入庫明細表存在"""
    return _get_or_create_ws(STOCK_IN_SHEET_NAME, ["入庫日期", "經手人", "書名", "數量", "來源", "備註"])

def _handle_stockin(event, text: str):
    """處理入庫（支援多種格式和錯誤引導）"""
    try:
        uid = getattr(event.source, "user_id", "")
        profile = line_bot_api.get_profile(uid)
        operator = profile.display_name or "系統"
    except Exception:
        operator = "系統"
    
    # 支援多種指令
    lines_after = text.replace("#買書", "").replace("#入庫", "").replace("#進書", "").strip()
    
    if not lines_after:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="請輸入書名與數量，格式範例：\n• S2*1 或 S2 1\n• S3*2 或 S3 2\n• 雅思1*2 或 雅思1 2\n\n⚠️ 必須明確指定數量"))
        return
    
    # 解析書名與數量（支援多種格式）
    items = []
    errors = []  # 記錄找不到的書名
    
    for line in lines_after.split("\n"):
        line = line.strip()
        if not line:
            continue
        
        book_candidate = None
        qty_str = None
        
        # 優先 1：檢查明確分隔符號（*、×、x、X）
        if re.search(r'[*×xX]', line):
            parts = re.split(r'[*×xX]', line, maxsplit=1)
            if len(parts) == 2:
                book_candidate = parts[0].strip()
                qty_str = parts[1].strip()
        
        # 優先 2：空格分隔（最後一段是數字）
        elif ' ' in line:
            parts = line.rsplit(maxsplit=1)
            if len(parts) == 2 and parts[1].strip().lstrip('-').isdigit():
                book_candidate = parts[0].strip()
                qty_str = parts[1].strip()
        
        # 如果沒有明確分隔符號或空格，跳過該行
        # （避免 s2、雅思1 等被誤判為「s × 2」、「雅思 × 1」）
        
        # 驗證數量
        if not qty_str or not book_candidate:
            continue
        
        try:
            qty = int(qty_str)
        except ValueError:
            continue
        
        # 查找書名
        matched = _find_book_exact(book_candidate)
        if matched:
            items.append({"name": matched, "qty": qty, "input": book_candidate})
        else:
            # 找不到，記錄錯誤並嘗試建議
            suggestions = _suggest_books(book_candidate, max_results=5)
            errors.append({
                "input": book_candidate,
                "qty": qty,
                "suggestions": suggestions
            })
    
    # 情況 1：完全找不到任何書
    if not items and not errors:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 無法辨識書名或數量\n\n請使用格式：\n• 書名*數量（如 S2*1）\n• 書名 數量（如 S2 1）\n\n或使用「#查書名」確認正確書名"))
        return
    
    # 情況 2：有錯誤（找不到的書名）
    if errors:
        # 儲存待修正狀態
        _PENDING[event.source.user_id] = {
            "type": "stockin_correction",
            "operator": operator,
            "items": items,  # 已找到的書
            "errors": errors  # 待修正的書
        }
        
        # 建立錯誤訊息
        msg_lines = []
        
        if items:
            msg_lines.append("✅ 已識別以下書籍：")
            for it in items:
                msg_lines.append(f"• {it['name']} × {it['qty']}")
            msg_lines.append("")
        
        msg_lines.append("❌ 以下書名需要確認：\n")
        
        for idx, err in enumerate(errors, start=1):
            msg_lines.append(f"{idx}. 「{err['input']}」× {err['qty']}")
            if err['suggestions']:
                msg_lines.append("   可能是：")
                for i, sug in enumerate(err['suggestions'][:3], start=1):
                    msg_lines.append(f"   {i}. {sug}")
            else:
                msg_lines.append("   ⚠️ 找不到類似書籍")
            msg_lines.append("")
        
        msg_lines.append("請回覆：")
        msg_lines.append("• 數字選擇建議書籍（如「1」）")
        msg_lines.append("• 或輸入正確書名")
        msg_lines.append("• 或回覆「取消」放棄")
        
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(msg_lines)))
        return
    
    # 情況 3：全部找到，合併相同書名
    merged = {}
    for it in items:
        merged[it["name"]] = merged.get(it["name"], 0) + int(it["qty"])
    final_items = [{"name": k, "qty": v} for k, v in merged.items()]
    
    has_negative = any(it["qty"] < 0 for it in final_items)
    
    # 儲存待確認
    _PENDING[event.source.user_id] = {
        "type": "stock_in_confirm",
        "expire_at": time.time() + CONFIRM_TTL_SEC,
        "operator": operator,
        "items": final_items
    }
    
    lines = [f"• {it['name']} × {it['qty']}" for it in final_items]
    suffix = "\n\n※ 含負數（自動標示來源：盤點調整）" if has_negative else ""
    msg = "請確認入庫項目：\n" + "\n".join(lines) + suffix + "\n\n回覆「OK / YES / Y」確認；或回覆「N」取消。"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

def _handle_stockin_correction(event, text: str) -> bool:
    """處理入庫修正流程"""
    pend = _PENDING.get(event.source.user_id)
    if not pend or pend.get("type") != "stockin_correction":
        return False
    
    # 移除可能重複輸入的指令
    user_input = text.strip()
    user_input = user_input.replace("#買書", "").replace("#入庫", "").replace("#進書", "").strip()
    
    # 檢查是否取消
    if user_input.upper() in ("取消", "N", "NO", ""):
        _PENDING.pop(event.source.user_id, None)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="已取消入庫"))
        return True
    
    errors = pend.get("errors", [])
    if not errors:
        return False
    
    # 取第一個錯誤進行處理
    first_error = errors[0]
    
    # 情況 1：使用者輸入數字選擇建議書籍
    if user_input.isdigit():
        choice = int(user_input)
        suggestions = first_error.get("suggestions", [])
        
        if 1 <= choice <= len(suggestions):
            selected_book = suggestions[choice - 1]
            
            # 加入已找到的書
            pend["items"].append({
                "name": selected_book,
                "qty": first_error["qty"],
                "input": first_error["input"]
            })
            
            # 移除已處理的錯誤
            errors.pop(0)
            
            # 檢查是否還有其他錯誤
            if errors:
                # 繼續處理下一個錯誤
                _show_next_stockin_error(event, pend)
            else:
                # 全部處理完成，進入確認流程
                _finalize_stockin_items(event, pend)
            
            return True
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 請輸入 1-{len(suggestions)} 的數字"))
            return True
    
    # 情況 2：使用者直接輸入書名
    matched = _find_book_exact(user_input)
    if matched:
        # 加入已找到的書
        pend["items"].append({
            "name": matched,
            "qty": first_error["qty"],
            "input": first_error["input"]
        })
        
        # 移除已處理的錯誤
        errors.pop(0)
        
        # 檢查是否還有其他錯誤
        if errors:
            _show_next_stockin_error(event, pend)
        else:
            _finalize_stockin_items(event, pend)
        
        return True
    else:
        # 還是找不到
        suggestions = _suggest_books(user_input, max_results=5)
        if suggestions:
            msg_lines = [f"找不到「{user_input}」，可能是："]
            for i, sug in enumerate(suggestions[:3], start=1):
                msg_lines.append(f"{i}. {sug}")
            msg_lines.append("\n請輸入數字選擇，或重新輸入正確書名")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(msg_lines)))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 找不到「{user_input}」，請使用「#查書名」確認正確書名，或回覆「取消」"))
        return True

def _show_next_stockin_error(event, pend):
    """顯示下一個待修正的書名"""
    errors = pend.get("errors", [])
    if not errors:
        return
    
    err = errors[0]
    msg_lines = [f"請確認「{err['input']}」× {err['qty']}：\n"]
    
    if err['suggestions']:
        for i, sug in enumerate(err['suggestions'][:3], start=1):
            msg_lines.append(f"{i}. {sug}")
        msg_lines.append("\n請輸入數字選擇，或輸入正確書名")
    else:
        msg_lines.append("⚠️ 找不到類似書籍")
        msg_lines.append("請輸入正確書名，或回覆「取消」")
    
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(msg_lines)))

def _finalize_stockin_items(event, pend):
    """完成入庫修正，進入最終確認"""
    # 合併相同書名
    merged = {}
    for it in pend["items"]:
        merged[it["name"]] = merged.get(it["name"], 0) + int(it["qty"])
    final_items = [{"name": k, "qty": v} for k, v in merged.items()]
    
    has_negative = any(it["qty"] < 0 for it in final_items)
    
    # 更新為確認狀態
    pend["type"] = "stock_in_confirm"
    pend["expire_at"] = time.time() + CONFIRM_TTL_SEC
    pend["items"] = final_items
    pend.pop("errors", None)
    
    lines = [f"• {it['name']} × {it['qty']}" for it in final_items]
    suffix = "\n\n※ 含負數（自動標示來源：盤點調整）" if has_negative else ""
    msg = "請確認入庫項目：\n" + "\n".join(lines) + suffix + "\n\n回覆「OK / YES / Y」確認；或回覆「N」取消。"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

def _write_stockin_rows(operator: str, items: list):
    """寫入入庫記錄"""
    ws = _ensure_stockin_sheet()
    rows = []
    for it in items:
        qty = int(it["qty"])
        source = "購買" if qty >= 0 else "盤點調整"
        rows.append([today_str(), operator, it["name"], qty, source, ""])
    _safe_append_rows(ws, rows)

# ============================================
# 新功能：#查書名
# ============================================
def _handle_search_books(event, text: str):
    """處理查書名指令（新功能）"""
    keyword = text.replace("#查書名", "").strip()
    
    if not keyword:
        msg = (
            "📚 查書名使用說明：\n\n"
            "請輸入關鍵字搜尋書籍，例如：\n"
            "• #查書名 韓文\n"
            "• #查書名 首爾\n"
            "• #查書名 startup\n"
            "• #查書名 多益\n"
            "• #查書名 兒童\n\n"
            "系統會列出所有符合的書籍名稱"
        )
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        return
    
    results = _search_books_by_keyword(keyword)
    
    if not results:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"找不到包含「{keyword}」的書籍"))
        return
    
    # 依語別分組
    grouped = {}
    for book in results:
        lang = book["lang"] or "其他"
        if lang not in grouped:
            grouped[lang] = []
        grouped[lang].append(book)
    
    # 格式化輸出
    lines = [f"找到 {len(results)} 本書籍：\n"]
    
    for lang, books in sorted(grouped.items()):
        lines.append(f"【{lang}】")
        for book in books[:20]:  # 每類最多20本
            stock_info = f"（庫存 {book['stock']}）" if book['stock'] != "" else ""
            lines.append(f"  • {book['name']} {stock_info}")
        if len(books) > 20:
            lines.append(f"  ... 還有 {len(books) - 20} 本")
        lines.append("")
    
    # LINE 訊息長度限制
    msg = "\n".join(lines)
    if len(msg) > 4500:
        msg = msg[:4500] + "\n\n⚠️ 結果過多，已截斷。請使用更精確的關鍵字。"
    
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

# ============================================
# 整理寄書（保留原功能）
# ============================================
def _handle_organize_order(event, text: str):
    """整理寄書功能（修復 M2：統一命名）"""
    lines_after = text.replace("#整理寄書", "").strip()
    data = parse_kv_lines(lines_after)
    
    name = data.get("姓名", "").strip()
    phone = data.get("電話", "").strip()
    address = data.get("寄送地址", "").strip()
    book_raw = data.get("書籍名稱", "").strip()
    biz_note = data.get("業務備註", "").strip()
    
    if not all([name, phone, address, book_raw]):
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 資料不完整（需：姓名、電話、地址、書籍名稱）"))
        return
    
    _PENDING[event.source.user_id] = {
        "type": "organize_order_confirm",
        "expire_at": time.time() + CONFIRM_TTL_SEC,
        "data": {
            "name": name,
            "phone": phone,
            "address": address,
            "book_raw": book_raw,
            "biz_note": biz_note
        }
    }
    
    msg = f"確認建立寄書？\n姓名：{name}\n電話：{phone}\n地址：{address}\n書籍：{book_raw}\n\n回覆「Y / YES / OK」確認；或回覆「N」取消。"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

# ============================================
# 處理待確認回答
# ============================================
CONFIRM_TTL_SEC = 10 * 60


def _handle_pending_answer(event, text: str) -> bool:
    """處理待確認回答"""
    user_id = event.source.user_id
    pend = _PENDING.get(user_id)
    if not pend:
        return False

    # 處理逐本選書流程
    if pend.get("type") == "book_selection_step":
        return _handle_book_selection_step(event, text)

    # 確認類（取消寄書、入庫、整理寄書）超過 10 分鐘就作廢，避免隔很久回一句 OK 被當成確認
    if pend.get("type") != "stockin_correction" and time.time() > pend.get("expire_at", time.time() + 1):
        _PENDING.pop(user_id, None)
        return False

    ans = unicodedata.normalize("NFKC", text).strip().upper()

    # 取消
    if ans in ("N", "NO", "取消"):
        _PENDING.pop(user_id, None)
        _reply(event, "已取消。")
        return True

    # 處理入庫修正（數字選擇或輸入書名）
    if pend.get("type") == "stockin_correction":
        return _handle_stockin_correction(event, text)

    # 確認
    if ans not in ("Y", "YES", "OK"):
        return False

    # 取消寄書：按確認當下用紀錄ID重新找列（中間有人建單，列號會往下移）
    if pend["type"] == "cancel_order":
        _PENDING.pop(user_id, None)
        ws = _ws(pend["sheet"])
        h = _get_header_map(ws)
        idxH = _col_idx(h, "業務備註", _col_idx(h, "備註", 8))
        idxL = _col_idx(h, "經手人", _col_idx(h, "已託運-經手人", 12))
        idxM = _col_idx(h, "寄送狀態", _col_idx(h, "狀態", 13))
        rows = _collect_rows_by_rid(ws, pend["rid"])
        todo = [(i, r) for i, r in rows if (r[idxM - 1] if len(r) >= idxM else "").strip() == "待處理"]
        if not todo:
            _reply(event, f"❌ {pend['rid']} 已經不是「待處理」，沒有刪除")
            return True
        append_note = f"[已刪除 {now_str_min()}]"
        for row_i, r in todo:
            curr_h = r[idxH - 1] if len(r) >= idxH else ""
            new_h = (curr_h + " " + append_note).strip() if curr_h else append_note
            _safe_update_cell(ws, row_i, idxH, new_h)
            _safe_update_cell(ws, row_i, idxL, pend["operator"])
            _safe_update_cell(ws, row_i, idxM, "已刪除")

        _reply(event, f"✅ 已刪除整筆寄書（{pend['rid']}）：{pend['stu']} 的 {pend['book_list']}")
        return True

    # 入庫確認
    if pend["type"] == "stock_in_confirm":
        _PENDING.pop(user_id, None)
        _write_stockin_rows(pend["operator"], pend["items"])
        lines = [f"{it['name']} × {it['qty']}" for it in pend["items"]]
        _reply(event, "✅ 入庫完成：\n" + "\n".join(lines))
        return True

    # 整理寄書確認
    if pend["type"] == "organize_order_confirm":
        _PENDING.pop(user_id, None)
        data = pend["data"]
        fake_text = (
            "#寄書\n"
            f"姓名：{data['name']}\n"
            f"電話：{data['phone']}\n"
            f"寄送地址：{data['address']}\n"
            f"書籍名稱：{data['book_raw']}\n"
            f"業務備註：{data['biz_note']}"
        )
        _handle_new_order(event, fake_text)
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
    except Exception as e:
        app.logger.exception(f"[CALLBACK] 未捕捉的例外: {e}")
        return "OK"
    return "OK"

@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    """處理文字訊息"""
    text = (event.message.text or "").strip()
    # 全形＃、「# 寄書」中間有空白都當指令
    text = re.sub(r"^[#＃]\s*", "#", text)
    
    # #我的ID（不受白名單限制）
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
    
    # 打了新指令就放棄上一個沒完成的流程（不再卡「您有未完成的選書流程」）
    if text.startswith("#"):
        _PENDING.pop(getattr(event.source, "user_id", ""), None)

    # 待確認流程
    if _handle_pending_answer(event, text):
        return
    
    # 白名單檢查
    if not _ensure_authorized(event, scope="text"):
        return
    
    # 處理指令
    if text.startswith("#查書名"):
        _handle_search_books(event, text)
        return
    
    if text.startswith("#整理寄書"):
        _handle_organize_order(event, text)
        return
    
    if text.startswith("#寄書"):
        _handle_new_order(event, text)
        return
    
    if text.startswith("#查詢寄書") or text.startswith("#查寄書"):
        _handle_query(event, text)
        return
    
    if text.startswith("#取消寄書") or text.startswith("#刪除寄書"):
        _handle_cancel_request(event, text)
        return
    
    if text.startswith("#刪除出書") or text.startswith("#取消出書"):
        _handle_delete_ship(event, text)
        return
    
    if text.startswith("#出書"):
        _start_ocr_session(getattr(event.source, "user_id", ""))
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"已啟用出書OCR（{OCR_SESSION_TTL_MIN} 分鐘）。請上傳出貨單照片。"))
        return
    
    if text.startswith("#買書") or text.startswith("#入庫") or text.startswith("#進書"):
        _handle_stockin(event, text)
        return
    
    # 其他文字不處理

@handler.add(MessageEvent, message=ImageMessage)
def handle_image_message(event):
    """處理圖片訊息"""
    if not _ensure_authorized(event, scope="image"):
        return
    
    uid = getattr(event.source, "user_id", "")
    if not _has_ocr_session(uid):
        return
    
    try:
        app.logger.info(f"[IMG] 收到圖片 user_id={uid} msg_id={event.message.id}")
        img_bytes = _download_line_image_bytes(event.message.id)
        
        if not _vision_client:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="❌ OCR 錯誤：Vision 未初始化（請設定 GOOGLE_SERVICE_ACCOUNT_JSON_NEW 並啟用 Vision API）。")
            )
            return
        
        text = _ocr_text_from_bytes(img_bytes)
        if LOG_OCR_RAW:
            app.logger.info(f"[OCR_TEXT]\n{text}")
        
        pairs, leftovers = _pair_ids_with_numbers(text)
        resp = _write_ocr_results(pairs, event)
        
        if leftovers:
            resp += "\n\n❗以下項目需人工檢核：\n" + "\n".join(leftovers[:MAX_LEFTOVER_ITEMS])
        
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=resp))
    except Exception as e:
        code = datetime.now(TZ).strftime("%Y%m%d%H%M%S")
        app.logger.exception("[OCR_ERROR]")
        try:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ OCR 錯誤（代碼 {code}）：{e}"))
        except Exception:
            pass
    finally:
        _clear_ocr_session(uid)

@app.route("/", methods=["GET"])
def index():
    """健康檢查"""
    try:
        names = [ws.title for ws in ss.worksheets()]
        return "OK / Worksheets: " + ", ".join(names)
    except Exception as e:
        return f"OK / (Sheets not loaded) {e}"

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
