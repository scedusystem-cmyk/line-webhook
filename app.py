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
from classplus_handler import parse_student_info, run_classplus_task, format_result_message
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
        
        sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_NEW", "")
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
    
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_NEW", "")
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

def parse_kv_lines(text: str) -> Dict[str, str]:
    """解析 key:value 格式文字，支援多種欄位名稱"""
    data = {}
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        if "：" in line:
            k, v = line.split("：", 1)
            data[k.strip()] = v.strip()
        elif ":" in line:
            k, v = line.split(":", 1)
            data[k.strip()] = v.strip()
    
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
    for key in ["寄送地址", "地址", "address", "Address"]:
        if key in data:
            normalized["寄送地址"] = data[key]
            break
    
    # 書籍欄位
    for key in ["書籍名稱", "書名", "book", "Book", "書籍"]:
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

def _find_book_exact(name: str) -> Optional[str]:
    """精確查找書名（處理全形/半形差異）"""
    books = _load_books()
    name_normalized = _normalize_text_for_search(name).lower().strip()
    
    # 1. 精確比對書名
    for book in books:
        book_name_normalized = _normalize_text_for_search(book["name"]).lower()
        if book_name_normalized == name_normalized:
            return book["name"]
    
    # 2. 模糊比對欄位（支援逗號和空格分隔）
    for book in books:
        fuzzy_normalized = _normalize_text_for_search(book["fuzzy"]).lower()
        # 先用逗號切分，再用空格切分
        fuzzy_names = []
        for part in fuzzy_normalized.split(','):
            fuzzy_names.extend([x.strip() for x in part.split() if x.strip()])
        if name_normalized in fuzzy_names:
            return book["name"]
    
    return None

def _suggest_books(wrong_name: str, max_results: int = MAX_BOOK_SUGGESTIONS) -> List[str]:
    """根據錯誤書名建議選項（優先關鍵字搜尋，處理全形/半形）"""
    books = _load_books()
    wrong_normalized = _normalize_text_for_search(wrong_name).lower().strip()
    
    # 策略 1：關鍵字搜尋（搜尋書名和模糊欄位）
    keyword_matches = []
    for book in books:
        search_text = _normalize_text_for_search(f"{book['name']} {book['fuzzy']}").lower()
        if wrong_normalized in search_text:
            keyword_matches.append(book["name"])
    
    if keyword_matches:
        app.logger.info(f"[BOOK] 關鍵字「{wrong_name}」找到 {len(keyword_matches)} 本書")
        return keyword_matches[:max_results]
    
    # 策略 2：模糊比對欄位精確匹配（支援逗號和空格分隔）
    for book in books:
        fuzzy_normalized = _normalize_text_for_search(book["fuzzy"]).lower()
        # 先用逗號切分，再用空格切分
        fuzzy_names = []
        for part in fuzzy_normalized.split(','):
            fuzzy_names.extend([x.strip() for x in part.split() if x.strip()])
        if wrong_normalized in fuzzy_names:
            app.logger.info(f"[BOOK] 模糊欄位精確匹配「{wrong_name}」→ {book['name']}")
            return [book["name"]]
    
    # 策略 3：相似度比對（difflib）
    candidates = []
    for book in books:
        # 比對書名
        book_name_normalized = _normalize_text_for_search(book["name"]).lower()
        ratio = difflib.SequenceMatcher(None, wrong_normalized, book_name_normalized).ratio()
        candidates.append((ratio, book["name"]))
        
        # 比對模糊欄位（支援逗號和空格分隔）
        fuzzy_normalized = _normalize_text_for_search(book["fuzzy"]).lower()
        fuzzy_names = []
        for part in fuzzy_normalized.split(','):
            fuzzy_names.extend([x.strip() for x in part.split() if x.strip()])
        for fuzzy in fuzzy_names:
            if fuzzy.strip():
                ratio2 = difflib.SequenceMatcher(None, wrong_normalized, fuzzy.strip()).ratio()
                candidates.append((ratio2, book["name"]))
    
    # 排序並去重
    candidates = sorted(set(candidates), key=lambda x: x[0], reverse=True)
    results = [name for score, name in candidates if score >= FUZZY_THRESHOLD]
    
    # 去重並限制數量
    seen = set()
    unique_results = []
    for name in results:
        if name not in seen:
            seen.add(name)
            unique_results.append(name)
            if len(unique_results) >= max_results:
                break
    
    if unique_results:
        app.logger.info(f"[BOOK] 相似度匹配「{wrong_name}」找到 {len(unique_results)} 本書")
    else:
        app.logger.info(f"[BOOK] 找不到「{wrong_name}」的建議書籍")
    
    return unique_results

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
    s = text.lower().replace("—", "-").replace("／", "/")
    
    # 檢查自取
    if "自取" in text or "self" in s or "pickup" in s:
        return "自取"
    
    # 檢查超商
    if any(k in s for k in ["7-11", "7/11", "7／11", "7–11", "711", "小七"]):
        return "7-11"
    if "全家" in s or "family" in s:
        return "全家"
    if "萊爾富" in s or "hi-life" in s or "hilife" in s:
        return "萊爾富"
    if "ok" in s or "ok超商" in s:
        return "OK"
    
    return None

def _normalize_address_for_compare(text: str) -> str:
    """正規化地址用於比對（處理台/臺差異）"""
    # 統一將「臺」轉換為「台」進行比對
    return text.replace("臺", "台").replace("台", "台")

def _find_zip_code(address: str) -> Optional[str]:
    """查詢郵遞區號（支援縣市+區域匹配，最長匹配優先）"""
    try:
        ws = _ws(ZIPREF_SHEET_NAME)
        rows = ws.get_all_records()
        
        # 正規化地址
        address_normalized = _normalize_address_for_compare(address)
        
        # 收集所有匹配的區域，並按長度排序（最長優先）
        matches = []
        for row in rows:
            # 支援兩種格式：
            # 格式1: 只有「區域」欄位（例：台南市北區）
            # 格式2: 分別有「縣市」和「區域」欄位
            
            city = str(row.get("縣市", "")).strip()
            district = str(row.get("區域", "")).strip()
            zip_code = str(row.get("郵遞區號", "")).strip()
            
            if not zip_code:
                continue
            
            # 建構完整區域名稱
            if city and district:
                # 格式2: 縣市 + 區域
                full_district = f"{city}{district}"
            elif district:
                # 格式1: 只有區域
                full_district = district
            else:
                continue
            
            # 正規化並比對
            full_district_normalized = _normalize_address_for_compare(full_district)
            
            if full_district_normalized in address_normalized:
                matches.append((len(full_district_normalized), zip_code, full_district))
        
        if matches:
            # 按匹配長度降序排序，取最長的
            matches.sort(key=lambda x: x[0], reverse=True)
            best_match = matches[0]
            app.logger.info(f"[ZIP] 找到郵遞區號 {best_match[1]} for {best_match[2]} (原地址: {address})")
            return best_match[1]
        
        app.logger.warning(f"[ZIP] 找不到郵遞區號: {address}")
        return None
    except Exception as e:
        app.logger.error(f"[ZIP] 查詢失敗: {e}")
        return None

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
def _validate_order_data(data: Dict[str, str]) -> Dict[str, List[str]]:
    """驗證寄書資料，只回傳真正有問題的欄位（支援多書逐本確認）"""
    errors = {
        "name": [],
        "phone": [],
        "address": [],
        "books": []
    }
    
    # 驗證姓名：有填就好
    name = data.get("name", "").strip()
    if not name:
        errors["name"].append("姓名為必填")
    
    # 驗證電話：09 開頭 + 10 位數
    phone_raw = data.get("phone", "").strip()
    if not phone_raw:
        errors["phone"].append("電話為必填")
    else:
        phone = normalize_phone(phone_raw)
        if not phone:
            errors["phone"].append(f"電話格式錯誤：「{phone_raw}」（需為 09 開頭的 10 碼手機號碼）")
    
    # 驗證地址：檢查是否為超商、自取、或是否有郵遞區號
    address = data.get("address", "").strip()
    if not address:
        errors["address"].append("地址為必填")
    else:
        # 檢查是否為超商或自取
        delivery_method = detect_delivery_method(address)
        if delivery_method:
            # 是超商或自取，放行
            app.logger.info(f"[VALIDATION] 偵測到寄送方式: {delivery_method}")
        else:
            # 不是超商或自取，需要郵遞區號
            zip_code = _find_zip_code(address)
            if not zip_code:
                errors["address"].append(f"找不到郵遞區號：「{address}」（請補充完整地址含區域，例：台南市北區）")
    
    # 驗證書籍（收集所有錯誤書名，但不立即提示建議）
    book_raw = data.get("book", "").strip()
    if not book_raw:
        errors["books"].append("書籍名稱為必填")
    else:
        book_names = [x.strip() for x in re.split(r"[,，、;；\n]+", book_raw) if x.strip()]
        invalid_books = []
        
        for book_name in book_names:
            matched = _find_book_exact(book_name)
            if not matched:
                invalid_books.append(book_name)
        
        if invalid_books:
            # 只記錄錯誤的書名，不在這裡產生建議
            errors["books"] = invalid_books
    
    # 移除空錯誤（只回傳真正有問題的）
    return {k: v for k, v in errors.items() if v}

def _format_validation_errors_simple(errors: Dict[str, List]) -> str:
    """格式化簡單驗證錯誤訊息（姓名、電話、地址）"""
    lines = ["❌ 發現以下問題：\n"]
    error_num = 1
    
    if "name" in errors:
        for err in errors["name"]:
            lines.append(f"{error_num}. {err}")
            error_num += 1
    
    if "phone" in errors:
        for err in errors["phone"]:
            lines.append(f"{error_num}. {err}")
            error_num += 1
    
    if "address" in errors:
        for err in errors["address"]:
            lines.append(f"{error_num}. {err}")
            error_num += 1
    
    lines.append("\n請重新輸入完整 #寄書 資料")
    return "\n".join(lines)

def _start_book_selection(event, validation_data: Dict, invalid_books: List[str], biz_note: str):
    """啟動逐本選書流程（新功能）"""
    user_id = event.source.user_id
    
    # 為每本錯誤的書找建議
    books_with_suggestions = []
    for wrong_name in invalid_books:
        suggestions = _suggest_books(wrong_name)
        books_with_suggestions.append({
            "wrong": wrong_name,
            "suggestions": suggestions
        })
    
    # 找到第一本有建議的書
    current_book = None
    for book_info in books_with_suggestions:
        if book_info["suggestions"]:
            current_book = book_info
            break
    
    if not current_book:
        # 所有書都找不到建議
        msg = "❌ 找不到以下書籍：\n"
        msg += "\n".join([f"• {b['wrong']}" for b in books_with_suggestions])
        msg += "\n\n請使用「#查書名」確認正確書名"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        return
    
    # 儲存選書狀態（加入超時機制）
    _PENDING[user_id] = {
        "type": "book_selection_step",
        "expire_at": time.time() + 300,  # 5分鐘超時
        "validation_data": validation_data,
        "biz_note": biz_note,
        "all_books": books_with_suggestions,
        "current_index": 0,
        "selected_books": []
    }
    
    # 顯示第一本書的選單
    _show_book_selection_prompt(event, current_book, 1, len(books_with_suggestions))

def _show_book_selection_prompt(event, book_info: Dict, current: int, total: int):
    """顯示選書提示（新函式）"""
    lines = [f"❌ 找不到書籍：「{book_info['wrong']}」（第 {current}/{total} 本）\n"]
    
    if book_info["suggestions"]:
        lines.append("💡 建議書籍：")
        for i, sugg in enumerate(book_info["suggestions"], start=1):
            lines.append(f"[{i}] {sugg}")
    
    lines.append("\n請回覆數字選擇，或回覆「取消」結束")
    
    msg = "\n".join(lines)
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))

def _handle_book_selection_step(event, text: str) -> bool:
    """處理逐本選書流程（新函式）"""
    user_id = event.source.user_id
    pend = _PENDING.get(user_id)
    
    if not pend or pend.get("type") != "book_selection_step":
        return False
    
    # 檢查超時
    if time.time() > pend.get("expire_at", 0):
        _PENDING.pop(user_id, None)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⏱️ 選書流程已超時，請重新輸入 #寄書"))
        return True
    
    ans = text.strip().upper()
    
    # 取消
    if ans in ("取消", "CANCEL", "N"):
        _PENDING.pop(user_id, None)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="已取消選書流程"))
        return True
    
    # 檢查是否為數字
    if not ans.isdigit():
        return False
    
    choice = int(ans)
    current_index = pend["current_index"]
    all_books = pend["all_books"]
    current_book = all_books[current_index]
    
    # 檢查選擇是否有效
    if choice < 1 or choice > len(current_book["suggestions"]):
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 無效的選項，請選擇 1-{len(current_book['suggestions'])}"))
        return True
    
    # 記錄選擇
    selected = current_book["suggestions"][choice - 1]
    pend["selected_books"].append(selected)
    app.logger.info(f"[BOOK] 使用者選擇：{current_book['wrong']} → {selected}")
    
    # 移到下一本
    next_index = current_index + 1
    
    # 找下一本有建議的書
    next_book = None
    while next_index < len(all_books):
        if all_books[next_index]["suggestions"]:
            next_book = all_books[next_index]
            pend["current_index"] = next_index
            break
        next_index += 1
    
    if next_book:
        # 還有下一本，繼續選
        _show_book_selection_prompt(event, next_book, next_index + 1, len(all_books))
        return True
    else:
        # 全部選完，建立訂單
        validation_data = pend["validation_data"]
        biz_note = pend["biz_note"]
        
        # 組合最終書名（包含已選擇的和原本正確的）
        original_books = [x.strip() for x in re.split(r"[,，、;；\n]+", validation_data["book"]) if x.strip()]
        final_books = []
        
        selected_index = 0
        for book_name in original_books:
            matched = _find_book_exact(book_name)
            if matched:
                final_books.append(matched)
            else:
                if selected_index < len(pend["selected_books"]):
                    final_books.append(pend["selected_books"][selected_index])
                    selected_index += 1
        
        final_book_str = "、".join(final_books)
        
        _PENDING.pop(user_id, None)
        _create_order_confirmed(
            event,
            validation_data["name"],
            validation_data["phone"],
            validation_data["address"],
            final_book_str,
            biz_note
        )
        return True

def _format_validation_errors(errors: Dict[str, List]) -> str:
    """格式化驗證錯誤訊息（只顯示真正有問題的欄位）"""
    lines = ["❌ 發現以下問題：\n"]
    error_num = 1
    
    # 姓名錯誤
    if "name" in errors:
        for err in errors["name"]:
            lines.append(f"{error_num}. {err}")
            error_num += 1
    
    # 電話錯誤
    if "phone" in errors:
        for err in errors["phone"]:
            lines.append(f"{error_num}. {err}")
            error_num += 1
    
    # 地址錯誤
    if "address" in errors:
        for err in errors["address"]:
            lines.append(f"{error_num}. {err}")
            error_num += 1
    
    # 書籍錯誤
    if "books" in errors:
        for err_item in errors["books"]:
            if isinstance(err_item, dict):
                wrong = err_item["wrong"]
                suggestions = err_item["suggestions"]
                lines.append(f"{error_num}. 書籍名稱錯誤：「{wrong}」")
                if suggestions:
                    lines.append("   → 建議書籍：")
                    for i, sugg in enumerate(suggestions, start=1):
                        lines.append(f"     [{i}] {sugg}")
                else:
                    lines.append("   → 找不到相似書籍，請使用「#查書名」確認")
                error_num += 1
            else:
                lines.append(f"{error_num}. {err_item}")
                error_num += 1
    
    lines.append("\n---")
    lines.append("📝 修正方式：")
    
    # 根據錯誤類型給予不同提示
    if "books" in errors and any(isinstance(e, dict) and e.get("suggestions") for e in errors["books"]):
        lines.append("• 書籍請回覆數字選擇（例：1）")
    
    if "name" in errors or "phone" in errors or "address" in errors:
        lines.append("• 請重新輸入完整 #寄書 資料（含修正項目）")
    
    lines.append("• 或回覆「N」取消本次登記")
    
    return "\n".join(lines)

def _handle_new_order(event, text: str):
    """處理新寄書（含驗證，支援逐本確認）"""
    user_id = event.source.user_id
    
    # 檢查是否有未完成的流程
    if user_id in _PENDING:
        pend_type = _PENDING[user_id].get("type", "")
        if pend_type == "book_selection_step":
            msg = "⚠️ 您有未完成的選書流程\n\n回覆「取消」可清除，或繼續完成選書"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
            return
    
    lines_after = text.replace("#寄書", "").strip()
    data = parse_kv_lines(lines_after)
    
    name = data.get("姓名", "").strip()
    phone_raw = data.get("電話", "").strip()
    address_raw = data.get("寄送地址", "").strip()
    book_raw = data.get("書籍名稱", "").strip()
    biz_note = data.get("業務備註", "").strip()
    
    # 驗證資料
    validation_data = {
        "name": name,
        "phone": phone_raw,
        "address": address_raw,
        "book": book_raw
    }
    
    errors = _validate_order_data(validation_data)
    
    # 如果有姓名、電話、地址錯誤，直接提示
    if "name" in errors or "phone" in errors or "address" in errors:
        error_msg = _format_validation_errors_simple(errors)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=error_msg))
        return
    
    # 如果有書籍錯誤，啟動逐本確認流程
    if "books" in errors:
        invalid_books = errors["books"]
        _start_book_selection(event, validation_data, invalid_books, biz_note)
        return
    
    # 無錯誤，直接建立訂單
    _create_order_confirmed(event, name, phone_raw, address_raw, book_raw, biz_note)

def _create_order_confirmed(event, name: str, phone_raw: str, address_raw: str, book_raw: str, biz_note: str):
    """確認無誤後建立訂單（根據實際表頭動態寫入）"""
    try:
        uid = getattr(event.source, "user_id", "")
        profile = line_bot_api.get_profile(uid)
        operator = profile.display_name or "系統"
    except Exception:
        operator = "系統"
    
    app.logger.info(f"[ORDER] 開始建立訂單 - 姓名:{name}, 電話:{phone_raw}, 書籍:{book_raw}")
    
    phone = normalize_phone(phone_raw)
    zip_code = _find_zip_code(address_raw)
    
    if WRITE_ZIP_TO_ADDRESS and zip_code:
        address = f"{zip_code} {address_raw}"
    else:
        address = address_raw
    
    app.logger.info(f"[ORDER] 處理後 - 電話:{phone}, 郵遞區號:{zip_code}, 地址:{address}")
    
    # 解析書名
    book_names = [x.strip() for x in re.split(r"[,，、;；\n]+", book_raw) if x.strip()]
    final_books = []
    for book_name in book_names:
        matched = _find_book_exact(book_name)
        if matched:
            final_books.append(matched)
        else:
            final_books.append(book_name)
    
    app.logger.info(f"[ORDER] 解析書名完成: {final_books}")
    
    try:
        app.logger.info(f"[ORDER] 準備寫入工作表: {MAIN_SHEET_NAME}")
        ws = _ws(MAIN_SHEET_NAME)
        h = _get_header_map(ws)
        app.logger.info(f"[ORDER] 表頭對應: {h}")
        
        all_vals = ws.get_all_values()
        header = all_vals[0] if all_vals else []
        app.logger.info(f"[ORDER] 實際表頭: {header}")
        app.logger.info(f"[ORDER] 目前資料列數: {len(all_vals)}")
        
        # 生成新 ID
        # 支援多種 ID 欄位名稱
        idx_rid = _col_idx(h, "寄書ID", _col_idx(h, "紀錄ID", 1))
        existing_ids = [r[idx_rid - 1] for r in all_vals[1:] if len(r) >= idx_rid and r[idx_rid - 1].startswith("R")]
        max_num = 0
        for eid in existing_ids:
            m = re.match(r"R(\d+)", eid)
            if m:
                max_num = max(max_num, int(m.group(1)))
        new_rid = f"R{max_num + 1:04d}"
        
        app.logger.info(f"[ORDER] 生成新ID: {new_rid} (目前最大編號: {max_num})")
        
        # 根據表頭欄位數量建立空白列
        num_cols = len(header)
        
        # 寫入多列
        for book in final_books:
            # 建立空白列（填滿所有欄位）
            row = [""] * num_cols
            
            # 根據表頭名稱填入對應資料
            # ID 欄位
            if "寄書ID" in h:
                row[h["寄書ID"] - 1] = new_rid
            elif "紀錄ID" in h:
                row[h["紀錄ID"] - 1] = new_rid
            
            # 建單日期（使用 yyyy-mm-dd hh:mm 格式）
            if "建單日期" in h:
                row[h["建單日期"] - 1] = now_str_min()  # 使用完整時間格式
            elif "建單時間" in h:
                row[h["建單時間"] - 1] = now_str_min()
            
            # 建單人
            if "建單人" in h:
                row[h["建單人"] - 1] = operator
            
            # 姓名
            if "學員姓名" in h:
                row[h["學員姓名"] - 1] = name
            elif "姓名" in h:
                row[h["姓名"] - 1] = name
            
            # 電話（加上單引號強制文字格式，避免開頭 0 被移除）
            if "學員電話" in h:
                row[h["學員電話"] - 1] = f"'{phone}" if phone else ""
            elif "電話" in h:
                row[h["電話"] - 1] = f"'{phone}" if phone else ""
            
            # 地址
            if "寄送地址" in h:
                row[h["寄送地址"] - 1] = address
            elif "地址" in h:
                row[h["地址"] - 1] = address
            
            # 書籍
            if "書籍名稱" in h:
                row[h["書籍名稱"] - 1] = book
            
            # 業務備註
            if "業務備註" in h:
                row[h["業務備註"] - 1] = biz_note
            
            # 寄送方式（根據地址判別）
            if "寄送方式" in h:
                delivery_method = detect_delivery_method(address)
                if delivery_method:
                    # 偵測到超商
                    row[h["寄送方式"] - 1] = delivery_method
                elif address:
                    # 有地址但不是超商 → 便利帶
                    row[h["寄送方式"] - 1] = "便利帶"
                else:
                    row[h["寄送方式"] - 1] = ""
            
            # 經手人（建單時不填，出貨時才填）
            if "經手人" in h:
                row[h["經手人"] - 1] = ""
            elif "已託運-經手人" in h:
                row[h["已託運-經手人"] - 1] = ""
            
            # 狀態
            if "寄送狀態" in h:
                row[h["寄送狀態"] - 1] = "待處理"
            elif "狀態" in h:
                row[h["狀態"] - 1] = "待處理"
            
            app.logger.info(f"[ORDER] 準備寫入: {row[:5]}... (共 {len(row)} 欄)")
            _safe_append_row(ws, row)
            app.logger.info(f"[ORDER] ✅ 成功建立寄書 {new_rid}: {name} / {book}")
        
        msg_lines = ["✅ 寄書建立完成"]
        msg_lines.append(f"建單日期：{now_str_min()}")
        msg_lines.append(f"姓名：{name}  |  電話：{phone}")
        msg_lines.append(f"地址：{address}")
        msg_lines.append(f"書籍：{', '.join(final_books)}")
        msg_lines.append("狀態：待處理")
        
        msg = "\n".join(msg_lines)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        app.logger.info(f"[ORDER] 訂單建立完成，已回覆使用者")
    except Exception as e:
        app.logger.error(f"[ORDER] ❌ 建立失敗: {e}", exc_info=True)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ 建立失敗: {e}"))
        raise

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
        for i, r in enumerate(all_vals[1:], start=2):
            if len(r) >= idx_rid and r[idx_rid - 1] == rid:
                _safe_update_cell(ws, i, idx_tracking, "")
                _safe_update_cell(ws, i, idx_date, "")
                _safe_update_cell(ws, i, idx_person, "")
                _safe_update_cell(ws, i, idx_status, "待處理")
                found = True
        
        if found:
            app.logger.info(f"[DELETE_SHIP] 已撤銷出書: {rid}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"✅ 已撤銷 {rid} 的出貨記錄"))
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
# 處理待確認回答（修復 S2：移除重複程式碼）
# ============================================
def _handle_pending_answer(event, text: str) -> bool:
    """處理待確認回答"""
    user_id = event.source.user_id
    pend = _PENDING.get(user_id)
    if not pend:
        return False
    
    # 處理逐本選書流程（新增）
    if pend.get("type") == "book_selection_step":
        return _handle_book_selection_step(event, text)
    
    ans = text.strip().upper()
    
    # 取消
    if ans == "N":
        _PENDING.pop(user_id, None)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="已取消。"))
        return True
    
    # 處理書籍選擇（數字）
    if pend.get("type") == "order_correction" and ans.isdigit():
        return _handle_book_selection(event, int(ans))
    
    # 處理入庫修正（數字選擇或輸入書名）
    if pend.get("type") == "stockin_correction":
        return _handle_stockin_correction(event, text)
    
    # 重新輸入
    if ans in ("重新輸入", "RETRY", "REDO"):
        _PENDING.pop(event.source.user_id, None)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="已清除，請重新輸入完整 #寄書 資料"))
        return True
    
    # 確認
    if ans not in ("Y", "YES", "OK"):
        return False
    
    # 取消寄書
    if pend["type"] == "cancel_order":
        ws = _ws(pend["sheet"])
        idxH = pend["idx"]["H"]
        idxL = pend["idx"]["L"]
        idxM = pend["idx"]["M"]
        
        append_note = f"[已刪除 {now_str_min()}]"
        for row_i in pend["rows"]:
            try:
                curr_h = ws.cell(row_i, idxH).value or ""
            except Exception:
                curr_h = ""
            new_h = (curr_h + " " + append_note).strip() if curr_h else append_note
            _safe_update_cell(ws, row_i, idxH, new_h)
            _safe_update_cell(ws, row_i, idxL, pend["operator"])
            _safe_update_cell(ws, row_i, idxM, "已刪除")
        
        msg = f"✅ 已刪除整筆寄書（{pend['rid']}）：{pend['stu']} 的 {pend['book_list']}"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=msg))
        _PENDING.pop(event.source.user_id, None)
        return True
    
    # 入庫確認
    if pend["type"] == "stock_in_confirm":
        _write_stockin_rows(pend["operator"], pend["items"])
        lines = [f"{it['name']} × {it['qty']}" for it in pend["items"]]
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="✅ 入庫完成：\n" + "\n".join(lines)))
        _PENDING.pop(event.source.user_id, None)
        return True
    
    # 整理寄書確認（修復 S2：只保留一份）
    if pend["type"] == "organize_order_confirm":
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
        _PENDING.pop(event.source.user_id, None)
        return True
    
    return False

def _handle_book_selection(event, choice: int) -> bool:
    """處理書籍選擇（新功能）"""
    pend = _PENDING.get(event.source.user_id)
    if not pend or pend.get("type") != "order_correction":
        return False
    
    errors = pend.get("errors", {})
    if "books" not in errors:
        return False
    
    # 找到第一個有建議的錯誤書名
    for err_item in errors["books"]:
        if isinstance(err_item, dict) and err_item.get("suggestions"):
            suggestions = err_item["suggestions"]
            if 1 <= choice <= len(suggestions):
                selected_book = suggestions[choice - 1]
                
                # 更新資料中的書名
                old_name = err_item["wrong"]
                current_books = pend["data"]["book"]
                new_books = current_books.replace(old_name, selected_book)
                pend["data"]["book"] = new_books
                
                # 重新驗證
                validation_data = pend["data"]
                new_errors = _validate_order_data(validation_data)
                
                if not new_errors:
                    # 無錯誤，直接建立訂單
                    _create_order_confirmed(
                        event,
                        validation_data["name"],
                        validation_data["phone"],
                        validation_data["address"],
                        new_books,
                        pend.get("biz_note", "")
                    )
                    _PENDING.pop(event.source.user_id, None)
                else:
                    # 還有其他錯誤，繼續引導
                    pend["errors"] = new_errors
                    error_msg = _format_validation_errors(new_errors)
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=error_msg))
                
                return True
    
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ 無效的選項"))
    return True

# ============================================
# ClassPlus 訂課處理
# ============================================
def _handle_classplus_order(event, text: str):
    """處理 #訂課 指令"""
    uid = getattr(event.source, "user_id", "")
    student_info = parse_student_info(text)

    if not student_info.get("name"):
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=(
                "❌ 格式錯誤，請依以下格式輸入：\n\n"
                "#訂課\n"
                "學生姓名：\n"
                "學生程度：\n"
                "信箱：\n"
                "學習備註："
            ))
        )
        return

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=f"⏳ 正在處理 {student_info['name']} 的訂課，請稍候...")
    )

    result = run_classplus_task(student_info)
    msg = format_result_message(result, student_info)

    try:
        if result.get("screenshot"):
            import io
            from linebot.models import ImageSendMessage
            # 上傳截圖並回傳
            line_bot_api.push_message(
                getattr(event.source, "group_id", uid) or uid,
                TextSendMessage(text=msg)
            )
        else:
            line_bot_api.push_message(
                getattr(event.source, "group_id", uid) or uid,
                TextSendMessage(text=msg)
            )
    except Exception as e:
        app.logger.error(f"[CLASSPLUS] 回傳訊息失敗: {e}")


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

@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    """處理文字訊息"""
    text = (event.message.text or "").strip()
    
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
    
    # 待確認流程
    if _handle_pending_answer(event, text):
        return
    
    # 白名單檢查
    if not _ensure_authorized(event, scope="text"):
        return
    
    # 處理指令
    if text.startswith("#訂課"):
        _handle_classplus_order(event, text)
        return

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
