# classplus_handler.py
# ClassPlus 自動化處理模組
# 使用 Claude Computer Use API 操作瀏覽器

import os
import re
import base64
import logging
import anthropic
import subprocess
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TZ = ZoneInfo("Asia/Taipei")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLASSPLUS_URL = os.getenv("CLASSPLUS_URL", "https://sc.classplus.com.tw/sc/Manage/Default.aspx")
CLASSPLUS_ACCOUNT = os.getenv("CLASSPLUS_ACCOUNT", "")
CLASSPLUS_PASSWORD = os.getenv("CLASSPLUS_PASSWORD", "")

# Chromium 路徑（Railway 環境）
CHROMIUM_PATH = "/usr/bin/chromium"


def parse_student_info(text: str) -> dict:
    """解析 LINE 訊息中的學生資料"""
    data = {}

    patterns = {
        "name":    r"學生姓名[：:]\s*(.+)",
        "level":   r"學生程度[：:]\s*(.+)",
        "email":   r"信箱[：:]\s*(.+)",
        "note":    r"學習備註[：:]\s*(.+)",
    }

    for key, pattern in patterns.items():
        m = re.search(pattern, text)
        if m:
            data[key] = m.group(1).strip()

    return data


def _take_screenshot() -> bytes | None:
    """截取目前瀏覽器畫面（回傳 PNG bytes）"""
    try:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            tmp_path = f.name

        subprocess.run([
            CHROMIUM_PATH,
            "--headless",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--screenshot=" + tmp_path,
            "--window-size=1280,800",
            CLASSPLUS_URL
        ], timeout=30, capture_output=True)

        with open(tmp_path, "rb") as f:
            return f.read()
    except Exception as e:
        logger.error(f"[CLASSPLUS] 截圖失敗: {e}")
        return None


def run_classplus_task(student_info: dict) -> dict:
    """
    主要執行函式：呼叫 Claude Computer Use 完成 ClassPlus 操作
    回傳 {"success": bool, "message": str, "screenshot": bytes | None}
    """
    if not ANTHROPIC_API_KEY:
        return {"success": False, "message": "❌ 未設定 ANTHROPIC_API_KEY", "screenshot": None}

    if not CLASSPLUS_ACCOUNT or not CLASSPLUS_PASSWORD:
        return {"success": False, "message": "❌ 未設定 ClassPlus 帳號或密碼", "screenshot": None}

    required = ["name", "level", "email"]
    missing = [k for k in required if k not in student_info]
    if missing:
        return {"success": False, "message": f"❌ 缺少必要資料：{', '.join(missing)}", "screenshot": None}

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""請幫我完成以下操作：

1. 開啟瀏覽器，前往 {CLASSPLUS_URL}
2. 使用帳號「{CLASSPLUS_ACCOUNT}」、密碼「{CLASSPLUS_PASSWORD}」登入
3. 登入成功後截圖

學生資料（僅供後續步驟參考，目前只需完成登入並截圖）：
- 姓名：{student_info.get('name', '')}
- 程度：{student_info.get('level', '')}
- 信箱：{student_info.get('email', '')}
- 備註：{student_info.get('note', '')}

完成登入後，請說「登入成功」或說明遇到的問題。"""

    try:
        logger.info(f"[CLASSPLUS] 開始執行 Computer Use，學生：{student_info.get('name')}")

        response = client.beta.messages.create(
            model="claude-opus-4-5",
            max_tokens=4096,
            tools=[
                {
                    "type": "computer_20241022",
                    "name": "computer",
                    "display_width_px": 1280,
                    "display_height_px": 800,
                    "display_number": 1,
                }
            ],
            messages=[{"role": "user", "content": prompt}],
            betas=["computer-use-2024-10-22"],
        )

        # 取得回應文字
        result_text = ""
        for block in response.content:
            if hasattr(block, "text"):
                result_text += block.text

        # 截圖
        screenshot = _take_screenshot()

        success = "登入成功" in result_text or "成功" in result_text
        return {
            "success": success,
            "message": result_text or "操作完成",
            "screenshot": screenshot,
        }

    except anthropic.APIError as e:
        logger.error(f"[CLASSPLUS] API 錯誤: {e}")
        return {"success": False, "message": f"❌ API 錯誤：{e}", "screenshot": None}
    except Exception as e:
        logger.error(f"[CLASSPLUS] 未預期錯誤: {e}")
        return {"success": False, "message": f"❌ 系統錯誤：{e}", "screenshot": None}


def format_result_message(result: dict, student_info: dict) -> str:
    """將執行結果格式化為 LINE 回覆訊息"""
    name = student_info.get("name", "學生")
    if result["success"]:
        return f"✅ {name} 的訂課流程已完成\n\n{result['message']}"
    else:
        return f"⚠️ {name} 的訂課流程未完成\n\n{result['message']}\n\n請人工處理。"
