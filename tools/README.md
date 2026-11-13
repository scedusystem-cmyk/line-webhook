# 寄書機器人回測工具

改 `app.py` 之前／之後各跑一次，確認沒有把舊的寫法改壞。全部在本機跑，不碰正式表（`send_line.py` 例外）。

## 資料（`tools/data/`，不進 GitHub）
- `books.csv`／`zip.csv`／`wl.csv`：書目主檔、郵遞區號參照表、白名單快照
  （重新抓：Google 表 gviz CSV 匯出，分頁名換成對應的）
- `msgs.json`：LINE 群組聊天紀錄解析結果，`python parse.py <聊天紀錄.txt> data/msgs.json` 產生
- `gt.json`：書名真值（業務過去手選的答案＋成功建單的對應），`python gt.py` 產生

## 工具
- `python evalbooks.py`：書名比對對真值打分（自動對／問且第幾個／問但沒列／自動錯）
- `python replay.py - replay`：把聊天紀錄所有 #寄書 用假表重播，統計建單／擋下／要問
- `python replay.py - flow`：固定的對話腳本（選書、多選、略過、取消、插隊、撤銷出書）
- `python replay.py - compare`：舊機器人建過的單，新版書名／郵遞區號是否一樣
- `python send_line.py "訊息"`：⚠️ 帶簽章模擬 LINE 送到**正式**機器人（Vic 身分），會真的寫表；
  機器人回覆會因假 reply token 失敗（正常）。測完用 `#取消寄書 R####` 再送 `Y` 標已刪除。
  需先在本資料夾 `railway link -p 7b32a707-8158-4476-bc7f-9289e688d3b4 -e Prod2 -s line-webhook`。

## 部署
正式機器人＝Railway charming-curiosity 專案 Prod2 環境的 `line-webhook`（沒接 GitHub）：
```
railway up . --path-as-root -p 7b32a707-8158-4476-bc7f-9289e688d3b4 -e Prod2 -s line-webhook --ci
```
退回：Railway 重新部署前一版。健康檢查：`https://line-webhook-prod2.up.railway.app/` 會列出 Google 表分頁。
