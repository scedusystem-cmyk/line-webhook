import os, sys, json, re, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stubs
app = stubs.load(None if sys.argv[1] in ("-", ".") else sys.argv[1])

HEADER = ["紀錄ID", "建單日期", "建單人", "學員姓名", "學員電話", "寄送地址", "書籍名稱", "業務備註", "寄送方式", "寄出日期", "託運單號", "經手人", "寄送狀態"]


class FakeWS:
    def __init__(self, title, rows):
        self.title = title
        self.rows = [list(r) for r in rows]

    def row_values(self, i):
        return list(self.rows[i - 1]) if i <= len(self.rows) else []

    def get_all_values(self):
        return [list(r) for r in self.rows]

    def insert_rows(self, values, row=2, **k):
        for off, v in enumerate(values):
            self.rows.insert(row - 1 + off, list(v))

    def insert_row(self, values, index=2, **k):
        self.rows.insert(index - 1, list(values))

    def update_cell(self, r, c, v):
        while len(self.rows[r - 1]) < c:
            self.rows[r - 1].append("")
        self.rows[r - 1][c - 1] = v

    def cell(self, r, c):
        class C: pass
        x = C(); x.value = self.rows[r - 1][c - 1]
        return x

    def append_rows(self, rows, **k):
        self.rows.extend(rows)


class FakeSS:
    def __init__(self):
        self.sheets = {"寄書任務": FakeWS("寄書任務", [HEADER, ["R0100", "2026-11-01 10:00", "x", "舊單", "'0911111111", "x", "StartUp 1", "", "便利帶", "", "", "", "待處理"]])}

    def worksheet(self, n):
        return self.sheets[n]


app.ss = FakeSS()
rows = []
for r in app._TEST_ZIPROWS:
    city = app._normalize_address_for_compare(r["縣市"]); dist = app._normalize_address_for_compare(r["區域"]); z = r["郵遞區號"].strip()
    if z and (city or dist): rows.append({"city": city, "dist": dist, "zip": z})
app._ZIP_CACHE["rows"] = rows; app._ZIP_CACHE["ts"] = 1e18

replies = []
class Api:
    def reply_message(self, tok, m): replies.append(m.text)
    def get_profile(self, uid):
        class P: display_name = "測試業務"
        return P()
app.line_bot_api = Api()
app._ensure_authorized = lambda e, scope="*": True


class Msg:
    def __init__(self, t): self.text = t
class Src:
    def __init__(self, u): self.user_id = u
class Ev:
    def __init__(self, t, u="U1"):
        self.message = Msg(t); self.source = Src(u); self.reply_token = "t"


def say(t, u="U1"):
    replies.clear()
    app.handle_text_message(Ev(t, u))
    return replies[-1] if replies else "(沒回應)"


mode = sys.argv[2]
if mode == "replay":
    M = json.load(open(os.path.join(stubs.DATA, "msgs.json"), encoding="utf-8"))
    c = collections.Counter(); ex = collections.defaultdict(list)
    for m in M:
        if m["bot"] or "#寄書" not in m["x"]:
            continue
        body = m["x"][m["x"].index("#寄書"):]
        app._PENDING.clear()
        r = say(body)
        k = r.split("\n")[0][:12]
        c[k] += 1
        ex[k].append(r.replace("\n", " / ")[:230])
    print(dict(c))
    for k in ex:
        if not k.startswith("✅"):
            print("==", k)
            for e in ex[k][:40]: print("  ", e)
    fixes = [e for e in ex.get("✅ 寄書建立完成", []) if "自動對應" in e]
    print("== 建單且有自動對應", len(fixes))
    for e in fixes[:25]: print("  ", e[e.index("書籍："):])
else:
    T = [
        ("#寄書\n學員姓名：凌儀蓁\n學員電話：0919574163\n寄送地址：新北市板橋區南雅南路二段144\n10號7樓\n書籍名稱：現在上的課本", None),
        ("#寄書\n學員姓名：王小明\n學員電話：0912345678\n寄送地址：新北市板橋區南雅南路二段144巷\n10號7樓\n書籍名稱：Let’s Go 6、StartUp5", None),
        ("#寄書\n學員姓名：王小明\n學員電話：0912345678\n寄送地址：台東市四維路一段307號\n書籍名稱：The Thinking Train C", None),
        ("1、3", None),
        ("#寄書\n學員姓名：王小明\n學員電話：0911022863(科辦A分機801)\n寄送地址：235新北市中和區建三路46號4樓\n書籍名稱：我的第一本韓語、生活情境4", None),
        ("2", None),
        ("略過", None),
        ("#寄書\n學員姓名：王小明\n學員電話：0912345678\n寄送地址：汐止力行店\n書籍名稱：多益", None),
        ("＃寄書\n學員姓名：王小明\n學員電話：0912345678\n寄送地址：全家汐止力行店\n書籍名稱：藍色多益", None),
        ("#查寄書 王小明", None),
        ("[1]", None),
        ("#寄書\n學員姓名：李小華\n學員電話：0912345678\n寄送地址：711 台南市歸仁區民權北路186號4樓之8\n書籍名稱：現在在上的韓文課本", None),
        ("#取消寄書 李小華", None),
        ("#寄書\n學員姓名：插隊\n學員電話：0922222222\n寄送地址：台中北區三民路1號\n書籍名稱：Try N5 + Try N4", "U2"),
        ("Y", None),
        ("#刪除出書 R0100", None),
        ("#寄書\n學員姓名：王\n學員電話：090965370\n寄送地址：北區公園路1號\n書籍名稱：雅思３", None),
    ]
    for t, u in T:
        print(">>", t.replace("\n", " / ")[:90], f"（{u}）" if u else "")
        print(say(t, u or "U1"))
        print()
    print("== 表上內容")
    for r in app.ss.sheets["寄書任務"].rows:
        print(" | ".join(r))

if mode == "compare":
    M = json.load(open(os.path.join(stubs.DATA, "msgs.json"), encoding="utf-8"))
    same_b = diff_b = same_a = diff_a = 0
    db, da = [], []
    for i, m in enumerate(M):
        if m["bot"] or "#寄書" not in m["x"]:
            continue
        old = None
        for j in range(i + 1, min(i + 3, len(M))):
            if M[j]["bot"]:
                if M[j]["x"].startswith("✅ 寄書建立完成"):
                    old = M[j]["x"]
                break
        if not old:
            continue
        body = m["x"][m["x"].index("#寄書"):]
        app._PENDING.clear()
        r = say(body)
        if not r.startswith("✅"):
            db.append(("新版沒直接建單", body.replace("\n", " / ")[:150], r.split("\n")[0][:40], r.replace("\n", " / ")[:200]))
            diff_b += 1
            continue
        ob = re.search(r"書籍：(.*)", old).group(1).strip(); nb = re.search(r"書籍：(.*)", r).group(1).strip()
        oa = re.search(r"地址：(.*)", old).group(1).strip(); na = re.search(r"地址：(.*)", r).group(1).strip()
        if ob == nb: same_b += 1
        else: diff_b += 1; db.append(("書不同", ob, nb))
        oz = re.match(r"^(\d{3})\s", oa); nz = re.match(r"^(\d{3})\s", na)
        if (oz and oz.group(1)) == (nz and nz.group(1)): same_a += 1
        else: diff_a += 1; da.append((oa, na))
    print("舊版有建單的：書相同", same_b, "不同/沒建", diff_b, "｜郵遞區號相同", same_a, "不同", diff_a)
    for x in db[:60]: print("  B", x)
    for x in da[:40]: print("  A", x)
