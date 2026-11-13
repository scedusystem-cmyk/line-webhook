import os, sys, json, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stubs
app = stubs.load(sys.argv[1] if len(sys.argv) > 1 else None)
gt=json.load(open(os.path.join(stubs.DATA,"gt.json"),encoding="utf-8"))
names={b["name"] for b in app._BOOK_CACHE["books"]}
def run(pairs, label):
    c=collections.Counter(); bad=[]
    seen=set()
    for inp,truth in pairs:
        if truth not in names or (inp,truth) in seen: continue
        seen.add((inp,truth))
        r=app._resolve_book(inp)
        if r["status"]=="ok":
            if r["book"]==truth: c["自動對"]+=1
            else: c["自動錯"]+=1; bad.append(("自動錯",inp,truth,r["book"]))
        elif r["status"]=="ask":
            if truth in r["cands"][:3]: c["問且第%d"%(r["cands"].index(truth)+1)]+=1
            else: c["問但沒列"]+=1; bad.append(("問沒列",inp,truth,r["cands"]))
        else: c["找不到"]+=1; bad.append(("找不到",inp,truth,""))
    print(label, sum(c.values()), dict(c))
    for b in bad[:40]: print("  ",b)
run(gt["sel"],"[過去要選的]")
run(gt["pairs"],"[過去成功的]")
extra=[("Try N5  +   Try  N4",None),("N4 N3 N2 N1",None),("【新多益單字大全｜進階篇】NEW TOEIC 決戰新制多益",None),("牛津2+韓文我的第一本40音",None),
 ("我的第一本韓文40音",None),("生活情境4",None),("正在上的多益課本",None),("Try 五十音",None),("1B",None),("現在上的教材",None),("目前上的",None),
 ("我的第一本韓語-【初階】",None),("雅思３",None),("自然發音Easy & go",None),("自然發音",None),("Spotlight聽力",None),("柴犬托比帆布杯套【2 個一組】",None),
 ("品牌設計杯套一組",None),("商務書信與禮儀",None),("Spotlight(SPR1)",None),("旅遊",None),("第一本韓國語 初階篇",None),("R21 4",None),
 ("Reading and Vocabulary 閱讀2",None),("新多益單字大全-〔基礎篇〕單字書",None),("多益單字大全-〔進階篇〕單字書",None),("ele a1",None),
 ("藍色多益",None),("首爾大學A1",None),("The Thinking Train C",None),("我的第一本韓語",None),("Let's Go 6 習作本",None),("1",None),("go",None),("多益",None),("現在在上的韓文課本",None)]
print("[過去完全找不到／特殊]")
for inp,_ in extra:
    r=app._resolve_book(inp)
    sp=app._split_books(inp)
    print("  ",inp,"→",r["status"],r.get("book") or r.get("cands"), "| 拆:",sp if len(sp)>1 else "")
