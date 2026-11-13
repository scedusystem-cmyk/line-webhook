import os
import json, re
M=json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"data","msgs.json"),encoding="utf-8"))
sel=[]
for i,m in enumerate(M):
    if m["bot"] and m["x"].startswith("❌ 找不到書籍："):
        w=re.search(r"「(.*?)」",m["x"]).group(1); sug=re.findall(r"\[\d\] (.*)",m["x"])
        for j in range(i+1,min(i+6,len(M))):
            if not M[j]["bot"]:
                d=re.sub(r"[\[\]\s]","",M[j]["x"].split(" ")[-1])
                if d.isdigit() and 1<=int(d)<=len(sug): sel.append((w,sug[int(d)-1]))
                break
pairs=[]
for i,m in enumerate(M):
    if m["bot"] or "#寄書" not in m["x"]: continue
    bl=re.search(r"(?:書籍名稱|書名|書籍)\s*[:：](.*?)(?:\n(?:業務備註|備註)|$)", m["x"], re.S)
    if not bl: continue
    for j in range(i+1,min(i+3,len(M))):
        if M[j]["bot"]:
            if M[j]["x"].startswith("✅ 寄書建立完成"):
                fb=re.search(r"書籍：(.*)",M[j]["x"]).group(1)
                ins=[x.strip() for x in re.split(r"[,，、;；\n]+", bl.group(1)) if x.strip()]
                outs=[x.strip() for x in fb.split(", ")]
                if len(ins)==len(outs): pairs+=list(zip(ins,outs))
            break
json.dump({"sel":sel,"pairs":pairs},open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"data","gt.json"),"w",encoding="utf-8"),ensure_ascii=False)
print(len(sel),len(pairs))
