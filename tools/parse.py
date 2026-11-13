import os
import re, sys, json, collections
src = open(sys.argv[1], encoding="utf-8").read().replace("\r\n","\n").split("\n")
msgs=[]; date=None
for ln in src:
    if re.match(r"^\d{4}\.\d{2}\.\d{2} 星期", ln): date=ln[:10]; continue
    m=re.match(r"^(\d{2}:\d{2}) (.*)$", ln)
    if m:
        body=m.group(2); bot=body.startswith("尚進小幫手 ")
        msgs.append({"d":date,"t":m.group(1),"bot":bot,"x":body[len("尚進小幫手 "):] if bot else body})
    elif msgs: msgs[-1]["x"]+="\n"+ln
json.dump(msgs, open(sys.argv[2],"w",encoding="utf-8"), ensure_ascii=False)
print(len(msgs), sum(m["bot"] for m in msgs))
c=collections.Counter(m["x"].split("\n")[0][:14] for m in msgs if m["bot"])
for k,v in c.most_common(40): print(v,k)
