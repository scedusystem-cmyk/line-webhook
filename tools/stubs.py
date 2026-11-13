import sys, os, types, csv
class Any_:
    def __init__(s,*a,**k): pass
    def __getattr__(s,n): return Any_()
    def __call__(s,*a,**k): return Any_()
def mod(name, **attrs):
    m = types.ModuleType(name); m.__dict__.update(attrs); sys.modules[name]=m; return m
class FakeFlask:
    def __init__(s,*a): import logging; s.logger=logging.getLogger("t"); s.logger.disabled=True
    def route(s,*a,**k): return lambda f: f
mod("flask", Flask=FakeFlask, request=None, abort=None)
class WNF(Exception): pass
mod("gspread", WorksheetNotFound=WNF, authorize=lambda c: None)
mod("google"); mod("google.oauth2"); mod("google.oauth2.service_account", Credentials=Any_())
class Handler:
    def __init__(s,*a): pass
    def add(s,*a,**k): return lambda f: f
mod("linebot", LineBotApi=Any_, WebhookHandler=Handler)
mod("linebot.exceptions", InvalidSignatureError=Exception)
class TSM:
    def __init__(s,text): s.text=text
mod("linebot.models", MessageEvent=1, TextMessage=1, ImageMessage=1, TextSendMessage=TSM)
mod("classplus_handler", parse_student_info=None, run_classplus_task=None, format_result_message=None)
os.environ["LINE_CHANNEL_SECRET"]="x"; os.environ["LINE_CHANNEL_ACCESS_TOKEN"]="x"
HERE=os.path.dirname(os.path.abspath(__file__))
DATA=os.path.join(HERE,"data")
REPO=os.path.dirname(HERE)
def load(src=None):
    src = src or REPO
    sys.path.insert(0, src)
    import io, contextlib
    with contextlib.redirect_stdout(io.StringIO()):
        import app
    rows=list(csv.DictReader(open(os.path.join(DATA,"books.csv"),encoding="utf-8-sig")))
    books=[{"name":r["書籍名稱"].strip(),"lang":r["語別"],"fuzzy":r["模糊比對書名"],"stock":r["現有庫存"]} for r in rows if r["是否啟用"].strip()=="使用中" and r["書籍名稱"].strip()]
    app._BOOK_CACHE["books"]=books; app._BOOK_CACHE["ts"]=1e18
    zrows=list(csv.DictReader(open(os.path.join(DATA,"zip.csv"),encoding="utf-8-sig")))
    app._TEST_ZIPROWS=zrows
    return app
