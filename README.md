# LINE Webhook on Railway (Flask)

Routes:
- GET /          -> health check
- POST /callback -> LINE Messaging API webhook

Env (set in Railway → Variables):
- LINE_CHANNEL_SECRET
- LINE_CHANNEL_ACCESS_TOKEN

Run locally:
```bash
pip install -r requirements.txt
export LINE_CHANNEL_SECRET=xxxx
export LINE_CHANNEL_ACCESS_TOKEN=xxxx
python app.py
```
