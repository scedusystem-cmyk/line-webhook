FROM python:3.11.9-slim

# 安裝 Chromium 和相關套件
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    fonts-noto-cjk \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# cache-bust: 2026-03-02-v1
COPY . .

EXPOSE 8080

CMD ["python", "app.py"]
