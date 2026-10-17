FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# 先裝依賴，利用 Docker 快取
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 主程式、Shioaji 封裝、前端模板與工具腳本（API 金鑰不打包，執行時以 .env 傳入）
COPY app.py sj_market.py login.py account.py concepts.json ./
COPY templates/ templates/

ENV PORT=7860
EXPOSE 7860

# 預設啟動網頁服務；傳入股票代碼則改為 CLI 模式
ENTRYPOINT ["python", "app.py"]
CMD ["web"]
