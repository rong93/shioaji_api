---
title: Shioaji Api
emoji: 📈
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
pinned: false
---

# 即時數據抓取與 AI 提示詞生成工具

這是一個基於 Python 與 Docker 的股票數據工具。台股行情透過 **永豐金證券 Shioaji API** 取得，籌碼與五檔則取自官方公開資料：

- **即時價格 / 開高低 / 成交量 / 漲跌**（台股）：永豐 Shioaji `snapshots`。
- **歷史 K 線與技術指標 (MA5/MA20/MA60、RSI-14、頸線、趨勢線)**（台股）：永豐 Shioaji `kbars`（1 分 K 本地聚合成日 K，並做檔案快取）。
- **五檔買賣單**：台灣證券交易所即時 API。
- **三大法人籌碼**：證交所 T86 與櫃買中心（Shioaji 沒有這項資料）。
- **成交量大增排行**：以永豐全市場快照計算「今日量 / 昨量」與「今日量 / 近 5 日均量」，找出盤中放量的股票。
- **美股**：維持使用 Yahoo Finance。
- **AI 提示詞生成**：自動輸出排版精美的 Markdown 文字，您只需一鍵複製並貼給 AI（如 ChatGPT 或 Claude），就能獲得買賣點與支撐壓力位分析。

---

## 事前準備：永豐 Shioaji API 金鑰

1. 需要有永豐金證券帳戶，並到官網「個人服務 › API 管理」申請 **API Key** 與 **Secret Key**（與登入帳密不同）。
2. 在專案根目錄建立 `.env`（可直接複製 `.env.example`）：

```
SHIOAJI_API_KEY=你的API_KEY
SHIOAJI_SECRET_KEY=你的SECRET_KEY
SHIOAJI_SIMULATION=true
```

- `SHIOAJI_SIMULATION=true` 為模擬環境，**查行情、歷史 K 線、全市場快照都可以用**，不需要簽署下單文件或憑證。
- `.env` 已列入 `.gitignore` 與 `.dockerignore`，不會被提交或打包進映像檔；執行容器時以 `--env-file .env` 傳入。
- 永豐對 API 有流量與查詢次數限制（詳見 [官方文件](https://sinotrade.github.io/zh/limit/)）；本工具已做快取（個股快照 15 秒、全市場快照 60 秒、日 K 檔案快取），請勿另外高頻輪詢。

---

## 專案檔案

| 檔案                   | 用途                                                             |
| ---------------------- | ---------------------------------------------------------------- |
| `app.py`               | 主程式：Flask 網頁服務 + CLI，分析、提示詞、排行                 |
| `sj_market.py`         | 永豐 Shioaji 封裝：登入、快照、日 K、全市場快照與快取            |
| `templates/index.html` | 前端畫面                                                         |
| `login.py`             | 工具腳本：測試登入、列出帳戶                                     |
| `account.py`           | 工具腳本：查餘額、持股、交割款、委託（正式環境需 `signed=true`） |
| `docker-compose.yml`   | 一鍵啟動網頁服務（含日 K / 行情快取 volume）                     |

---

## 快速開始 (使用 Docker)

請確保您的電腦已安裝並啟動了 Docker。

### 最簡單：Docker Compose 啟動網頁服務

```bash
docker compose up --build
```

啟動後打開 **`http://localhost:5000`** 或 **`http://localhost:7860`**。`Ctrl + C` 停止；下次直接 `docker compose up` 即可（快取會保留在 volume）。

測試登入或查帳戶：

```bash
docker compose run --rm --entrypoint python web login.py
docker compose run --rm --entrypoint python web account.py
```

### 1. 建置 Docker 映像檔（手動方式）

在 `README.md` 所在的目錄開啟終端機，執行以下指令來建置映像檔：

```bash
docker build -t stock-ai-prompt .
```

### 2. 執行查詢

本工具提供兩種執行方式（都要加 `--env-file .env` 讓容器讀到金鑰）：

#### 方式 A：直接在指令中傳入股票代碼 (推薦，快速方便)

直接在執行指令最後加上股票代碼（支援台股如 `2330`，美股如 `AAPL`）：

```bash
docker run --rm -it --env-file .env stock-ai-prompt 2330
```

#### 方式 B：互動式輸入

直接執行容器，程式會提示您輸入代碼：

```bash
docker run --rm -it --env-file .env stock-ai-prompt
```

執行後畫面會顯示：
`請輸入股票代碼 (例如台股 2330 或美股 AAPL): `  
此時輸入 `2330` 並按下 Enter 即可。

#### 方式 C：啟動網頁服務 (Web UI)

如果您需要直覺的前端畫面進行輸入、查看數據並一鍵複製，可在終端機執行以下指令啟動並保持運行：

```bash
docker run -it -p 5000:7860 -p 7860:7860 --env-file .env --name stock_web --rm stock-ai-prompt
```

啟動後，您可以在瀏覽器打開以下任一網址使用（皆能正常開啟，但瀏覽器紀錄會依網址獨立儲存）：

- **`http://localhost:5000`**（若先前已有搜尋紀錄，使用此網址可保留歷史紀錄）
- **`http://localhost:7860`**（新連接埠）

網頁中「排行」卡片的 **量增** 分頁即為成交量大增排行：可切換全市場 / 上市 / 上櫃、對昨量 / 對 5 日均量，並設定「今日至少幾張」過濾冷門股。按「重新整理」會重新讀取，但全市場快照在 60 秒內共用同一份資料。

> [!NOTE]
>
> - 此指令會在終端機前台保持運行並顯示即時日誌（Log），您可以直接按下 **`Ctrl + C`** 停止並關閉服務。
> - 加上了 `--rm` 參數後，當您按下 `Ctrl + C` 關閉時，Docker 會自動清理並刪除 `stock_web` 容器，下次啟動時便不會再遇到名稱佔用的 `Conflict` 衝突問題。
> - 如果因為異常退出或重複啟動，遇到名稱已被佔用的錯誤（Conflict. The container name "/stock_web" is already in use），請先執行以下指令清理舊容器：
>
>   ```bash
>   docker rm -f stock_web
>   ```

---

## 如何使用產出的提示詞？

1. 執行完上述指令後，終端機畫面上會印出以 `# 股票分析提示詞 (Stock Analysis Prompt)` 開頭的一大段 Markdown 文字。
2. **複製** 該段文字的所有內容。
3. **貼上** 到任何 AI 對話框中（例如 ChatGPT, Claude 等）。
4. AI 將會根據最即時的股價、均線黃金/死亡交叉狀態、RSI 強弱勢，為您分析：
   - 目前的多空走勢型態
   - 上方的關鍵壓力位與下方的支撐位
   - 具體的買入、賣出或觀望決策
   - 推薦的建倉區間、停利點與防守停損點

---

## 本地開發 (如果不使用 Docker)

如果您想直接在本機執行 Python 腳本，請依序執行以下指令：

1. **安裝依賴套件**（並確認專案目錄已有填好的 `.env`，程式會自動讀取）：

   ```bash
   pip install -r requirements.txt
   ```

2. **執行主程式 (CLI 模式)**：

   ```bash
   python app.py 2330
   ```

3. **啟動網頁服務 (Web 模式)**：
   ```bash
   python app.py web
   ```
   啟動後，請在瀏覽器打開：**`http://127.0.0.1:5000`**。
