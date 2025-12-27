# Kamino Monitor

這是一個用來監控 Solan Kamino Finance 倉位健康度的工具。
當你的 LTV (Loan-To-Value) 過高或 Health Factor 過低時，會透過 Apprise 發送通知。

## 功能

*   **自動解析 SNS**: 支援直接使用 `.sol` 域名 (例如 `metaass.sol`)，程式會自動解析為錢包地址。
*   **多市場監控**: 預設監控 Kamino Main Market 與 JLP Market。
*   **彈性通知**: 整合 Apprise，支援 Telegram, Discord, Slack, Email 等多種通知方式。
*   **Docker 化**: 提供 Docker Compose 設定，方便部署。

## 快速開始

### 1. 設定通知 (Apprise)

請確認你的主機上已有 Apprise 設定檔，路徑通常為 `/etc/apprise/apprise.yml`。
Docker Compose 會將此路徑 mount 進容器內。

格式範例:
```yaml
urls:
  - tgram://bot_token/chat_id
  - discord://webhook_id/webhook_token
```

### 2. 啟動服務

確認 `docker-compose.yml` 中的環境變數符合你的需求：

```yaml
environment:
  - USER_WALLET=xxx.sol  # 你的錢包地址或是 SNS 域名
  - CHECK_INTERVAL=60        # 檢查頻率 (秒)
  - LTV_THRESHOLD_PCT=75.0   # LTV 觸發通知門檻 (%)
  - HF_THRESHOLD=1.15        # Health Factor 觸發通知門檻
```

然後執行：

```bash
docker compose up -d
```

### 3. 查看 Log

```bash
docker compose logs -f
```

## 開發與修改

程式碼位於 `src/monitor.py`。如果不使用 Docker，也可以直接執行：

```bash
pip install -r requirements.txt
python src/monitor.py
```
