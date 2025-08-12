# Flink MongoDB CDC 實戰專案

本專案示範如何以 Apache Flink 同步 MongoDB Replica Set 的變更資料，並動態串流至 Kafka（支援動態 topic 選擇、二進位 payload）。適合資料工程、事件驅動系統、微服務場景，亦友善於 Go 後端工程師客製改造。

---

## 專案架構

- **Flink 1.20**：串接 MongoDB 與 Kafka，並進行 CDC（Change Data Capture）流處理。
- **MongoDB Replica Set**：三節點（Primary/Secondary/Arbiter）模擬高可用。
- **Kafka 8.0**：事件匯流與多 topic 支援。
- **Docker Compose**：一鍵啟動本地整合環境。

```text
┌─────────────┐     CDC     ┌─────────────────┐   Stream   ┌────────────┐
│ MongoDB RS  │ ────────▶  │    Flink Job     │ ────────▶ │   Kafka    │
│ (demo DB)   │            │ MongoDBToKafka   │           │ (動態topic)│
└─────────────┘            └─────────────────┘           └────────────┘
```

---

## 快速啟動指令

```bash
# 1. 啟動服務與初始化 replica set
./start.sh

# 2. 執行 Flink main 類別 (本機開發需安裝 Java, Maven)
cd flink-test
mvn clean package
# 亦可將 fat-jar 丟上 jobmanager Web UI (localhost:8081)
