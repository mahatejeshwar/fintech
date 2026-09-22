# 🏦 Fintech Real-Time Fraud Detection & Transaction Monitoring System

A real-time credit card fraud detection pipeline built on Databricks using a **medallion architecture** (Bronze → Silver → Gold) with Lakeflow Spark Declarative Pipelines (SDP). It ingests live transactions from Kafka, cross-references them against a fraud watchlist, and sends email alerts to affected customers.

---

## 📐 Architecture Flow

```
Kafka (Confluent Cloud)          JSON Files (UC Volume)
  credit_card_transactions         fraud_watchlist JSON files
         │                                │
    ┌────▼────┐                      ┌────▼────┐
    │ BRONZE  │                      │ BRONZE  │
    │transac- │                      │ fraud_  │
    │ tions   │                      │watchlist│
    └────┬────┘                      └────┬────┘
         │                                │
    ┌────▼────┐  ┌───────────┐    ┌────▼────┐
    │ SILVER  │  │ SILVER    │    │ SILVER  │
    │transac- │  │ customers │    │ fraud_  │
    │ tions   │  │ (batch)   │    │watchlist│
    └────┬────┘  └─────┬─────┘    └────┬────┘
         │              │               │
         └──────────────┼───────────────┘
                        │
                 ┌──────▼──────┐
                 │    GOLD     │
                 │ fraud_card  │
                 │   _alert    │
                 └──────┬──────┘
                        │
               ┌────────▼────────┐
               │  EMAIL ALERTS   │
               │ (Gmail SMTP)    │
               └─────────────────┘
```

---

## 📂 Layer-by-Layer Breakdown

### 1. Setup & Infrastructure (Top-Level Notebooks)

| File | Description |
|------|-------------|
| `Secret_Scope_02` | Creates the Databricks secret scope `fintech-scope` and stores two secrets: `kafka_connection_details` (Confluent Cloud bootstrap servers, topic, API key/secret as JSON) and `gmail_api_key` (Gmail app password for sending emails). Uses the Databricks Secrets API directly via REST calls. |
| `Kafka_Test_01` | Tests Kafka connectivity to Confluent Cloud. Reads from the `credit_card_transactions` topic in both batch and streaming mode, parses Kafka messages, and writes test data to `fintech.bronze.transactions_batch_test` and `fintech.bronze.transactions_streaming_test`. |
| `send_email_03` | Tests email sending via Gmail SMTP. Retrieves the Gmail API key from the `fintech-scope` secret scope and sends a simple HTML test email. |

### 2. File Generator

| File | Description |
|------|-------------|
| `fraud_watchlist_file_generator/fraud_watchlist_data_generator` | Reads `fraud_watchlist.csv` using pandas, converts each row to a JSON file, and writes them one-by-one to `/Volumes/fintech/source/fraud_watchlist/source_data/` with a 5-second delay between files (simulating real-time file arrivals for Auto Loader). Supports resume logic — scans existing JSON files to find the latest `watchlist_id` and only processes new rows. |

### 3. Bronze Layer (Raw Ingestion)

| File | Source | Destination Table | Description |
|------|--------|-------------------|-------------|
| `fintech_streaming/bronze/transactions_bronze.py` | Kafka stream | `fintech.bronze.transactions` | SDP streaming table that reads live credit card transactions from Confluent Cloud Kafka. Authenticates with SASL_SSL/PLAIN and stores raw Kafka fields (key, value, topic, partition, offset, timestamp). |
| `fintech_streaming/bronze/fraud_watchlist_bronze.py` | Auto Loader (JSON) | `fintech.bronze.fraud_watchlist` | SDP streaming table using Auto Loader (`cloudFiles`) to ingest JSON fraud watchlist files from UC Volume. Captures file metadata and rescued data. |

### 4. Silver Layer (Cleaning & Parsing)

| File | Source | Destination Table | Description |
|------|--------|-------------------|-------------|
| `fintech_streaming/silver/transactions_silver.py` | Bronze transactions | `fintech.silver.transactions` | Parses the raw Kafka `value` column (JSON string) into a structured schema (transaction_id, customer_id, card_number, merchant details, amount, currency, etc.). Applies 5 data quality expectations: drops rows with null IDs/card/merchant, warns on amounts ≤ 0. |
| `fintech_streaming/silver/fraud_watchlist_silver.py` | Bronze watchlist | `fintech.silver.fraud_watchlist` | Cleans fraud watchlist data: uppercases IDs/risk_level/action, converts `effective_from` from string to timestamp, adds silver ingestion timestamp. |
| `fintect_customer_silver_load/silver/customers_silver.py` | Bronze customers | `fintech.silver.customers` | Loads customer reference data (name, gender, age, location, income, card details, email, transaction limits). Drops rows with null `customer_id` (data quality expectation). Separate batch pipeline. |

### 5. Gold Layer (Fraud Detection)

| File | Source | Destination Table | Description |
|------|--------|-------------------|-------------|
| `fintech_streaming/gold/fraud_card_alert.py` | Silver (all 3) | `fintech.gold.fraud_card_alert` | The core fraud detection logic. Joins streaming transactions with streaming fraud watchlist on `card_number = entity_id` (inner join, 5-minute watermarks). Left joins with customers for email/name. Produces a rich alert record with full transaction and watchlist details. Alert type: `FRAUD_WATCHLIST_MATCH`. |

### 6. Alert/Notification Layer (Email Sending)

| File | Source | Destination | Description |
|------|--------|-------------|-------------|
| `fintech_streaming/alert/fraud_card_alert_email_notifier.py` | `fintech.gold.fraud_card_alert` | Gmail SMTP | SDP `foreach_batch_sink` that sends detailed HTML fraud alert emails to affected customers. Masks card number (last 4 digits), includes fraud details, transaction details, watchlist info, and immediate action instructions. |
| `fintech_streaming/alert/high_value_transaction_email_notifier.py` | `fintech.gold.high_value_transactions_alert` | Gmail SMTP | SDP `foreach_batch_sink` that sends HTML email alerts when a transaction exceeds the customer's configured transaction limit. Includes amount vs. limit comparison. |

---

## 📊 Summary Table

| Component | File | Layer | Source | Destination Table |
|-----------|------|-------|--------|-------------------|
| Secret scope setup | `Secret_Scope_02` | Infra | Databricks API | `fintech-scope` (secrets) |
| Kafka test | `Kafka_Test_01` | Infra | Confluent Kafka | `fintech.bronze.*_test` |
| Email test | `send_email_03` | Infra | Gmail SMTP | — |
| Watchlist file generator | `fraud_watchlist_data_generator` | Data gen | CSV → JSON files | UC Volume |
| Transactions ingestion | `transactions_bronze.py` | Bronze | Kafka stream | `fintech.bronze.transactions` |
| Watchlist ingestion | `fraud_watchlist_bronze.py` | Bronze | Auto Loader (JSON) | `fintech.bronze.fraud_watchlist` |
| Transactions parsing | `transactions_silver.py` | Silver | Bronze transactions | `fintech.silver.transactions` |
| Watchlist cleaning | `fraud_watchlist_silver.py` | Silver | Bronze watchlist | `fintech.silver.fraud_watchlist` |
| Customer cleaning | `customers_silver.py` | Silver | Bronze customers | `fintech.silver.customers` |
| Fraud detection | `fraud_card_alert.py` | Gold | Silver (all 3) | `fintech.gold.fraud_card_alert` |
| Fraud email alerts | `fraud_card_alert_email_notifier.py` | Alert | Gold fraud alerts | Gmail SMTP |
| High-value alerts | `high_value_transaction_email_notifier.py` | Alert | Gold high-value | Gmail SMTP |

---

## 🔄 How It Works

1. **Live credit card transactions** stream in from Confluent Cloud Kafka topic `credit_card_transactions`
2. **Fraud watchlist entries** are generated from a CSV file and written as individual JSON files to a UC Volume (simulating real-time arrivals)
3. Both data streams are **ingested** into the Bronze layer (Kafka for transactions, Auto Loader for watchlist)
4. Both are **cleaned and parsed** into structured tables in the Silver layer, with data quality checks
5. The Gold layer **joins** transactions against the fraud watchlist on `card_number = entity_id` — any match is a fraud alert
6. The alert layer **sends detailed HTML email alerts** to affected customers via Gmail SMTP, including transaction details, risk level, and action required

---

## 🔧 Prerequisites

- Databricks workspace with Unity Catalog enabled
- Confluent Cloud Kafka cluster with topic `credit_card_transactions`
- Databricks secret scope `fintech-scope` with secrets:
  - `kafka_connection_details` — JSON with `bootstrap_servers`, `topic`, `api_key`, `api_secret`
  - `gmail_api_key` — Gmail app password for SMTP authentication
- UC Volume `/Volumes/fintech/source/fraud_watchlist/source_data/` for watchlist JSON files
- UC catalog/schema `fintech` with `bronze`, `silver`, and `gold` schemas

---

## 🚀 Getting Started

1. Run `Secret_Scope_02` to create the secret scope and store Kafka + Gmail credentials
2. Run `fraud_watchlist_data_generator` to populate the UC Volume with watchlist JSON files
3. Create a Databricks pipeline referencing the bronze, silver, gold, and alert `.py` files
4. Start the pipeline — transactions will flow from Kafka, watchlist entries from Auto Loader, and fraud alerts will be sent via email
