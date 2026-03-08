# Crypto Data Platform

A containerized **end-to-end data engineering pipeline** that ingests cryptocurrency market data, processes it using a **Medallion architecture (Bronze → Silver → Gold)**, and delivers analytics through a **Power BI dashboard**.

The platform is fully orchestrated using **Apache Airflow** and runs locally using **Docker containers**.

---

# Architecture

```
CoinGecko API
      │
      ▼
Airflow (Orchestration)
      │
      ▼
Bronze Layer (Raw API Data)
      │
      ▼
Silver Layer (Cleaned & Structured Data)
      │
      ▼
Gold Layer (Star Schema Data Warehouse)
      │
      ▼
SQL Server Data Warehouse
      │
      ▼
Power BI Analytics Dashboard
```

---

# Tech Stack

## Data Engineering

* Python
* Apache Airflow
* Docker
* SQL Server 2022
* Pandas
* SQLAlchemy
* CoinGecko API

## Analytics

* Power BI Desktop

---

# Project Structure

```
crypto-data-platform
│
├── airflow/                # Airflow configuration
├── docker/                 # Docker containers and compose file
├── etl/
│   ├── extract/            # API ingestion scripts
│   ├── transform/          # Bronze → Silver → Gold transformations & Loading logic
│   └── load/               # Data warehouse loading logic
│
├── warehouse/              # SQL warehouse related assets
│
├── dashboards/
│   └── powerbi/
│       └── crypto_market_analytics_dashboard.pbix
│
├── tests/
├── config/
├── requirements.txt
└── README.md
```

---

# Data Pipeline

The Airflow DAG **crypto_market_pipeline** orchestrates the workflow.

Pipeline stages:

```
ingest_crypto_api
        ↓
bronze_to_silver_transform
        ↓
silver_to_gold_transform
        ↓
data_quality_check
```

The DAG runs on a **daily schedule**.

---

# Data Warehouse Design

A **Star Schema** is implemented in the Gold layer.

## Dimension Tables

### mart.dim_coin

```
coin_key
coin_id
symbol
name
```

### mart.dim_date

```
date_key
full_date
year
month
day
```

## Fact Table

### mart.fact_crypto_market

```
coin_key
date_key
ingestion_timestamp
current_price
market_cap
total_volume
price_change_24h
market_cap_rank
```

The fact table is **append-only**, allowing the warehouse to maintain **historical market snapshots**.

---

# Dashboard Features

The Power BI dashboard provides:

* Total crypto market capitalization
* Total trading volume
* Top 10 cryptocurrencies by market cap
* Top 10 gainers (24h)
* Top 10 losers (24h)
* Coin market metrics table
* Market trend visualization
* Snapshot timestamp slicer for historical analysis
<img width="1622" height="1025" alt="image" src="https://github.com/user-attachments/assets/0bf67394-0405-4795-be87-b37a4b775d43" />

---

# Running the Project

## Start the platform

```
cd docker
docker compose up -d
```

## Access Airflow

```
http://localhost:8080
```

Airflow orchestrates the full pipeline using the DAG:

```
crypto_market_pipeline
```

---

# Data Source

CoinGecko API

https://www.coingecko.com/en/api

---

# Author

**Amanuel Birri**
