import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

# Database configuration
server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
driver = os.getenv("DB_DRIVER")

connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
    f"?driver={driver.replace(' ', '+')}"
    "&TrustServerCertificate=yes"
)

engine = create_engine(
"mssql+pyodbc://sa:StrongPass123!@crypto-sqlserver:1433/CryptoWarehouse?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
)

# API endpoint
url = "https://api.coingecko.com/api/v3/coins/markets"

all_data = []

# Collect multiple pages (≈1000 rows)
for page in range(1, 5):

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": page
    }

    try:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            logging.warning(f"API error on page {page}: {response.status_code}")
            time.sleep(5)
            continue

        data = response.json()

        if isinstance(data, list):
            all_data.extend(data)
        else:
            logging.error(f"Unexpected response format on page {page}")
            continue

    except Exception as e:
        logging.error(f"Request failed on page {page}: {e}")
        continue

    # Prevent API rate limiting
    time.sleep(1)

# Convert API data → DataFrame
df = pd.DataFrame(all_data)

if df.empty:
    logging.info("No data retrieved from API.")
    exit()

df = df[[
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_percentage_24h",
    "market_cap_rank"
]]

df = df.rename(columns={
    "id": "coin_id",
    "price_change_percentage_24h": "price_change_24h"
})

# Add ingestion timestamp
df["ingestion_timestamp"] = pd.Timestamp.utcnow()

# Load to Bronze layer
df.to_sql(
    "crypto_market_raw",
    engine,
    schema="raw",
    if_exists="append",
    index=False
)

logging.info(f"{len(df)} records inserted into raw.crypto_market_raw")