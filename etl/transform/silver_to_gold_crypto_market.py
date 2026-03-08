import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import date

load_dotenv()

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

# -------------------------
# Load Silver data
# -------------------------

query = "SELECT * FROM clean.crypto_market"
df = pd.read_sql(query, engine)

# -------------------------
# Build dim_coin
# -------------------------

dim_coin = df[["coin_id", "symbol", "name"]].drop_duplicates().reset_index(drop=True)

dim_coin["coin_key"] = dim_coin.index + 1

dim_coin = dim_coin[["coin_key", "coin_id", "symbol", "name"]]

dim_coin.to_sql(
    "dim_coin",
    engine,
    schema="mart",
    if_exists="replace",
    index=False
)

# -------------------------
# Build dim_date
# -------------------------

df["full_date"] = date.today()

dim_date = pd.DataFrame()
dim_date["full_date"] = df["full_date"].drop_duplicates()

dim_date["year"] = pd.to_datetime(dim_date["full_date"]).dt.year
dim_date["month"] = pd.to_datetime(dim_date["full_date"]).dt.month
dim_date["day"] = pd.to_datetime(dim_date["full_date"]).dt.day

dim_date["date_key"] = pd.to_datetime(dim_date["full_date"]).dt.strftime("%Y%m%d").astype(int)

dim_date = dim_date[["date_key", "full_date", "year", "month", "day"]]

dim_date.to_sql(
    "dim_date",
    engine,
    schema="mart",
    if_exists="replace",
    index=False
)

# -------------------------
# Build fact table
# -------------------------

fact = df.merge(dim_coin, on=["coin_id", "symbol", "name"])

fact["date_key"] = pd.to_datetime(fact["full_date"]).dt.strftime("%Y%m%d").astype(int)

fact_table = fact[[
    "coin_key",
    "date_key",
    "ingestion_timestamp",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_24h",
    "market_cap_rank"
]]

# -------------------------
# Incremental Load Logic
# -------------------------

try:
    latest_ts_query = """
    SELECT MAX(ingestion_timestamp) AS latest_ts
    FROM mart.fact_crypto_market
    """
    
    latest_ts = pd.read_sql(latest_ts_query, engine).iloc[0]["latest_ts"]

    if latest_ts is not None:
        fact_table = fact_table[fact_table["ingestion_timestamp"] > latest_ts]

except Exception:
    # Table does not exist yet
    pass

# -------------------------
# Write Fact Table
# -------------------------

if not fact_table.empty:

    fact_table.to_sql(
        "fact_crypto_market",
        engine,
        schema="mart",
        if_exists="append",
        index=False
    )

    print(f"{len(fact_table)} new records written to mart.fact_crypto_market")

else:
    print("No new records to load.")