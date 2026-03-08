import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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

# ------------------------------------------------
# Get latest timestamp already processed in Silver
# ------------------------------------------------

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT MAX(ingestion_timestamp) FROM clean.crypto_market")
    ).scalar()

latest_timestamp = result if result else "2000-01-01"

# ------------------------------------------------
# Pull only NEW Bronze data
# ------------------------------------------------

query = f"""
SELECT
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    price_change_24h,
    market_cap_rank,
    ingestion_timestamp
FROM raw.crypto_market_raw
WHERE ingestion_timestamp > '{latest_timestamp}'
"""

df = pd.read_sql(query, engine)

if df.empty:
    print("No new data to process.")
    exit()

# ------------------------------------------------
# Deduplicate
# ------------------------------------------------

df = df.sort_values("market_cap_rank", ascending=True)
df = df.drop_duplicates(subset=["coin_id"], keep="first")

# ------------------------------------------------
# Append to Silver
# ------------------------------------------------

df.to_sql(
    "crypto_market",
    engine,
    schema="clean",
    if_exists="append",
    index=False
)

print(f"{len(df)} new records written to clean.crypto_market")