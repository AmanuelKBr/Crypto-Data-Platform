import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "mssql+pyodbc://sa:StrongPass123!@crypto-sqlserver:1433/CryptoWarehouse?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
)

bronze = pd.read_sql("SELECT COUNT(*) AS cnt FROM raw.crypto_market_raw", engine)
silver = pd.read_sql("SELECT COUNT(*) AS cnt FROM clean.crypto_market", engine)

dim_coin = pd.read_sql("SELECT COUNT(*) AS cnt FROM mart.dim_coin", engine)
dim_date = pd.read_sql("SELECT COUNT(*) AS cnt FROM mart.dim_date", engine)
fact = pd.read_sql("SELECT COUNT(*) AS cnt FROM mart.fact_crypto_market", engine)

print("Bronze rows:", bronze["cnt"][0])
print("Silver rows:", silver["cnt"][0])

print("dim_coin rows:", dim_coin["cnt"][0])
print("dim_date rows:", dim_date["cnt"][0])
print("fact_crypto_market rows:", fact["cnt"][0])