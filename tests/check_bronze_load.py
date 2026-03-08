import os
import pandas as pd
from sqlalchemy import create_engine
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

engine = create_engine(connection_string)

query = """
SELECT TOP 10 *
FROM raw.crypto_market_raw
ORDER BY ingestion_timestamp DESC
"""

df = pd.read_sql(query, engine)

print(df)
print(f"\nRows returned: {len(df)}")