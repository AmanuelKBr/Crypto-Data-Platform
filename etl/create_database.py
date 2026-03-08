import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

server = os.getenv("DB_SERVER")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
driver = os.getenv("DB_DRIVER")

connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}:1433/master"
    f"?driver={driver.replace(' ', '+')}"
    "&TrustServerCertificate=yes"
)

engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")

with engine.connect() as conn:
    conn.execute(text("IF DB_ID('CryptoWarehouse') IS NULL CREATE DATABASE CryptoWarehouse"))
    print("Database ensured: CryptoWarehouse")