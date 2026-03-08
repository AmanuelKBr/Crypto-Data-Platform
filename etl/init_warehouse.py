import os
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

engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")

with open("warehouse/init_schema.sql", "r") as f:
    sql_commands = f.read().split("GO")

with engine.connect() as conn:
    for command in sql_commands:
        command = command.strip()
        if command:
            conn.execute(text(command))

print("Warehouse schemas created: raw, clean, mart")