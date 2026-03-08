IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'raw')
BEGIN
    EXEC('CREATE SCHEMA raw')
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'clean')
BEGIN
    EXEC('CREATE SCHEMA clean')
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'mart')
BEGIN
    EXEC('CREATE SCHEMA mart')
END
GO

IF OBJECT_ID('raw.crypto_market_raw', 'U') IS NULL
BEGIN
    CREATE TABLE raw.crypto_market_raw (
        ingestion_id INT IDENTITY(1,1) PRIMARY KEY,
        coin_id NVARCHAR(100),
        symbol NVARCHAR(50),
        name NVARCHAR(100),
        current_price FLOAT,
        market_cap FLOAT,
        total_volume FLOAT,
        price_change_24h FLOAT,
        market_cap_rank INT,
        ingestion_timestamp DATETIME DEFAULT GETDATE()
    )
END
GO

IF OBJECT_ID('clean.crypto_market', 'U') IS NULL
BEGIN
    CREATE TABLE clean.crypto_market (
        coin_id NVARCHAR(100),
        symbol NVARCHAR(50),
        name NVARCHAR(100),
        current_price FLOAT,
        market_cap FLOAT,
        total_volume FLOAT,
        price_change_24h FLOAT,
        market_cap_rank INT,
        ingestion_timestamp DATETIME
    )
END
GO

IF OBJECT_ID('mart.dim_coin', 'U') IS NULL
BEGIN
    CREATE TABLE mart.dim_coin (
        coin_key INT IDENTITY(1,1) PRIMARY KEY,
        coin_id NVARCHAR(100),
        symbol NVARCHAR(50),
        name NVARCHAR(100)
    )
END
GO

IF OBJECT_ID('mart.dim_date', 'U') IS NULL
BEGIN
    CREATE TABLE mart.dim_date (
        date_key INT PRIMARY KEY,
        full_date DATE,
        year INT,
        month INT,
        day INT
    )
END
GO

IF OBJECT_ID('mart.fact_crypto_market', 'U') IS NULL
BEGIN
    CREATE TABLE mart.fact_crypto_market (
        coin_key INT,
        date_key INT,
        current_price FLOAT,
        market_cap FLOAT,
        total_volume FLOAT,
        price_change_24h FLOAT,
        market_cap_rank INT
    )
END
GO