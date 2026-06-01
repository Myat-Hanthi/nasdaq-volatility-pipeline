CREATE DATABASE IF NOT EXISTS nasdaq
COMMENT 'NASDAQ-100 volatility analysis database';

USE nasdaq;

DROP TABLE IF EXISTS stock_features;

CREATE EXTERNAL TABLE stock_features (
    `date`          INT,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          BIGINT,
    name            STRING,
    sector          STRING,
    log_return      DOUBLE,
    volatility_20d  DOUBLE,
    volatility_60d  DOUBLE,
    atr_14d         DOUBLE,
    year            INT,
    month           INT,
    quarter         INT
)
PARTITIONED BY (ticker STRING)
STORED AS PARQUET
LOCATION '/user/maria_dev/nasdaq/processed'
TBLPROPERTIES ("parquet.compress"="SNAPPY");

MSCK REPAIR TABLE stock_features;

SELECT COUNT(*) AS total_rows FROM stock_features;

SELECT
    ticker,
    DATE_ADD('1970-01-01', `date`)   AS trade_date,
    close,
    ROUND(log_return, 6)             AS log_return,
    ROUND(volatility_20d, 6)         AS volatility_20d,
    ROUND(atr_14d, 4)                AS atr_14d,
    sector
FROM stock_features
WHERE ticker = 'AAPL'
ORDER BY `date`
LIMIT 5;
