USE nasdaq;

-- Query 1a: Average volatility by calendar month
SELECT
    month,
    ROUND(AVG(volatility_20d), 6)    AS avg_volatility_20d,
    ROUND(AVG(volatility_60d), 6)    AS avg_volatility_60d,
    ROUND(MAX(volatility_20d), 6)    AS max_volatility_20d,
    ROUND(MIN(volatility_20d), 6)    AS min_volatility_20d,
    COUNT(DISTINCT ticker)            AS ticker_count,
    COUNT(*)                          AS observation_count
FROM stock_features
WHERE
    volatility_20d IS NOT NULL
    AND volatility_20d > 0
GROUP BY month
ORDER BY avg_volatility_20d DESC;

-- Query 1b: Average volatility by year and month
SELECT
    year,
    month,
    ROUND(AVG(volatility_20d), 6)    AS avg_volatility_20d,
    ROUND(MAX(volatility_20d), 6)    AS max_volatility_20d,
    COUNT(DISTINCT ticker)            AS ticker_count
FROM stock_features
WHERE
    volatility_20d IS NOT NULL
    AND volatility_20d > 0
GROUP BY year, month
ORDER BY year ASC, month ASC;
