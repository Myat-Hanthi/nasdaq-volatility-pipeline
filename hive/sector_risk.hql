USE nasdaq;

-- Query 2a: Sector risk ranking overall
SELECT
    sector,
    COUNT(DISTINCT ticker)           AS ticker_count,
    ROUND(AVG(volatility_20d), 6)   AS avg_volatility_20d,
    ROUND(AVG(volatility_60d), 6)   AS avg_volatility_60d,
    ROUND(MAX(volatility_20d), 6)   AS peak_volatility,
    ROUND(AVG(atr_14d), 4)          AS avg_atr_14d,
    COUNT(*)                         AS observation_count
FROM stock_features
WHERE
    sector IS NOT NULL
    AND volatility_20d IS NOT NULL
    AND volatility_20d > 0
GROUP BY sector
ORDER BY avg_volatility_20d DESC;

-- Query 2b: Sector risk by year
SELECT
    s.sector,
    s.year,
    ROUND(s.avg_vol, 6)                              AS avg_volatility_20d,
    ROUND(idx.index_avg_vol, 6)                      AS index_avg_volatility,
    ROUND((s.avg_vol - idx.index_avg_vol)
          / idx.index_avg_vol * 100, 2)              AS pct_above_index,
    s.ticker_count
FROM (
    SELECT
        sector,
        year,
        AVG(volatility_20d)     AS avg_vol,
        COUNT(DISTINCT ticker)  AS ticker_count
    FROM stock_features
    WHERE
        sector IS NOT NULL
        AND volatility_20d IS NOT NULL
        AND volatility_20d > 0
    GROUP BY sector, year
) s
JOIN (
    SELECT
        year,
        AVG(volatility_20d) AS index_avg_vol
    FROM stock_features
    WHERE
        volatility_20d IS NOT NULL
        AND volatility_20d > 0
    GROUP BY year
) idx ON s.year = idx.year
ORDER BY s.year ASC, avg_volatility_20d DESC;

-- Query 2c: Sector volatility by month
SELECT
    sf.month,
    sf.sector,
    ROUND(AVG(sf.volatility_20d), 6)                 AS avg_volatility,
    ROUND(AVG(sf.volatility_20d) - overall.index_monthly_vol, 6)
                                                      AS deviation_from_index
FROM stock_features sf
JOIN (
    SELECT
        month,
        AVG(volatility_20d) AS index_monthly_vol
    FROM stock_features
    WHERE
        volatility_20d IS NOT NULL
        AND volatility_20d > 0
    GROUP BY month
) overall ON sf.month = overall.month
WHERE
    sf.sector IS NOT NULL
    AND sf.volatility_20d IS NOT NULL
    AND sf.volatility_20d > 0
GROUP BY sf.month, sf.sector, overall.index_monthly_vol
ORDER BY sf.month ASC, avg_volatility DESC;
