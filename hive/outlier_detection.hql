--Analysis Question 3: Which individual stocks behave as high-risk outliers relative to the NASDAQ-100 index?

--Outlier types:
-- Persistent outlier - stock is always high-risk(structural risk)
--Temporary outlier - stock spikes in specific periods only


USE nasdaq;
--Query 3a: Overall outlier ranking(all time)
-- Ranks every stock by how far above the index avg it sits

SELECT
    s.ticker,
    s.sector,
    ROUND(s.avg_vol, 6)                             AS avg_volatility_20d,
    ROUND(idx.index_avg, 6)                         AS index_avg_volatility,
    ROUND(idx.index_std, 6)                         AS index_std_volatility,
    ROUND((s.avg_vol - idx.index_avg)
          / idx.index_std, 4)                       AS z_score,
    ROUND((s.avg_vol - idx.index_avg)
          / idx.index_avg * 100, 2)                 AS pct_above_index,
    CASE
        WHEN (s.avg_vol - idx.index_avg) / idx.index_std > 2.0
            THEN 'EXTREME OUTLIER'
        WHEN (s.avg_vol - idx.index_avg) / idx.index_std > 1.5
            THEN 'HIGH OUTLIER'
        WHEN (s.avg_vol - idx.index_avg) / idx.index_std > 1.0
            THEN 'MODERATE OUTLIER'
        WHEN (s.avg_vol - idx.index_avg) / idx.index_std < -1.0
            THEN 'LOW RISK'
        ELSE 'NORMAL'
    END                                             AS risk_classification,
    s.trading_days
FROM (
    -- Per-ticker averages
    SELECT
        ticker,
        sector,
        AVG(volatility_20d)  AS avg_vol,
        COUNT(*)             AS trading_days
    FROM stock_features
    WHERE
        volatility_20d IS NOT NULL
        AND volatility_20d > 0
        AND NOT (volatility_20d <=> CAST('NaN' AS DOUBLE))
    GROUP BY ticker, sector
) s
JOIN (
    -- Index-wide statistics (single row)
    SELECT
        AVG(volatility_20d)    AS index_avg,
        STDDEV(volatility_20d) AS index_std
    FROM stock_features
    WHERE
        volatility_20d IS NOT NULL
        AND volatility_20d > 0
        AND NOT (volatility_20d <=> CAST('NaN' AS DOUBLE))
) idx ON 1 = 1
ORDER BY z_score DESC;

--Query 3b: Persisten vs temporary outliers
SELECT
    ticker,
    sector,
    COUNT(DISTINCT year)                            AS years_in_data,
    SUM(CASE WHEN year_vol > idx_avg + idx_std
             THEN 1 ELSE 0 END)                     AS years_as_outlier,
    ROUND(SUM(CASE WHEN year_vol > idx_avg + idx_std
             THEN 1 ELSE 0 END) * 100.0
          / COUNT(DISTINCT year), 1)                AS pct_years_as_outlier,
    CASE
        WHEN SUM(CASE WHEN year_vol > idx_avg + idx_std
                 THEN 1 ELSE 0 END) * 100.0
             / COUNT(DISTINCT year) >= 75
            THEN 'PERSISTENT'
        WHEN SUM(CASE WHEN year_vol > idx_avg + idx_std
                 THEN 1 ELSE 0 END) > 0
            THEN 'TEMPORARY'
        ELSE 'STABLE'
    END                                             AS outlier_type
FROM (
    -- Yearly volatility per ticker
    SELECT
        sf.ticker,
        sf.sector,
        sf.year,
        AVG(sf.volatility_20d)  AS year_vol,
        idx.idx_avg             AS idx_avg,
        idx.idx_std             AS idx_std
    FROM stock_features sf
    JOIN (
        SELECT
            year,
            AVG(volatility_20d)    AS idx_avg,
            STDDEV(volatility_20d) AS idx_std
        FROM stock_features
        WHERE
            volatility_20d IS NOT NULL
            AND volatility_20d > 0
            AND NOT (volatility_20d <=> CAST('NaN' AS DOUBLE))
        GROUP BY year
    ) idx ON sf.year = idx.year
    WHERE
        sf.volatility_20d IS NOT NULL
        AND sf.volatility_20d > 0
        AND NOT (sf.volatility_20d <=> CAST('NaN' AS DOUBLE))
    GROUP BY sf.ticker, sf.sector, sf.year, idx.idx_avg, idx.idx_std
) yearly
GROUP BY ticker, sector
ORDER BY pct_years_as_outlier DESC, years_as_outlier DESC;

--Query 3c: Top 10 extreme outlier stocks(summary)
SELECT
    s.ticker,
    s.sector,
    ROUND(s.avg_vol * 100, 4)                       AS avg_volatility_pct,
    ROUND((s.avg_vol - idx.index_avg)
          / idx.index_std, 2)                       AS z_score,
    ROUND(s.avg_atr, 2)                             AS avg_atr_14d
FROM (
    SELECT
        ticker,
        sector,
        AVG(volatility_20d) AS avg_vol,
        AVG(atr_14d)        AS avg_atr
    FROM stock_features
    WHERE
        volatility_20d IS NOT NULL
        AND volatility_20d > 0
        AND NOT (volatility_20d <=> CAST('NaN' AS DOUBLE))
    GROUP BY ticker, sector
) s
JOIN (
    SELECT
        AVG(volatility_20d)    AS index_avg,
        STDDEV(volatility_20d) AS index_std
    FROM stock_features
    WHERE
        volatility_20d IS NOT NULL
        AND volatility_20d > 0
        AND NOT (volatility_20d <=> CAST('NaN' AS DOUBLE))
) idx ON 1 = 1
ORDER BY z_score DESC
LIMIT 10;

