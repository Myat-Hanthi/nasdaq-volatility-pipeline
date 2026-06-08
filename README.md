nasdaq-volatility-pipeline

A big data pipeline built to analyze the stock volatility patterns of NASDAQ-100 using Hadoop ecosystems tools.

Problem Definition

Volatility is an important measure in quantifying and evaluating investments and financial risks. Understanding how much a security's price moves over time and why stocks become volatile has a profound impact on the precision of risk models.
However, traditional tools like Excel or plain pandas cannot efficiently handle hundreds of millions of data points produced from analyzing volatility across 100+ stocks over multiple years. This project aims to build a scalable big data pipeline to solve this problem.

Why is this a big data problem?

It describes 3Vs of big data.

- Volume : 10 years of NASDAQ-100 daily OHLCV data across 165 collected tickers (84 processed) in Parquet format. With 23 engineered features the dataset reaches 108MB+ cumulative (42MB local + 66MB HDFS) and grows daily.
- Variety : The data contains multiple data types and feature families : OHLCV market data, trading volume, rolling volatility windows(5d/10d/20d/60d/90d), beta, ATR, momentum indicators, price range ratios and sector classifications.
- Velocity : Through automated daily scripts, the data is updated incrementally which simulates a real ingestion pipeline.Kafka streaming layer is added to monitor real-time volatility.

Analysis Questions

1.Monthly volatility trends : Which months historically demonstrates the highest average volatility across NASDAQ-100 constituents?

- Objectives : Identifying recurring high-volatility periods, detecting seasonal market instability, compare volatility during earnings seasons or macroeconomic events

2.Sector-level risk comparison: Which sectors (eg: tech, biotech, consumer discretionary) shows the highest and lowest volatility levels? Do sectors spike together?

- Objectives: Ranking sectors by historical risk, measuring correlation of volatility spikes between sectors, detecting whether market-wide shocks affect sectors uniformly or asymmetrically

3.Outlier detection: Relative to the NASDAQ-100 index, which individual stocks behave as high-risk outliers?

- Objectives: Detect unusual risk securities, calculate deviation from index-level volatility, detect persistent vs temporary outliers

Data Source

- yfinance Python library : daily OHLCV for all NASDAQ-100 tickers
-NASDAQ-100 ticker list : Component stocks, sector classifications (scraped from Wikipedia)

Tech Stack

- Data Collection : Python + yfinance + web scraping (Wikipedia)
- Storage : HDFS (Parquet format)
- Data Preprocessing : Apache Spark (PySpark)
- Analysis : Apache Hive (HiveQL)
- Visualization : Plotly + Matplotlib
- Automation : Shell script

Environment

- Cluster : HDP Sandbox
- Language : Python 3.8+
- Key Libraries: yfinance, pyspark, plotly, matplotlib

Implementation plan (Pipeline)

- Data Collection : yfinance API -> Python ingestion script
- HDFS Storage : Raw CSV -> Parquet format -> store in HDFS
- Spark Preprocessing : remove missing values & trading halts -> compute std of log returns -> compute 5 rolling volatility widows(5d/10d/20d/60d/90d) -> compute ATR (Average True Range)->momentum indicators
- Hive Analysis : Monthly volatility trends (GROUP BY months), sector-level risk aggregation (GROUP BY sector), Outlier stocks (JOIN with sector averages,z-score classification)
- Kafka Streaming: Real-time price producer-> volatility consumer with WATCH/WARNING/ALERT classifications
- Visualization : Volatility heatmap (stock x month), sector risk bar chart, Time-series plot (Volatility spikes)
- Automation : End-to-end daily pipeline shell script
Results
Dataset
- 165 tickers collected, 84 processed after Spark cleaning
- 10 years of data(2016-2026), 211,520 processed rows
- 23 engineered features per row
- 108MB cumulative data(42MB local + 66MB HDFS)

Monthly Volatility (Q1)

- April is the most volatile month (avg 0.0213)-: earnings season peak
- September is the calmest month (avg 0.0145): post-summer quiet period
- 2022 was the most volatile year driven by Fed rate hikes
- April 2025 spike caused by tariff shock

Sector Risk (Q2)

- Highest risk: Technology (0.0215), Consumer Discretionary(0.0194)
- Energy enters top tier(0.0186) with 10 years data
- Lowest risk: Utilities (0.0124), Consumer Staples (0.0133)
- Sectors responds asymmetrically to market shocks

Outlier Stocks (Q3)

- Top outlier: AMD (z-score 1.62, avg volatility 3.36%)
- TSLA second (z-score 1.52, ATR $8.45/day)
- No extreme outliers (z > 2.0) — index self-selects for established companies

Real-Time Kafka Streaming(June 8,2026)


- MRVL triggered ALERT at 8.3x sector baseline (return: -18.3%)
- MRNA triggered ALERT at 3.6x sector baseline (return: -8.4%)
- AMD triggered ALERT at 3.1x sector baseline (return: -11.5%)
- META triggered WARNING at 2.1x sector baseline

Notes
- Beta computation skipped due to PySpark version constraints on HDP Sandbox 3.0.1
- ANSS and WBA excluded — both delisted during the study period

AI Tool Usage

- Claude AI: debugging assistance for Python/PySpark/HiveQL errors, 
  code review, README proofreading

