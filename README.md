nasdaq-volatility-pipeline

A big data pipeline built to analyze the stock volatility patterns of NASDAQ-100 using Hadoop ecosystems tools.

Problem Definition

Volatility is an important measure in quantifying and evaluating investments and financial risks. Understanding how much a security's price moves over time and why stocks become volatile has a profound impact on the precision of risk models.
However, traditional tools like Excel or plain pandas cannot efficiently handle hundreds of millions of data points produced from analyzing volatility across 100+ stocks over multiple years. This project aims to build a scalable big data pipeline to solve this problem.

Why is this a big data problem?

It describes 3Vs of big data.

- Volume : 5 years of NASDAQ-100 daily OHLCV data across 100+ tickers in Parquet format. With engineered features it could reach 150MB+ and grow overtime along with the addition of new features or tickers.
- Variety : The data contains multiple data types and feature families : OHLCV market data, trading volume, rolling volatility, beta, ATR and other derived indicators.
- Velocity : Through automated daily scripts, the data is updated incrementally which simulates a real ingestion pipeline.

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
- Spark Preprocessing : remove missing values & trading halts -> compute std of log returns -> compute beta (NASDAQ-100 ETF) -> compute ATR (Average True Range)
- Hive Analysis : Monthly volatility trends (GROUP BY months), sector-level risk aggregation (GROUP BY sector), Outlier stocks (JOIN with sector averages)
- Visualization : Volatility heatmap (stock x month), sector risk bar chart, Time-series plot (Volatility spikes)

Results
Monthly Volatility (Q1)

- May is historically the most volatile month (avg 0.0242)
- July is the calmest month (avg 0.0176)
- 2022 was the most volatile year driven by Fed rate hikes
- April 2025 spike caused by tariff shock

Sector Risk (Q2)

- Highest risk: Financials (0.0243), Technology (0.0232)
- Lowest risk: Utilities (0.0120), Consumer Staples (0.0129)
- Sectors do NOT spike uniformly — Tech and Financials lead every spike

Outlier Stocks (Q3)

- Top outlier: MRNA (z-score 1.75, avg volatility 3.97%)
- TSLA second (z-score 1.33, ATR $13.41/day)
- No extreme outliers (z > 2.0) — index self-selects for established companies

Real-Time Kafka Streaming

- MRVL triggered ALERT at 8.3x sector baseline (return: -18.3%)
- MRNA triggered ALERT at 3.6x sector baseline (return: -8.4%)
- AMD triggered ALERT at 3.1x sector baseline (return: -11.5%)
- META triggered WARNING at 2.1x sector baseline

Notes

- ANSS and WBA were excluded — both delisted during the study period
- 60 out of 97 tickers retained after Spark cleaning step
- Beta computation skipped due to PySpark version constraints on HDP Sandbox 3.0.1
- Kafka broker runs on HDP Sandbox at 172.18.0.2:6667

AI Tool Usage

- Claude AI: pipeline architecture design,AI assisted code implementation and debugging
