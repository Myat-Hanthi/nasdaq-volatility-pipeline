# nasdaq-volatility-pipeline
A big data pipeline built to analyze the stock volatility patterns of NASDAQ-100 using Hadoop ecosystems tools.

Problem Definition

Volatility is an important measure in quantifying and evaluating invesments and financial risks. Understanding how much a security's price moves over time and why stocks become volatile has a profound impact on the precision of risk models.

However, traditional tools like Excel or plain pandas cannot efficiently handle hundreds of millions of data points produced from analyzing volatility across 100+ stocks over multiple years. This project aims to build a scalable big data pipeline to solve this problem.

Why is this a big data problem?
It describes 3Vs of big data.
1. Volume : 5 years of NASDAQ-100 daily OHLCV data acreoss 100+ tickers in Parquet format. With engineered features it could reach 150MB+ and grow overtime along with the addition of new features or tickers.
2. Variety : The data contains mulitple data types and feature families : OHLCV market data, trading volume, rolling volatility, beta, ATR and other derived indicators.
3. Velocity : Through automated daily scripts, the data is updated incrementally which simulates a real ingestion pipeline.

Analysis Questions
1. Monthly volatility trends : Which months historically demonstrates the highest average volatility across NASDAQ-100 constituents?
   - Obejectives : Identifying recurring high-volatility periods, detecting seasonal                        market instability,compare volatility during earnings seasons or
                   macroeconomic events
2. Sector-levl risk comparison: Which sectors (eg: tech, biotech, consumer discretionary) shows the highest and lowest volatitliy levels? Do sectors spike together?
   - Objectives: Ranking sectors by historical risk, measuring correlation of volatility                  spikes between sectors, detecting whether market-wide shocks affect                      sectors uniformly or asymmertically 
3. Outlier detection: Relative to the NASDAQ-100 index , which individual stocks behave as high-risk outliers?
   - Objectives: Detect unsusal risk securities, calculate deviation from index-level                     volatility, detect persisten vs temporary outliers


Data Source 
- yfinance Python library : daily OHLCV for all NASDAQ-100 tickers
- NASDAQ-100 ticker list : Component stocks, sector classifications

Tech Stack
- Data Collection : Python + yfinance
- Storage : HDFS(Parquet format)
- Data Preprocessing : Apache Spark (PySpark)
- Analysis : Apache Hive (HiveQL)
- Visualization : Plotly + Matplotlib
- Automation : Shell script

Environment 
- Cluster : HDP Sandbox
- Language : Python 3.8+
- Key Libraries: yfinance, pyspark, plotly, matplotlib



