# -*- coding: utf-8 -*-
"""
Reads raw OHLCV CSVs from HDFS, engineers volatility features, and writes
enriched Parquet files back to HDFS.

What this job does (in order):
  1. Load all ticker CSVs from HDFS into a single Spark DataFrame
  2. Join sector metadata
  3. Clean: remove nulls, zero-volume days (trading halts)
  4. Compute log returns
  5. Compute rolling volatility (20-day and 60-day windows)
  6. Compute beta against QQQ (NASDAQ-100 ETF)
  7. Compute ATR (Average True Range, 14-day)
  8. Write output as Parquet to HDFS

"""

import argparse
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, DoubleType, LongType
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# chema 
# Explicitly define schema so Spark doesn't have to infer it from every file.
# This is faster and prevents type mismatches across tickers.

OHLCV_SCHEMA = StructType([
    StructField("ticker", StringType(),  nullable=False),
    StructField("date",   StringType(),  nullable=False),  # read as string, cast later
    StructField("open",   DoubleType(),  nullable=True),
    StructField("high",   DoubleType(),  nullable=True),
    StructField("low",    DoubleType(),  nullable=True),
    StructField("close",  DoubleType(),  nullable=True),
    StructField("volume", LongType(),    nullable=True),
])

META_SCHEMA = StructType([
    StructField("ticker", StringType(), nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("sector", StringType(), nullable=True),
])


# Spark Session 

def build_spark_session():
    """
    Create a SparkSession configured for HDP Sandbox.
    """
    return (
        SparkSession.builder
        .appName("nasdaq-volatility-preprocess")
        .master("local[*]")
        # Reduce shuffle partitions — default 200 is too many for ~100 tickers
        .config("spark.sql.shuffle.partitions", "8")
        # Enable Hive support so we can write directly to Hive tables later
        .config("spark.sql.catalogImplementation", "hive")
        # Parquet compression
        .config("spark.sql.parquet.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )


# Step 1: Load Data 

def load_ohlcv(spark, input_hdfs):
    """
    Load all ticker CSVs from HDFS into one DataFrame.

    """
    log.info("Loading OHLCV data from: %s", input_hdfs)

    df = (
        spark.read
        .schema(OHLCV_SCHEMA)
        .option("header", "true")
        .option("mode", "DROPMALFORMED")   # skip rows with wrong column count
        .csv(input_hdfs + "/*.csv")
    )

    # Cast date string -> proper DateType
    df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    row_count = df.count()
    ticker_count = df.select("ticker").distinct().count()
    log.info("Loaded %d rows across %d tickers", row_count, ticker_count)

    return df


def load_metadata(spark, meta_hdfs):
    """
    Load the sector metadata CSV.
    This gives  the sector label for each ticker,
    which is needed for Analysis Question 2 (sector risk).
    """
    log.info("Loading metadata from: %s", meta_hdfs)

    meta = (
        spark.read
        .schema(META_SCHEMA)
        .option("header", "true")
        .csv(meta_hdfs)
    )

    log.info("Metadata loaded: %d tickers", meta.count())
    return meta


#Step 2: Clean Data

def clean(df):
    """
    Remove bad rows from the raw data.

    What is removed and why:
    - Null OHLCV values: can't compute features on missing prices
    - Zero/null volume: indicates a trading halt or data error.
      On halted days, prices don't move — including them would
      artificially suppress volatility readings.
    - Negative prices: data corruption
    - close <= 0: log return would be undefined (log of zero/negative)
    """
    log.info("Cleaning data...")

    before = df.count()

    df = df.filter(
        F.col("open").isNotNull()   &
        F.col("high").isNotNull()   &
        F.col("low").isNotNull()    &
        F.col("close").isNotNull()  &
        F.col("volume").isNotNull() &
        (F.col("volume") > 0)       &
        (F.col("close")  > 0)       &
        (F.col("open")   > 0)       &
        (F.col("high")   > 0)       &
        (F.col("low")    > 0)
    )

    after = df.count()
    log.info("Cleaned: removed %d bad rows (%d -> %d)", before - after, before, after)

    return df


# Step 3: Join Sector Metadata 

def join_metadata(df, meta):
    """
    Left join sector labels onto the price data.

     use a left join (not inner) so tickers without metadata
    are kept but have null sector — better than silently dropping data.
    """
    log.info("Joining sector metadata...")
    df = df.join(meta.select("ticker", "sector", "name"), on="ticker", how="left")
    return df


# Step 4: Compute Log Returns

def compute_log_returns(df):
    """
    Compute daily log return for each ticker.

    Formula: log_return = ln(close_t / close_{t-1})

    Why log returns?
    - Symmetric: +10% and -10% are equal magnitude in log space
    - Time-additive: weekly return = sum of daily log returns
    - Stationary: more suitable for statistical analysis than raw prices
    - Normally distributed: volatility models assume normality

    Implementation:
    - Window partitioned by ticker, ordered by date
    - lag(1) gets the previous day's close for each ticker independently
    - Compute ln(close / prev_close)
    - First row per ticker has no previous day → null (filtered out later
      in rolling window calculations automatically)
    """
    log.info("Computing log returns...")

    # Define a window: for each ticker, ordered by date
    # This ensures lag() looks at the previous trading day
    # for THAT ticker only — not across tickers
    ticker_window = Window.partitionBy("ticker").orderBy("date")

    df = df.withColumn(
        "prev_close",
        F.lag("close", 1).over(ticker_window)
    )

    df = df.withColumn(
        "log_return",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
            F.log(F.col("close") / F.col("prev_close"))
        ).otherwise(None)
    )

    # Drop the intermediate prev_close column — not needed downstream
    df = df.drop("prev_close")

    return df


#  Step 5: Rolling Volatility

def compute_rolling_volatility(df):
    """
    Compute rolling standard deviation of log returns.

    volatility_20d  = std(log_returns, 20-day window)  ← ~1 month
    volatility_60d  = std(log_returns, 60-day window)  ← ~1 quarter

    This is the standard financial definition of realized volatility.
    Higher std dev = more price movement = more risk.

    Window definition:
    - partitionBy("ticker"): compute separately per stock
    - orderBy("date"): chronological order
    - rowsBetween(-19, 0): current row + 19 previous rows = 20 rows total
      (-59, 0) for the 60-day window

    Why rowsBetween instead of rangeBetween?
    - rowsBetween counts actual trading days (rows in the data)
    - rangeBetween would count calendar days, which misses weekends/holidays
    - Want exactly 20 trading days, not 20 calendar days
    """
    log.info("Computing rolling volatility (5d, 10d, 20d, 60d, 90d)...")

    ticker_date_window_5 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-4, 0)    # 5 trading days (~1 week)
    )

    ticker_date_window_10 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-9, 0)    # 10 trading days (~2 weeks)
    )

    ticker_date_window_20 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-19, 0)   # 20 trading days (~1 month)
    )

    ticker_date_window_60 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-59, 0)   # 60 trading days (~1 quarter)
    )

    ticker_date_window_90 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-89, 0)   # 90 trading days (~4 months)
    )

    # Rolling volatility — std dev of log returns
    df = df.withColumn(
        "volatility_5d",
        F.stddev("log_return").over(ticker_date_window_5)
    )

    df = df.withColumn(
        "volatility_10d",
        F.stddev("log_return").over(ticker_date_window_10)
    )

    df = df.withColumn(
        "volatility_20d",
        F.stddev("log_return").over(ticker_date_window_20)
    )

    df = df.withColumn(
        "volatility_60d",
        F.stddev("log_return").over(ticker_date_window_60)
    )

    df = df.withColumn(
        "volatility_90d",
        F.stddev("log_return").over(ticker_date_window_90)
    )

    # Rolling mean return (momentum indicator)
    df = df.withColumn(
        "mean_return_20d",
        F.avg("log_return").over(ticker_date_window_20)
    )

    df = df.withColumn(
        "mean_return_60d",
        F.avg("log_return").over(ticker_date_window_60)
    )

    # Rolling max and min close (price range indicator)
    df = df.withColumn(
        "rolling_max_20d",
        F.max("close").over(ticker_date_window_20)
    )

    df = df.withColumn(
        "rolling_min_20d",
        F.min("close").over(ticker_date_window_20)
    )

    # Price range ratio — how wide is the 20-day price range?
    df = df.withColumn(
        "price_range_ratio_20d",
        F.when(
            F.col("rolling_min_20d") > 0,
            (F.col("rolling_max_20d") - F.col("rolling_min_20d")) / F.col("rolling_min_20d")
        ).otherwise(None)
    )

    return df


# Step 6: Beta 

def compute_beta(df):
    """
    Compute 60-day rolling beta for each stock against the NASDAQ-100 index.

    Beta formula:
        beta = cov(stock_return, index_return) / var(index_return)

    What beta means:
        beta > 1.0 : stock amplifies index moves (higher risk)
        beta = 1.0 : stock moves exactly with the index
        beta < 1.0 : stock is more stable than the index
        beta < 0.0 : stock moves opposite to the index (rare)

    Implementation approach:
    Since we don't have a separate QQQ feed, we approximate the
    NASDAQ-100 index return by computing the equal-weighted average
    log return across all 97 tickers on each date.

    
    Steps:
    1. Compute daily mean log return across all tickers (proxy index)
    2. Join index return back to each ticker row
    3. Compute rolling covariance and variance using window functions
    4. beta = rolling_cov / rolling_var
    """
    log.info("Computing beta (60-day rolling, vs equal-weighted index)...")

    # Step 6a: Compute index return = mean of all tickers' log_return per date
    index_returns = df.groupBy("date").agg(
        F.avg("log_return").alias("index_return")
    )

    # Step 6b: Join index return onto each row
    df = df.join(index_returns, on="date", how="left")

    # Step 6c: Rolling window (60 trading days) per ticker
    beta_window = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-59, 0)
    )

    # Step 6d: Compute rolling covariance and variance
    # Spark doesn't have a built-in rolling covariance function,
    # so we use the mathematical identity:
    #   cov(X, Y) = E[XY] - E[X]*E[Y]
    df = df.withColumn(
        "rolling_cov",
        F.avg(F.col("log_return") * F.col("index_return")).over(beta_window) -
        F.avg("log_return").over(beta_window) *
        F.avg("index_return").over(beta_window)
    )

    df = df.withColumn(
        "rolling_var_index",
        F.avg(F.col("index_return") * F.col("index_return")).over(beta_window) -
        F.avg("index_return").over(beta_window) *
        F.avg("index_return").over(beta_window)
    )

    # Step 6e: Beta = cov / var (guard against division by zero)
    df = df.withColumn(
        "beta_60d",
        F.when(
            F.col("rolling_var_index") > 0,
            F.col("rolling_cov") / F.col("rolling_var_index")
        ).otherwise(None)
    )

    # Drop intermediate columns
    df = df.drop("rolling_cov", "rolling_var_index")

    return df


#Step 7: ATR (Average True Range) 

def compute_atr(df):
    """
    Compute the 14-day Average True Range (ATR).


    True Range = max of these three values:
        1. high - low                    (today's range)
        2. |high - previous_close|       (gap up)
        3. |low  - previous_close|       (gap down)

    ATR_14 = rolling mean of True Range over 14 trading days

    Why ATR alongside log return volatility?
    - Log return volatility is percentage-based (relative)
    - ATR is dollar-based (absolute)
    - A $1 move means very different things for a $10 stock vs $1000 stock
    - ATR captures the actual dollar risk per share
    - Together they give a fuller picture of risk

    """
    log.info("Computing ATR (14-day)...")

    ticker_window = Window.partitionBy("ticker").orderBy("date")

    # Get previous day's close
    df = df.withColumn(
        "prev_close_atr",
        F.lag("close", 1).over(ticker_window)
    )

    # True Range = max of the three components
    df = df.withColumn(
        "true_range",
        F.when(
            F.col("prev_close_atr").isNotNull(),
            F.greatest(
                F.col("high") - F.col("low"),
                F.abs(F.col("high") - F.col("prev_close_atr")),
                F.abs(F.col("low")  - F.col("prev_close_atr"))
            )
        ).otherwise(F.col("high") - F.col("low"))  # first row fallback
    )

    # ATR = 14-day rolling mean of True Range
    atr_window = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-13, 0)   # 14 trading days
    )

    df = df.withColumn(
        "atr_14d",
        F.avg("true_range").over(atr_window)
    )

    # Drop intermediates
    df = df.drop("prev_close_atr", "true_range")

    return df


# Step 8: Add Time Columns 

def add_time_columns(df):
    """
    Extract year, month, quarter from date.

    """
    log.info("Adding time columns (year, month, quarter)...")

    df = df.withColumn("year",    F.year("date"))
    df = df.withColumn("month",   F.month("date"))
    df = df.withColumn("quarter", F.quarter("date"))

    return df


# Step 9: Write Parquet 

def write_parquet(df, output_hdfs):
    """
    Write the enriched DataFrame to HDFS as Parquet.

    Output schema:
        ticker, date, open, high, low, close, volume,
        name, sector,
        log_return,
        volatility_20d, volatility_60d,
        beta_60d, atr_14d,
        year, month, quarter
    """
    log.info("Writing Parquet to: %s", output_hdfs)

    # Select and order final columns cleanly
    final_cols = [
        "ticker", "date", "open", "high", "low", "close", "volume",
        "name", "sector",
        "log_return",
        "volatility_5d", "volatility_10d", "volatility_20d",
        "volatility_60d", "volatility_90d",
        "mean_return_20d", "mean_return_60d",
        "rolling_max_20d", "rolling_min_20d", "price_range_ratio_20d",
        "atr_14d",
        "year", "month", "quarter",
    ]

    df = df.select(final_cols)

    (
        df.write
        .mode("overwrite")
        .partitionBy("ticker")
        .parquet(output_hdfs)
    )

    log.info("Parquet write complete → %s", output_hdfs)

    # Print a quick summary of what was written
    row_count = df.count()
    ticker_count = df.select("ticker").distinct().count()
    log.info("Output: %d rows across %d tickers", row_count, ticker_count)

    # Show a sample of the output
    log.info("Sample output (5 rows):")
    df.select(
        "ticker", "date", "close",
        "log_return", "volatility_20d",
        "beta_60d", "atr_14d", "sector"
    ).filter(F.col("ticker") == "AAPL").orderBy("date").show(5, truncate=False)


#  CLI 

def parse_args():
    parser = argparse.ArgumentParser(
        description="NASDAQ-100 Spark Preprocessing Job (Stage 3)"
    )
    parser.add_argument(
        "--input-hdfs",
        required=True,
        help="HDFS path to raw CSV directory (e.g. /user/maria_dev/nasdaq/raw/csv)"
    )
    parser.add_argument(
        "--meta-hdfs",
        required=True,
        help="HDFS path to metadata CSV (e.g. /user/maria_dev/nasdaq/raw/meta/nasdaq100_meta.csv)"
    )
    parser.add_argument(
        "--output-hdfs",
        required=True,
        help="HDFS path for Parquet output (e.g. /user/maria_dev/nasdaq/processed)"
    )
    return parser.parse_args()


# Main 

def main():
    args = parse_args()

    log.info("=" * 60)
    log.info("  NASDAQ-100 Spark Preprocessing Job")
    log.info("  Input  : %s", args.input_hdfs)
    log.info("  Meta   : %s", args.meta_hdfs)
    log.info("  Output : %s", args.output_hdfs)
    log.info("=" * 60)

    # Build Spark session
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # suppress verbose Spark logs

    # Run pipeline steps in order
    df   = load_ohlcv(spark, args.input_hdfs)      # Step 1
    meta = load_metadata(spark, args.meta_hdfs)     # Step 1b
    df   = clean(df)                                # Step 2
    df   = join_metadata(df, meta)                  # Step 3
    df   = compute_log_returns(df)                  # Step 4
    df   = compute_rolling_volatility(df)           # Step 5
    df   = compute_beta(df)                         # Step 6
    df   = compute_atr(df)                          # Step 7
    df   = add_time_columns(df)                     # Step 8
    write_parquet(df, args.output_hdfs)             # Step 9

    log.info("Stage 3 complete.")
    spark.stop()


if __name__ == "__main__":
    main()
