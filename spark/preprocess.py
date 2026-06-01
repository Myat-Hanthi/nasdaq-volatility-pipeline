# -*- coding: utf-8 -*-
import argparse
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import(
    StructType, StructField,
    StringType, DataType, DoubleType, LongType
)

#logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

#schema 
#Define schema so Spark doesn't have to infer it from every file
#Faster and prevents type mismatches across tickers


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

#Spark Session
def build_spark_session():
    return(
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

#Load data
#load all ticker CSVs from HDFS into one dataframe
#spark reads all csvs in the directory in parallel
def load_ohlcv(spark, input_hdfs):
    log.info("Loading OHLCV data from: %s", input_hdfs)
    df = (
        spark.read
        .schema(OHLCV_SCHEMA)
        .option("header","true")
        .option("mode","DROPMALFORMED") #skip rows with wrong column count
        .csv(input_hdfs + "/*.csv")
    )

    #cast data string-> proper datatype
    df = df.withColumn("date", F.to_date(F.col("date"),"yyyy-MM-dd"))
    row_count = df.count()
    ticker_count = df.select("ticker").distinct().count()
    log.info("Loaded %d rows across %d tickers",row_count, ticker_count)

    return df

def load_metadata(spark, meta_hdfs):
    log.info("Loading metadata from: %s", meta_hdfs)

    meta = (
        spark.read
        .schema(META_SCHEMA)
        .option("header","true")
        .csv(meta_hdfs)
    )

    log.info("Metadata loaded: %d tickers",meta.count())
    return meta

#Clean Data
def clean(df):
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
    log.info("Cleaned: remove %d bad rows (%d -> %d)",before - after,before,after)

    return df

#Join Sector Metadata
#left join sector labels onto the price data
def join_metadata(df, meta):
    log.info("Joining sector metadata...")
    df = df.join(meta.select("ticker","sector","name"), on="ticker", how="left")
    return df

#Compute log returns
#compute daily log returns for each tickers
def compute_log_returns(df):
    log.info("Computing log returns...")

    #define a window: for each ticker, ordered by date
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
        ).otherwise(F.lit(None).cast("double"))
    )

    # Drop the intermediate prev_close column — not needed downstream
    df = df.drop("prev_close")

    return df
#Rolling Volatility
def compute_rolling_volatility(df):
    log.info("Computing rolling volatility(20-day and 60-day)...")

    ticker_date_window_20 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-19,0) #20 trading days
    )

    ticker_date_window_60 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-59,0) #60 trading days

    )

    df = df.withColumn(
        "volatility_20d",
        F.stddev("log_return").over(ticker_date_window_20)
    )

    df = df.withColumn(
        "volatility_60d",
        F.stddev("log_return").over(ticker_date_window_60)
    )

    return df
#Beta

def compute_beta(df):
    log.info("Computing beta(60-day rolling, vs equal-weighted index)...")

    #Compute index return = mean of all tickers' log_return per date
    index_returns = df.groupBy("date").agg(
        F.avg("log_return").alias("index_return")
    )

    #Join index return onto each row
    df = df.join(index_returns, on="date", how="left")

    #Rolling window(60 trading days) per ticker
    beta_window = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-59, 0)
    )

    #Compute rolling covariance and variance 
    df =  df.withColumn(
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

    #Beta = cov / var (guard against division by zero)
    f = df.withColumn(
        
        F.when(
            F.col("rolling_var_index") > 0,
            F.col("rolling_cov") / F.col("rolling_var_index")
        ).otherwise(F.lit(None).cast("double"))
    )

    #drop intermediate columns 
    df = df.drop("rolling_cov","rolling_var_index")

    return df

#ATR(Average True Range)
def compute_atr(df):

    log.info("Computing ATR (14-day)...")
    ticker_window = Window.partitionBy("ticker").orderBy("date")

    #get previous day's close
    df = df.withColumn(
        "prev_close_atr",
        F.lag("close",1).over(ticker_window)
    )

    #True Range = max of the three components
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

    #ATR = 14-day rolling mean of True Range

    atr_window = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-13, 0)   # 14 trading days
    )

    df = df.withColumn(
        "atr_14d",
        F.avg("true_range").over(atr_window)
    )

    #drop intermediates
    df = df.drop("prev_close_atr", "true_range")

    return df

#Add time columns
#extract year month quarter from data
def add_time_columns(df):
    log.info("Adding time columns (year, month, quarter)...")

    df = df.withColumn("year", F.year("date"))
    df = df.withColumn("month",   F.month("date"))
    df = df.withColumn("quarter", F.quarter("date"))

    return df

#write parquet
def write_parquet(df, output_hdfs):
    log.info("Writing Parquet to: %s", output_hdfs)

    # Select and order final columns cleanly
    final_cols = [
        "ticker", "date", "open", "high", "low", "close", "volume",
        "name", "sector",
        "log_return",
        "volatility_20d", "volatility_60d",
        
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
         "atr_14d", "sector"
    ).filter(F.col("ticker") == "AAPL").orderBy("date").show(5, truncate=False)

#CLI

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

#Main
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
#    df   = compute_beta(df)                         # Step 6
    df   = compute_atr(df)                          # Step 7
    df   = add_time_columns(df)                     # Step 8
    write_parquet(df, args.output_hdfs)             # Step 9

    log.info("Stage 3 complete.")
    spark.stop()

if __name__ == "__main__":
    main()
