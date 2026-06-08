"""
What this does:
    Fetches the latest price for each NASDAQ-100 ticker via yfinance,
    computes the intraday return, and publishes each price update as a
    JSON message to the Kafka topic 'nasdaq-volatility'.

    Simulates a real-time market data feed by publishing one batch
    of prices every 30 seconds (configurable via INTERVAL_SECONDS).

Kafka Message Format:
    {
        "ticker":     "AAPL",
        "timestamp":  "2026-06-08 09:30:00",
        "price":      191.42,
        "prev_close": 190.15,
        "return":     0.00668,
        "volume":     12345678,
        "sector":     "Technology"
    }
"""
import json
import time
import logging
import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Configuration 
KAFKA_BROKER   = "172.18.0.2:6667"
KAFKA_TOPIC    = "nasdaq-volatility"
INTERVAL_SECS  = 30       # seconds between batches
BATCH_SIZE     = 10       # tickers per batch (avoid rate limiting)

# Top tickers to stream (subset for demo — add more as needed)
STREAM_TICKERS = [
    ("AAPL",  "Technology"),
    ("MSFT",  "Technology"),
    ("NVDA",  "Technology"),
    ("TSLA",  "Consumer Discretionary"),
    ("MRNA",  "Healthcare"),
    ("AMD",   "Technology"),
    ("AMZN",  "Consumer Discretionary"),
    ("GOOGL", "Communication Services"),
    ("META",  "Communication Services"),
    ("DDOG",  "Technology"),
    ("CRWD",  "Technology"),
    ("MRVL",  "Technology"),
    ("COIN",  "Financials"),
    ("MELI",  "Consumer Discretionary"),
    ("NFLX",  "Communication Services"),
]


def get_producer():
    """Create and return a Kafka producer."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=3,
        )
        log.info("Kafka producer connected to %s", KAFKA_BROKER)
        return producer
    except Exception as e:
        log.error("Failed to connect to Kafka: %s", e)
        log.error("Make sure Kafka is running: netstat -tln | grep 6667")
        raise

def fetch_prices(tickers):
    """
    Fetch latest prices and simulate intraday return variation.
    Adds small random noise to returns to simulate real tick data.
    """
    import yfinance as yf
    import random

    results = []
    ticker_symbols = [t[0] for t in tickers]
    sector_map     = {t[0]: t[1] for t in tickers}

    try:
        data = yf.download(
            ticker_symbols,
            period="5d",
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        if data.empty:
            return results

        import pandas as pd
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.swaplevel(0, 1)
            data.columns.names = ["Ticker", "Price"]
            data = data.stack(level=0).reset_index()
            data.columns.name = None
            data = data.rename(columns={
                "Ticker": "ticker",
                "Date":   "date",
                "Close":  "close",
                "Volume": "volume",
            })
        else:
            data = data.reset_index().rename(columns={
                "Date":  "date",
                "Close": "close",
                "Volume":"volume",
            })
            data["ticker"] = ticker_symbols[0]

        import math
        for ticker in ticker_symbols:
            df = data[data["ticker"] == ticker].sort_values("date")
            if len(df) < 2:
                continue

            # Use actual daily returns + small noise to simulate tick variation
            for i in range(1, len(df)):
                prev_close = float(df.iloc[i-1]["close"])
                curr_close = float(df.iloc[i]["close"])
                if prev_close <= 0 or curr_close <= 0:
                    continue

                base_return = math.log(curr_close / prev_close)
                # Add small intraday noise (+/- 0.5%)
                noisy_return = base_return + random.gauss(0, 0.005)
                volume = int(df.iloc[i]["volume"]) if "volume" in df.columns else 0

                results.append({
                    "ticker":     ticker,
                    "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "price":      round(curr_close * (1 + random.gauss(0, 0.002)), 4),
                    "prev_close": round(prev_close, 4),
                    "return":     round(noisy_return, 6),
                    "volume":     volume,
                    "sector":     sector_map.get(ticker, "Unknown"),
                })

    except Exception as e:
        log.warning("Price fetch error: %s", e)

    return results





def publish_batch(producer, messages):
    """Publish a batch of price messages to Kafka."""
    sent = 0
    for msg in messages:
        try:
            future = producer.send(
                KAFKA_TOPIC,
                key=msg["ticker"],
                value=msg,
            )
            future.get(timeout=10)
            sent += 1
            log.info(
                "  -> %-6s  price=%.2f  return=%+.4f  sector=%s",
                msg["ticker"], msg["price"], msg["return"], msg["sector"]
            )
        except Exception as e:
            log.warning("Failed to send %s: %s", msg["ticker"], e)

    producer.flush()
    return sent


def create_topic_if_not_exists():
    """Create the Kafka topic if it doesn't already exist."""
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
        existing = admin.list_topics()

        if KAFKA_TOPIC not in existing:
            topic = NewTopic(
                name=KAFKA_TOPIC,
                num_partitions=3,
                replication_factor=1,
            )
            admin.create_topics([topic])
            log.info("Created Kafka topic: %s", KAFKA_TOPIC)
        else:
            log.info("Kafka topic already exists: %s", KAFKA_TOPIC)

        admin.close()
    except Exception as e:
        log.warning("Could not create topic (may already exist): %s", e)


def main():
    parser = argparse.ArgumentParser(description="NASDAQ-100 Kafka Price Producer")
    parser.add_argument("--once", action="store_true",
                        help="Publish one batch and exit (no loop)")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("  NASDAQ-100 Volatility Stream — Kafka Producer")
    log.info("  Broker : %s", KAFKA_BROKER)
    log.info("  Topic  : %s", KAFKA_TOPIC)
    log.info("  Tickers: %d", len(STREAM_TICKERS))
    log.info("  Mode   : %s", "single batch" if args.once else "continuous loop")
    log.info("=" * 55)

    # Setup
    create_topic_if_not_exists()
    producer = get_producer()

    batch_num = 0
    try:
        while True:
            batch_num += 1
            log.info("--- Batch %d ---", batch_num)

            # Fetch latest prices
            messages = fetch_prices(STREAM_TICKERS)

            if messages:
                sent = publish_batch(producer, messages)
                log.info("Batch %d complete: %d/%d messages sent to '%s'",
                         batch_num, sent, len(messages), KAFKA_TOPIC)
            else:
                log.warning("Batch %d: no price data retrieved", batch_num)

            if args.once:
                break

            log.info("Waiting %ds before next batch...", INTERVAL_SECS)
            time.sleep(INTERVAL_SECS)

    except KeyboardInterrupt:
        log.info("Producer stopped by user.")
    finally:
        producer.close()
        log.info("Kafka producer closed.")


if __name__ == "__main__":
    main()
