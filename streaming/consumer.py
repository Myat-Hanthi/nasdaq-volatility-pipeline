"""
What this does:
    Reads price update messages from the 'nasdaq-volatility' Kafka topic,
    maintains a rolling window of recent returns for each ticker,
    computes real-time rolling volatility, and prints alerts when a stock's
    volatility spikes above a configurable threshold.

Alert Levels:
    WATCH   : volatility > 1.5x the baseline (moderate spike)
    WARNING : volatility > 2.0x the baseline (significant spike)
    ALERT   : volatility > 3.0x the baseline (extreme spike)
"""

import json
import math
import time
import logging
import argparse
import sys
from datetime import datetime
from collections import defaultdict, deque

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__import__("os").path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Configuration 
KAFKA_BROKER     = "172.18.0.2:6667"
KAFKA_TOPIC      = "nasdaq-volatility"
CONSUMER_GROUP   = "nasdaq-volatility-monitor"
WINDOW_SIZE      = 20       # rolling window (number of messages per ticker)
ALERT_THRESHOLD  = 2.0      # multiplier above baseline to trigger WARNING
BASELINE_VOL     = 0.018    # index average volatility (from our Hive analysis)

# Sector baselines from our Hive Q2 analysis
SECTOR_BASELINES = {
    "Technology":             0.02318,
    "Financials":             0.02432,
    "Healthcare":             0.02067,
    "Consumer Discretionary": 0.02198,
    "Communication Services": 0.01952,
    "Consumer Staples":       0.01287,
    "Industrials":            0.01632,
    "Real Estate":            0.01671,
    "Utilities":              0.01198,
    "Energy":                 0.01876,
}


class VolatilityMonitor:
    """
    Maintains a rolling window of log returns per ticker and
    computes real-time volatility for alert detection.
    """

    def __init__(self, window_size=20, threshold=2.0):
        self.window_size  = window_size
        self.threshold    = threshold
        # deque automatically drops oldest values when maxlen is reached
        self.returns      = defaultdict(lambda: deque(maxlen=window_size))
        self.sectors      = {}
        self.alert_counts = defaultdict(int)
        self.msg_count    = 0
        self.start_time   = datetime.now()

    def update(self, message):
        """Process one incoming Kafka message."""
        ticker  = message.get("ticker")
        ret     = message.get("return")
        sector  = message.get("sector", "Unknown")

        if not ticker or ret is None:
            return

        self.returns[ticker].append(ret)
        self.sectors[ticker] = sector
        self.msg_count += 1

    def compute_volatility(self, ticker):
        """
        Compute rolling standard deviation of log returns.
        Returns None if fewer than 3 data points available.
        """
        rets = list(self.returns[ticker])
        n    = len(rets)
        if n < 2:
            return None

        mean  = sum(rets) / n
        var   = sum((r - mean) ** 2 for r in rets) / max(n - 1, 1)
        return math.sqrt(var)

    def check_alert(self, ticker):
        """
        Compare current volatility against baseline.
        Returns (volatility, alert_level) tuple.
        """
        vol = self.compute_volatility(ticker)
        if vol is None:
            return None, None

        sector   = self.sectors.get(ticker, "Unknown")
        baseline = SECTOR_BASELINES.get(sector, BASELINE_VOL)
        ratio    = vol / baseline if baseline > 0 else 0

        if ratio >= 3.0:
            level = "ALERT"
        elif ratio >= 2.0:
            level = "WARNING"
        elif ratio >= 1.5:
            level = "WATCH"
        else:
            level = "NORMAL"

        return vol, level

    def print_dashboard(self):
        """Print a real-time dashboard of current volatility levels."""
        tickers = sorted(self.returns.keys())
        if not tickers:
            return

        elapsed = (datetime.now() - self.start_time).seconds

        print("\n" + "=" * 65)
        print("  NASDAQ-100 REAL-TIME VOLATILITY MONITOR")
        print("  Messages: {}  |  Tickers: {}  |  Uptime: {}s".format(
            self.msg_count, len(tickers), elapsed))
        print("=" * 65)
        print("  {:<6}  {:<22}  {:>10}  {:>8}  {}".format(
            "TICKER", "SECTOR", "VOL (20d)", "VS BASE", "STATUS"))
        print("  " + "-" * 62)

        alerts = []
        for ticker in tickers:
            vol, level = self.check_alert(ticker)
            if vol is None:
                continue

            sector   = self.sectors.get(ticker, "Unknown")
            baseline = SECTOR_BASELINES.get(sector, BASELINE_VOL)
            ratio    = vol / baseline if baseline > 0 else 0

            # Status indicator
            if level == "ALERT":
                status = "*** ALERT ***"
                alerts.append(ticker)
            elif level == "WARNING":
                status = "!! WARNING"
                alerts.append(ticker)
            elif level == "WATCH":
                status = "~ WATCH"
            else:
                status = "OK"

            print("  {:<6}  {:<22}  {:>10.6f}  {:>7.1f}x  {}".format(
                ticker,
                sector[:22],
                vol,
                ratio,
                status
            ))

        print("=" * 65)
        if alerts:
            print("  ACTIVE ALERTS: {}".format(", ".join(alerts)))
        print()


def get_consumer():
    """Create and return a Kafka consumer."""
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",     # start from latest messages
            enable_auto_commit=True,
            consumer_timeout_ms=60000,       # 5 second timeout between messages
        )
        log.info("Kafka consumer connected to %s", KAFKA_BROKER)
        log.info("Subscribed to topic: %s", KAFKA_TOPIC)
        return consumer
    except Exception as e:
        log.error("Failed to connect to Kafka consumer: %s", e)
        raise


def main():
    parser = argparse.ArgumentParser(description="NASDAQ-100 Real-Time Volatility Consumer")
    parser.add_argument("--threshold", type=float, default=ALERT_THRESHOLD,
                        help="Alert threshold multiplier (default: 2.0)")
    parser.add_argument("--window", type=int, default=WINDOW_SIZE,
                        help="Rolling window size in messages (default: 20)")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("  NASDAQ-100 Volatility Monitor — Kafka Consumer")
    log.info("  Broker    : %s", KAFKA_BROKER)
    log.info("  Topic     : %s", KAFKA_TOPIC)
    log.info("  Group     : %s", CONSUMER_GROUP)
    log.info("  Window    : %d messages", args.window)
    log.info("  Threshold : %.1fx sector baseline", args.threshold)
    log.info("=" * 55)

    monitor  = VolatilityMonitor(window_size=args.window, threshold=args.threshold)
    consumer = get_consumer()

    log.info("Waiting for messages from producer...")
    log.info("(Start producer.py in another terminal)")

    dashboard_interval = 5   # print dashboard every N messages
    last_dashboard     = 0

    try:
        for message in consumer:
            data = message.value

            if not isinstance(data, dict):
                continue

            monitor.update(data)

            ticker = data.get("ticker", "?")
            price  = data.get("price", 0)
            ret    = data.get("return", 0)
            sector = data.get("sector", "?")

            log.info(
                "Received: %-6s  $%.2f  return=%+.4f  sector=%s",
                ticker, price, ret, sector
            )

            # Check for immediate alert
            vol, level = monitor.check_alert(ticker)
            if vol and level in ("WARNING", "ALERT"):
                log.warning(
                    "*** %s *** %s  vol=%.6f  (%.1fx above %s baseline)",
                    level, ticker, vol,
                    vol / SECTOR_BASELINES.get(sector, BASELINE_VOL),
                    sector
                )

            # Print full dashboard periodically
            if monitor.msg_count - last_dashboard >= dashboard_interval:
                monitor.print_dashboard()
                last_dashboard = monitor.msg_count

    except KeyboardInterrupt:
        log.info("Consumer stopped by user.")
        monitor.print_dashboard()
    except Exception as e:
        log.error("Consumer error: %s", e)
    finally:
        consumer.close()
        log.info("Kafka consumer closed.")
        log.info("Total messages processed: %d", monitor.msg_count)


if __name__ == "__main__":
    main()
