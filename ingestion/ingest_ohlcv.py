#Ingestion : Data Collection
#Fetch the NASDAQ-100 constituent list(with sector labels) and download 5 years of daily OHLCV data for every ticker via yfinance

import os
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
import urllib.request

#logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

#Constants
RAW_DIR = Path("data/raw")
YEARS_BACK = 5
RETRY_LIMIT = 3
RETRY_DELAY = 5 #5secs between retries
BATCH_PAUSE = 2 #seconds between ticker batches(rate-limit courtesy)
BATCH_SIZE = 10 #tickers per yfinance batch call

#NASDAQ-100 Constituent List
#Source :https://en.wikipedia.org/wiki/Nasdaq-100
#only to use when webscraping fails

NASDAQ100_FALLBACK = [
    ("AAPL","Apple Inc.","Technology"),
    ("MSFT","Microsoft Corporation","Technology"),
    ("NVDA","NVIDIA Corporation","Technology"),
    ("AMZN","Amazon.com Inc.","Consumer Discretionary"),
    ("META","Meta Platforms Inc.","Communication Services"),
    ("GOOGL","Alphabet Inc. Class A","Communication Services"),
    ("GOOG","Alphabet Inc. Class C","Communication Services"),
    ("TSLA","Tesla Inc.","Consumer Discretionary"),
    ("AVGO","Broadcom Inc.","Technology"),
    ("COST","Costco Wholesale Corporation","Consumer Staples"),
    ("NFLX","Netflix Inc.","Communication Services"),
    ("AMD","Advanced Micro Devices Inc.","Technology"),
    ("ADBE","Adobe Inc.","Technology"),
    ("QCOM","Qualcomm Inc.","Technology"),
    ("TMUS","T-Mobile US Inc.","Communication Services"),
    ("INTU","Intuit Inc.","Technology"),
    ("AMAT","Applied Materials Inc.","Technology"),
    ("CSCO","Cisco Systems Inc.","Technology"),
    ("TXN","Texas Instruments Inc.","Technology"),
    ("AMGN","Amgen Inc.","Healthcare"),
    ("MU","Micron Technology Inc.","Technology"),
    ("ISRG","Intuitive Surgical Inc.","Healthcare"),
    ("BKNG","Booking Holdings Inc.","Consumer Discretionary"),
    ("ADP","Automatic Data Processing Inc.","Technology"),
    ("LRCX","Lam Research Corporation","Technology"),
    ("PANW","Palo Alto Networks Inc.","Technology"),
    ("VRTX","Vertex Pharmaceuticals Inc.","Healthcare"),
    ("REGN","Regeneron Pharmaceuticals Inc.","Healthcare"),
    ("KLAC","KLA Corporation","Technology"),
    ("CRWD","CrowdStrike Holdings Inc.","Technology"),
    ("SNPS","Synopsys Inc.","Technology"),
    ("CDNS","Cadence Design Systems Inc.","Technology"),
    ("MRVL","Marvell Technology Inc.","Technology"),
    ("ORLY","O'Reilly Automotive Inc.","Consumer Discretionary"),
    ("PYPL","PayPal Holdings Inc.","Financials"),
    ("CTAS","Cintas Corporation","Industrials"),
    ("MAR","Marriott International Inc.","Consumer Discretionary"),
    ("FTNT","Fortinet Inc.","Technology"),
    ("MNST","Monster Beverage Corporation","Consumer Staples"),
    ("NXPI","NXP Semiconductors N.V.","Technology"),
    ("MELI","MercadoLibre Inc.","Consumer Discretionary"),
    ("PEP","PepsiCo Inc.","Consumer Staples"),
    ("ROST","Ross Stores Inc.","Consumer Discretionary"),
    ("CPRT","Copart Inc.","Industrials"),
    ("ODFL","Old Dominion Freight Line Inc.","Industrials"),
    ("KDP","Keurig Dr Pepper Inc.","Consumer Staples"),
    ("DDOG","Datadog Inc.","Technology"),
    ("GILD","Gilead Sciences Inc.","Healthcare"),
    ("MDLZ","Mondelez International Inc.","Consumer Staples"),
    ("PCAR","PACCAR Inc.","Industrials"),
    ("IDXX","IDEXX Laboratories Inc.","Healthcare"),
    ("WDAY","Workday Inc.","Technology"),
    ("HON","Honeywell International Inc.","Industrials"),
    ("CEG","Constellation Energy Corporation","Energy"),
    ("EA","Electronic Arts Inc.","Communication Services"),
    ("FAST","Fastenal Company","Industrials"),
    ("BIIB","Biogen Inc.","Healthcare"),
    ("ZS","Zscaler Inc.","Technology"),
    ("DXCM","DexCom Inc.","Healthcare"),
    ("CRM","Salesforce Inc.","Technology"),
    ("NOW","ServiceNow Inc.","Technology"),
    ("ANSS","ANSYS Inc.","Technology"),
    ("TEAM","Atlassian Corporation","Technology"),
    ("VRSK","Verisk Analytics Inc.","Industrials"),
    ("FISV","Fiserv Inc.","Financials"),
    ("COIN","Coinbase Global Inc.","Financials"),
    ("EQIX","Equinix Inc.","Real Estate"),
    ("MPWR","Monolithic Power Systems Inc.","Technology"),
    ("ILMN","Illumina Inc.","Healthcare"),
    ("GEHC","GE HealthCare Technologies Inc.","Healthcare"),
    ("MRNA","Moderna Inc.","Healthcare"),
    ("SBUX","Starbucks Corporation","Consumer Discretionary"),
    ("VRSN","VeriSign Inc.","Technology"),
    ("ABNB","Airbnb Inc.","Consumer Discretionary"),
    ("PLD","Prologis Inc.","Real Estate"),
    ("SBAC","SBA Communications Corporation","Real Estate"),
    ("TTWO","Take-Two Interactive Software Inc.","Communication Services"),
    ("LULU","Lululemon Athletica Inc.","Consumer Discretionary"),
    ("XEL","Xcel Energy Inc.","Utilities"),
    ("AEP","American Electric Power Company Inc.","Utilities"),
    ("CHTR","Charter Communications Inc.","Communication Services"),
    ("CMCSA","Comcast Corporation","Communication Services"),
    ("WBA","Walgreens Boots Alliance Inc.","Consumer Staples"),
    ("INTC","Intel Corporation","Technology"),
    ("KHC","The Kraft Heinz Company","Consumer Staples"),
    ("DLTR","Dollar Tree Inc.","Consumer Discretionary"),
    ("EBAY","eBay Inc.","Consumer Discretionary"),
    ("MCD","McDonald's Corporation","Consumer Discretionary"),
    ("SNOW","Snowflake Inc.","Technology"),
    ("ZM","Zoom Video Communications Inc.","Communication Services"),
    ("CSGP","CoStar Group Inc.","Industrials"),
    ("ON","ON Semiconductor Corporation","Technology"),
    ("SMCI","Super Micro Computer Inc.","Technology"),
    ("ARM","Arm Holdings plc","Technology"),
    ("ASML","ASML Holding N.V.","Technology"),
    ("TSM","Taiwan Semiconductor Mfg. Co. Ltd.","Technology"),
    ("GFS","GlobalFoundries Inc.","Technology"),
    ("SIRI","Sirius XM Holdings Inc.","Communication Services"),
    ("WBD","Warner Bros. Discovery Inc.","Communication Services"),
]

#helper functions

_WIKI_TICKER_COLS = ["Ticker","Symbol","Ticker symbol"]
_WIKI_NAME_COLS = ["Company","Security","Company name"]
_WIKI_SECTOR_COLS = ["GICS Sector","Sector","GICS sector"]
_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return the first candidate column name that exists in df, else None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _scrape_wikipedia() -> pd.DataFrame:
    """
    Scrape the NASDAQ-100 constituent table from Wikipedia.
    Target tables with id='constituents' for robustness against page edits.
    Scan all tables if the id is not found.
    Return a DataFrame with columns [ticker, name, sector].
    Raises ValueError if no usable table is found.
    """
    log.info("Scraping NASDAQ-100 constituent list from Wikipedia...")

    #Try targeted id first(stable)
    try:
        tables = pd.read_html(_WIKI_URL, attrs={"id": "constituents"})
        if tables:
            df = tables[0]
            ticker_col = _find_col(df, _WIKI_TICKER_COLS)
            name_col = _find_col(df, _WIKI_NAME_COLS)
            sector_col = _find_col(df, _WIKI_SECTOR_COLS)
            if ticker_col and sector_col:
                df = df.rename(columns={
                    ticker_col: "ticker",
                    name_col: "name",
                    sector_col: "sector",
                })
                df = df[["ticker","name","sector"]].dropna(subset=["ticker"])
                df["ticker"] = df["ticker"].str.strip()
                df = df.drop_duplicates(subset="ticker").reset_index(drop=True)
                log.info(" Wikipedia scrape (id=constituents): %d tickers found", len(df))
                return df
    except Exception as e:
        log.debug("Targeted table scrape failed: %s - trying all tables", e)

    #Fallback: scan all tables that looks like constituents on the page
    tables = pd.read_html(_WIKI_URL)
    for i, df in enumerate(tables):
        ticker_col = _find_col(df, _WIKI_TICKER_COLS)
        sector_col = _find_col(df, _WIKI_SECTOR_COLS)
        if ticker_col and sector_col and len(df) >= 90:
            name_col = _find_col(df, _WIKI_NAME_COLS)
            df = df.rename(columns={
                ticker_col: "ticker",
                name_col: "name",
                sector_col: "sector",
            })
            df = df[["ticker", "name", "sector"]].dropna(subset=["ticker"])
            df["ticker"] = df["ticker"].str.strip()
            df = df.drop_duplicates(subset="ticker").reset_index(drop=True)
            log.info(" Wikipedia scrape (table[%d]): %d tickers found", i, len(df))
            return df
    raise ValueError("No NASDAQ-100 constituent table found on Wikipedia page.")

def build_metadata_df() -> pd.DataFrame:
    """
    Return the NASDAQ-100 constituent metadata as a tidy DataFrame.
    1. Scrape live from wiki
    2. If scraping fails, use hardcoded fallback list
    """
    try:
        df = _scrape_wikipedia()
        if len(df) >= 90:
            return df
        log.warning("Wikipedia returned only %d tickers - falling back.", len(df))
    except Exception as e:
        log.warning("Wikipedia scrape failed: %s - using fallback list.", e)
    log.info("Using hardcoded fallback constituent list (%d tickers).", len(NASDAQ100_FALLBACK))
    return pd.DataFrame(NASDAQ100_FALLBACK, columns=["ticker","name","sector"])


def date_range(incremental: bool) -> Tuple[str, str]:
    """Return (start_date, end_date) strings for the yfinance download."""
    end = datetime.today()
    start = (end - timedelta(days=1)) if incremental else (end - timedelta(days=YEARS_BACK * 365))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def download_batch(tickers: List[str], start: str, end: str) -> pd.DataFrame:
    """
    Download OHLCV for a list of tickers in one yfinance batch call.
    Returns DF: [ticker, date, open, high, low, close, volume]
    """
    raw = yf.download(
        tickers,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=True,  #adjusts for splits & dividends
        progress=False,
        threads=True,
    )

    if raw.empty:
        return pd.DataFrame()

    #yfinance returns a MultiIndex when >1 ticker is requested
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.swaplevel(0, 1)  # swap to (Ticker, Price)
        raw.columns.names = ['Ticker', 'Price']
        raw = raw.stack(level=0).reset_index()
        raw.columns.name = None
        raw = raw.rename(columns={
            "Ticker":  "ticker",
            "Date":    "date",
            "Open":    "open",
            "High":    "high",
            "Low":     "low",
            "Close":   "close",
            "Volume":  "volume",
       })
    
    else:
        #single ticker - flat columns
        raw = raw.reset_index().rename(columns={
            "Date":   "date",
            "Open":   "open",
            "High":   "high",
            "Low":    "low",
            "Close":  "close",
            "Volume": "volume",
        })
        raw["ticker"] = tickers[0]


    cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
    raw = raw[[c for c in cols if c in raw.columns]]
    raw["date"] = pd.to_datetime(raw["date"]).dt.date

    return raw

def save_ticker_csv(df: pd.DataFrame, ticker: str) -> Path:
    """Write a single ticker's data to data/raw/<TICKER>.csv."""
    out_path = RAW_DIR / "{}.csv".format(ticker)
    subset = df[df["ticker"] == ticker].copy()

    if out_path.exists():
        existing = pd.read_csv(out_path, parse_dates=["date"])
        existing["date"] = pd.to_datetime(existing["date"]).dt.date
        subset = pd.concat([existing, subset]).drop_duplicates(subset=["date"]).sort_values("date")

    
    subset.to_csv(out_path, index=False)
    return out_path


def fetch_with_retry(tickers: List[str], start: str, end: str) -> pd.DataFrame:
    """Wrapper around download_batch with exponential-backoff retries."""
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            df = download_batch(tickers, start, end)
            if not df.empty:
                return df
            log.warning("Empty response for batch %s (attempt %d)", tickers, attempt)
        except Exception as exc:
            log.warning("Attempt %d failed for batch %s: %s", attempt, tickers, exc)
        if attempt < RETRY_LIMIT:
            time.sleep(RETRY_DELAY * attempt)

    log.error("All %d attempts failed for batch: %s", RETRY_LIMIT, tickers)
    return pd.DataFrame()


#Main Ingestion logic


def ingest(tickers: List[str], incremental: bool) -> Dict:
    """
    Downloads OHLCV data for given ticker list and saves per-ticker CSVs.
    Returns a summary dict with counts of success/failure.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    start, end = date_range(incremental)
    mode = "incremental (latest day)" if incremental else "full history ({}->{})".format(start, end)
    log.info("Starting ingestion - mode: %s - %d tickers", mode, len(tickers))

    results = {"success": [], "failed": [], "rows_written": 0}

    
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch_num, batch in enumerate(batches, start=1):
        log.info("Batch %d/%d - tickers: %s", batch_num, len(batches), batch)

        df = fetch_with_retry(batch, start, end)
        if df.empty:
            results["failed"].extend(batch)
            continue

        for ticker in batch:
            ticker_df = df[df["ticker"] == ticker] if "ticker" in df.columns else df
            if ticker_df.empty:
                log.warning("No data returned for %s", ticker)
                results["failed"].append(ticker)
                continue

            path = save_ticker_csv(df, ticker)
            n_rows = len(ticker_df)
            results["success"].append(ticker)
            
            results["rows_written"] += n_rows
            log.info(" ✓ %-6s  %d rows  ->  %s", ticker, n_rows, path)

        if batch_num < len(batches):
            time.sleep(BATCH_PAUSE)

    return results


def save_metadata(tickers: List[str]) -> Path:
    """Save the constituent metadata CSV."""
    meta_df = build_metadata_df()
    meta_df = meta_df[meta_df["ticker"].isin(tickers)]
    out_path = RAW_DIR / "nasdaq100_meta.csv"
    meta_df.to_csv(out_path, index=False)
    log.info("Metadata saved -> %s (%d tickers)", out_path, len(meta_df))
    return out_path


def print_summary(results: Dict, elapsed: float) -> None:
    """Print a clean run summary."""
    total = len(results["success"]) + len(results["failed"])
    success = len(results["success"])
    failed = len(results["failed"])

    print("\n" + "=" * 60)
    print("  INGESTION SUMMARY")
    print("=" * 60)
    print("  Tickers processed : {}".format(total))
    print("  Success           : {}".format(success))
    print("  Failed            : {}".format(failed))
    print("  Total rows written: {:,}".format(results["rows_written"]))
    print("  Elapsed time      : {:.1f}s".format(elapsed))
    if results["failed"]:
        print("\n  Failed tickers  : {}".format(", ".join(results["failed"])))
    print("=" * 60 + "\n")


#CLI Entry point

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NASDAQ-100 OHLCV ingestion script (Stage 1)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Pull only the most recent trading day (append mode)",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="Pull a specific subset of tickers instead of the full NDX-100",
    )
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        metavar="PATH",
        help="Directory to write CSV files (default: data/raw)",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()

    global RAW_DIR
    RAW_DIR = Path(args.output_dir)

    meta_df = build_metadata_df()
    all_tickers = meta_df["ticker"].tolist()
    target_tickers = args.tickers if args.tickers else all_tickers

    if args.tickers:
        unknown = [t for t in args.tickers if t not in all_tickers]
        if unknown:
            log.warning("Unknown tickers (will attempt anyway): %s", unknown)

    save_metadata(target_tickers)

    t0 = time.time()
    results = ingest(target_tickers, incremental=args.incremental)
    elapsed = time.time() - t0

    print_summary(results, elapsed)

    if results["failed"]:
        raise SystemExit(1)

if __name__ == "__main__":
    main()
