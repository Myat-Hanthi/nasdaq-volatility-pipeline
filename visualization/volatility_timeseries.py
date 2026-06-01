import os
import subprocess
from datetime import datetime, timedelta
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import warnings
warnings.filterwarnings('ignore')

OUTPUT_DIR = "visualization/outputs"
HIVE_DB    = "nasdaq"
TOP_TICKERS = ['MRNA', 'TSLA', 'MRVL', 'DDOG', 'AMD']
TICKER_COLORS = {
    'MRNA': '#ff7b72',
    'TSLA': '#ffa657',
    'MRVL': '#d2a8ff',
    'DDOG': '#79c0ff',
    'AMD':  '#56d364',
}


def fetch_hive_data(tickers):
    """Fetch time-series volatility from Hive."""
    ticker_list = "','".join(tickers)
    query = """
    USE {db};
    SELECT ticker, DATE_ADD('1970-01-01', `date`) AS trade_date, volatility_20d
    FROM stock_features
    WHERE ticker IN ('{tickers}')
    AND volatility_20d IS NOT NULL AND volatility_20d > 0
    ORDER BY ticker, `date`;
    """.format(db=HIVE_DB, tickers=ticker_list)

    index_query = """
    USE {db};
    SELECT DATE_ADD('1970-01-01', `date`) AS trade_date, AVG(volatility_20d) AS avg_vol
    FROM stock_features
    WHERE volatility_20d IS NOT NULL AND volatility_20d > 0
    GROUP BY `date` ORDER BY `date`;
    """.format(db=HIVE_DB)

    print("[INFO] Fetching time-series data from Hive...")
    ticker_data = {}
    index_data  = {}

    try:
        result = subprocess.run(['hive', '-e', query],
                                capture_output=True, text=True, timeout=180)
        for line in result.stdout.strip().split('\n'):
            parts = line.strip().split('\t')
            if len(parts) == 3:
                try:
                    ticker = parts[0].strip()
                    date   = datetime.strptime(parts[1].strip(), '%Y-%m-%d')
                    vol    = float(parts[2].strip())
                    if ticker not in ticker_data:
                        ticker_data[ticker] = {'dates': [], 'vols': []}
                    ticker_data[ticker]['dates'].append(date)
                    ticker_data[ticker]['vols'].append(vol)
                except (ValueError, IndexError):
                    continue

        result2 = subprocess.run(['hive', '-e', index_query],
                                 capture_output=True, text=True, timeout=180)
        for line in result2.stdout.strip().split('\n'):
            parts = line.strip().split('\t')
            if len(parts) == 2:
                try:
                    date = datetime.strptime(parts[0].strip(), '%Y-%m-%d')
                    vol  = float(parts[1].strip())
                    index_data[date] = vol
                except (ValueError, IndexError):
                    continue

        if ticker_data:
            print("[INFO] Fetched data for: {}".format(list(ticker_data.keys())))
            return ticker_data, index_data
    except Exception as e:
        print("[WARN] Hive fetch failed: {}".format(e))
    return None, None


def build_mock_data(tickers):
    """Generate realistic mock time-series data."""
    print("[INFO] Using mock data...")
    np.random.seed(42)

    start = datetime(2021, 6, 1)
    dates = [start + timedelta(days=i) for i in range(0, 5*365)
             if (start + timedelta(days=i)).weekday() < 5]

    base_vols = {'MRNA':0.038,'TSLA':0.035,'MRVL':0.032,'DDOG':0.031,'AMD':0.030}

    ticker_data = {}
    for ticker in tickers:
        base = base_vols.get(ticker, 0.025)
        vols = []
        v = base
        for d in dates:
            if datetime(2022, 1, 1) <= d <= datetime(2022, 12, 31):
                target = base * 1.4
            elif datetime(2025, 4, 1) <= d <= datetime(2025, 5, 15):
                target = base * 1.5
            else:
                target = base
            v = v * 0.97 + target * 0.03 + np.random.normal(0, base * 0.05)
            v = max(0.005, v)
            vols.append(v)
        ticker_data[ticker] = {'dates': dates, 'vols': vols}

    # Index average
    idx_vols = []
    v = 0.018
    for d in dates:
        target = 0.025 if datetime(2022,1,1) <= d <= datetime(2022,12,31) else 0.018
        v = v * 0.97 + target * 0.03 + np.random.normal(0, 0.001)
        v = max(0.005, v)
        idx_vols.append(v)
    index_data = {d: v for d, v in zip(dates, idx_vols)}

    return ticker_data, index_data


def plot_timeseries(ticker_data, index_data, output_path):
    """Plot multi-line time-series with event annotations."""
    fig, ax = plt.subplots(figsize=(16, 7))
    fig.patch.set_facecolor('#0d1117')
    ax.set_facecolor('#0d1117')

    # Plot index average
    if index_data:
        idx_dates = sorted(index_data.keys())
        idx_vols  = [index_data[d] for d in idx_dates]
        ax.plot(idx_dates, idx_vols, color='#8b949e', linewidth=2.0,
                linestyle='--', alpha=0.8, label='Index Average', zorder=2)

    # Plot each ticker
    for ticker, series in ticker_data.items():
        color = TICKER_COLORS.get(ticker, '#ffffff')
        ax.plot(series['dates'], series['vols'], color=color,
                linewidth=1.5, alpha=0.9, label=ticker, zorder=3)
        if series['vols']:
            peak_idx = series['vols'].index(max(series['vols']))
            ax.scatter(series['dates'][peak_idx], series['vols'][peak_idx],
                       color=color, s=40, zorder=5, alpha=0.9)

    # Event shading
    events = [
        ('2022-01-01', '2022-12-31', '#ff7b72', 'Fed Rate\nHike Cycle'),
        ('2025-04-01', '2025-05-15', '#ffa657', 'Tariff\nShock'),
    ]
    for start_str, end_str, color, label in events:
        start_dt = datetime.strptime(start_str, '%Y-%m-%d')
        end_dt   = datetime.strptime(end_str, '%Y-%m-%d')
        ax.axvspan(start_dt, end_dt, alpha=0.10, color=color, zorder=0)

    # Event labels after data plotted so ylim is correct
    y_top = ax.get_ylim()[1] * 0.92
    ax.text(datetime(2022, 7, 1), y_top, 'Fed Rate\nHike Cycle',
            ha='center', fontsize=8, color='#ff7b72', fontweight='bold')
    ax.text(datetime(2025, 4, 23), y_top, 'Tariff\nShock',
            ha='center', fontsize=8, color='#ffa657', fontweight='bold')

    # Axes formatting
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.xticks(rotation=45, ha='right', fontsize=8, color='#8b949e')
    plt.yticks(fontsize=8, color='#8b949e')
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: '{:.1f}%'.format(x * 100))
    )
    ax.yaxis.grid(True, color='#21262d', linewidth=0.8)
    ax.set_axisbelow(True)
    for spine in ['top', 'right']:
        ax.spines[spine].set_visible(False)
    ax.spines['bottom'].set_color('#21262d')
    ax.spines['left'].set_color('#21262d')
    ax.set_ylabel('20-Day Rolling Volatility (%)',
                  fontsize=10, color='#8b949e', labelpad=10)
    ax.legend(fontsize=9, facecolor='#161b22', edgecolor='#30363d',
              labelcolor='#e6edf3', loc='upper right', framealpha=0.9)

    # Title
    fig.text(0.5, 0.97, 'NASDAQ-100 Volatility Spikes Over Time',
             ha='center', fontsize=16, fontweight='bold', color='#e6edf3')
    fig.text(0.5, 0.93,
             'Top 5 Outlier Stocks vs Index Average  |  20-Day Rolling Volatility  |  2021-2026',
             ha='center', fontsize=9, color='#8b949e')

    plt.tight_layout(rect=[0, 0.05, 1, 0.91])
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='#0d1117', edgecolor='none')
    plt.close()
    print("[INFO] Time-series saved -> {}".format(output_path))


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'volatility_timeseries.png')

    ticker_data, index_data = fetch_hive_data(TOP_TICKERS)
    if not ticker_data:
        ticker_data, index_data = build_mock_data(TOP_TICKERS)

    plot_timeseries(ticker_data, index_data, output_path)
    print("[INFO] Done.")


if __name__ == "__main__":
    main()
