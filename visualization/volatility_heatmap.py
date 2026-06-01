import os
import sys
import subprocess
import numpy as np
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Rectangle
import warnings
warnings.filterwarnings('ignore')

#Config
OUTPUT_DIR = "visualization/outputs"
HIVE_DB = "nasdaq"

MONTH_LABELS = [
    'Jan','Feb','Mar','Apr','May','Jun',
    'Jul','Aug','Sep','Oct','Nov','Dec'
]

#Fetch data
def fetch_hive_data():
    query = """
    USE {db};
    SELECT
        ticker,
        month,
        AVG(volatility_20d) AS avg_vol
    FROM stock_features
    WHERE
        volatility_20d IS NOT NULL
        AND volatility_20d > 0
        AND sector IS NOT NULL
    GROUP BY ticker, month
    ORDER BY ticker, month;
""".format(db=HIVE_DB)
    
    print("[INFO] Fetching heatmap data from HIVE...")

    try:
        result = subprocess.run(
            ['hive', '-e', query],
            capture_output=True,
            text=True,
            timeout=120
        )
        lines = result.stdout.strip().split('\n')
        #filter out Hive info lines - keep only data rows(ticker\tmonth\tvalue)
        rows = []
        for line in lines:
            parts = line.strip().split('\t')
            if len(parts) == 3:
                try:
                    ticker = parts[0].strip()
                    month  = int(parts[1].strip())
                    vol    = float(parts[2].strip())
                    if ticker and 1 <= month <= 12:
                        rows.append((ticker, month, vol))
                except (ValueError, IndexError):
                    continue
        print("[INFO] Fetched {} rows".format(len(rows)))
        return rows
    except Exception as e:
        print("[WARN] Hive fetch failed: {} - using mock data".format(e))
        return None
    
def build_mock_data():
    print("[INFO] Using mock data for testing...")

    tickers = [
        'MRNA','TSLA','MRVL','DDOG','AMD','NVDA','CRWD','MU',
        'MELI','LRCX','META','AMZN','GOOGL','AAPL','MSFT',
        'NFLX','PYPL','COIN','REGN','VRTX','AMGN','GILD',
        'COST','PEP','MDLZ','KDP','HON','CTAS','ODFL',
        'EQIX','PLD','SBAC','XEL','AEP','CEG'
    ]

    #Base volatiltiy per ticker (higher for biotech/high-growth)
    base_vol = {t: np.random.uniform(0.012, 0.040) for t in tickers}
    for t in ['MRNA','TSLA','MELI','CRWD','DDOG']:
        base_vol[t] = np.random.uniform(0.030, 0.045)
    for t in ['XEL','AEP','CEG','PEP','COST']:
        base_vol[t] = np.random.uniform(0.008, 0.016)

    # Monthly multipliers (May/Feb more volatile, Jul/Sep calmer)
    month_mult = {
        1:1.05, 2:1.15, 3:1.10, 4:1.12, 5:1.20,
        6:1.0,  7:0.90, 8:1.05, 9:0.95, 10:1.0,
        11:1.08, 12:1.0
    }

    rows = []
    for ticker in tickers:
        for month in range(1, 13):
            vol = base_vol[ticker] * month_mult[month] * np.random.uniform(0.85, 1.15)
            rows.append((ticker, month, vol))
    return rows

def build_pivot(rows):
    from collections import defaultdict

    #build dict: ticker -> {month->vol}
    data = defaultdict(dict)
    for ticker, month, vol in rows:
        data[ticker][month] = vol

    #sort tickers by their average volatility (highest at top)

    tickers = sorted(data.keys(),
                     key=lambda t: np.mean(list(data[t].values())),
                     reverse=True)
    
    #Build matrix (NaN where data is missing)

    matrix = np.full((len(tickers),12), np.nan)
    for i, ticker in enumerate(tickers):
        for month, vol in data[ticker].items():
            matrix[i, month - 1] = vol

    return matrix, tickers

def plot_heatmap(matrix, tickers,output_path):
    n_tickers = len(tickers)
    n_months  = 12

    # Dynamic figure height based on number of tickers
    fig_height = max(10, n_tickers * 0.35)
    fig, ax = plt.subplots(figsize=(16, fig_height))

    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")

    valid = matrix[~np.isnan(matrix)]
    vmin  = np.percentile(valid, 1)
    vmax  = np.percentile(valid, 99)

    im = ax.imshow(
        matrix,
        aspect='auto',
        cmap='RdYlBu_r',
        vmin=vmin,
        vmax=vmax,
        interpolation='nearest'
    )

    for i in range(n_tickers):
        for j in range(n_months):
            val = matrix[i, j]
            if not np.isnan(val):
                # White text on dark cells, dark text on light cells
                normalized = (val - vmin) / (vmax - vmin)
                text_color = 'white' if normalized > 0.6 or normalized < 0.3 else '#1a1a1a'
                ax.text(
                    j, i,
                    '{:.3f}'.format(val),
                    ha='center', va='center',
                    fontsize=6.5,
                    color=text_color,
                    fontweight='bold'
                )
 
    # Axes labels
    ax.set_xticks(range(n_months))
    ax.set_xticklabels(MONTH_LABELS, fontsize=10, color='#e6edf3', fontweight='bold')
    ax.set_yticks(range(n_tickers))
    ax.set_yticklabels(tickers, fontsize=8.5, color='#e6edf3', fontfamily='monospace')
 
    # Move x-axis ticks to top
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position('top')
 
    # Grid lines between cells
    ax.set_xticks(np.arange(-0.5, n_months, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, n_tickers, 1), minor=True)
    ax.grid(which='minor', color='#21262d', linewidth=0.8)
    ax.tick_params(which='minor', bottom=False, left=False)
 
    # Remove outer spines
    for spine in ax.spines.values():
        spine.set_visible(False)
 
    # Colorbar
    cbar = fig.colorbar(im, ax=ax, shrink=0.4, aspect=20, pad=0.02)
    cbar.ax.yaxis.set_tick_params(color='#e6edf3', labelsize=8)
    cbar.outline.set_visible(False)
    cbar.ax.set_facecolor('#0d1117')
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color='#e6edf3')
    cbar.set_label('20-Day Rolling Volatility', color='#e6edf3',
                   fontsize=9, labelpad=10)
 
    # Title and subtitle
    fig.text(0.5, 0.98,
             'NASDAQ-100 Volatility Heatmap',
             ha='center', va='top',
             fontsize=18, fontweight='bold', color='#e6edf3')
    fig.text(0.5, 0.965,
             'Average 20-Day Rolling Volatility by Stock and Calendar Month  |  2021-2026',
             ha='center', va='top',
             fontsize=10, color='#8b949e')
 
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='#0d1117', edgecolor='none')
    plt.close()
    print("[INFO] Heatmap saved -> {}".format(output_path))
 

#Main

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'volatility_heatmap.png')
 
    # Try Hive first, fall back to mock data
    rows = fetch_hive_data()
    if not rows:
        rows = build_mock_data()
 
    matrix, tickers = build_pivot(rows)
    print("[INFO] Matrix shape: {} tickers x {} months".format(*matrix.shape))
 
    plot_heatmap(matrix, tickers, output_path)
    print("[INFO] Done. Open: {}".format(output_path))
 
 
if __name__ == "__main__":
    main()
