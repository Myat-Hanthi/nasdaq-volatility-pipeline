import os
import subprocess
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import warnings
warnings.filterwarnings('ignore')

OUTPUT_DIR = "visualization/outputs"
HIVE_DB    = "nasdaq"

# Sector color palette — each sector gets a distinct color
SECTOR_COLORS = {
    'Technology':             '#58a6ff',
    'Healthcare':             '#3fb950',
    'Financials':             '#f78166',
    'Consumer Discretionary': '#d2a8ff',
    'Communication Services': '#ffa657',
    'Consumer Staples':       '#79c0ff',
    'Industrials':            '#56d364',
    'Real Estate':            '#e3b341',
    'Energy':                 '#ff7b72',
    'Utilities':              '#8b949e',
}
DEFAULT_COLOR = '#6e7681'

def fetch_hive_data():
    """Fetch sector-level volatility stats from Hive"""
    query = """
    USE {db};
    SELECT
        sector,
        COUNT(DISTINCT ticker)         AS ticker_count,
        AVG(volatility_20d)            AS avg_vol,
        STDDEV(volatility_20d)         AS std_vol,
        MAX(volatility_20d)            AS max_vol,
        AVG(atr_14d)                   AS avg_atr
    FROM stock_features
    WHERE
        sector IS NOT NULL
        AND volatility_20d IS NOT NULL
        AND volatility_20d > 0
    GROUP BY sector
    ORDER BY avg_vol DESC;
    """.format(db=HIVE_DB)

    print("[INFO] Fetching sector data from Hive")
    try:
        result = subprocess.run(
            ['hive', '-e', query],
            capture_output=True, text=True, timeout=120
        )
        rows = []
        for line in result.stdout.strip().split('\n'):
            parts = line.strip().split('\t')
            if len(parts) == 6:
                try:
                    rows.append({
                        'sector':       parts[0].strip(),
                        'ticker_count': int(parts[1].strip()),
                        'avg_vol':      float(parts[2].strip()),
                        'std_vol':      float(parts[3].strip()),
                        'max_vol':      float(parts[4].strip()),
                        'avg_atr':      float(parts[5].strip()),
                    })
                except (ValueError, IndexError):
                    continue
        if rows:
            print("[INFO] Fetched {} sectors".format(len(rows)))
            return rows
    except Exception as e:
        print("[WARN] Hive fetch failed: {}".format(e))
    return None

def build_mock_data():
    """Realistic mock sector data based on actual query results."""
    print("[INFO] Using mock data...")
    return [
        {'sector': 'Financials',             'ticker_count': 3,  'avg_vol': 0.02432, 'std_vol': 0.01821, 'max_vol': 0.10488, 'avg_atr': 12.30},
        {'sector': 'Technology',             'ticker_count': 28, 'avg_vol': 0.02318, 'std_vol': 0.01243, 'max_vol': 0.09868, 'avg_atr':  4.21},
        {'sector': 'Consumer Discretionary', 'ticker_count': 8,  'avg_vol': 0.02198, 'std_vol': 0.01102, 'max_vol': 0.08819, 'avg_atr': 18.45},
        {'sector': 'Healthcare',             'ticker_count': 9,  'avg_vol': 0.02067, 'std_vol': 0.01387, 'max_vol': 0.11149, 'avg_atr':  6.87},
        {'sector': 'Communication Services', 'ticker_count': 6,  'avg_vol': 0.01952, 'std_vol': 0.01021, 'max_vol': 0.07127, 'avg_atr':  8.32},
        {'sector': 'Real Estate',            'ticker_count': 3,  'avg_vol': 0.01671, 'std_vol': 0.00891, 'max_vol': 0.05233, 'avg_atr': 11.20},
        {'sector': 'Industrials',            'ticker_count': 6,  'avg_vol': 0.01632, 'std_vol': 0.00743, 'max_vol': 0.04821, 'avg_atr':  3.91},
        {'sector': 'Consumer Staples',       'ticker_count': 5,  'avg_vol': 0.01287, 'std_vol': 0.00612, 'max_vol': 0.04231, 'avg_atr':  2.14},
        {'sector': 'Utilities',              'ticker_count': 2,  'avg_vol': 0.01198, 'std_vol': 0.00521, 'max_vol': 0.03912, 'avg_atr':  1.87},
        {'sector': 'Energy',                 'ticker_count': 1,  'avg_vol': 0.01876, 'std_vol': 0.00934, 'max_vol': 0.06234, 'avg_atr':  3.42},
    ]

def plot_sector_bar(data, output_path):
    # Sort by avg_vol descending
    data = sorted(data, key=lambda x: x['avg_vol'], reverse=True)

    sectors     = [d['sector'] for d in data]
    avg_vols    = [d['avg_vol'] for d in data]
    std_vols    = [d['std_vol'] for d in data]
    ticker_cnts = [d['ticker_count'] for d in data]
    colors      = [SECTOR_COLORS.get(s, DEFAULT_COLOR) for s in sectors]

    # Index average (mean of all sector averages, weighted)
    index_avg = np.mean(avg_vols)

    fig, ax = plt.subplots(figsize=(13, 8))
    fig.patch.set_facecolor('#0d1117')
    ax.set_facecolor('#161b22')

    y_pos = range(len(sectors))

    # Draw bars
    bars = ax.barh(
        y_pos,
        avg_vols,
        xerr=std_vols,
        color=colors,
        alpha=0.88,
        height=0.65,
        error_kw={
            'ecolor': '#8b949e',
            'elinewidth': 1.2,
            'capsize': 4,
            'capthick': 1.2,
        }
    )

    # Annotate each bar with value and ticker count
    for i, (bar, val, cnt) in enumerate(zip(bars, avg_vols, ticker_cnts)):
        ax.text(
            val + 0.0003,
            bar.get_y() + bar.get_height() / 2,
            '{:.4f}  ({} stocks)'.format(val, cnt),
            va='center', ha='left',
            fontsize=9, color='#e6edf3',
            fontweight='bold'
        )
 
    # Index average vertical line
    ax.axvline(
        x=index_avg,
        color='#f0f6fc',
        linewidth=1.5,
        linestyle='--',
        alpha=0.7,
        label='Index Average: {:.4f}'.format(index_avg)
    )
 
    # Axes styling
    ax.set_yticks(y_pos)
    ax.set_yticklabels(sectors, fontsize=11, color='#e6edf3', fontweight='bold')
    ax.set_xlabel('Average 20-Day Rolling Volatility', fontsize=10,
                  color='#8b949e', labelpad=10)
    ax.tick_params(axis='x', colors='#8b949e', labelsize=8)
    ax.tick_params(axis='y', colors='#e6edf3')
 
    # Grid
    ax.xaxis.grid(True, color='#21262d', linewidth=0.8, alpha=0.8)
    ax.set_axisbelow(True)
 
    # Remove spines
    for spine in ['top', 'right', 'left']:
        ax.spines[spine].set_visible(False)
    ax.spines['bottom'].set_color('#21262d')
 
    # Legend
    ax.legend(
        fontsize=9,
        facecolor='#161b22',
        edgecolor='#30363d',
        labelcolor='#e6edf3',
        loc='lower right'
    )
 
    # Title
    fig.text(0.5, 0.97,
             'NASDAQ-100 Sector Risk Comparison',
             ha='center', va='top',
             fontsize=16, fontweight='bold', color='#e6edf3')
    fig.text(0.5, 0.925,
             'Average 20-Day Volatility by Sector  |  Error bars = Std Dev  |  2021-2026',
             ha='center', va='top',
             fontsize=9, color='#8b949e')
 
    # Invert y-axis so highest risk is at top
    ax.invert_yaxis()
 
    plt.tight_layout(rect=[0, 0, 1, 0.92])
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='#0d1117', edgecolor='none')
    plt.close()
    print("[INFO] Bar chart saved -> {}".format(output_path))
 
 
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'sector_bar_chart.png')
 
    data = fetch_hive_data()
    if not data:
        data = build_mock_data()
 
    plot_sector_bar(data, output_path)
    print("[INFO] Done.")
 
 
if __name__ == "__main__":
    main()
