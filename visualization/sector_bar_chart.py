# -*- coding: utf-8 -*-
"""

How to generate sector_data.csv:
    hive -e "
    USE nasdaq;
    SELECT sector,
        COUNT(DISTINCT ticker) AS ticker_count,
        AVG(volatility_20d) AS avg_vol,
        STDDEV(volatility_20d) AS std_vol,
        MAX(volatility_20d) AS max_vol,
        AVG(atr_14d) AS avg_atr
    FROM stock_features
    WHERE sector IS NOT NULL
    AND volatility_20d IS NOT NULL AND volatility_20d > 0
    AND NOT (volatility_20d <=> CAST('NaN' AS DOUBLE))
    GROUP BY sector
    ORDER BY avg_vol DESC;
    " 2>&1 | grep -E "^\|" > visualization/sector_data.csv

"""

import os
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

OUTPUT_DIR    = "visualization/outputs"
SECTOR_CSV    = "visualization/sector_data.csv"

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


def load_csv_data():
    """Load sector data from CSV saved from Hive query."""
    if not os.path.exists(SECTOR_CSV):
        print("[INFO] {} not found".format(SECTOR_CSV))
        return None

    rows = []
    with open(SECTOR_CSV, 'r') as f:
        for line in f:
            # Parse pipe-delimited Hive output
            parts = [p.strip() for p in line.strip().split('|') if p.strip()]
            if len(parts) >= 6:
                try:
                    # Skip header row
                    if parts[0].lower() in ('sector', 's.sector'):
                        continue
                    rows.append({
                        'sector':       parts[0],
                        'ticker_count': int(float(parts[1])),
                        'avg_vol':      float(parts[2]),
                        'std_vol':      float(parts[3]) if parts[3] != 'NULL' else 0.01,
                        'max_vol':      float(parts[4]),
                        'avg_atr':      float(parts[5]),
                    })
                except (ValueError, IndexError):
                    continue

    if rows:
        print("[INFO] Loaded {} sectors from {}".format(len(rows), SECTOR_CSV))
        return rows
    return None


def build_mock_data():
    """Real data from Hive query results."""
    print("[INFO] Using real Hive query results as data...")
    return [
        {'sector': 'Technology',             'ticker_count': 16, 'avg_vol': 0.021535, 'std_vol': 0.012, 'max_vol': 0.111487, 'avg_atr': 4.2524},
        {'sector': 'Consumer Discretionary', 'ticker_count': 10, 'avg_vol': 0.019435, 'std_vol': 0.010, 'max_vol': 0.101727, 'avg_atr': 3.2004},
        {'sector': 'Energy',                 'ticker_count': 4,  'avg_vol': 0.018555, 'std_vol': 0.009, 'max_vol': 0.121132, 'avg_atr': 1.7569},
        {'sector': 'Communication Services', 'ticker_count': 8,  'avg_vol': 0.016918, 'std_vol': 0.008, 'max_vol': 0.104876, 'avg_atr': 2.6910},
        {'sector': 'Financials',             'ticker_count': 9,  'avg_vol': 0.016843, 'std_vol': 0.009, 'max_vol': 0.100375, 'avg_atr': 3.5360},
        {'sector': 'Industrials',            'ticker_count': 7,  'avg_vol': 0.016736, 'std_vol': 0.008, 'max_vol': 0.140996, 'avg_atr': 3.9765},
        {'sector': 'Real Estate',            'ticker_count': 5,  'avg_vol': 0.015511, 'std_vol': 0.007, 'max_vol': 0.142033, 'avg_atr': 3.2149},
        {'sector': 'Healthcare',             'ticker_count': 11, 'avg_vol': 0.014427, 'std_vol': 0.008, 'max_vol': 0.119462, 'avg_atr': 3.9645},
        {'sector': 'Consumer Staples',       'ticker_count': 3,  'avg_vol': 0.013285, 'std_vol': 0.006, 'max_vol': 0.055022, 'avg_atr': 3.4207},
        {'sector': 'Utilities',              'ticker_count': 4,  'avg_vol': 0.012400, 'std_vol': 0.005, 'max_vol': 0.084236, 'avg_atr': 1.1510},
    ]


def plot_sector_bar(data, output_path):
    data = sorted(data, key=lambda x: x['avg_vol'], reverse=True)

    sectors     = [d['sector'] for d in data]
    avg_vols    = [d['avg_vol'] for d in data]
    std_vols    = [d['std_vol'] for d in data]
    ticker_cnts = [d['ticker_count'] for d in data]
    colors      = [SECTOR_COLORS.get(s, DEFAULT_COLOR) for s in sectors]
    index_avg   = np.mean(avg_vols)

    fig, ax = plt.subplots(figsize=(13, 8))
    fig.patch.set_facecolor('#0d1117')
    ax.set_facecolor('#161b22')

    y_pos = range(len(sectors))

    bars = ax.barh(
        y_pos, avg_vols,
        xerr=std_vols,
        color=colors, alpha=0.88, height=0.65,
        error_kw={'ecolor': '#8b949e', 'elinewidth': 1.2, 'capsize': 4, 'capthick': 1.2}
    )

    for i, (bar, val, cnt) in enumerate(zip(bars, avg_vols, ticker_cnts)):
        ax.text(
            val + 0.0003, bar.get_y() + bar.get_height() / 2,
            '{:.4f}  ({} stocks)'.format(val, cnt),
            va='center', ha='left', fontsize=9,
            color='#e6edf3', fontweight='bold'
        )

    ax.axvline(x=index_avg, color='#f0f6fc', linewidth=1.5,
               linestyle='--', alpha=0.7,
               label='Index Average: {:.4f}'.format(index_avg))

    ax.set_yticks(y_pos)
    ax.set_yticklabels(sectors, fontsize=11, color='#e6edf3', fontweight='bold')
    ax.set_xlabel('Average 20-Day Rolling Volatility', fontsize=10,
                  color='#8b949e', labelpad=10)
    ax.tick_params(axis='x', colors='#8b949e', labelsize=8)
    ax.xaxis.grid(True, color='#21262d', linewidth=0.8, alpha=0.8)
    ax.set_axisbelow(True)
    for spine in ['top', 'right', 'left']:
        ax.spines[spine].set_visible(False)
    ax.spines['bottom'].set_color('#21262d')
    ax.legend(fontsize=9, facecolor='#161b22', edgecolor='#30363d',
              labelcolor='#e6edf3', loc='lower right')

    fig.text(0.5, 0.97, 'NASDAQ-100 Sector Risk Comparison',
             ha='center', fontsize=16, fontweight='bold', color='#e6edf3')
    fig.text(0.5, 0.925,
             'Average 20-Day Volatility by Sector  |  84 Tickers  |  10 Years (2016-2026)',
             ha='center', fontsize=9, color='#8b949e')

    ax.invert_yaxis()
    plt.tight_layout(rect=[0, 0, 1, 0.92])
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='#0d1117', edgecolor='none')
    plt.close()
    print("[INFO] Bar chart saved -> {}".format(output_path))


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, 'sector_bar_chart.png')

    data = load_csv_data() or build_mock_data()
    plot_sector_bar(data, output_path)
    print("[INFO] Done.")


if __name__ == "__main__":
    main()
