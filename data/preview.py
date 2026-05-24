import os
import pandas as pd

# ==============================================================
# Configuration: preview.py resides in the same data/ directory
# as the target .parquet files.
# ==============================================================
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

EXPECTED_FILES = [
    "BTC_USDT_1h.parquet",
    "BTC_USDT_1m.parquet",
    "ETH_USDT_1h.parquet",
    "ETH_USDT_1m.parquet",
]

EXPECTED_COLS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']


def inspect_file(file_name: str) -> None:
    """
    Run a full quality inspection on a single parquet file.
    Checks include: file existence, schema validation, timestamp
    integrity, gap detection, NaN counts, and OHLC logic.
    """
    file_path = os.path.join(DATA_DIR, file_name)
    sep = "=" * 60

    print(f"\n{sep}")
    print(f"  FILE: {file_name}")
    print(sep)

    # ----------------------------------------------------------
    # 1. File existence and size
    # ----------------------------------------------------------
    if not os.path.exists(file_path):
        print(f"  [ERROR] File not found: {file_path}")
        return

    file_size_mb = os.path.getsize(file_path) / (1024 ** 2)
    print(f"  File size         : {file_size_mb:.2f} MB")

    # ----------------------------------------------------------
    # 2. Load parquet
    # ----------------------------------------------------------
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"  [ERROR] Failed to read parquet file: {e}")
        return

    # ----------------------------------------------------------
    # 3. Basic info
    # ----------------------------------------------------------
    print(f"  Total rows        : {len(df):,}")
    print(f"  Columns           : {df.columns.tolist()}")

    # ----------------------------------------------------------
    # 4. Schema validation — check for missing expected columns
    # ----------------------------------------------------------
    missing_cols = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing_cols:
        print(f"  [WARNING] Missing columns: {missing_cols}")
    else:
        print(f"  [OK] All expected columns are present.")

    # ----------------------------------------------------------
    # 5. Timestamp parsing
    # ----------------------------------------------------------
    if 'timestamp' not in df.columns:
        print("  [ERROR] No 'timestamp' column found — skipping time-based checks.")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    bad_ts = df['timestamp'].isna().sum()
    if bad_ts > 0:
        print(f"  [WARNING] Unparseable timestamps: {bad_ts:,} rows dropped.")
    df = df.dropna(subset=['timestamp']).sort_values('timestamp')

    # ----------------------------------------------------------
    # 6. Time range coverage
    # ----------------------------------------------------------
    ts_min = df['timestamp'].min()
    ts_max = df['timestamp'].max()
    span   = ts_max - ts_min
    print(f"  Earliest candle   : {ts_min}")
    print(f"  Latest candle     : {ts_max}")
    print(f"  Total span        : {span.days} days ({span.days / 365.25:.2f} years)")

    # ----------------------------------------------------------
    # 7. Duplicate timestamp detection
    # ----------------------------------------------------------
    dup_count = df['timestamp'].duplicated().sum()
    if dup_count > 0:
        print(f"  [WARNING] Duplicate timestamps found: {dup_count:,}")
    else:
        print(f"  [OK] No duplicate timestamps.")

    # ----------------------------------------------------------
    # 8. Gap detection
    #    A gap is defined as any interval exceeding 1.5x the
    #    modal (most common) interval for this timeframe.
    # ----------------------------------------------------------
    time_diffs     = df['timestamp'].diff().dropna()
    expected_delta = time_diffs.mode()[0]
    gaps           = time_diffs[time_diffs > expected_delta * 1.5]

    print(f"  Expected interval : {expected_delta}")
    if len(gaps) > 0:
        print(f"  [WARNING] Gaps detected: {len(gaps):,} total")
        top_gaps = gaps.nlargest(5)
        for idx, gap in top_gaps.items():
            gap_time = df.loc[idx, 'timestamp']
            print(f"    Gap of {gap} ending at {gap_time}")
    else:
        print(f"  [OK] No gaps detected.")

    # ----------------------------------------------------------
    # 9. NaN audit across OHLCV columns
    # ----------------------------------------------------------
    price_cols = ['open', 'high', 'low', 'close', 'volume']
    available  = [c for c in price_cols if c in df.columns]
    nan_report = df[available].isna().sum()
    total_nans = nan_report.sum()

    if total_nans > 0:
        print(f"  [WARNING] NaN values detected:")
        for col, cnt in nan_report[nan_report > 0].items():
            print(f"    {col}: {cnt:,} NaN(s)")
    else:
        print(f"  [OK] No NaN values in OHLCV columns.")

    # ----------------------------------------------------------
    # 10. OHLC logical consistency check
    #     Validates: high >= low, high >= open, low <= open
    # ----------------------------------------------------------
    if all(c in df.columns for c in ['open', 'high', 'low', 'close']):
        invalid_hl = (df['high'] < df['low']).sum()
        invalid_oh = (df['high'] < df['open']).sum()
        invalid_ol = (df['low']  > df['open']).sum()

        if invalid_hl + invalid_oh + invalid_ol > 0:
            print(f"  [WARNING] OHLC logic violations found:")
            print(f"    high < low  : {invalid_hl:,} rows")
            print(f"    high < open : {invalid_oh:,} rows")
            print(f"    low  > open : {invalid_ol:,} rows")
        else:
            print(f"  [OK] OHLC logic is valid (high >= low, high >= open, low <= open).")

    # ----------------------------------------------------------
    # 11. Head and tail preview
    # ----------------------------------------------------------
    display_cols = [c for c in EXPECTED_COLS if c in df.columns]
    print(f"\n  HEAD (first 3 rows):")
    print(df[display_cols].head(3).to_string(index=False))
    print(f"\n  TAIL (last 3 rows):")
    print(df[display_cols].tail(3).to_string(index=False))

    # ----------------------------------------------------------
    # 12. Descriptive statistics for close price
    # ----------------------------------------------------------
    if 'close' in df.columns:
        stats = df['close'].describe()
        print(f"\n  Close price statistics:")
        print(f"    min  = {stats['min']:.4f}")
        print(f"    max  = {stats['max']:.4f}")
        print(f"    mean = {stats['mean']:.4f}")
        print(f"    std  = {stats['std']:.4f}")


# ==============================================================
# Entry point
# ==============================================================
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  QUANT DATA QUALITY INSPECTOR")
    print("=" * 60)
    print(f"\n  Scanning directory: {DATA_DIR}\n")

    for fname in EXPECTED_FILES:
        inspect_file(fname)

    print("\n" + "=" * 60)
    print("  Inspection complete.")
    print("=" * 60 + "\n")