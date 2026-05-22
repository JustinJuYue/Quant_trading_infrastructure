import os
import pandas as pd

# ============================================================
# 📁 配置：preview.py 与 .parquet 文件在同一 data/ 目录下
# ============================================================
DATA_DIR = os.path.dirname(os.path.abspath(__file__))  # ✅ 修复核心

EXPECTED_FILES = [
    "BTC_USDT_1h.parquet",
    "BTC_USDT_1m.parquet",
    "ETH_USDT_1h.parquet",
    "ETH_USDT_1m.parquet",
]

EXPECTED_COLS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

# ============================================================
# 🔍 单文件检查函数
# ============================================================
def inspect_file(file_name: str):
    file_path = os.path.join(DATA_DIR, file_name)
    sep = "=" * 60

    print(f"\n{sep}")
    print(f"  📄 FILE: {file_name}")
    print(sep)

    # ── 1. 文件存在性检查 ──────────────────────────────────────
    if not os.path.exists(file_path):
        print(f"  ❌ File NOT FOUND: {file_path}")
        return

    file_size_mb = os.path.getsize(file_path) / (1024 ** 2)
    print(f"  💾 File size      : {file_size_mb:.2f} MB")

    # ── 2. 读取文件 ────────────────────────────────────────────
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"  ❌ Failed to read parquet: {e}")
        return

    # ── 3. 基础信息 ────────────────────────────────────────────
    print(f"  📊 Total rows     : {len(df):,}")
    print(f"  📋 Columns        : {df.columns.tolist()}")

    # ── 4. 列完整性检查 ────────────────────────────────────────
    missing_cols = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing_cols:
        print(f"  ⚠️  Missing columns : {missing_cols}")
    else:
        print(f"  ✅ All expected columns present")

    # ── 5. timestamp 解析 ──────────────────────────────────────
    if 'timestamp' not in df.columns:
        print("  ❌ No 'timestamp' column — cannot proceed with time checks.")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    bad_ts = df['timestamp'].isna().sum()
    if bad_ts > 0:
        print(f"  ⚠️  Unparseable timestamps : {bad_ts:,} rows")
    df = df.dropna(subset=['timestamp']).sort_values('timestamp')

    # ── 6. 时间范围 ────────────────────────────────────────────
    ts_min = df['timestamp'].min()
    ts_max = df['timestamp'].max()
    span   = ts_max - ts_min
    print(f"  🕐 Earliest candle: {ts_min}")
    print(f"  🕐 Latest  candle : {ts_max}")
    print(f"  📅 Total span     : {span.days} days  ({span.days / 365.25:.2f} years)")

    # ── 7. 重复时间戳检查 ──────────────────────────────────────
    dup_count = df['timestamp'].duplicated().sum()
    if dup_count > 0:
        print(f"  ⚠️  Duplicate timestamps : {dup_count:,}")
    else:
        print(f"  ✅ No duplicate timestamps")

    # ── 8. 缺口检测（Gap Detection）──────────────────────────
    time_diffs   = df['timestamp'].diff().dropna()
    expected_delta = time_diffs.mode()[0]
    gaps = time_diffs[time_diffs > expected_delta * 1.5]
    print(f"  ⏱️  Expected interval : {expected_delta}")
    if len(gaps) > 0:
        print(f"  ⚠️  Gaps detected     : {len(gaps):,} gaps")
        top_gaps = gaps.nlargest(5)
        for idx, gap in top_gaps.items():
            gap_time = df.loc[idx, 'timestamp']
            print(f"      └─ {gap} gap ending at {gap_time}")
    else:
        print(f"  ✅ No gaps detected")

    # ── 9. NaN 检查 ────────────────────────────────────────────
    price_cols = ['open', 'high', 'low', 'close', 'volume']
    available  = [c for c in price_cols if c in df.columns]
    nan_report = df[available].isna().sum()
    total_nans = nan_report.sum()
    if total_nans > 0:
        print(f"  ⚠️  NaN values detected:")
        for col, cnt in nan_report[nan_report > 0].items():
            print(f"      └─ {col}: {cnt:,} NaNs")
    else:
        print(f"  ✅ No NaN values in OHLCV columns")

    # ── 10. 价格合理性检查 ─────────────────────────────────────
    if all(c in df.columns for c in ['open', 'high', 'low', 'close']):
        invalid_hl = (df['high'] < df['low']).sum()
        invalid_oh = (df['high'] < df['open']).sum()
        invalid_ol = (df['low']  > df['open']).sum()
        if invalid_hl + invalid_oh + invalid_ol > 0:
            print(f"  ⚠️  OHLC logic violations:")
            print(f"      └─ high < low  : {invalid_hl:,} rows")
            print(f"      └─ high < open : {invalid_oh:,} rows")
            print(f"      └─ low  > open : {invalid_ol:,} rows")
        else:
            print(f"  ✅ OHLC logic valid (high ≥ low, etc.)")

    # ── 11. Head & Tail 预览 ───────────────────────────────────
    display_cols = [c for c in EXPECTED_COLS if c in df.columns]
    print(f"\n  📌 HEAD (first 3 rows):")
    print(df[display_cols].head(3).to_string(index=False))
    print(f"\n  📌 TAIL (last 3 rows):")
    print(df[display_cols].tail(3).to_string(index=False))

    # ── 12. 统计摘要 ───────────────────────────────────────────
    if 'close' in df.columns:
        stats = df['close'].describe()
        print(f"\n  📈 CLOSE price stats:")
        print(f"      min={stats['min']:.4f}  max={stats['max']:.4f}  "
              f"mean={stats['mean']:.4f}  std={stats['std']:.4f}")


# ============================================================
# 🚀 主程序
# ============================================================
if __name__ == "__main__":
    print("\n" + "🔎 " * 20)
    print("   QUANT DATA QUALITY INSPECTOR")
    print("🔎 " * 20)
    print(f"\n  📂 Scanning directory: {DATA_DIR}\n")  # ✅ 打印实际扫描路径，方便调试

    for fname in EXPECTED_FILES:
        inspect_file(fname)

    print("\n" + "=" * 60)
    print("  ✅ Inspection complete.")
    print("=" * 60 + "\n")