import pandas as pd

df = pd.read_parquet('/Users/JustinJu/Desktop/Quant_Infrastructure/Quant_trading_infrastructure/data/BTC_USDT_1m.parquet')
print(f"number of rows: {len(df)}")
print(df.tail())
print(df.head())