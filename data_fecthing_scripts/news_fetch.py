import pandas as pd
df = pd.read_parquet("../data/stock_news.parquet")   # uses pyarrow under the hood
df.to_csv("../data/stock_news.csv", index=False)
