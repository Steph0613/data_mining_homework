from pybloom_live import BloomFilter
import pandas as pd

def deduplicate_with_bloom(df, id_col='id', time_col='timestamp', name_col='chinese_name', capacity=10_000_000, error_rate=0.001):
    bf = BloomFilter(capacity=capacity, error_rate=error_rate)
    keep_indices = []

    for i, row in df.iterrows():
        key = str(row[id_col]) + str(row[time_col]) + str(row[name_col])
        if key not in bf:
            bf.add(key)
            keep_indices.append(i)

    dedup_df = df.loc[keep_indices]
    removed = len(df) - len(dedup_df)
    return dedup_df, removed
