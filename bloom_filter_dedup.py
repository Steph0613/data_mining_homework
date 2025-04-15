from pybloom_live import BloomFilter
import pandas as pd

def deduplicate_with_bloom(df, id_col='id', time_col='last_login', name_col='fullname',
                           capacity=20_000_000, error_rate=0.001):
    bf = BloomFilter(capacity=capacity, error_rate=error_rate)
    new_rows = []

    for _, row in df.iterrows():
        id_val = str(row.get(id_col, '') or '')
        time_val = str(row.get(time_col, '') or '')
        name_val = str(row.get(name_col, '') or '')
        key = id_val + time_val + name_val  

        if key not in bf:
            bf.add(key)
            new_rows.append(row)

    dedup_df = pd.DataFrame(new_rows).reset_index(drop=True)
    removed = len(df) - len(dedup_df)
    return dedup_df, removed
