import dask
import dask.dataframe as dd
import pandas as pd
import os
import json
from bloom_filter_dedup import deduplicate_with_bloom

def is_valid_purchase_json(x):
    try:
        if not isinstance(x, str) or x.strip() in ['null', '{}', '']:
            return False
        json.loads(x)
        return True
    except:
        return False

def run_eda(data_path, output_path):
    dask.config.set(scheduler='threads', num_workers=2)
    df = dd.read_parquet(data_path, engine='pyarrow', chunksize="100MB")
    sample_df = df.sample(frac=0.05).compute()
    dedup_df, dup_removed = deduplicate_with_bloom(sample_df)

    total_before = len(sample_df)
    total_after_dedup = len(dedup_df)

   
    na_removed = dedup_df.isnull().any(axis=1).sum()

    rows_before_json_filter = len(dedup_df)
    dedup_df = dedup_df[dedup_df['purchase_history'].apply(is_valid_purchase_json)]
    json_removed = rows_before_json_filter - len(dedup_df)

    age_removed = dedup_df[~dedup_df['age'].between(0, 100)].shape[0]
    income_removed = dedup_df[dedup_df['income'] < 0].shape[0]

    dup_rate = round(dup_removed / total_before * 100, 2)
    na_rate = round(na_removed / total_after_dedup * 100, 2)
    final_total = dedup_df.shape[0]
    age_rate = round(age_removed / final_total * 100, 2)
    income_rate = round(income_removed / final_total * 100, 2)

    stats = dedup_df.describe()
    missing_ratio = dedup_df.isnull().mean() * 100
    missing_ratio = missing_ratio.round(2).astype(str) + '%'
    stats.to_csv(os.path.join(output_path, 'basic_stats.csv'))
    missing_ratio.to_csv(os.path.join(output_path, 'missing_stats.csv'))

    stats_text = f'''
移除重复记录: {dup_removed} （占原始样本 {dup_rate}%）
总存在缺失值的行数: {na_removed} （占去重后数据 {na_rate}%）
无效 purchase_history 移除条数: {json_removed}

各字段异常值移除:
- age: {age_removed} 条，占比 {age_rate}%
- income: {income_removed} 条，占比 {income_rate}%

数据实例总数: {final_total}
属性个数: {dedup_df.shape[1]}

数据示例:
{dedup_df.head(5).to_string(index=False)}

标签属性频数统计:
gender 分布统计:
{dedup_df["gender"].value_counts()}

country 分布统计:
{dedup_df["country"].value_counts().head(10)}

is_active 分布统计:
{dedup_df["is_active"].value_counts()}

purchase_categories 分布统计:
{dedup_df["purchase_history"].apply(lambda x: json.loads(x).get("categories", "未知")).value_counts()}
'''
    with open(os.path.join(output_path, 'data_cleaning_stats.txt'), 'w', encoding='utf-8') as f:
        f.write(stats_text)

    dedup_df.to_parquet(os.path.join(output_path, 'dedup_sample.parquet'))
