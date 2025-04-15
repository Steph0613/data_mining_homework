import pandas as pd
import json
import os
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

def run_user_profile_analysis(data_path, output_path):
    sample_df = pd.read_parquet(os.path.join(output_path, 'dedup_sample.parquet'))

    def parse_json(x):
        try:
            d = json.loads(x)
            return pd.Series({
                'avg_price': d.get('avg_price', None),
                'categories': d.get('categories', None),
                'num_items': len(d.get('items', [])) if 'items' in d else 0
            })
        except:
            return pd.Series({'avg_price': None, 'categories': None, 'num_items': 0})

    parsed = sample_df['purchase_history'].apply(parse_json)
    sample_df['avg_price'] = pd.to_numeric(parsed['avg_price'], errors='coerce')
    sample_df['num_items'] = parsed['num_items']

    profile_df = sample_df[['id', 'age', 'income', 'avg_price', 'num_items']]
    profile_df = profile_df.dropna(subset=['age', 'income', 'avg_price'])  # ✅ 更灵活的字段级过滤

    if profile_df.empty:
        print("⚠️ 无可用于聚类的用户画像样本，跳过用户画像分析")
        return

    scaler = StandardScaler()
    X = scaler.fit_transform(profile_df.drop(columns=['id']))

    kmeans = KMeans(n_clusters=4, random_state=42)
    profile_df['cluster'] = kmeans.fit_predict(X)

    profile_df.to_csv(os.path.join(output_path, 'user_profiles_clustered.csv'), index=False)

    plt.figure()
    sns.scatterplot(data=profile_df, x='income', y='avg_price' \
    '', hue='cluster', palette='Set2')
    plt.title("User Clusters by Income & Average Purchase Price")
    plt.legend(loc='upper right')
    plt.savefig(os.path.join(output_path, 'user_clusters.png'))
