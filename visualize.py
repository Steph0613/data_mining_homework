import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

def run_visualizations(data_path, output_path):
    sample_df = pd.read_parquet(
        os.path.join(output_path, 'dedup_sample.parquet'),
        columns=['age', 'gender', 'income', 'country', 'is_active', 'purchase_history']
    ).sample(frac=0.01, random_state=42)

    def parse_json(x):
        try:
            if not isinstance(x, str) or x.strip() in ['null', '{}', '']:
                return pd.Series({'avg_price': None, 'categories': None})
            d = json.loads(x)
            return pd.Series({'avg_price': d.get('avg_price', None), 'categories': d.get('categories', None)})
        except:
            return pd.Series({'avg_price': None, 'categories': None})

    parsed = sample_df['purchase_history'].apply(parse_json)
    sample_df['avg_price'] = parsed['avg_price']
    sample_df['categories'] = parsed['categories']
    sample_df.drop(columns=['purchase_history'], inplace=True)
    sample_df['avg_price'] = pd.to_numeric(sample_df['avg_price'], errors='coerce')

    # 图1：性别分布
    plt.figure()
    sns.countplot(x='gender', data=sample_df)
    plt.title("性别分布")
    plt.savefig(os.path.join(output_path, 'gender.png'))

    # 图2：年龄箱线图
    plt.figure()
    sns.boxplot(x='age', data=sample_df)
    plt.title("年龄分布箱线图")
    plt.savefig(os.path.join(output_path, 'age_box.png'))

    # 图3：品类平均消费额（带空判断）
    avg_price_by_cat = sample_df.groupby('categories')['avg_price'].mean().sort_values(ascending=False)
    if not avg_price_by_cat.empty:
        plt.figure()
        avg_price_by_cat.plot(kind='bar')
        plt.title("各品类平均消费额")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(output_path, 'avg_price_by_categories.png'))
    else:
        print("⚠️ 无有效品类消费数据，跳过 avg_price_by_categories 图像生成")

    # 图4：活跃与否 vs 平均消费额
    plt.figure()
    active_price = sample_df.groupby('is_active')['avg_price'].mean()
    active_price.plot(kind='bar', color=['gray', 'skyblue'])
    plt.title("活跃与否用户平均消费额对比")
    plt.ylabel("平均消费额")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, 'active_vs_avg_price.png'))

    # 图5：国家 vs 收入分布箱线图（限制前10国家）
    plt.figure(figsize=(10, 6))
    top_countries = sample_df['country'].value_counts().nlargest(10).index
    sns.boxplot(data=sample_df[sample_df['country'].isin(top_countries)],
                x='country', y='income')
    plt.title("不同国家用户收入分布")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, 'income_by_country.png'))
