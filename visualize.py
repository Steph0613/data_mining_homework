import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os

import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']      
plt.rcParams['axes.unicode_minus'] = False  

def run_visualizations(data_path, output_path):
    sample_df = pd.read_parquet(os.path.join(output_path, 'dedup_sample.parquet'))

    def parse_json(x):
        try:
            d = json.loads(x)
            return pd.Series({'average_price': d.get('average_price', None), 'category': d.get('category', None)})
        except:
            return pd.Series({'average_price': None, 'category': None})

    parsed = sample_df['purchase_history'].apply(parse_json)
    sample_df['average_price'] = parsed['average_price']
    sample_df['category'] = parsed['category']

    plt.figure()
    sns.countplot(x='gender', data=sample_df)
    plt.savefig(os.path.join(output_path, 'gender.png'))

    plt.figure()
    sns.boxplot(x='age', data=sample_df)
    plt.savefig(os.path.join(output_path, 'age_box.png'))

    plt.figure()
    sample_df.groupby('category')['average_price'].mean().sort_values(ascending=False).plot(kind='bar')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, 'avg_price_by_category.png'))
