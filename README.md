# 用户行为数据挖掘项目

本项目旨在处理两个大规模的用户行为数据集（10G 与 30G），通过分布式计算和用户画像分析，挖掘潜在的用户分群结构，为高价值用户识别和推荐系统提供数据支持。

## 项目运行

### 1. 准备数据

Parquet 数据放入如下目录：

```bash
./data/10G_data/     # 解压 10G parquet 数据集
./data/30G_data/     # 解压 30G parquet 数据集
```

确保 `config.py` 中路径正确：

```python
DATA_PATH_10G = './data/10G_data/'
OUTPUT_PATH_10G = './outputs/10G/'
```

如分析 30G 数据，修改为：

```python
DATA_PATH_10G = './data/30G_data/'
OUTPUT_PATH_10G = './outputs/30G/'
```

---

### 2. 运行代码

```bash
python main.py
```

将执行以下步骤：

- 数据读取与采样
- 使用布隆过滤器去重
- 描述性统计与缺失值检测
- 可视化图表生成
- 用户画像特征构建与 KMeans 聚类
- 所有结果输出至 `outputs/10G/` 或 `outputs/30G/`

---

## 输出说明

运行后，你将在 `outputs/10G/` 或 `outputs/30G/` 中看到：

- `basic_stats.csv`：字段统计信息
- `missing_stats.csv`：缺失值比例
- `data_cleaning_stats.txt`：清洗过程详细统计（去重数量、分布等）
- 图像：
  - `age_box.png`：年龄分布箱线图
  - `avg_price_by_category.png`：类别均价图
  - `gender.png`：性别分布图
  - `user_clusters.png`：用户聚类图
- `user_profiles_clustered.csv`：聚类结果保存

---

