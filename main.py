import time
import os
from config import DATA_PATH, OUTPUT_PATH
from eda import run_eda
from visualize import run_visualizations
from user_profile import run_user_profile_analysis

os.makedirs(OUTPUT_PATH, exist_ok=True)
start_time = time.time()

print("字段统计与缺失值分析...")
run_eda(DATA_PATH, OUTPUT_PATH)

print("可视化绘图...")
run_visualizations(DATA_PATH, OUTPUT_PATH)

print("用户画像分析...")
run_user_profile_analysis(DATA_PATH, OUTPUT_PATH)

end_time = time.time()
with open(os.path.join(OUTPUT_PATH, 'total_running_time.txt'), 'w') as f:
    f.write(f"Total time: {end_time - start_time:.2f} seconds\n")
print(f"全部完成，总时间：{end_time - start_time:.2f} 秒")
