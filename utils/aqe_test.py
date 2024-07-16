from pyhive import hive
import os
from time import time_ns
import matplotlib.pyplot as plt
import numpy as np

conn = hive.Connection(host='192.168.90.171', port=10000, username='hejiahao')
cursor = conn.cursor()
cursor.execute('USE tpcds_sf3000')

def tune_aqe():
    # cursor.execute('SET spark.sql.adaptive.skewedJoin.enable=true')
    # cursor.execute('SET spark.sql.adaptive.skewedpartitionMaxSplits=3')
    # cursor.execute('SET spark.sql.adaptive.skewedPartitionFactor=3')
    # cursor.execute('SET spark.sql.adaptive.skewedPartitionSizeThreshold=52428800')
    # cursor.execute('SET spark.sql.adaptive.skewedPartitionRowCountThreshold=5000000')
    # cursor.execute('SET spark.sql.adaptive.maxNumPostShufflePartitions=1000')
    # cursor.execute('SET spark.sql.adaptive.minNumPostShufflePartitions=10')
    # cursor.execute('SET spark.sql.adaptive.shuffle.targetPostShuffleInputSize=60')
    cursor.execute('SET spark.sql.adaptive.coalescePartitions.enabled=false')
    print('Tuned AQE')

def reset_aqe():
    # cursor.execute('RESET spark.sql.adaptive.skewedJoin.enable')
    # cursor.execute('RESET spark.sql.adaptive.skewedpartitionMaxSplits')
    # cursor.execute('RESET spark.sql.adaptive.skewedPartitionFactor')
    # cursor.execute('RESET spark.sql.adaptive.skewedPartitionSizeThreshold')
    # cursor.execute('RESET spark.sql.adaptive.skewedPartitionRowCountThreshold')
    # cursor.execute('RESET spark.sql.adaptive.maxNumPostShufflePartitions')
    # cursor.execute('RESET spark.sql.adaptive.minNumPostShufflePartitions')
    # cursor.execute('RESET spark.sql.adaptive.shuffle.targetPostShuffleInputSize')
    cursor.execute('RESET spark.sql.adaptive.coalescePartitions.enabled')
    print('Reset AQE')
def execute_query(query):
    begin = time_ns()
    cursor.execute(query)
    end = time_ns()
    return end - begin

f_list = os.listdir('./benchmark/queries/tpcds_sf100')
f_list = ['sql_087.sql']

cursor.execute('CLEAR CACHE')
tune_aqe()
optimized_result = []
for f_name in sorted(f_list):
    with open('./benchmark/queries/tpcds_sf3000/' + f_name, 'r') as f:
        sql = f.read().strip().split(';')
        time_sum = 0
        for s in sql:
            if s != '':
                time1 = execute_query(s.strip()) / 1_000
                time2 = execute_query(s.strip()) / 1_000
                time3 = execute_query(s.strip()) / 1_000
                time4 = execute_query(s.strip()) / 1_000
                time5 = execute_query(s.strip()) / 1_000
                time_sum += (time1 + time2 + time3 + time4 + time5) / 5
        optimized_result.append(time_sum)
        print(f_name, time_sum)

cursor.execute('CLEAR CACHE')
reset_aqe()
original_result = []
for f_name in sorted(f_list):
    with open('./benchmark/queries/tpcds_sf3000/' + f_name, 'r') as f:
        sql = f.read().strip().split(';')
        time_sum = 0
        for s in sql:
            if s != '':
                time1 = execute_query(s.strip()) / 1_000
                time2 = execute_query(s.strip()) / 1_000
                time3 = execute_query(s.strip()) / 1_000
                time4 = execute_query(s.strip()) / 1_000
                time5 = execute_query(s.strip()) / 1_000
                time_sum += (time1 + time2 + time3 + time4 + time5) / 5
        original_result.append(time_sum)
        print(f_name, time_sum)

print(f'relative improvement: {(np.sum(original_result) - np.sum(optimized_result)) / np.sum(original_result)}')
best = []
for i in range(len(original_result)):
    best.append(min(original_result[i], optimized_result[i]))
print(f'best improvement: {(np.sum(original_result) - np.sum(best)) / np.sum(original_result)}')

# 使用plt画出对比的柱状图
# 设置柱子的宽度
bar_width = 0.2

# 生成对应每个结果的x坐标
x = np.arange(len(original_result))

# 绘制柱状图
plt.bar(x - bar_width/2, original_result, width=bar_width, label='Original')
plt.bar(x + bar_width/2, optimized_result, width=bar_width, label='Optimized')

# 设置x轴刻度标签
plt.xticks(x, [i.split('.')[0] for i in sorted(f_list)])

# 添加图例
plt.legend()

# 显示图形
plt.show()

# relative improvement: 0.026486498850080243
# best improvement: 0.08546974459939405