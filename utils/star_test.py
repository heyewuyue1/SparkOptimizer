from pyhive import hive
import os
from time import time_ns
import matplotlib.pyplot as plt
import numpy as np

conn = hive.Connection(host='202.112.113.146', port=10000, username='hejiahao')
cursor = conn.cursor()
cursor.execute('USE tpcds_sf10')

def turn_off_cbo():
    cursor.execute('SET spark.sql.cbo.enabled=false')
    cursor.execute('SET spark.sql.cbo.joinReorder.dp.star.filter=false')
    cursor.execute('SET spark.sql.cbo.starSchemaDetection=false')
    cursor.execute('SET spark.sql.cbo.joinReorder.enabled=false')

def turn_on_cbo():
    cursor.execute('SET spark.sql.cbo.enabled=true')
    cursor.execute('SET spark.sql.cbo.joinReorder.dp.star.filter=true')
    cursor.execute('SET spark.sql.cbo.starSchemaDetection=true')
    cursor.execute('SET spark.sql.cbo.joinReorder.enabled=true')

def execute_query(query):
    begin = time_ns()
    cursor.execute(query)
    end = time_ns()
    return end - begin

f_list = os.listdir('./benchmark/queries/tpcds_fix')

turn_off_cbo()
print('cbo off')
original_result = []
for f_name in sorted(f_list)[71:72]:
    with open('./benchmark/queries/tpcds_fix/' + f_name, 'r') as f:
        sql = f.read().strip().split(';')
        time_sum = 0
        for s in sql:
            if s != '':
                time1 = execute_query(s.strip()) / 1_000
                time2 = execute_query(s.strip()) / 1_000
                time_sum += (time1 + time2) / 2
        original_result.append(time_sum)
        print(f_name, time_sum)

turn_on_cbo()
print('cbo on')
optimized_result = []
for f_name in sorted(f_list)[71:72]:
    with open('./benchmark/queries/tpcds_fix/' + f_name, 'r') as f:
        sql = f.read().strip().split(';')
        time_sum = 0
        for s in sql:
            if s != '':
                time1 = execute_query(s.strip()) / 1_000
                time2 = execute_query(s.strip()) / 1_000
                time_sum += (time1 + time2) / 2
        optimized_result.append(time_sum)
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
plt.xticks(x, [i.split('.')[0] for i in sorted(f_list)[71:72]])

# 添加图例
plt.legend()

# 显示图形
plt.show()

