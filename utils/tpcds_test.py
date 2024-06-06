from pyhive import hive
import os
import time

conn = hive.Connection(host='192.168.90.171', port=10000, username='hejiahao')
cursor = conn.cursor()
cursor.execute('USE tpcds_sf100')
max_time = 0

for f_name in sorted(os.listdir('benchmark/queries/tpcds_fix/')):
    if f_name.endswith('.sql'):
        f_path = os.path.join('benchmark/queries/tpcds_fix/', f_name)
        with open(f_path, 'r') as f:
            sql = f.read().split(';')
            print(f_name)
            time_sum = 0
            for s in sql:
                if s != '':
                    begin = time.time_ns()
                    result = cursor.execute(s)
                    elapse_time = int((time.time_ns() - begin) / 1_000)
                    time_sum += elapse_time
            if elapse_time > max_time:
                max_time = elapse_time
            print(cursor.fetchall())
            print(time_sum)

print(max_time)