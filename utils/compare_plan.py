from pyhive import hive
import os

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

with open('benchmark/queries/tpcds_fix/72.sql') as f:
    sql = f.read()
    turn_off_cbo()
    cursor.execute('EXPLAIN FORMATTED ' + sql)
    original_plan = cursor.fetchall()[0][0]

    turn_on_cbo()
    cursor.execute('EXPLAIN FORMATTED ' + sql)
    optimized_plan = cursor.fetchall()[0][0]

print(original_plan)
print('---------------------------------------')
print(optimized_plan)