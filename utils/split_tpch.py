from pyhive import hive

print(f'check and load database tpch_sf1...')
conn = hive.Connection(host='202.112.113.146', port='10000', username='hejiahao')
cursor = conn.cursor()
# cursor.execute(f'DROP DATABASE IF EXISTS tpch_sf1 CASCADE')
# cursor.execute(f'CREATE DATABASE IF NOT EXISTS tpch_sf1')
cursor.execute(f'USE tpch_sf1')
with open(f'./benchmark/schemas/tpch_sf1.sql', 'r') as f:
    query = f.read()
    query = query.split(';')
    for q in query:
        if q.strip() != '':
            cursor.execute(q)
            print(q.split()[5])