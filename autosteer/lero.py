import pandas as pd
from connectors.spark_connector_ssh import SparkConnector
from autosteer.planToSQL import generate_sql, add_quotes, replace_ONNOT, replace_INSET, allColumnList

def reverse_parser(plan):
    sql = generate_sql(plan)
    sql = add_quotes(sql, allColumnList)
    sql = replace_ONNOT(sql)
    sql = replace_INSET(sql)
def fill_real_card(card_file):
    # 第一步：读取 CSV 文件
    df = pd.read_csv(card_file,delimiter=':')

    # 假设数据库中已经存在相应的数据，可以使用 sqlite3 连接数据库
    # 实际应用中你可以替换成 MySQL, PostgreSQL 或其他数据库的连接
    conn = SparkConnector()

    # 第二步：找到 Card_Real 列为空的行
    empty_card_real_rows = df[df['Card_Real'].isna()]

    # 第三步：解析 Query_Plan 列，逆向解析为 SQL 语句
    for index, row in empty_card_real_rows.iterrows():
        query_plan = row['Query_Plan']
        sql_query = reverse_parser(query_plan)  # 逆向解析得到 SQL 语句
        real_cardinality = eval(conn.execute(sql_query).result)  # 执行 SQL 查询，获取真实基数
        df.at[index, 'Card_Real'] = real_cardinality  # 更新 DataFrame 中的 Card_Real 列
    # 第五步：保存更新后的 DataFrame 回到 CSV 文件
    df.to_csv(card_file, index=False, sep=':')
