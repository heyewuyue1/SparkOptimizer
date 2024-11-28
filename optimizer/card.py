import pandas as pd
from optimizer.planToSQL import generate_sql, add_quotes, replace_ONNOT, replace_INSET, allColumnList, preprocess
from utils.custom_logging import logger

def reverse_parser(plan):
    plan = preprocess(plan)
    if plan == '':
        return ''
    sql = generate_sql(plan)
    sql = add_quotes(sql, allColumnList)
    sql = replace_ONNOT(sql)
    sql = replace_INSET(sql)
    return sql
    
def fill_real_card(card_file, connector):
    conn = connector()
    # 第一步：读取 CSV 文件
    df = pd.read_csv(card_file,delimiter=';')
    df = df.drop_duplicates("hash")
    # 第二步：找到 card 列为空的行
    empty_card_real_rows = df[df['card'] == 0]
    logger.info(f"Generating cardinalities for {len(empty_card_real_rows)} sub-plans...")
    # 第三步：解析 Query_Plan 列，逆向解析为 SQL 语句
    for index, row in empty_card_real_rows.iterrows():
        if index != 1:
            continue
        query_plan = row['plan']
        print(query_plan)
        sql_query = reverse_parser(query_plan)  # 逆向解析得到 SQL 语句
        print('--------------')
        print(sql_query)
        break
        if sql_query != '':
            try:
                logger.debug(sql_query)
                result = conn.execute(sql_query).result
                real_cardinality = result[0][0]  # 执行 SQL 查询，获取真实基数
                logger.info(f'Get real card: {real_cardinality}')
            except:
                real_cardinality = 0
                logger.info(f'Something wrong while executing subplan, skipping this one...')
            df.at[index, 'card'] = real_cardinality  # 更新 DataFrame 中的 Card_Real 列
        # 第五步：保存更新后的 DataFrame 回到 CSV 文件
        df.to_csv(card_file, index=False, sep=';')
