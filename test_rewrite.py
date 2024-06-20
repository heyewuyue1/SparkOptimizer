from rewriter import call_rewriter
import os
import re
from pyhive import hive
from utils.custom_logging import logger

conn = hive.Connection(host='202.112.113.146', port=10000, username='hejiahao')
cursor = conn.cursor()
cursor.execute('USE tpcds_sf10')

def check_syntax(query):
    logger.info(f'checking syntax for query: {query}')
    try:
        cursor.execute(f'EXPLAIN FORMATTED {query}')
        logger.info('OK')
    except Exception as e:
        logger.error(e)

def post_process(query):
    # 正则表达式匹配 "FETCH NEXT xxx ROWS ONLY"
    fetch_pattern = re.compile(r"FETCH\s+NEXT\s+(\d+)\s+ROWS\s+ONLY", re.IGNORECASE)
    
    # 使用替换模式替换 "FETCH NEXT xxx ROWS ONLY" 为 "LIMIT xxx"
    query = fetch_pattern.sub(r"LIMIT \1", query)

    query = re.sub(r'"\$(\w+)"', r'dollar\1', query)
    return query

def replace_date_operations(sql):
    # 正则表达式模式，匹配 date_add(cast('2002-08-04' as date), 30) 和 date_add(cast('2002-08-04' as date), -30)
    date_add_pattern = re.compile(r"date_add\s*\(\s*cast\s*\(\s*'(\d{4}-\d{1,2}-\d{1,2})'\s*as\s*date\s*\)\s*,\s*(-?\d+)\s*\)", re.IGNORECASE)
    
    # 替换函数
    def replace_date_add_match(match):
        date = match.group(1)
        interval = match.group(2)
        operator = '+' if int(interval) > 0 else '-'
        interval = int(interval) if int(interval) >= 0 else -int(interval)
        return f"(cast('{date}' as date) {operator} interval '{interval}' day)"
    
    # 使用正则表达式替换
    sql = date_add_pattern.sub(replace_date_add_match, sql)
    
    return sql

def pre_process(sql):
    sql = sql.replace('\n', ' ')
    sql = sql.replace(' returns', ' calcite_returns')
    sql = sql.replace('(returns)', '(calcite_returns)')
    sql = sql.replace('(returns,', '(calcite_returns,')
    sql = sql.replace(' year ', ' calcite_year ')
    sql = sql.replace(',year ', ',calcite_year ')
    sql = sql.replace('.year ', '.calcite_year ')
    sql = sql.replace('. year ', '. calcite_year ')
    sql = sql.replace('year, ', 'calcite_year, ')
    sql = sql.replace('d_calcite_year', 'd_year')
    sql = sql.replace('this_calcite_year', 'this_year')
    sql = sql.replace(' at', ' calcite_at')
    sql = sql.replace('substr', 'substring')
    sql = sql.replace('d1.d_date + 5', "d1.d_date + interval '5' day")
    sql = replace_date_operations(sql)
    sql = sql.replace('`', '"')
    sql = sql.replace('TEXT', 'CHAR')

    pattern_iif = r'IIF\((.*?),\s+(.*?),\s+(.*?)\)'
    matches_iif = re.findall(pattern_iif, sql)
    for i in matches_iif:
        sql = sql.replace('IIF(' + i[0] + ', ' + i[1] + ', ' + i[2] + ')',
                                'CASE WHEN ' + i[0] + ' THEN ' + i[1] + ' ELSE ' + i[2] + ' END')

    pattern_len = r'LENGTH\((.*?)\)'
    matches_len = re.findall(pattern_len, sql)
    for i in matches_len:
        sql = sql.replace('LENGTH(' + i + ')', 'CHAR_LENGTH(CAST(' + i + ' AS VARCHAR))')
    return sql


rule_list = ['AGGREGATE_EXPAND_DISTINCT_AGGREGATES', 'AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN',
                 'AGGREGATE_JOIN_TRANSPOSE_EXTENDED', 'AGGREGATE_PROJECT_MERGE', 'AGGREGATE_ANY_PULL_UP_CONSTANTS',
                 'AGGREGATE_UNION_AGGREGATE', 'AGGREGATE_UNION_TRANSPOSE', 'AGGREGATE_VALUES', 'AGGREGATE_INSTANCE',
                 'AGGREGATE_REMOVE', 'FILTER_AGGREGATE_TRANSPOSE', 'FILTER_CORRELATE', 'FILTER_INTO_JOIN',
                 'JOIN_CONDITION_PUSH', 'FILTER_MERGE', 'FILTER_MULTI_JOIN_MERGE', 'FILTER_PROJECT_TRANSPOSE',
                 'FILTER_SET_OP_TRANSPOSE', 'FILTER_TABLE_FUNCTION_TRANSPOSE', 'FILTER_SCAN',
                 'FILTER_REDUCE_EXPRESSIONS', 'PROJECT_REDUCE_EXPRESSIONS', 'FILTER_INSTANCE', 'JOIN_EXTRACT_FILTER',
                 'JOIN_PROJECT_BOTH_TRANSPOSE', 'JOIN_PROJECT_LEFT_TRANSPOSE', 'JOIN_PROJECT_RIGHT_TRANSPOSE',
                 'JOIN_LEFT_UNION_TRANSPOSE', 'JOIN_RIGHT_UNION_TRANSPOSE', 'SEMI_JOIN_REMOVE',
                 'JOIN_REDUCE_EXPRESSIONS', 'JOIN_LEFT_INSTANCE', 'JOIN_RIGHT_INSTANCE', 'PROJECT_CALC_MERGE',
                 'PROJECT_CORRELATE_TRANSPOSE', 'PROJECT_MERGE', 'PROJECT_MULTI_JOIN_MERGE', 'PROJECT_REMOVE',
                 'PROJECT_TO_CALC', 'PROJECT_SUB_QUERY_TO_CORRELATE', 'PROJECT_REDUCE_EXPRESSIONS',
                 'PROJECT_INSTANCE', 'CALC_MERGE', 'CALC_REMOVE', 'SORT_JOIN_TRANSPOSE', 'SORT_PROJECT_TRANSPOSE',
                 'SORT_UNION_TRANSPOSE', 'SORT_REMOVE_CONSTANT_KEYS', 'SORT_REMOVE', 'SORT_INSTANCE',
                 'SORT_FETCH_ZERO_INSTANCE', 'UNION_MERGE', 'UNION_REMOVE', 'UNION_TO_DISTINCT',
                 'UNION_PULL_UP_CONSTANTS', 'UNION_INSTANCE', 'INTERSECT_INSTANCE', 'MINUS_INSTANCE']

db_id = 'tpcds'
f_list = os.listdir(f'benchmark/queries/tpcds_sf100/')
for f_name in f_list[54: ]:
    with open(f'benchmark/queries/tpcds_sf100/{f_name}') as f:
        logger.info(f"rewriting {f_name}")
        query = f.read()
        query = pre_process(query)
        for rule in rule_list:
            rewrite = call_rewriter(db_id, query, [rule])
            if rewrite == 'NA':
                logger.error('Syntax Error, skipping this one.')
                break
            if rewrite != query:
                logger.info(f'effective rewrite rule: {rule}')
                rewrite = post_process(rewrite)
                check_syntax(rewrite)