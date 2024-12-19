import json
import subprocess
import re
from utils.custom_logging import logger

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
    sql = sql.replace('30_days', '"30_days"')
    sql = sql.replace('31_60_days', '"31_60_days"')
    sql = sql.replace('61_90_days', '"61_90_days"')
    sql = sql.replace('91_120_days', '"91_120_days"')
    sql = sql.replace('30days', '"30days"')
    sql = sql.replace('31_60days', '"31_60days"')
    sql = sql.replace('61_90days', '"61_90days"')
    sql = sql.replace('91_120days', '"91_120days"')
    sql = sql.replace("' calcite_year ", "' year ")

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

def post_process(query):
    query = query.replace('SUBSTRING', 'SUBSTR')
    query = query.replace('DECIMAL(19, 0)', 'DECIMAL(17, 2)')
    query = query.replace('$', 'dollar')
    # 正则表达式匹配 "FETCH NEXT xxx ROWS ONLY"
    fetch_pattern = re.compile(r"FETCH\s+NEXT\s+(\d+)\s+ROWS\s+ONLY", re.IGNORECASE)
    
    # 使用替换模式替换 "FETCH NEXT xxx ROWS ONLY" 为 "LIMIT xxx"
    query = fetch_pattern.sub(r"LIMIT \1", query)

    # query = re.sub(r'"\$(\w+)"', r'dollar\1', query)
    query = re.sub(r'"(\w+days)"', r'\1', query)
    
    return query

def call_rewriter(args: tuple):
    db_id, sql_input, hintset = args
    # Provide a list of strings as input
    input_list = [db_id, pre_process(sql_input), hintset.get_all_knobs()]
    # Convert the input list to a JSON string
    input_string = json.dumps(input_list)
    command = 'java -cp rewriter_java.jar src/rule_rewriter.java'

    process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    # Wait for the subprocess to finish and capture the output
    output, error = process.communicate(input=input_string)

    # rew = output.replace("\u001B[32m", '').replace("\u001B[0m", '')
    
    output = output.replace("\u001B[32m", '').replace("\u001B[0m", '').split('\n')
    ind = 0
    
    if 'No changed!' in output:
        hintset.plan = sql_input
        return hintset
    for i in output:
        if not i.startswith('SELECT') and not i.startswith('select') and not i.startswith('with ') and not i.startswith('WITH '):
            continue
        else:
            ind = output.index(i)
            break
    
    queries = output[ind:-3]
    output = ' '.join(queries).replace('"', '')
    if 'select' in output or 'SELECT' in output or 'Select' in output:
        # change the functions edited to fit calcite back to original ones
        hintset.plan = post_process(output)
        return hintset
    else:
        logger.error(f'Some error occurred during rewriting')
        logger.debug(f'Input: {sql_input}')
        logger.debug(f"Output: {output}")
        logger.debug(f"Error: {error}")
        hintset.plan = 'NA'
        return hintset

def rewrite(args: tuple):
    db_id, sql_input, hintset = args
    # Provide a list of strings as input
    input_list = [db_id, pre_process(sql_input), hintset]
    # Convert the input list to a JSON string
    input_string = json.dumps(input_list)
    command = 'java -cp rewriter_java.jar src/rule_rewriter.java'

    process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    # Wait for the subprocess to finish and capture the output
    output, error = process.communicate(input=input_string)

    output = output.replace("\u001B[32m", '').replace("\u001B[0m", '').split('\n')
    ind = 0

    if 'No changed!' in output:
        return sql_input
    for i in output:
        if not i.startswith('SELECT') and not i.startswith('select') and not i.startswith('with ') and not i.startswith('WITH '):
            pass
        else:
            ind = output.index(i)
            break
    
    # logger.debug(f'raw output: {rew}')
    
    queries = output[ind:-3]
    # print(' '.join(queries))
    output = ' '.join(queries).replace('"', '')
    if 'select' in output or 'SELECT' in output or 'Select' in output:
        # change the functions edited to fit calcite back to original ones
        return post_process(output)
    else:
        logger.error(f'Some error occurred during rewriting')
        logger.debug(f'Input: {sql_input}')
        logger.debug(f"Output: {output}")
        logger.debug(f"Error: {error}")
        return 'NA'