import os
import sqlparse
import math
from utils.custom_logging import logger
from utils.config import read_config
from utils.util import read_sql_file
from storage import register_predicate_rewrite
default = read_config()['DEFAULT']

def get_sqls(path):
    f_list = sorted(os.listdir(path))
    sql_list = []
    for file in f_list:
        with open(path + file) as f:
            sql = f.read().strip().split(';')
            for i in range(len(sql)):
                if sql[i] != '':
                    sql_list.append(sql[i])
    return sql_list

def find_main_where(tokens):
    current_pos = 0
    for token in tokens:
        if token.value.upper().startswith('WHERE'):
            return current_pos
        current_pos += len(str(token))
    return -1

def find_main_select(tokens):
    current_pos = 0
    for token in tokens:
        if token.value.upper().startswith('SELECT'):
            return current_pos
        current_pos += len(str(token))
    return -1

def find_main_from(tokens):
    parenthesis_level = 0
    current_pos = 0
    for token in tokens:
        if token.ttype in (sqlparse.tokens.Punctuation,) and token.value == '(':
            parenthesis_level += 1
        elif token.ttype in (sqlparse.tokens.Punctuation,) and token.value == ')':
            parenthesis_level -= 1
        elif token.value.upper() == 'FROM' and parenthesis_level == 0:
            return current_pos

        current_pos += len(str(token))
    return -1

def find_main_group_by(tokens):
    parenthesis_level = 0
    current_pos = 0

    for token in tokens:
        # 更新括号层次
        if token.ttype in (sqlparse.tokens.Punctuation,) and token.value == '(':
            parenthesis_level += 1
        elif token.ttype in (sqlparse.tokens.Punctuation,) and token.value == ')':
            parenthesis_level -= 1

        # 查找主查询中的GROUP关键字
        elif token.value.upper() == 'GROUP BY' and parenthesis_level == 0:
            # 提取GROUP BY后面的列
            group_by_columns = []
            # 继续遍历token，直到下一个分号或结束
            for next_token in tokens[tokens.index(token) + 1:]:
                if next_token.value.upper() == 'ROLLUP' or next_token.value == '(' or next_token.value == ')':
                    continue
                if next_token.ttype != sqlparse.tokens.Keyword:
                    group_by_columns.append(next_token.value)
                else:
                    # 返回GROUP BY列的字符串形式
                    return ''.join(group_by_columns)

        current_pos += len(str(token))
    return '*'  # 如果没有找到GROUP BY，则返回*

def find_main_having(tokens):
    parenthesis_level = 0
    current_pos = 0
    for token in tokens:
        if token.ttype in (sqlparse.tokens.Punctuation,) and token.value == '(':
            parenthesis_level += 1
        elif token.ttype in (sqlparse.tokens.Punctuation,) and token.value == ')':
            parenthesis_level -= 1
        elif token.value.upper() == 'HAVING' and parenthesis_level == 0:
            return current_pos

        current_pos += len(str(token))
    return -1

def find_main_order_by(tokens):
    parenthesis_level = 0
    current_pos = 0
    for token in tokens:
        if token.ttype in (sqlparse.tokens.Punctuation,) and token.value == '(':
            parenthesis_level += 1
        elif token.ttype in (sqlparse.tokens.Punctuation,) and token.value == ')':
            parenthesis_level -= 1
        elif token.value.upper() == 'ORDER BY' and parenthesis_level == 0:
            return current_pos

        current_pos += len(str(token))
    return -1

def find_main_limit(tokens):
    parenthesis_level = 0
    current_pos = 0
    for token in tokens:
        if token.ttype in (sqlparse.tokens.Punctuation,) and token.value == '(':
            parenthesis_level += 1
        elif token.ttype in (sqlparse.tokens.Punctuation,) and token.value == ')':
            parenthesis_level -= 1
        elif token.value.upper() == 'LIMIT' and parenthesis_level == 0:
            return current_pos
        current_pos += len(str(token))
    return -1

def find_selected_cols(sql, sel_loc, from_loc):
    raw_list = sql[sel_loc+6:from_loc].split(',')
    for i in range(len(raw_list)):
        raw_list[i] = raw_list[i].strip()
    return raw_list

def agg_in_cols(col_list):
    for col in col_list:
        if '(' in col and ')' in col:
            return True
    return False

def modify(sql):
    sql = sqlparse.format(sql, strip_comments=True)
    parsed = sqlparse.parse(sql)
    for statement in parsed:
        sel_loc = find_main_select(statement.tokens)
        from_loc = find_main_from(statement.tokens)
        selected_cols = find_selected_cols(sql, sel_loc, from_loc)
        by_str = find_main_group_by(statement.tokens)
        if sel_loc == -1 or from_loc == -1:
            logger.warning(f'Main select or from not found in query: {sql}, sel_loc: {sel_loc}, from_loc: {from_loc}')
        by_str = by_str.strip()
        if (agg_in_cols(selected_cols) and by_str == '*') or by_str == '':
            return sql
        if 'rollup' in by_str:
            by_str = by_str.strip().split('rollup')[1][1:-1]
        modified_sql = sql[:sel_loc+6] + ' ' + by_str + ', ' + sql[sel_loc + 6:]
    return modified_sql
    
def get_min_max_str_of_ith_col(result, i): 
    min = float('inf')
    max = float('-inf')
    set_i = set()
    for tuple in result:
        try:
            if type(tuple[i]) != str:
                if float(tuple[i]) < min:
                    min = float(tuple[i])
                if float(tuple[i]) > max:
                    max = float(tuple[i])

            if type(tuple[i]) == str:
                set_i.add(tuple[i])
        except Exception as e:
            return None
    return min, max, set_i

def generate_min_max_from_result(result, conn): 
    if result == 'EmptyResult':
        return {}
    cols = [desc[0] for desc in conn.cursor.description]
    min_max_dict = {}
    for i in range(len(cols)):
        if not '(' in cols[i] and not ')' in cols[i]: 
            min_max_str = get_min_max_str_of_ith_col(result, i)
            if min_max_str is not None and ((min_max_str[0] != float('inf') and min_max_str[1] != float('-inf')) or len(min_max_str[2])>0):
                min_max_dict[cols[i]] = min_max_str
    return min_max_dict

def add_predicate(sql, min_max_dict): 
    statement = sqlparse.parse(sql)[0]
    predicate_list = []
    having_list = []
    sel_loc = find_main_select(statement.tokens)
    from_loc = find_main_from(statement.tokens)
    selected_cols = find_selected_cols(sql, sel_loc, from_loc)
    agg_cols = []
    for col in selected_cols:
        if '(' in col and ')' in col:
            agg_cols.append(col.split()[-1])
    for col in min_max_dict:
        min_val, max_val, str_set = min_max_dict[col]
        if min_val != float('inf') or max_val != float('-inf'):
            if col in agg_cols:
                having_list.append(f'{col} >= {math.floor(min_val)} and {col} <= {math.ceil(max_val)}')
            else:
                predicate_list.append(f'{col} >= {math.floor(min_val)} and {col} <= {math.ceil(max_val)}')
        if len(str_set) > 1: # null
            str_set = tuple(str_set)
            if col in agg_cols:
                having_list.append(f'{col} in {str_set}')
            else:
                predicate_list.append(f'{col} in {str_set}')      
    if len(predicate_list) > 0:
        pred_str = ' and '.join(predicate_list) + ' and '
    else:
        pred_str = ''
    if len(having_list) > 0:
        having_str = ' and '.join(having_list) + ' and '
    else:
        having_str = ''
    sql = sqlparse.format(sql, strip_comments=True)
    parsed = sqlparse.parse(sql)
    for statement in parsed:
        
        if having_str != '':
            having_position = find_main_having(statement.tokens)
            if having_position != -1:
                sql = sql[:having_position + 6] + ' ' + having_str + sql[having_position + 6:]
            else:
                having_str = 'HAVING ' + having_str[:-5]
                group_by_position = find_main_group_by(statement.tokens)
                if group_by_position != '*':
                    order_by_position = find_main_order_by(statement.tokens)
                    if order_by_position != -1:
                        sql = sql[:order_by_position] + ' ' + having_str + ' ' + sql[order_by_position:]
                    else:
                        limit_position = find_main_limit(statement.tokens)
                        sql = sql[:limit_position] + ' ' + having_str + sql[limit_position:]
        if pred_str != '':
            where_position = find_main_where(statement.tokens)
            if where_position != -1:
                sql = sql[:where_position+5] + ' ' + pred_str + ' ' + sql[where_position+5:]
    return sql

def rewrite_and_test_syntax(connector, query):
        conn = connector()
        sql = read_sql_file(f'benchmark/queries/{default["BENCHMARK"]}/{query}')
        modified_sql = modify(sql)
        logger.debug(f'Modified into: {modified_sql}')
        try:
            res = conn.execute(modified_sql).result
        except:
            logger.error(f'Modified version of {query} failed, Skipping this one...')
            return

        min_max_dict = generate_min_max_from_result(res, conn)
        

        if min_max_dict == {}:
            logger.info(f'No available predicate for {query}, skipping this one...')
            return
        else:
            logger.info(f'Successfully generate predicates for {query}')
            pred_sql = add_predicate(sql, min_max_dict)   
        logger.debug(f'Rewrite into: {pred_sql}')
        
        try:
            conn.execute(pred_sql)
            register_predicate_rewrite(sql, pred_sql)
            logger.info(f'Serialized a new predicate rewrite for {query}')
        except:
            logger.info(f'Rewrite version of {query} did not pass syntax check, skipping this one...')
        
