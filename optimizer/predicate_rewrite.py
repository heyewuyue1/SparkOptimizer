import os
import sqlparse
import sys
import math
sys.path.append('/Users/a/Documents/SparkOptimizer/')
from utils.custom_logging import logger
from utils.config import read_config
from utils.util import read_sql_file
from storage import register_predicate_rewrite
from connectors.spark_connector_hive import SparkConnector
default = read_config()['DEFAULT']
rewrite = read_config()['REWRITE']

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
    parenthesis_level = 0
    current_pos = 0
    for token in tokens:
        if token.ttype in (sqlparse.tokens.Punctuation,) and token.value == '(':
            parenthesis_level += 1
        elif token.ttype in (sqlparse.tokens.Punctuation,) and token.value == ')':
            parenthesis_level -= 1
        elif token.value.upper().startswith('WHERE') and parenthesis_level == 0:
            return current_pos

        current_pos += len(str(token))
    return -1

def find_main_select(tokens):
    parenthesis_level = 0
    current_pos = 0
    for token in tokens:
        if token.ttype in (sqlparse.tokens.Punctuation,) and token.value == '(':
            parenthesis_level += 1
        elif token.ttype in (sqlparse.tokens.Punctuation,) and token.value == ')':
            parenthesis_level -= 1
        elif token.value.upper() == 'SELECT' and parenthesis_level == 0:
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

def make_alias_dict(selected_cols):
    alias_dict = {}
    for col in selected_cols:
        if ' as ' in col:
            alias_dict[col.split(' as ')[1].strip()] = col.split(' as ')[0].strip()
        elif ' AS ' in col:
            alias_dict[col.split(' AS ')[1].strip()] = col.split(' AS ')[0].strip()
        elif len(col.split()) == 2:
            alias_dict[col.split()[1]] = col.split()[0]
    return alias_dict

def find_numerical_col(sql, conn):
    try:
        result = conn.execute(f'DESC QUERY {sql}', want_result=True).result
    except:
        return []
    col = []
    for row in result:
        if row[1] == 'bigint' or row[1].startswith('decimal') or row[1] == 'double':
            col.append(row[0])
    
    return col

def agg_in_cols(col_list):
    for col in col_list:
        if '(' in col and ')' in col:
            return True
    return False

def modify(sql, conn):
    sql = sqlparse.format(sql, strip_comments=True)
    statement = sqlparse.parse(sql)[0]
    sel_loc = find_main_select(statement.tokens)
    from_loc = find_main_from(statement.tokens)
    selected_cols = find_selected_cols(sql, sel_loc, from_loc)
    alias_dict = make_alias_dict(selected_cols)
    num_col = find_numerical_col(sql, conn)
    num_col_without_agg = []
    for i in range(len(num_col)):
        if num_col[i] in alias_dict:
            if not '(' in alias_dict[num_col[i]]:
                num_col_without_agg.append(num_col[i])
        else:
            if not '(' in num_col[i]:
                num_col_without_agg.append(num_col[i])
    if len(num_col_without_agg) == 0:
        return sql, alias_dict
    min_max_list = []
    for col in num_col_without_agg:
        min_max_list.append(f'MIN({col}) as min_{col}, MAX({col}) as max_{col}')
    min_max_str = ',\n'.join(min_max_list)
    modified_sql = f'SELECT \n{min_max_str} \nFROM ({sql})'
    return modified_sql, alias_dict

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

def generate_min_max_from_result(result, alias_dict, conn): 
    cols = [desc[0] for desc in conn.cursor.description]
    min_max_dict = {}
    for i in range(len(cols)):
        col_name = cols[i][4:]
        if col_name in alias_dict:
            col_name = alias_dict[col_name]
        if col_name not in min_max_dict:
            min_max_dict[col_name] = [float('inf'), float('-inf'), set()]
        if cols[i].startswith('min_'):
            min_max_dict[col_name][0] = result[0][i]
        if cols[i].startswith('max_'):
            min_max_dict[col_name][1] = result[0][i]
    return min_max_dict

def tranverse_generate_min_max_from_result(structure, conn):
    def tranverse(cur_structure):
        try:
            result = conn.execute(cur_structure['query']).result
        except:
            cur_structure['min_max'] = {}
            logger.error(f'Modified version of {query} failed, Skipping this one...')
            return
        min_max_dict = generate_min_max_from_result(result, conn)
        cur_structure['min_max'] = min_max_dict
        for subquery in cur_structure['subqueries']:
            tranverse(subquery)
    tranverse(structure)

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
                predicate_list.append(f'{col} between {math.floor(min_val)} and {math.ceil(max_val)}')
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

def tranverse_add_predicate(structure):
    def tranverse(cur_structure):
        modified_sql = add_predicate(cur_structure['query'], cur_structure['min_max'])
        cur_structure['rewrite'] = modified_sql
        for subquery in cur_structure['subqueries']:
            tranverse(subquery)
    tranverse(structure)

def parse_sub_query(sql):
    def find_subqueries(query, start_id):
        subqueries = []
        parsed = sqlparse.parse(query)[0]
        if 'with' in query.lower():
            sel_loc = find_main_select(parsed.tokens)
            begin = query.find('(')
            p_level = 1
            j = begin
            for i in range(begin + 1, sel_loc):
                if query[i] == '(':
                    p_level += 1
                    if p_level == 1:
                        j = i
                elif query[i] == ')':
                    p_level -= 1
                    if p_level == 0:
                        subqueries.append({
                            'id': start_id,
                            'begin': j+1,
                            'query': query[j+1:i],
                            'subqueries': find_subqueries(query[j+1:i], start_id + 1),
                        })
            parsed = sqlparse.parse(query[sel_loc:])[0]
        # 使用sqlparse找出所有完整括号包裹的表达式，排除括号中不包含select的情况
        for token in parsed.tokens:
            if token.is_group:
                subquery = token.value
                if 'select' in subquery.lower():
                    #找出subquery中第一次(和最后一次)出现的位置，以此为依据递归查找subquery
                    first_paren = subquery.find('(')
                    last_paren = subquery.rfind(')')
                    if first_paren != -1 and last_paren != -1:
                        subquery = subquery[first_paren+1:last_paren]
                    subqueries.append({
                        'id': start_id,
                        'begin': query.find(subquery),
                        'query': subquery,
                        'subqueries': find_subqueries(subquery, start_id + 1),
                    })
        return subqueries
    return {
            'id': 0,
            'begin': 0,
            'query': sql,
            'subqueries': find_subqueries(sql, 1),
        }

def get_leaf_list(structure):
    leaf_list = []
    def tranverse(cur_structure):
        if cur_structure['subqueries'] == []:
            leaf_list.append({
                'query': cur_structure['query'],
                'modified': '',
                'min_max': {},
                'rewrite': '',
            })
        for subquery in cur_structure['subqueries']:
            tranverse(subquery)
    tranverse(structure)
    return leaf_list

def rewrite_and_test_syntax(connector, query):
        conn = connector()
        sql = read_sql_file(f'benchmark/queries/{default["BENCHMARK"]}/{query}')
        structure = parse_sub_query(sql)
        if rewrite['PRED_LEVEL'] == 'bottom':
            # 只对叶子节点进行改写试试
            leaf_list = get_leaf_list(structure)
            for i in range(len(leaf_list)):
                modified_sql, alias_dict = modify(leaf_list[i]['query'], conn)  # 没modify出*
                if modified_sql == leaf_list[i]['query']:
                    logger.info(f'No available min_max_info for {query}\'s {i}th subquery, skipping this one...')
                    break
                try:
                    res = conn.execute(modified_sql, want_result=True).result
                    leaf_list[i]['modified'] = modified_sql
                except:
                    logger.error(f'Modified version of {query}\'s {i}th subquery failed, Skipping this one...')
                    break
                min_max_dict = generate_min_max_from_result(res, alias_dict, conn)
                if min_max_dict == {}:
                    logger.info(f'No available min_max_info for {query}\'s {i}th subquery, skipping this one...')
                    return
                else:
                    logger.info(f'Successfully generate min_max_dict for {query}\'s {i}th subquery')
                    leaf_list[i]['min_max'] = min_max_dict
                    pred_sql = add_predicate(leaf_list[i]['query'], min_max_dict)
                    logger.debug(f'Rewrite {query}\'s {i}th subquery into: {pred_sql}')
                    try:
                        conn.execute(f'EXPLAIN FORMATTED {pred_sql}')  # 只能用EXPLAIN FORMATTED来检查语法
                        leaf_list[i]['rewrite'] = pred_sql
                        logger.info(f'Serialized a new predicate rewrite for {query}\'s {i}th subquery')
                    except:
                        logger.info(f'Rewrite version of {query}\'s {i}th subquery did not pass syntax check, skipping this one...')
            org_sql = sql
            for i in range(len(leaf_list)):
                if leaf_list[i]['rewrite'] != '':
                    sql = sql.replace(leaf_list[i]['query'], leaf_list[i]['rewrite'])
            logger.debug(f'Rewrite {query} into: {sql}')
        elif rewrite['PRED_LEVEL'] == 'top':
            modified_sql, alias_dict = modify(sql, conn)
            if modified_sql == sql:
                logger.info(f'No available min_max_info for {query}, skipping this one...')
                return
            try:
                res = conn.execute(modified_sql, want_result=True).result
            except:
                logger.error(f'Modified version of {query} failed, Skipping this one...')
                return
            min_max_dict = generate_min_max_from_result(res, alias_dict, conn)
            if min_max_dict == {}:
                logger.info(f'No available min_max_info for {query}, skipping this one...')
                return
            else:
                logger.info(f'Successfully generate min_max_dict for {query}')
            pred_sql = add_predicate(sql, min_max_dict)
            logger.debug(f'Rewrite {query} into: {pred_sql}')
            try:
                conn.execute(pred_sql)  # 完整SQL可以直接execute检查语法
                logger.info(f'Serialized a new predicate rewrite for {query}')
            except:
                logger.info(f'Rewrite version of {query} did not pass syntax check, skipping this one...')
            org_sql = sql
            sql = pred_sql
        
        try:
            conn.execute(sql)
            register_predicate_rewrite(org_sql, sql)
            logger.info(f'Serialized a new predicate rewrite for {query}')
        except:
            logger.info(f'Rewrite version of {query} did not pass syntax check, skipping this one...')
        
if __name__ == '__main__':
    query = 'query78.sql'
    rewrite_and_test_syntax(SparkConnector, query)