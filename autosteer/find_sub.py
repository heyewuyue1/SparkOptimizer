import os
import sqlparse
from utils.config import read_config
from utils.custom_logging import setup_custom_logger
from storage import register_mv_rewrite
logger = setup_custom_logger('VIEW')
default = read_config()['DEFAULT']

after_from = False
after_with = False
as_count = 1

def as_count_function(sql_query):
    as_count_temp = 0
    end_index = 0
    lower_sql_query = sql_query.lower()
    as_index = lower_sql_query.find(' as')
    while as_index != -1:
        start_index = as_index+2
        parentheses = 0
        for i in range(start_index, len(lower_sql_query)):
            char = lower_sql_query[i]
            if char == '(':
                parentheses += 1
            elif char == ')':
                parentheses -= 1
                if parentheses == 0:  # 如果栈为空，表示找到配对的右括号
                    as_count_temp += 1
                    end_index = i
                    break
        as_index = lower_sql_query.find(' as',end_index+1)
    return as_count_temp

def split_as(sql_query):
    ##由于with as后面可能会有多个并列的as，只保留as后面第一个大括号的内容
    lower_sql_query = sql_query.lower()
    global as_count
    query_split = []
    # 开始查找 'as' 关键字的位置
    # 循环查找所有的 'as'
    as_index = lower_sql_query.find(' as')
    for i in range(0, as_count):
            # 从 'as' 向后查找，找到第一个左括号
            # 索引从as后开始
            index = as_index + 2
            parentheses = 0
            start_index = 0
            end_index = 0
            while index < len(lower_sql_query) :
                if lower_sql_query[index] == '(':
                    parentheses += 1
                    if parentheses == 1:
                        start_index = index
                elif lower_sql_query[index] == ')':
                    parentheses -= 1
                    if parentheses == 0:
                        end_index = index
                        break
                index += 1
            query_split_alone = lower_sql_query[start_index+1:end_index].strip()
            query_split.append(query_split_alone)
            as_index = lower_sql_query.find(' as', end_index + 1)

    return query_split

def get_with_alias(sql_query):
    #找到with 和 as之间的视图名字
    # 将 SQL 语句转换为小写，便于搜索
    lower_sql_query = sql_query.lower()
    global as_count
    as_count_temp = as_count-1
    # 初始化结果列表
    names = []
    # 开始查找 'as' 关键字的位置,因为能用这个函数必定有with，必定有一个as，先找到第一个with
    with_index = lower_sql_query.find('with')
    # 循环查找所有的 'as',只能是with带着的as，不是把变量做别称的as
    # 从 'as' 前向后查找，找到前一个字符串（视图名称）
    start_index = with_index + 5 #由于有空格，加5
    end_index = start_index
    while end_index < len(lower_sql_query) and lower_sql_query[end_index] not in [' ', ',']:
        end_index += 1

    # 提取 'as' 前的字符串并去掉空格
    name = sql_query[start_index:end_index].strip()

    # 将提取的视图名称添加到列表中,防止有两个字符串有共同前后缀，要加一个空格
    #只找到第一个as前的表名
    names.append(name)
    end_index += 3 #越过第一个as
    parentheses = 0
    for i in range(end_index, len(lower_sql_query)-2):
        if(lower_sql_query[i] == '('):
            parentheses += 1
        elif(lower_sql_query[i] == ')'):
            parentheses -= 1
        elif parentheses == 0:
            if lower_sql_query[i] == ' 'and lower_sql_query[i+1] == 'a' and lower_sql_query[i+2] == 's':
                end_index = i
                start_index = i-1
                while start_index >= 0 and lower_sql_query[start_index] not in [' ', ',']:
                    start_index -= 1
                name = sql_query[start_index + 1:end_index].strip()
                names.append(name)
                as_count_temp -= 1
                if as_count_temp == 0:
                    break
    return names

def remove_with_clause(sql_query):
    # 找到 "with" 的位置并删除with后面紧跟着的子句
    global as_count
    as_count_temp = as_count

    with_index = sql_query.lower().find("with")

    if with_index == -1:
        return sql_query.strip()  # 如果没有找到，返回原始字符串并去除多余空格

    # 初始化栈和索引
    stack = []
    start_index = with_index  # 从 "WITH" 开始
    end_index = None

    # 遍历字符串，查找配对的括号
    for i in range(with_index, len(sql_query)):
        char = sql_query[i]
        if char == '(':
            stack.append(char)  # 遇到左括号，压入栈
        elif char == ')':
            if stack:
                stack.pop()  # 遇到右括号，弹出栈
            if not stack:  # 如果栈为空，表示找到配对的右括号
                as_count_temp -= 1
                if as_count_temp == 0:
                    end_index = i
                    break

    # 如果找到了配对的右括号，则返回 "WITH" 之前的部分
    if end_index is not None:
        return sql_query[:with_index].strip() + sql_query[end_index + 1:].strip()  # 去掉 WITH 和括号之间的内容
    return sql_query.strip()  # 如果没有找到配对的右括号，返回原始字符串并去除多余空格

def remove_outer_parentheses_after_from(sql_query):
    # 找到 "FROM" 关键字之后的最外层括号并删除
    from_index = sql_query.lower().find("from")
    if sql_query[from_index+5] == "(":
        open_parentheses = 1
        close_parentheses = 0
        # 从左括号右边一个开始遍历
        for i in range(from_index + 6, len(sql_query)):
            if sql_query[i] == '(':
                open_parentheses += 1
            elif sql_query[i] == ')':
                close_parentheses += 1
            if open_parentheses == close_parentheses:
            #删除最外层的括号
                return sql_query[:from_index + 5] + sql_query[from_index + 6:i] + sql_query[i + 1:]

    return sql_query  # 如果没有括号，返回原字符串

def find_subquery(tokens):
    subquery = ""
    global after_from
    global after_with
    global as_count
    from_flag = False
    with_flag = False
    for token in tokens:
        #print(f"Type: {token.ttype}, Value: {token.value}")
        # 当找到 FROM/with 关键字时开始记录子查询
        if token.value == 'from' and not after_from:
            after_from = True
            continue
        if token.value == 'with' and not after_with:
            after_with = True
            continue
        # 只需要一个from子句即可
        if from_flag:
            break
        if with_flag:
            break
        if after_from:
            # 跟踪括号层级，构建子查询
            if token.ttype is None:
                # from子句 token 是 None 类型，表示它可能是一个完整的子查询
                subquery_str = str(token.value)
                parentheses = 0
                for sub in subquery_str:
                    if sub == '(':
                        parentheses += 1
                        if parentheses > 1:
                            subquery += sub
                    elif sub == ')':
                        parentheses -= 1
                        if parentheses == 0:
                            from_flag = True
                            break
                        if parentheses >= 1:
                            subquery += sub
                    elif parentheses >= 1:
                            subquery += sub
                if from_flag:
                    continue
                else:
                    after_from = False
        if after_with:
            if token.ttype is None:
                # with子句后面 全是 None 类型,可能并列了多个as
                subquery_str = str(token.value)
                as_count = as_count_function(subquery_str) #全局as变量
                as_count_temp = as_count    #临时用，用来循环
                parentheses = 0
                for sub in subquery_str:
                    if sub == '(':
                        parentheses += 1
                        if parentheses > 1:
                            subquery += sub
                    elif sub == ')':
                        parentheses -= 1
                        if parentheses == 0:
                            as_count_temp -= 1
                            if as_count_temp == 0:
                                with_flag = True
                                break
                            else:
                                subquery += "\n"
                                continue
                        if parentheses >= 1:
                            subquery += sub
                    elif parentheses >= 1:
                            subquery += sub

    # 处理返回的 subquery，去除最外层的括号
    return subquery.strip() if after_from or after_with else None

def create_table_from_subquery(subquery, table_name):
    return f"CREATE TABLE {table_name} AS\n{subquery};"

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def generate_rewrite_mv(query):
    global after_from
    global after_with
    global as_count
    after_from = False
    after_with = False
    as_count = 1
    sql_file_path = f'benchmark/queries/{default["benchmark"]}/{query}'
    sql_query = read_sql_file(sql_file_path)
    # 解析 SQL 查询
    parsed = sqlparse.parse(sql_query)
    tokens = [token for stmt in parsed for token in stmt.tokens]
    
    # 寻找 FROM/with 后的子查询部分
    subquery = find_subquery(tokens)
    #print(subquery)
    if subquery:
        # 获取文件名，并生成表名
        file_name = os.path.basename(sql_file_path)
        base_name = os.path.splitext(file_name)[0]

        #with用的表结构
        table_names = [f"{base_name}_{i}" for i in range(1, as_count + 1)]
        #from用的表结构
        table_name = f"{base_name}_1"
        # 生成 CREATE TABLE 语句
        if after_with:
            names = get_with_alias(sql_query)
            #print(names)
            ##先修改创建表的代码，防止这些表本身创建的时候就彼此有依赖关系
            for i in range(0, as_count):
                sql_query = sql_query.replace(f" {names[i]}", f" {table_names[i]}")
                sql_query = sql_query.replace(f",{names[i]}", f",{table_names[i]}")
            #再划分不同的表

            query_split = split_as(sql_query)

            create_table_query = ""
            for i in range(0, as_count):
                #print("这是一个"+query_split[i])
                create_table_query += create_table_from_subquery(query_split[i], table_names[i])
                create_table_query += "\n"
        if after_from:
            # 生成 CREATE TABLE 语句
            create_table_query = create_table_from_subquery(subquery, table_name)

            # # 保存 CREATE TABLE 语句到指定文件夹
            # query_subq_output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)),
            #                                         '../benchmark/queries/query_subq')
            # os.makedirs(query_subq_output_directory, exist_ok=True)

        # # 保存 CREATE TABLE 语句到指定文件夹
        # query_subq_output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)),
        #                                         '../benchmark/queries/query_subq')
        # os.makedirs(query_subq_output_directory, exist_ok=True)

        # # 保存为 {原文件名}_subq.sql
        # create_table_file_name = f"{base_name}_subq.sql"
        # create_table_file_path = os.path.join(query_subq_output_directory, create_table_file_name)

        # with open(create_table_file_path, 'w') as f:
        #     f.write(create_table_query)

        # print(f"CREATE TABLE 语句已保存到: {create_table_file_path}")

        if after_from:
            # from的方法：替换原 SQL 查询中的子查询为表名
            new_sql_query1 = sql_query.replace(subquery, table_name)

            #删除from后面的最外层括号
            new_sql_query = remove_outer_parentheses_after_from(new_sql_query1)
        if after_with:
            # with 的方法
            #删除掉with后面紧跟着的子句
            new_sql_query = remove_with_clause(sql_query)
            #用表名替换掉视图名
            #for i in range(1, as_count + 1):
                #new_sql_query1 = new_sql_query1.replace(names[i-1], table_names[i-1])
            #new_sql_query = new_sql_query1
        # new_sql_file_name = f"{base_name}_new.sql"

        # 保存新的 SQL 查询语句到指定文件夹
        # new_query_output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)),
        #                                         '../benchmark/queries/new_query')
        # os.makedirs(new_query_output_directory, exist_ok=True)

        # new_sql_file_path = os.path.join(new_query_output_directory, new_sql_file_name)
        # with open(new_sql_file_path, 'w') as f:
        #     f.write(new_sql_query)
        register_mv_rewrite(sql_query, create_table_query, new_sql_query)

        logger.info(f'Serialized a new material view for {query}')
    else:
        logger.info(f'Subquery not found')
    # except Exception as e:
    #     logger.warning(f'Something went wrong when rewriting {query}, {e}')
