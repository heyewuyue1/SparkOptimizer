import re
from utils.config import read_config

card_cfg = read_config()['CARD']

def remove_hash_numbers(text):
    return re.sub(r'#\d+L?', '', text)

def get_all_columns(allColumnPath):
    # 读取 SQL 文件
    with open(allColumnPath, 'r') as file:
        allColumns = file.read().split('\n')
    return allColumns

allColumnPath = card_cfg['COLUMNS']
allColumnList = get_all_columns(allColumnPath)

def add_quotes(text,allowed_list):
    res = add_quotes_to_in_content(text)
    res = add_quotes_to_equal(res,allowed_list)
    res = add_quotes_to_big(res,allowed_list)
    res = add_quotes_to_less(res,allowed_list)
    res = add_quotes_to_StartsWith(res)
    return res

def add_quotes_to_in_content(text):
    # 定位 "In ()" 格式的内容
    pattern = r"IN\s*\((.*?)\)"  # 匹配 In 和括号内的内容
    matches = list(re.finditer(pattern, text))  # 找到所有匹配项并转为列表
    # 如果没有匹配项，直接返回原始文本
    if not matches:
        return text
    modified_text = text
    for match in matches:
        # 提取括号内的内容
        content = match.group(1)
        # 按逗号分割，并为每个部分加上双引号
        quoted_content = ', '.join(f'"{item.strip()}"' for item in content.split(','))
        # 替换原始内容
        modified_text = modified_text.replace(match.group(0), f"IN ({quoted_content})")
    return modified_text

def add_quotes_to_equal(text, allowed_list):
    # 正则表达式匹配 '= ' 后面紧跟的内容，直到遇到 ')'
    pattern = r'=\s*([^()]+?)(?=\))'
    matches = list(re.finditer(pattern, text))  # 找到所有匹配项并转为列表
    # 如果没有匹配项，直接返回原始文本
    if not matches:
        return text
    modified_text = text
    for match in matches:
        # 获取匹配到的内容
        content = match.group(1)
        # 如果内容不在允许的列表中，给内容加上双引号
        if content not in allowed_list:
            newContent = f'= "{content}"'
        else:
            newContent =  f'= {content}'
        # 替换原始内容
        # modified_text = modified_text.replace(match.group(0), f"{newContent}")
        modified_text = re.sub(re.escape(match.group(0)), f"{newContent}", modified_text, count=1)
    return modified_text

def add_quotes_to_big(text, allowed_list):
    # 正则表达式匹配 '= ' 后面紧跟的内容，直到遇到 ')'
    pattern = r'>\s+([^()]+?)(?=\))'
    matches = list(re.finditer(pattern, text))  # 找到所有匹配项并转为列表
    # 如果没有匹配项，直接返回原始文本
    if not matches:
        return text
    modified_text = text
    for match in matches:
        # 获取匹配到的内容
        content = match.group(1)
        # 如果内容不在允许的列表中，给内容加上双引号
        if content not in allowed_list:
            newContent = f'> "{content}"'
        else:
            newContent =  f'> {content}'
        # 替换原始内容
        # modified_text = modified_text.replace(match.group(0), f"{newContent}")
        modified_text = re.sub(re.escape(match.group(0)), f"{newContent}", modified_text, count=1)
    return modified_text

def add_quotes_to_bigequal(text, allowed_list):
    # 正则表达式匹配 '= ' 后面紧跟的内容，直到遇到 ')'
    pattern = r'>=\s*([^()]+?)(?=\))'
    matches = list(re.finditer(pattern, text))  # 找到所有匹配项并转为列表
    # 如果没有匹配项，直接返回原始文本
    if not matches:
        return text
    modified_text = text
    for match in matches:
        # 获取匹配到的内容
        content = match.group(1)
        # 如果内容不在允许的列表中，给内容加上双引号
        if content not in allowed_list:
            newContent = f'>= "{content}"'
        else:
            newContent =  f'>= {content}'
        # 替换原始内容
        modified_text = modified_text.replace(match.group(0), f"{newContent}")
    return modified_text

def add_quotes_to_less(text, allowed_list):
    # 正则表达式匹配 '= ' 后面紧跟的内容，直到遇到 ')'
    pattern = r'<\s+([^()]+?)(?=\))'
    matches = list(re.finditer(pattern, text))  # 找到所有匹配项并转为列表
    # 如果没有匹配项，直接返回原始文本
    if not matches:
        return text
    modified_text = text
    for match in matches:
        # 获取匹配到的内容
        content = match.group(1)
        # 如果内容不在允许的列表中，给内容加上双引号
        if content not in allowed_list:
            newContent = f'< "{content}"'
        else:
            newContent =  f'< {content}'
        # 替换原始内容
        # modified_text = modified_text.replace(match.group(0), f"{newContent}")
        modified_text = re.sub(re.escape(match.group(0)), f"{newContent}", modified_text, count=1)
    return modified_text

def add_quotes_to_lessequal(text, allowed_list):
    # 正则表达式匹配 '= ' 后面紧跟的内容，直到遇到 ')'
    pattern = r'<=\s*([^()]+?)(?=\))'
    matches = list(re.finditer(pattern, text))  # 找到所有匹配项并转为列表
    # 如果没有匹配项，直接返回原始文本
    if not matches:
        return text
    modified_text = text
    for match in matches:
        # 获取匹配到的内容
        content = match.group(1)
        # 如果内容不在允许的列表中，给内容加上双引号
        if content not in allowed_list:
            newContent = f'<= "{content}"'
        else:
            newContent =  f'<= {content}'
        # 替换原始内容
        modified_text = modified_text.replace(match.group(0), f"{newContent}")
    return modified_text

def add_quotes_to_StartsWith(text):
    # 匹配以"StartsWith("开头，到")"之前，中间有且仅有两个部分且用逗号分隔的内容
    pattern = r'StartsWith\(([^,]+),\s*([^)]*)\)'
    
    def replace(match):
        first_part = match.group(1)
        second_part = match.group(2)
        
        # 处理第二部分，加上双引号
        second_part_quoted = f'"{second_part}"'
        
        return f'StartsWith({first_part}, {second_part_quoted})'
    
    # 使用正则表达式进行替换
    result = re.sub(pattern, replace, text)
    
    return result

def replace_ONNOT(text):
    res = text.replace("ONNOT","ON NOT")
    return res

def replace_INSET(text):
    if 'INSET' not in text:
        return text
    beginIndex = text.find("INSET ")
    for ind in range(beginIndex,len(text)):
        if text[ind:ind+3] == 'AND' or text[ind] == ')':
            break

    if text[ind] == ')':
        text = text[:ind] + ')' + text[ind:]
    elif text[ind:ind+3] == 'AND':
        text = text[:ind] + ') ' + text[ind:]
    
    text = text[:beginIndex] + 'IN (' + text[beginIndex+6:]
    return text

def extract_Relation(row):
    relation_start = row.index('Relation ', 0) + len('Relation ')
    relation_end = row.index('[', relation_start)
    relation = row[relation_start:relation_end].strip()
    relation = remove_hash_numbers(relation)
    return relation

def extract_Filter(row):
    filter_start = row.index('Filter ', 0) + len('Filter ')
    filter_ = row[filter_start:].strip()
    filter_ = remove_hash_numbers(filter_)
    return filter_

def extract_Project(row):
    project_start = row.index('Project ', 0) + len('Project [')
    project_end = row.index(']', project_start)
    project = row[project_start:project_end].strip()
    project = remove_hash_numbers(project)
    return project

def extract_JoinInner(row):
    join_start = row.index('Join Inner', 0) + len('Join Inner, ')
    join = row[join_start:].strip()
    join = remove_hash_numbers(join)
    return join

def extract_JoinLeftOuter(row):
    join_start = row.index('Join LeftOuter', 0) + len('Join LeftOuter, ')
    join = row[join_start:].strip()
    join = remove_hash_numbers(join)
    return join

def get_stack(plan):
    rows = plan.split('\n')
    flagStack = []
    contentStack = []
    for row in rows:
        if 'Project' in row:
            project = extract_Project(row)
            flagStack.append('Project')
            contentStack.append(project)
        elif 'Join Inner' in row:
            join = extract_JoinInner(row)
            flagStack.append('Join Inner')
            contentStack.append(join)
        elif 'Join LeftOuter' in row:
            join = extract_JoinLeftOuter(row)
            flagStack.append('Join LeftOuter')
            contentStack.append(join)
        elif 'Filter' in row:
            filter_ = extract_Filter(row)
            flagStack.append('Filter')
            contentStack.append(filter_)
        elif 'Relation' in row:
            relation = extract_Relation(row)
            flagStack.append('Relation')
            contentStack.append(relation)
    return flagStack,contentStack

def generate_sql(plan):
    flagStack,contentStack = get_stack(plan)
    sqlStack = []
    while len(flagStack) > 0:
        flag = flagStack.pop()
        content = contentStack.pop()
        if flag == 'Relation':
            sqlStack.append('FROM ' + content)
        elif flag == 'Filter':
            sqlTmp = sqlStack.pop()
            sqlStack.append(sqlTmp+ '\n' + 'WHERE ' + content)
        elif flag == 'Project':
            sqlTmp = sqlStack.pop()
            sqlStack.append('SELECT ' + content + '\n' + sqlTmp)
        elif flag == 'Join Inner':
            sqlTmp1 = sqlStack.pop()
            sqlTmp2 = sqlStack.pop()
            if sqlTmp1[:4] == 'FROM':
                sqlTmp1 = 'SELECT *\n' + sqlTmp1
            if sqlTmp2[:4] == 'FROM':
                sqlTmp2 = 'SELECT *\n' + sqlTmp2
            sqlStack.append('FROM\n' + '(' + sqlTmp1 + ')' + '\n' + 'JOIN\n' + '(' +  sqlTmp2 + ')' + '\nON' + content)
        elif flag == 'Join LeftOuter':
            sqlTmp1 = sqlStack.pop()
            sqlTmp2 = sqlStack.pop()
            if sqlTmp1[:4] == 'FROM':
                sqlTmp1 = 'SELECT *\n' + sqlTmp1
            if sqlTmp2[:4] == 'FROM':
                sqlTmp2 = 'SELECT *\n' + sqlTmp2
            sqlStack.append('FROM\n' + '(' + sqlTmp1 + ')' + '\n' + 'Left JOIN\n' + '(' +  sqlTmp2 + ')' + '\nON' + content)
    sql = sqlStack[0]
    if sql[:4] == 'FROM':
        sql = 'SELECT COUNT(*)\n' + sql + ';'
    else:
        sql = 'SELECT COUNT(*)\n' + 'FROM\n' + '(' + sql + ');' 
    return sql

# subPlanDir = '/spark-3.4.0/spark/logs/subPlansNonduplicate_sf3000.txt'
subLogicalPlans = []
# with open(subPlanDir, 'r') as file:
#     content = file.read()
#     subLogicalPlans = content.split("\n\n\n")

converSQLs = []
converSQLSet = set()
count = 0
for plan in subLogicalPlans:
    if 'list#' in plan or 'Aggregate' in plan:
        continue
    try:
        converSQLs.append(generate_sql(plan))
        count += 1
    except:
        print(plan)
        print('\n')


for i in range(len(converSQLs)):
    sql = converSQLs[i]
    sql = add_quotes(sql,allColumnList)
    sql = replace_ONNOT(sql)
    sql = replace_INSET(sql)
    converSQLs[i] = sql

# print(len(converSQLs))

# with open('/spark-3.4.0/spark/logs/convertSQLsNonduplicate_sf3000.txt', 'w') as output_file:
#      for sql in converSQLs:
#          output_file.write(sql + '\n\n')
