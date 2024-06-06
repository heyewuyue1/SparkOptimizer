import re
import os

def replace_quotes(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        text = file.read()
    
    # 正则表达式匹配并替换
    # 这里用的正则表达式会找到as后跟一个或多个空格，然后是双引号，内部可以有多个单词或空格，结尾是双引号
    updated_text = re.sub(r'as\s+"([\w\s]+)"', lambda m: 'as ' + m.group(1).replace(' ', '_'), text)

    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(updated_text)

# 调用函数
# 请确保将'input.txt'和'output.txt'替换为你实际的文件路径
f_list = os.listdir('benchmark/queries/tpcds_fix/')
for f_name in f_list:
    replace_quotes(f'benchmark/queries/tpcds_fix/{f_name}', f'benchmark/queries/tpcds_fix/{f_name}')
