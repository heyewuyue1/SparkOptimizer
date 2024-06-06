# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Run AutoSteer's training mode to explore alternative query plans"""
from typing import Type
import storage
import os
import sys

import connectors.connector
from connectors.spark_connector import SparkConnector
from utils.arguments_parser import get_parser
from utils.custom_logging import logger
from autosteer.dp_exploration import explore_optimizer_configs
from autosteer.query_span import run_get_query_span
from tqdm import tqdm
from pyhive import hive
import configparser
import json
from utils.config import read_config

config = configparser.ConfigParser()
config.read('config.cfg')
default = config['DEFAULT']

def approx_query_span_and_run(connector: Type[connectors.connector.DBConnector], benchmark: str, query: str):
    run_get_query_span(connector, benchmark, query)
    connector = connector()
    explore_optimizer_configs(connector, f'{benchmark}/{query}')


def analyze(cursor):
    """统计db"""
    cursor.execute('SET spark.sql.cbo.enabled=true;')
    cursor.execute('SET spark.sql.statistics.histogram.enabled=true;') # 开启统计直方图
    cursor.execute('SET spark.sql.statistics.histogram.numBins=50;')  # 设定bins数量
    cursor.execute('show tables;')
    table_result = cursor.fetchall()
    for i in table_result:
        exe='ANALYZE TABLE '+i[1]+' COMPUTE STATISTICS FOR ALL COLUMNS;'
        cursor.execute(exe)

def get_column_stat(cursor,dic_column_stat,col_name,table_name):
    database = default['BENCHMARK']
    cursor.execute(f'USE {database}')
    cursor.execute('DESC FORMATTED ' +table_name+' '+ col_name+' ;' )
    re = cursor.fetchall()
    if col_name not in dic_column_stat:
        dic_column_stat[col_name] = {}
    if re[3][0]=='min' and re[4][0]=='max' and re[9][0]=='histogram':
        dic_column_stat[col_name] = {'table':table_name, 'data_type':re[1][1] ,'min':re[3][1], 'max':re[4][1], 'height_bin':re[9:]}
    return dic_column_stat

def get_dic2(cursor):
    database = default['BENCHMARK']
    cursor.execute(f'USE {database}')
    dic_table_columns={}
    dic_column_stat={}
    cursor.execute('show tables;')
    table_result = cursor.fetchall()
    for i in table_result:
        table_name = i[1]
        exe=('DESC FORMATTED '+ table_name +' ;')
        cursor.execute(exe)
        table_re = cursor.fetchall()
        if table_name not in dic_table_columns:
            dic_table_columns[table_name] = []
        for j in table_re:
            if j[0] != '':
                col_name = j[0]
                dic_table_columns[table_name].append(j[0])
                get_column_stat(cursor,dic_column_stat,col_name,table_name)
            else:
                break
    return dic_table_columns,dic_column_stat

def dic_to_json(database, dic_table_columns,dic_column_stat):
    """把dic存成json文件"""
    file_name = "dic_column_stat.json"
    # 打开文件并写入 JSON 格式的字典内容
    with open(f'data/{database}/{file_name}', "w") as file:
        json.dump(dic_column_stat, file, indent=4)
    file_name = "dic_table_columns.json"
    # 打开文件并写入 JSON 格式的字典内容
    with open(f'data/{database}/{file_name}', "w") as file:
        json.dump(dic_table_columns, file, indent=4)


def check_and_load_database():
    database = default['DATABASE']
    logger.info(f'check and load database {database}...')
    conn = hive.Connection(host=default['THRIFT_SERVER_URL'], port=default['THRIFT_PORT'], username=default['THRIFT_USERNAME'])
    cursor = conn.cursor()
    # cursor.execute(f'DROP DATABASE IF EXISTS {database} CASCADE')
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
    cursor.execute(f'USE {database}')
    with open(f'./benchmark/schemas/{database}.sql', 'r') as f:
        query = f.read()
        query = query.split(';')
        for q in query:
            if q.strip() != '':
                cursor.execute(q)
    logger.info(f'load database {database} successfully')
    # analyse tables
    # if not os.path.exists(f'data/{database}'):
    #     os.makedirs(f'data/{database}')
    #     analyze(cursor)
    #     logger.info(f'analyze database {database} successfully')
    #     # analyzed dic → json
    #     dic_table_columns,dic_column_stat = get_dic2(cursor)
    #     dic_to_json(database, dic_table_columns,dic_column_stat)
    #     logger.info(f'get dic_table_columns and dic_column_stat successfully')
    # else:
    #     logger.info(f'database {database} has been analyzed')


if __name__ == '__main__':
    args = get_parser().parse_args()
    storage.TESTED_DATABASE = default['DATABASE']
    check_and_load_database()
    if default['BENCHMARK'] is None or not os.path.isdir('benchmark/queries/' + default['BENCHMARK']):
        logger.fatal('Cannot access the benchmark directory containing the sql files with path=%s', default['BENCHMARK'])
        sys.exit(1)

    storage.BENCHMARK_ID = storage.register_benchmark(default['BENCHMARK'])
    logger.info('Run training mode')
    f_list = sorted(os.listdir('benchmark/queries/' + default['BENCHMARK']))
    logger.info('Found the following SQL files: %s', f_list)
    for query in tqdm(f_list[:5]):
        logger.info('run Q%s...', query)
        approx_query_span_and_run(SparkConnector, default['BENCHMARK'], query)
    most_frequent_knobs = storage.get_most_disabled_rules()
    logger.info('Training ended. Most frequent disabled rules: %s', most_frequent_knobs)
    bo = storage.get_best_optimizers()
    bo.to_csv(f'data/best_{default["DATABASE"]}.csv', header=True, index=False)
    best_improve = storage.get_best_imporovement()
    logger.info(f'Best improvement: {best_improve}')
