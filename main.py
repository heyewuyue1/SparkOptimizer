# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Run AutoSteer's training mode to explore alternative query plans"""
from typing import Type
import storage
import os
import sys
import connectors
from utils.arguments_parser import get_parser
from utils.custom_logging import logger
from utils.config import read_config
from autosteer.dp_exploration import explore_optimizer_configs, explore_rewrite_configs
from autosteer.query_span import run_get_query_span
from autosteer.rewrite_span import run_get_rewrite_span
from autosteer.lero import fill_real_card
from autosteer.find_sub import generate_rewrite_mv
from autosteer.predicate_rewrite import rewrite_and_test_syntax
from tqdm import tqdm
import pandas as pd
import paramiko

config = read_config()
default = config['DEFAULT']
rewrite = config['REWRITE']
hint = config['HINT']
connection = config['CONNECTION']
if connection['CONNECTOR'] == 'hive':
    from connectors.spark_connector_hive import SparkConnector
if connection['CONNECTOR'] == 'ssh':
    from connectors.spark_connector_ssh import SparkConnector

def approx_query_span_and_run(connector: Type[connectors.connector.DBConnector], query_path: str,
                              sql, rewrite_method: str):
    run_get_query_span(connector,  query_path, sql, rewrite_method)
    connector = connector()
    explore_optimizer_configs(connector, query_path, sql, rewrite_method)


def approx_rewrite_span_and_run(connector: Type[connectors.connector.DBConnector], benchmark: str, query: str):
    run_get_rewrite_span(connector, benchmark, query)
    connector = connector()
    explore_rewrite_configs(connector, f'{benchmark}/{query}')


def check_and_load_database():
    hostname = '192.168.90.173'
    port = 22
    username = 'root'
    password = 'root'
    # 创建SSH客户端
    client = paramiko.SSHClient()
    # 自动添加主机密钥
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # 连接远程主机
    client.connect(hostname, port, username, password)

    database = default['DATABASE']
    logger.info(f'check and load database {database}...')
    stdin, stdout, stderr = client.exec_command(f'spark-sql -e "use {database}";')

    logger.info(f'load database {database} successfully')

if __name__ == '__main__':
    args = get_parser().parse_args()
    storage.TESTED_DATABASE = default['STORAGE']
    # check_and_load_database()
    if default['BENCHMARK'] is None or not os.path.isdir('benchmark/queries/' + default['BENCHMARK']):
        logger.fatal('Cannot access the benchmark directory containing the sql files with path=%s',
                     default['BENCHMARK'])
        sys.exit(1)
    # storage.BENCHMARK_ID = storage.register_benchmark(default['BENCHMARK'])
    f_list = sorted(os.listdir('benchmark/queries/' + default['BENCHMARK']))  # 测试全部sql
    logger.info('Found the following SQL files: %s', f_list)

    ### test
    if args.test:
        logger.info('Running testing mode.')
        conn = SparkConnector()
        time_default = 0
        time_optimized = 0
        best_df = pd.read_csv(test['OPTIMIZER'])
        best_df = best_df.fillna('None')
        default_err = []
        optimized_err = []

        ### original
        q, exc_rules = conn.set_disabled_knobs('', [])
        conf_cbo = conn.turn_off_cbo()
        for i in range(eval(test['REPEATS'])):

            logger.info(f'Running default for {i + 1} time...')
            time_sum = 0
            for query in tqdm(f_list):
                if '35' in query:
                    logger.info(f"{query},pass")
                    continue
                logger.debug('run Q%s without optimization...', query)
                sql = storage.read_sql_file(f'benchmark/queries/{test["BENCHMARK"]}/{query}')
                try:
                    result = conn.execute(sql, conf_cbo, exc_rules)
                except:
                    logger.warning(f'{query} default execution failed.')
                    default_err.append(query)
                logger.info(f'query:{query}; time: {result.time_usecs}')  ###
                time_sum += result.time_usecs

            logger.info(f'Total time(default) for {i + 1}th running: {time_sum}')
            conn.clear_cache()
            time_default += time_sum

        ### optimized
        conf_oncbo = conn.turn_on_cbo()
        for i in range(eval(test['REPEATS'])):

            logger.info(f'Running {test["REWRITE_METHOD"]}_optimized for {i + 1} time...')
            time_sum = 0
            for j in tqdm(range(len(f_list))):
                if '35' in f_list[j]:
                    logger.info(f"{f_list[j]},pass")
                    continue
                logger.debug('run Q%s with optimization...', f_list[j])
                sql = storage.read_sql_file(f'benchmark/queries/{test["BENCHMARK"]}/{f_list[j]}')

                if test['REWRITE_METHOD'] == 'greedy':
                    sql = best_df[best_df['sql'] == sql]['rewrite'].to_list()[0]
                    hint_set = best_df[best_df['rewrite'] == sql]['knobs'].tolist()[0].split(',')
                elif test['REWRITE_METHOD'] == 'mcts':
                    sql = best_df[best_df['sql'] == sql]['mcts'].to_list()[0]
                    hint_set = best_df[best_df['mcts'] == sql]['mcts_knobs'].tolist()[0].split(
                        ',') if 'mcts_knobs' in best_df else ['None']

                if hint_set[0] == 'None':
                    hint_set = []
                logger.debug(f'Found best hint set: {hint_set}')
                q, exc_rules = conn.set_disabled_knobs(hint_set, sql)

                try:
                    conf = ','.join([conf_oncbo])
                    result = conn.execute(sql, conf, exc_rules)
                except:
                    logger.warning(f'{f_list[j]} optimized execution failed.')
                    optimized_err.append(j)
                logger.info(f'query:{f_list[j]}; time: {result.time_usecs}')  ####
                time_sum += result.time_usecs
                # conn.set_disabled_knobs([], sql)  ### 1

            logger.info(f'Total time({test["REWRITE_METHOD"]}_optimized) for {i + 1}th running: {time_sum}')
            conn.clear_cache()
            time_optimized += time_sum
        logger.info(f'time_default: {time_default}')
        logger.info(f'time_optimized: {time_optimized}')
        logger.info(f'Best improvement: {(time_default - time_optimized) / time_default}')

    ### train
    else:
        if default['USE_REWRITE'] == 'true':
            logger.info(f'Use Rewrite')
            if rewrite['METHOD'] == 'greedy':
                logger.info('Rewrite Method: greedy')
                for query in f_list:
                    logger.info('Rewriting %s...', query)
                    approx_rewrite_span_and_run(SparkConnector, default['BENCHMARK'], query)
                best_rewrites = storage.save_best_rewrite()
                best_rewrites.to_csv(config['REWRITE']['REWRITE_EXP'], sep=';', index_label='id')
                logger.info(f'Saved best rewrites to {config["REWRITE"]["REWRITE_EXP"]}')
            elif rewrite['METHOD'] == 'predicate':
                logger.info('Rewrite Method: predicate')
                for query in f_list:
                    logger.info(f'Rewriting {query}...')
                    rewrite_and_test_syntax(SparkConnector, query)
                best_rewrites = storage.save_best_predicate_rewrite()
                best_rewrites.to_csv(config['REWRITE']['REWRITE_EXP'], sep=';', index_label='id')
            elif rewrite['METHOD'] == 'view':
                logger.info('Rewrite Method: view')
                for query in f_list:
                    logger.info(f'Rewriting {query}...')
                    generate_rewrite_mv(query)
                best_rewrites = storage.save_best_mv_rewrite()
                best_rewrites.to_csv(config['REWRITE']['REWRITE_EXP'], sep=';', index_label='id')

        if default['USE_HINT'] == 'true':
            if default['USE_REWRITE'] == 'true':
                logger.info('Using rewrited sql as input to genenrate corresponding hint')
                sql_list = best_rewrites['rewrite_sql'].tolist()
            else:
                logger.info('Using original sql as input to genenrate corresponding hint')
                sql_list = []
                for query in f_list:
                    with open(f'benchmark/queries/{default["benchmark"]}/{query}') as f:
                        sql = f.read().strip()
                        sql_list.append(sql)
            logger.info(f'total sql: {len(sql_list)}')
            for i in range(sql_list):
                logger.info(f'Optimizing sql_list[{i}]...')
                # 如果采用的改写方法是mcts需要把appox的对象换成用mcts改写过的查询
                query_path = f'sql_list[{i}]'
                storage.register_query(query_path, sql)
                if default['USE_REWRITE'] == 'true':
                    approx_query_span_and_run(SparkConnector, query_path, sql_list[i], rewrite['METHOD'])
                else:
                    approx_query_span_and_run(SparkConnector, query_path, sql_list[i], 'None')
            best_optimizations = storage.save_best_optimization()
            best_optimizations.to_csv(hint['HINT_EXP'], sep=';', index_label='id')
            most_frequent_knobs = storage.get_most_disabled_rules()
            logger.info('Most frequent knobs: %s', most_frequent_knobs)

        if default['USE_CARD'] == 'true':
            fill_real_card(default['CARD_EXP'])
