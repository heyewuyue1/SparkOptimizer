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
from autosteer.dp_exploration import explore_optimizer_configs, explore_rewrite_configs
from autosteer.query_span import run_get_query_span
from autosteer.rewrite_span import run_get_rewrite_span
from tqdm import tqdm
from pyhive import hive
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('config.cfg')
default = config['DEFAULT']
test = config['TEST']


def approx_query_span_and_run(connector: Type[connectors.connector.DBConnector], benchmark: str, query: str, best_rewrites, rewrite_method: str):
    run_get_query_span(connector, benchmark, query, best_rewrites, rewrite_method)
    connector = connector()
    explore_optimizer_configs(connector, f'{benchmark}/{query}', best_rewrites, rewrite_method)

def approx_rewrite_span_and_run(connector: Type[connectors.connector.DBConnector], benchmark: str, query: str):
    run_get_rewrite_span(connector, benchmark, query)
    connector = connector()
    explore_rewrite_configs(connector, f'{benchmark}/{query}')

def check_and_load_database():
    database = default['DATABASE']
    logger.info(f'check and load database {database}...')
    conn = hive.Connection(host=default['THRIFT_SERVER_URL'], port=default['THRIFT_PORT'], username=default['THRIFT_USERNAME'])
    cursor = conn.cursor()
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
    cursor.execute(f'USE {database}')
    logger.info(f'load database {database} successfully')

if __name__ == '__main__':
    args = get_parser().parse_args()
    if args.debug:
        logger.setLevel('DEBUG')
    storage.TESTED_DATABASE = default['DATABASE']
    check_and_load_database()
    if default['BENCHMARK'] is None or not os.path.isdir('benchmark/queries/' + default['BENCHMARK']):
        logger.fatal('Cannot access the benchmark directory containing the sql files with path=%s', default['BENCHMARK'])
        sys.exit(1)
    storage.BENCHMARK_ID = storage.register_benchmark(default['BENCHMARK'])
    f_list = sorted(os.listdir('benchmark/queries/' + default['BENCHMARK']))  #测试全部sql
    logger.info('Found the following SQL files: %s', f_list)
    
    ### test
    if args.test:
        logger.info('Running testing mode.')
        conn = SparkConnector()
        time_default = 0
        time_optimized = 0
        best_df = pd.read_csv(test['OPTIMIZER'])
        default_err = []
        optimized_err = []
        
        ### original
        conn.set_disabled_knobs('',[])
        conn.turn_off_cbo()
        for i in range(eval(test['REPEATS'])):
            logger.info(f'Running default for {i + 1} time...')
            time_sum = 0
            for query in tqdm(f_list):
                logger.debug('run Q%s without optimization...', query)
                sql = storage.read_sql_file(f'benchmark/queries/{test["BENCHMARK"]}/{query}')
                try:
                    result = conn.execute(sql)
                except:
                    logger.warning(f'{query} default execution failed.')
                    default_err.append(query)
                logger.debug(f'time: {result.time_usecs}')
                time_sum += result.time_usecs
            logger.info(f'Total time(default) for {i + 1}th running: {time_sum}')
            conn.clear_cache()
            time_default += time_sum
        
        ### optimized
        conn.turn_on_cbo()
        for i in range(eval(test['REPEATS'])):
            logger.info(f'Running {test["REWRITE_METHOD"]}_optimized for {i + 1} time...')
            time_sum = 0
            for j in tqdm(range(len(f_list))):
                logger.debug('run Q%s with optimization...', f_list[j])
                sql = storage.read_sql_file(f'benchmark/queries/{test["BENCHMARK"]}/{f_list[j]}')

                if test['REWRITE_METHOD'] == 'greedy':
                    sql = best_df[best_df['sql'] == sql]['rewrite'].to_list()[0]
                    hint_set = best_df[best_df['rewrite'] == sql]['knobs'].tolist()[0].split(',')
                elif test['REWRITE_METHOD'] == 'mcts':
                    sql = best_df[best_df['sql'] == sql]['mcts'].to_list()[0]
                    hint_set = best_df[best_df['mcts'] == sql]['mcts_knobs'].tolist()[0].split(',') if 'mcts_knobs' in best_df else ['None']

                if hint_set[0] == 'None':
                    hint_set = []
                logger.debug(f'Found best hint set: {hint_set}')
                conn.set_disabled_knobs(hint_set, sql)
                
                try:
                    result = conn.execute(sql)
                except:
                    logger.warning(f'{f_list[j]} optimized execution failed.')
                    optimized_err.append(j)
                logger.debug(f'time: {result.time_usecs}')
                time_sum += result.time_usecs
                conn.set_disabled_knobs([], sql)  # 恢复
            logger.info(f'Total time({test["REWRITE_METHOD"]}_optimized) for {i + 1}th running: {time_sum}')
            conn.clear_cache()
            time_optimized += time_sum
        logger.info(f'time_default: {time_default}')
        logger.info(f'time_optimized: {time_optimized}')
        logger.info(f'Best improvement: {(time_default - time_optimized) / time_default}')
    
    ### train
    else:
        if default['REWRITE_METHOD'] == 'greedy':
            for query in tqdm(f_list):
                logger.info('Rewriting %s...', query)
                approx_rewrite_span_and_run(SparkConnector, default['BENCHMARK'], query)
            best_rewrites = storage.save_best_rewrite()
        elif default['REWRITE_METHOD'] == 'mcts':
            best_rewrites = pd.read_csv(test['OPTIMIZER'])

        for query in tqdm(f_list):
            logger.info('Optimizing %s...', query)

            if default['REWRITE_METHOD'] == 'mcts':  # 向storage登记每条mcts改写的sql
                query_path = f'benchmark/queries/{default["benchmark"]}/{query}'
                path = query.split('/')[-1]
                sql = best_rewrites.loc[best_rewrites['path']==path,'mcts'].tolist()[0]
                best_rewrites.loc[best_rewrites['path'] == path, 'query_path'] = query_path  # 向best_rewrites中添加query_path列
                storage.register_query(query_path, sql)

            approx_query_span_and_run(SparkConnector, default['BENCHMARK'], query, best_rewrites, default['REWRITE_METHOD'])

        best_optimizations = storage.save_best_optimization()
        most_frequent_knobs = storage.get_most_disabled_rules()
        logger.info('Training ended. Most frequent disabled rules: %s', most_frequent_knobs)
        
        best_rewrites['query_path'] = best_rewrites['query_path'].astype(str)
        best_optimizations['query_path'] = best_optimizations['query_path'].astype(str)

        if default['REWRITE_METHOD'] == 'greedy':
            best_config = best_rewrites.merge(best_optimizations, on='query_path')[['sql','rewrite','knobs']]
        elif default['REWRITE_METHOD'] == 'mcts':
            best_optimizations = best_optimizations.rename(columns={'knobs': 'mcts_knobs'}) 
            best_config = best_rewrites.merge(best_optimizations, on='query_path')[['mcts','mcts_knobs']]

        best_config['schema'] = 'tpcds'
        best_config.to_csv(test['OPTIMIZER'], header=True, index=True)  ###
        best_improve = storage.get_best_improvement()
        logger.info(f'Best improvement: {best_improve}')