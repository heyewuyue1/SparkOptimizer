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
import pandas as pd

config = configparser.ConfigParser()
config.read('config.cfg')
default = config['DEFAULT']
test = config['TEST']


def approx_query_span_and_run(connector: Type[connectors.connector.DBConnector], benchmark: str, query: str):
    run_get_query_span(connector, benchmark, query)
    connector = connector()
    explore_optimizer_configs(connector, f'{benchmark}/{query}')


def check_and_load_database():
    database = default['DATABASE']
    logger.info(f'check and load database {database}...')
    conn = hive.Connection(host=default['THRIFT_SERVER_URL'], port=default['THRIFT_PORT'], username=default['THRIFT_USERNAME'])
    cursor = conn.cursor()
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
    cursor.execute(f'USE {database}')
    with open(f'./benchmark/schemas/{database}.sql', 'r') as f:
        query = f.read()
        query = query.split(';')
        for q in query:
            if q.strip() != '':
                cursor.execute(q)
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
    f_list = sorted(os.listdir('benchmark/queries/' + default['BENCHMARK']))
    logger.info('Found the following SQL files: %s', f_list)
    if args.test:
        logger.info('Running testing mode.')
        conn = SparkConnector()
        time_default = 0
        time_optimized = 0
        best_df = pd.read_csv(test['OPTIMIZER'])
        default_err = []
        optimized_err = []

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
            logger.info(f'Total time(default): {time_sum}')
            time_default += time_sum
        
        conn.turn_on_cbo()
        for i in range(eval(test['REPEATS'])):
            logger.info(f'Running optimized for {i + 1} time...')
            time_sum = 0
            for i in tqdm(range(len(f_list))):
                logger.debug('run Q%s with optimization...', f_list[i])
                sql = storage.read_sql_file(f'benchmark/queries/{test["BENCHMARK"]}/{f_list[i]}')
                hint_set = best_df[best_df['query_id'] == i + 1]['disabled_rules'].tolist()[0].split(',')
                if hint_set[0] == 'None':
                    hint_set = []
                logger.debug(f'Found best hint set: {hint_set}')
                conn.set_disabled_knobs(hint_set, sql)
                try:
                    result = conn.execute(sql)
                except:
                    logger.warning(f'{f_list[i]} optimized execution failed.')
                    optimized_err.append(query)
                logger.debug(f'time: {result.time_usecs}')
                time_sum += result.time_usecs
                conn.set_disabled_knobs([], sql)
            logger.info(f'Total time(optimized): {time_sum}')

        logger.info(f'Best improvement: {(time_default - time_optimized) / time_default}')
    else:
        for query in tqdm(f_list):
            logger.info('run Q%s...', query)
            approx_query_span_and_run(SparkConnector, default['BENCHMARK'], query)
        most_frequent_knobs = storage.get_most_disabled_rules()
        logger.info('Training ended. Most frequent disabled rules: %s', most_frequent_knobs)
        bo = storage.get_best_optimizers()
        bo.to_csv(test['OPTIMIZER'], header=True, index=False)
        best_improve = storage.get_best_imporovement()
        logger.info(f'Best improvement: {best_improve}')