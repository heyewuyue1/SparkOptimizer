# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Run AutoSteer's training mode to explore alternative query plans"""
from typing import Type
import storage
import os
import sys
import connectors
import csv
from utils.arguments_parser import get_parser
from utils.custom_logging import logger
from utils.config import read_config
from utils.util import read_sql_file
from optimizer.dp_exploration import explore_optimizer_configs, explore_rewrite_configs
from optimizer.query_span import run_get_query_span
from optimizer.rewrite_span import run_get_rewrite_span
from optimizer.card import fill_real_card
from optimizer.find_sub import generate_rewrite_mv, create_mv
from optimizer.predicate_rewrite import rewrite_and_test_syntax

config = read_config()
default = config['DEFAULT']
rewrite = config['REWRITE']
hint = config['HINT']
card = config['CARD']
connection = config['CONNECTION']
if connection['CONNECTOR'] == 'hive':
    from connectors.spark_connector_hive import SparkConnector
if connection['CONNECTOR'] == 'ssh':
    from connectors.spark_connector_ssh import SparkConnector

def approx_query_span_and_run(connector: Type[connectors.connector.DBConnector], query_path: str,
                              sql, rewrite_method: str):
    if run_get_query_span(connector,  query_path, sql, rewrite_method):
        connector = connector()
        explore_optimizer_configs(connector, query_path, sql, rewrite_method)


def approx_rewrite_span_and_run(connector: Type[connectors.connector.DBConnector], sql, query: str):
    run_get_rewrite_span(connector, sql, query)
    connector = connector()
    explore_rewrite_configs(connector, sql, query)

if __name__ == '__main__':
    args = get_parser().parse_args()
    storage.TESTED_DATABASE = default['STORAGE']
    if default['BENCHMARK'] is None or not os.path.isdir('benchmark/queries/' + default['BENCHMARK']):
        logger.fatal('Cannot access the benchmark directory containing the sql files with path=%s',
                     default['BENCHMARK'])
        sys.exit(1)
    f_list = sorted(os.listdir('benchmark/queries/' + default['BENCHMARK'])) # 测试全部sql
    logger.info('Found the following SQL files: %s', f_list)
    if default['USE_REWRITE'] == 'true':
        logger.info(f'Use Rewrite')
        if rewrite['METHOD'] == 'greedy':
            logger.info('Rewrite Method: greedy')
            for query in f_list:
                logger.info('Rewriting %s...', query)
                sql = read_sql_file(f"benchmark/queries/{default['benchmark']}/{query}")
                storage.register_query(query, sql)
                approx_rewrite_span_and_run(SparkConnector, sql, query)
            best_rewrites = storage.save_best_rewrite()
            best_rewrites.to_csv(read_config()['REWRITE']['REWRITE_EXP'], sep=';', index_label='id', quoting=csv.QUOTE_NONE, escapechar='\\')
            logger.info(f'Saved best rewrites to {config["REWRITE"]["REWRITE_EXP"]}')
        elif rewrite['METHOD'] == 'predicate':
            logger.info('Rewrite Method: predicate')
            for query in f_list:
                logger.info(f'Rewriting {query}...')
                rewrite_and_test_syntax(SparkConnector, query)
            best_rewrites = storage.save_best_predicate_rewrite()
            best_rewrites.to_csv(read_config()['REWRITE']['REWRITE_EXP'], sep=';', index_label='id', quoting=csv.QUOTE_NONE, escapechar='\\')
        elif rewrite['METHOD'] == 'view':
            logger.info('Rewrite Method: view')
            for query in f_list:
                if query in ['query23a.sql', 'query23b.sql']:
                    logger.info(f'Rewriting {query}...')
                    generate_rewrite_mv(query)
                    create_mv(query, SparkConnector)
            best_rewrites = storage.save_best_mv_rewrite()
            best_rewrites.to_csv(read_config()['REWRITE']['REWRITE_EXP'], sep=';', index_label='id', quoting=csv.QUOTE_NONE, escapechar='\\')

    if default['USE_HINT'] == 'true':
        if default['USE_REWRITE'] == 'true':
            logger.info('Using rewrited sql as input to genenrate corresponding hint')
            sql_list = best_rewrites['rewrite_sql'].tolist()
        else:
            logger.info('Using original sql as input to genenrate corresponding hint')
            sql_list = []
            for query in f_list:
                sql = read_sql_file(f'benchmark/queries/{default["benchmark"]}/{query}')
                sql_list.append(sql)
        logger.info(f'total sql: {len(sql_list)}')
        for i in range(len(sql_list)):
            logger.info(f'Optimizing sql_list[{i}]...')
            # 如果采用的改写方法是mcts需要把appox的对象换成用mcts改写过的查询
            query_path = f'sql_list[{i}]'
            storage.register_query(query_path, sql_list[i])
            if default['USE_REWRITE'] == 'true':
                approx_query_span_and_run(SparkConnector, query_path, sql_list[i], rewrite['METHOD'])
            else:
                approx_query_span_and_run(SparkConnector, query_path, sql_list[i], 'None')
        best_optimizations = storage.save_best_optimization()
        best_optimizations.to_csv(hint['HINT_EXP'], sep=';', index_label='id', quoting=csv.QUOTE_NONE, escapechar='\\')
        most_frequent_knobs = storage.get_most_disabled_rules()
        logger.info('Most frequent knobs: %s', most_frequent_knobs)

    if default['USE_CARD'] == 'true':
        fill_real_card(card['CARD_EXP'], SparkConnector)
