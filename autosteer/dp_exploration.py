# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""This module coordinates the query span approximation and the generation of new optimizer configurations for a query"""
import connectors.connector
import storage
from autosteer.optimizer_config import HintSetExploration
from autosteer.rewriter_config import RewriteExploration
from autosteer.rewriter import rewrite
from utils.custom_logging import logger
from utils.config import read_config
from utils.util import read_sql_file, hash_sql_result, hash_query_plan
REWRITE_SCHEMA = read_config()['REWRITE']['SCHEMA']

def register_query_config_and_measurement(query_path, disabled_rules, logical_plan, timed_result=None, initial_call=False) -> bool:
    """Register a new optimizer configuration and return whether the plan was already known"""

    result_fingerprint = hash_sql_result(timed_result.result) if timed_result is not None else None
    if timed_result is not None and not storage.register_query_fingerprint(query_path, result_fingerprint):
        logger.warning('Result fingerprint=%s does not match existing fingerprint!', result_fingerprint)

    plan_hash = int(hash_query_plan(str(logical_plan)), 16) & ((1 << 31) - 1)

    is_duplicate = storage.register_query_config(query_path, disabled_rules, logical_plan, plan_hash)
    if is_duplicate:
        logger.info('Plan is already known (according to hash)')
    if not initial_call and timed_result is not None:
        storage.register_measurement(query_path, disabled_rules, walltime=timed_result.time_usecs, input_data_size=0, nodes=1)
    return is_duplicate

def register_rewrite_config_and_measurement(query_path, rewrite_rules, rewrite_sql, rewrite_sql_plan, timed_result=None, initial_call=False) -> bool:
    """Register a new optimizer configuration and return whether the plan was already known"""

    result_fingerprint = hash_sql_result(timed_result.result) if timed_result is not None else None
    if timed_result is not None and not storage.register_query_fingerprint(query_path, result_fingerprint):
        logger.warning('Result fingerprint=%s does not match existing fingerprint!', result_fingerprint)

    plan_hash = int(hash_query_plan(str(rewrite_sql_plan)), 16) & ((1 << 31) - 1)
    logger.debug(f'Plan Hash: {plan_hash}')
    is_duplicate = storage.register_rewrite_config(query_path, rewrite_rules, rewrite_sql, plan_hash)
    if is_duplicate:
        logger.info('Plan is already known (according to hash)')
    if not initial_call and timed_result is not None:
        storage.register_rewrite_measurement(query_path, rewrite_rules, walltime=timed_result.time_usecs, input_data_size=0, nodes=1)
    return is_duplicate

def explore_rewrite_configs(connector: connectors.connector.DBConnector, sql_query, query_path):
    """Use dynamic programming to find good rewrite configs"""
    logger.info('Start exploring rewrite configs for query %s', query_path)
    rewrite_exploration = RewriteExploration(query_path)
    num_duplicate_plans = 0
    sql_query_raw = sql_query
    while rewrite_exploration.has_next():
        sql_query = rewrite((REWRITE_SCHEMA, sql_query, rewrite_exploration.next()))
        logger.debug(f'Rewrites into: {sql_query}')
        query_plan = connector.explain(sql_query)
        # Check if a new query plan is generated
        if register_rewrite_config_and_measurement(query_path, rewrite_exploration.get_disabled_opts_rules(), sql_query, query_plan, timed_result=None, initial_call=True):
            num_duplicate_plans += 1
            sql_query = sql_query_raw
            continue
        execute_rewrite_set(rewrite_exploration, connector, query_path, sql_query, query_plan)
        sql_query = sql_query_raw
    logger.info('Found %s duplicated query plans!', num_duplicate_plans)

### 
def explore_optimizer_configs(connector: connectors.connector.DBConnector, query_path, sql_query, rewrite_method):
    """Use dynamic programming to find good optimizer configs"""
    
    logger.info('Start exploring optimizer configs for query %s, by %s', query_path,rewrite_method)
    hint_set_exploration = HintSetExploration(query_path)
    num_duplicate_plans = 0
    sql_query_raw = sql_query
    while hint_set_exploration.has_next():
        sql_query = connector.set_disabled_knobs(hint_set_exploration.next(),sql_query)
        query_plan = connector.explain(sql_query)
        # Check if a new query plan is generated
        if register_query_config_and_measurement(query_path, hint_set_exploration.get_disabled_opts_rules(), query_plan, timed_result=None, initial_call=True):
            num_duplicate_plans += 1
            sql_query = sql_query_raw
            continue
        execute_hint_set(hint_set_exploration, connector, query_path, sql_query, query_plan)
        sql_query = sql_query_raw
    logger.info('Found %s duplicated query plans!', num_duplicate_plans)


def execute_hint_set(config: HintSetExploration, connector: connectors.connector.DBConnector, query_path: str, sql_query: str, query_plan: str):
    """Execute and register measurements for an optimizer configuration"""
    for _ in range(int(read_config()['DEFAULT']['REPEATS'])):
        try:
            timed_result = connector.execute(sql_query)
        # pylint: disable=broad-except
        except:
            logger.fatal('Optimizer %s cannot be disabled for %s, ignoring...', config.get_disabled_opts_rules(), query_path)
            # timed_result = connector.TimedResult('FAILED', 60_000_000)
            break

        if register_query_config_and_measurement(query_path, config.get_disabled_opts_rules(), query_plan, timed_result):
            logger.info('config results in already known query plan!')
            break

def execute_rewrite_set(config: RewriteExploration, connector: connectors.connector.DBConnector, query_path: str, sql_query: str, query_plan: str):
    """Execute and register measurements for an optimizer configuration"""
    for _ in range(int(read_config()['DEFAULT']['REPEATS'])):
        try:
            timed_result = connector.execute(sql_query)
            result_fingerprint = hash_sql_result(timed_result.result) if timed_result is not None else None
            if timed_result is not None and not storage.register_query_fingerprint(query_path, result_fingerprint):
                logger.warning('Result fingerprint=%s does not match existing fingerprint!', result_fingerprint)
                raise
        # # pylint: disable=broad-except
        except Exception as e:
            logger.fatal(sql_query)
            logger.error(e)
            logger.fatal('Rewrite rule %s cannot be used for %s, ignoring...', config.get_disabled_opts_rules(), query_path)
            break

        if register_rewrite_config_and_measurement(query_path, config.get_disabled_opts_rules(), sql_query, query_plan, timed_result):
            logger.info('config results in already known query plan!')
            break