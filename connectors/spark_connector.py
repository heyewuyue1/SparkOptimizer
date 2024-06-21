# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""This module provides a connection to a Spark cluster that is used for benchmarking"""
from pyhive import hive
import time
import re
from utils.custom_logging import logger
from connectors.connector import DBConnector
import configparser
import re
import sqlparse
from sqlparse.tokens import DML

EXCLUDED_RULES = 'spark.sql.optimizer.excludedRules'

def _postprocess_plan(plan) -> str:
    """Remove random ids from the explained query plan"""
    pattern = re.compile(r'#\d+L?|\[\d+]||\[plan_id=\d+\]')
    return re.sub(pattern, '', plan)

def check_Broadcast(query,joinhint_knobs):
   # Remove comments from SQL to avoid parsing issues
    query = sqlparse.format(query, strip_comments=True)
    
    # Parse the SQL
    parsed = sqlparse.parse(query)
    
    if not parsed:
        logger.error(f'Syntax Error in Query: {query}')
    
    # Use a helper function to recursively find the main SELECT
    def find_main_select(tokens):
        parenthesis_level = 0
        current_pos = 0

        for token in tokens:
            if token.ttype in (sqlparse.tokens.Punctuation,) and token.value == '(':
                parenthesis_level += 1
            elif token.ttype in (sqlparse.tokens.Punctuation,) and token.value == ')':
                parenthesis_level -= 1
            elif token.ttype is DML and token.value.upper() == 'SELECT' and parenthesis_level == 0:
                return current_pos
            
            current_pos += len(str(token))
        
        return -1
    
    for statement in parsed:
        position = find_main_select(statement.tokens)
        if position == -1:
            return query
    
    broadcast_str = ''
    for i in range(len(joinhint_knobs)):
        table_name = joinhint_knobs[i].split(' ')[1]
        broadcast_str = broadcast_str + table_name + ','
    broadcast_str = broadcast_str[:-1]
    try:
        query = query[:position] + 'select /*+ BROADCAST(' +broadcast_str + ') */' + query[position+6:] 
    except Exception as e:
        logger.error(f'Error when query is {query}, and joinhint is {joinhint_knobs}')
        return False
    return query

class SparkConnector(DBConnector):
    """This class implements the AutoSteer-G connector for a Spark cluster accepting SQL statements"""
    def __init__(self):
        super().__init__()
        self.config = configparser.ConfigParser()
        self.config.read('./config.cfg')
        defaults = self.config['DEFAULT']
        for i in range(5):
            try:
                self.conn = hive.Connection(host=defaults['THRIFT_SERVER_URL'], port=defaults['THRIFT_PORT'], username=defaults['THRIFT_USERNAME'], database=defaults['DATABASE'])
                logger.debug('SparkSQL connector conntects to thrift server: ' + defaults['THRIFT_SERVER_URL'] + ':' + defaults['THRIFT_PORT'])
                break
            except:
                logger.warning(f'Atempt {i + 1} Failed to connect to thrift server, retrying...')
        self.cursor = self.conn.cursor()
        # self.cursor.execute('SET spark.sql.cbo.enabled=true')

    def execute(self, query) -> DBConnector.TimedResult:
        max_retry = eval(self.config['DEFAULT']['MAX_RETRY'])
        for i in range(max_retry):
            try:
                begin = time.time_ns()
                self.cursor.execute(query)
                collection = self.cursor.fetchall()
                elapsed_time_usecs = int((time.time_ns() - begin) / 1_000)
                break
            except Exception as e:
                if i == max_retry - 1:
                    logger.fatal(f'Execution failed {max_retry} times.')
                    logger.fatal(e)
                    raise
                else:
                    logger.warning('Execution failed %s times, try again...', str(i + 1))
        logger.debug('QUERY RESULT %s', str(collection)[:100].encode('utf-8') if len(str(collection)) > 100 else collection)
        # logger.debug('QUERY RESULT %s', collection[0])
        collection = 'EmptyResult' if len(collection) == 0 else collection[0]
        logger.debug('Hash(QueryResult) = %s', str(hash(str(collection))))
        return DBConnector.TimedResult(collection, elapsed_time_usecs)

    def explain(self, query) -> str:
        # timed_result_c = self.execute(f'EXPLAIN COST {query}')
        timed_result = self.execute(f'EXPLAIN FORMATTED {query}')
        # database = self.config['DEFAULT']['BENCHMARK']
        # result = get_rowcount.get_explain(database, timed_result.result[0], timed_result_c.result[0])

        return _postprocess_plan(timed_result.result[0])
        # return _postprocess_plan(result)

    def set_disabled_knobs(self, knobs, query) -> str:
        """Toggle a list of knobs"""
        binary_knobs = []
        joinhint_knobs = []
        for rule in knobs:
            if 'Broadcast' not in rule:
                binary_knobs.append(rule) 
            else:
                joinhint_knobs.append(rule)
        if len(binary_knobs) == 0:
            self.cursor.execute(f'RESET {EXCLUDED_RULES}')
        else:
            formatted_knobs = [f'org.apache.spark.sql.catalyst.optimizer.{rule}' for rule in binary_knobs]
            self.cursor.execute(f'SET {EXCLUDED_RULES}={",".join(formatted_knobs)}')
        if len(joinhint_knobs) > 0:
            if 'Broadcast' in joinhint_knobs[0]:
                new_query = check_Broadcast(query,joinhint_knobs)
                if new_query != False:
                    return new_query
            elif 'Merge' in joinhint_knobs:
                pass
            elif 'ShuffleHash' in joinhint_knobs:
                pass
            elif 'ShuffleNestedLoop' in joinhint_knobs:
                pass
            return query
        else:
            return query

    def get_knob(self, knob: str) -> bool:
        """Get current status of a knob"""
        self.cursor.execute(f'SET {EXCLUDED_RULES}')
        excluded_rules = self.cursor.fetchall()[0]
        logger.info('Current excluded rules: %s', excluded_rules)
        if excluded_rules is None:
            return True
        else:
            return not knob in excluded_rules

    def turn_off_cbo(self):
        self.cursor.execute('SET spark.sql.cbo.enabled=false')
        self.cursor.execute('SET spark.sql.cbo.joinReorder.dp.star.filter=false')
        self.cursor.execute('SET spark.sql.cbo.starSchemaDetection=false')
        self.cursor.execute('SET spark.sql.cbo.joinReorder.enabled=false')

    def turn_on_cbo(self):
        self.cursor.execute('SET spark.sql.cbo.enabled=true')
        self.cursor.execute('SET spark.sql.cbo.joinReorder.dp.star.filter=true')
        self.cursor.execute('SET spark.sql.cbo.starSchemaDetection=true')
        self.cursor.execute('SET spark.sql.cbo.joinReorder.enabled=true')

    @staticmethod
    def get_name() -> str:
        return 'spark'

    @staticmethod
    def get_knobs() -> list:
        """Static method returning all knobs defined for this connector"""
        config = configparser.ConfigParser()
        config.read('./config.cfg')
        defaults = config['DEFAULT']
        with open(defaults['KNOB'], 'r', encoding='utf-8') as f:
            return [line.replace('\n', '') for line in f.readlines()]

if __name__ == '__main__':
    import os     
    connector = SparkConnector()
    knobs = connector.get_knobs()
    broadcast_list = []
    f_list = sorted(os.listdir('benchmark/queries/tpcds_sf100'))
    for f_name in f_list:
        with open(f'benchmark/queries/tpcds_sf100/{f_name}','r') as file:
            query = file.read()
        for knob in knobs:
            if 'Broadcast' in knob:
                query = connector.set_disabled_knobs([knob],query)
                try:
                    connector.explain(query)
                except:
                    print(query)
