from pyhive import hive
import time
import re
from utils.custom_logging import logger
from connectors.connector import DBConnector
import configparser
import re
import sqlparse
from sqlparse.tokens import DML
import paramiko

EXCLUDED_RULES = 'spark.sql.optimizer.excludedRules'


def _postprocess_plan(plan) -> str:
    """Remove random ids from the explained query plan"""
    pattern = re.compile(r'#\d+L?|\[\d+]||\[plan_id=\d+\]')
    return re.sub(pattern, '', plan)

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
def check_Broadcast(query,joinhint_knobs):
   # Remove comments from SQL to avoid parsing issues
    query = sqlparse.format(query, strip_comments=True)
    
    # Parse the SQL
    parsed = sqlparse.parse(query)
    
    if not parsed:
        logger.error(f'Syntax Error in Query: {query}')
    
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
        """
        hostname = '192.168.90.173'
        port = 22
        username = 'root'
        password = 'root'
        for i in range(5):
            try:
                # 创建SSH客户端
                self.client = paramiko.SSHClient()
                # 自动添加主机密钥
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                # 连接远程主机
                self.client.connect(hostname, port, username, password)
                logger.debug('SparkSQL connector conntects to SSH: ' + hostname)
                break
            except:
                logger.warning(f'Atempt {i + 1} Failed to connect to SSH, retrying...')
        """

    def execute(self, query, conf=[],exc_rules='') -> DBConnector.TimedResult:  ### 1
        max_retry = eval(self.config['DEFAULT']['MAX_RETRY'])
        if len(conf)>0:
            confs = '  '.join([f'--conf {i}' if len(i)>0 else '' for i in conf.split(',')])
        else: confs=''
        exe_command = f""" source /etc/profile; /usr/local/spark/bin/spark-sql \
         --database {self.config['DEFAULT']['DATABASE']} \
         --driver-cores 8 \
         --driver-memory 20g  \
         --num-executors 24   \
         --executor-cores 8  \
         --executor-memory 52948m   \
         --master yarn   \
         --conf spark.task.cpus=1   \
         --conf spark.sql.orc.impl=native   \
         --conf spark.default.parallelism=600 \
         --conf spark.sql.shuffle.partitions=600 \
         --conf spark.sql.autoBroadcastJoinThreshold=100m   \
         --conf spark.sql.broadcastTimeout=600  \
         --conf spark.network.timeout=600   \
         --conf spark.sql.adaptive.enabled=true  \
         --conf spark.locality.wait=0   \
         --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
         --conf spark.serializer=org.apache.spark.serializer.KryoSerializer   \
         --conf spark.executor.memoryOverhead=1g   \
         --conf spark.sql.sources.parallelPartitionDiscovery.parallelism=60 \
         --conf spark.sql.files.minPartitionNum=180 \
         --conf spark.sql.files.maxPartitionBytes=256m \
         --conf spark.nodemanager.numas=4 \
         --conf spark.sql.cli.print.header=true \
         {confs} \
          -e "{exc_rules} {query} " """
        for i in range(max_retry):
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect('192.168.90.173', 22, 'root', 'root')
                stdin, stdout, stderr = client.exec_command(exe_command)
                out = str(stdout.read().decode())
                err = str(stderr.read().decode())
                client.close()
                # print(exe_command)
                # print(out)
                if "RESET" in exc_rules:
                    collection = out ### 
                else:
                    # logger.info(f"exc_rules:  {out.splitlines()[0]}")
                    collection = '\n'.join(out.splitlines()[1:]) 
                collection = out
                time_line = err.split('\n')[-2]
                m = re.search("Time taken: (\d*.\d*) seconds",time_line)
                elapsed_time_usecs = eval(m.group(1))
                break
            except Exception as e:
                if i == max_retry - 1:
                    print(err)
                    logger.fatal(err)
                    logger.fatal(f'Execution failed {max_retry} times.')
                    logger.fatal(str(e)[:1000])
                    raise
                else:
                    logger.warning('Execution failed %s times, try again...', str(i + 1))
        logger.debug('QUERY RESULT %s', str(collection)[:100].encode('utf-8') if len(str(collection)) > 100 else collection)
        # logger.debug('QUERY RESULT %s', collection[0])
        # collection = 'EmptyResult' if len(collection) == 0 else collection[0]
        logger.debug('Hash(QueryResult) = %s', str(hash(str(collection))))
        return DBConnector.TimedResult(collection, elapsed_time_usecs)

    def explain(self, query) -> str:
        # timed_result_c = self.execute(f'EXPLAIN COST {query}')
        timed_result = self.execute(f'EXPLAIN FORMATTED {query}')
        # database = self.config['DEFAULT']['BENCHMARK']
        # result = get_rowcount.get_explain(database, timed_result.result[0], timed_result_c.result[0])

        return _postprocess_plan(timed_result.result[0])
        # return _postprocess_plan(result)
    
    def clear_cache(self):  #### 1
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('192.168.90.173', 22, 'root', 'root')
        stdin, stdout, stderr = client.exec_command("echo 3 > /proc/sys/vm/drop_caches;free -g")
        out = str(stdout.read().decode())
        logger.info('clean cache:')
        logger.info(out)
    def set_disabled_knobs(self, knobs, query): #### 1
        """Toggle a list of knobs"""
        binary_knobs = []
        joinhint_knobs = []
        for rule in knobs:
            if 'Broadcast' not in rule:
                binary_knobs.append(rule) 
            else:
                joinhint_knobs.append(rule)
        if len(binary_knobs) == 0:
            exc_rules = f"RESET {EXCLUDED_RULES};" 
        else:
            formatted_knobs = [f'org.apache.spark.sql.catalyst.optimizer.{rule}' for rule in binary_knobs]
            exc_rules = f"SET {EXCLUDED_RULES}={','.join(formatted_knobs)};"

        if len(joinhint_knobs) > 0:
            if 'Broadcast' in joinhint_knobs[0]:
                new_query = check_Broadcast(query,joinhint_knobs)
                if new_query != False:
                    return new_query,exc_rules
            elif 'Merge' in joinhint_knobs:
                pass
            elif 'ShuffleHash' in joinhint_knobs:
                pass
            elif 'ShuffleNestedLoop' in joinhint_knobs:
                pass
            return query,exc_rules
        else:
            return query,exc_rules

    def get_knob(self, knob: str) -> bool:
        """Get current status of a knob"""
        self.cursor.execute(f'SET {EXCLUDED_RULES}')
        excluded_rules = self.cursor.fetchall()[0]
        logger.info('Current excluded rules: %s', excluded_rules)
        if excluded_rules is None:
            return True
        else:
            return not knob in excluded_rules

    def turn_off_cbo(self): #### 1
        cbo_knobs = ['spark.sql.cbo.enabled=False','spark.sql.cbo.joinReorder.dp.star.filter=False',
                     'spark.sql.cbo.starSchemaDetection=False','spark.sql.cbo.joinReorder.enabled=False']
        return ','.join(cbo_knobs)

    def turn_on_cbo(self): #### 1
        cbo_knobs = ['spark.sql.cbo.enabled=True','spark.sql.cbo.joinReorder.dp.star.filter=True',
                     'spark.sql.cbo.starSchemaDetection=True','spark.sql.cbo.joinReorder.enabled=True']
        return ','.join(cbo_knobs)

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
