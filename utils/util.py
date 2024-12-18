import operator
from functools import reduce
import hashlib
import paramiko
from utils.custom_logging import logger

def check_and_load_database(database):
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
    logger.info(f'check and load database {database}...')
    stdin, stdout, stderr = client.exec_command(f'spark-sql -e "use {database}";')
    logger.info(f'load database {database} successfully')

def read_sql_file(filename, encoding='utf-8') -> str:
    """Read SQL file, remove comments, and return a list of sql statements as a string"""
    with open(filename, encoding=encoding) as f:
        file = f.read().strip()
    statements = file.split('\n')
    return '\n'.join(filter(lambda line: not line.startswith('--'), statements))


def hash_sql_result(s):
    """Generate a hash fingerprint for the result retrieved from the connector to assert that results are (probably) identical.
    Its important to round floats here, e.g. using 2 decimal places."""
    if len(s) == 0:  # empty query result
        return 42
    flattened_result = reduce(operator.concat, str(s))
    normalized_result = list(sorted(map(lambda item: str(round(item, 1)) if isinstance(item, float) else str(item), flattened_result)))

    sha256 = hashlib.sha256()
    for item in normalized_result:
        sha256.update(str(item).encode())
    return int.from_bytes(sha256.digest()[:4], 'big')


def hash_query_plan(s):
    """Generate a hash fingerprint for the result retrieved from the connector to assert that results are (probably) identical.
    Its important to round floats here, e.g. using 2 decimal places."""
    flattened_result = reduce(operator.concat, s)
    normalized_result = tuple(map(lambda item: round(item, 2) if isinstance(item, float) else item, flattened_result))
    sha256 = hashlib.sha256()
    for item in normalized_result:
        sha256.update(str(item).encode())
    return sha256.hexdigest()


def flatten(l):
    return [item for sublist in l for item in sublist]

def is_not_number(s):
    try:
        float(s)
        return False
    except ValueError:
        pass
    try:
        import unicodedata
        unicodedata.numeric(s)
        return False
    except (TypeError, ValueError):
        pass
    return True