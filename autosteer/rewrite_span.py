from utils.custom_logging import logger
from utils.config import read_config
import storage
import numpy as np
from multiprocessing.pool import ThreadPool as Pool
from autosteer.hintset import HintSet
from autosteer.rewriter import call_rewriter
from utils.util import hash_query_plan
import queue

N_THREADS = int(read_config()['DEFAULT']['EXPLAIN_THREADS'])
REWRITE_SCHEMA = read_config()['REWRITE']['SCHEMA']
FAILED = 'FAILED'
rule_list = ['AGGREGATE_EXPAND_DISTINCT_AGGREGATES', 'AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN',
                 'AGGREGATE_JOIN_TRANSPOSE_EXTENDED', 'AGGREGATE_PROJECT_MERGE', 'AGGREGATE_ANY_PULL_UP_CONSTANTS',
                 'AGGREGATE_UNION_AGGREGATE', 'AGGREGATE_UNION_TRANSPOSE', 'AGGREGATE_VALUES', 'AGGREGATE_INSTANCE',
                 'AGGREGATE_REMOVE', 'FILTER_AGGREGATE_TRANSPOSE', 'FILTER_CORRELATE', 'FILTER_INTO_JOIN',
                 'JOIN_CONDITION_PUSH', 'FILTER_MERGE', 'FILTER_MULTI_JOIN_MERGE', 'FILTER_PROJECT_TRANSPOSE',
                 'FILTER_SET_OP_TRANSPOSE', 'FILTER_TABLE_FUNCTION_TRANSPOSE', 'FILTER_SCAN',
                 'FILTER_REDUCE_EXPRESSIONS', 'PROJECT_REDUCE_EXPRESSIONS', 'FILTER_INSTANCE', 'JOIN_EXTRACT_FILTER',
                 'JOIN_PROJECT_BOTH_TRANSPOSE', 'JOIN_PROJECT_LEFT_TRANSPOSE', 'JOIN_PROJECT_RIGHT_TRANSPOSE',
                 'JOIN_LEFT_UNION_TRANSPOSE', 'JOIN_RIGHT_UNION_TRANSPOSE', 'SEMI_JOIN_REMOVE',
                 'JOIN_REDUCE_EXPRESSIONS', 'JOIN_LEFT_INSTANCE', 'JOIN_RIGHT_INSTANCE', 'PROJECT_CALC_MERGE',
                 'PROJECT_CORRELATE_TRANSPOSE', 'PROJECT_MERGE', 'PROJECT_MULTI_JOIN_MERGE', 'PROJECT_REMOVE',
                 'PROJECT_TO_CALC', 'PROJECT_SUB_QUERY_TO_CORRELATE', 'PROJECT_REDUCE_EXPRESSIONS',
                 'PROJECT_INSTANCE', 'CALC_MERGE', 'CALC_REMOVE', 'SORT_JOIN_TRANSPOSE', 'SORT_PROJECT_TRANSPOSE',
                 'SORT_UNION_TRANSPOSE', 'SORT_REMOVE_CONSTANT_KEYS', 'SORT_REMOVE', 'SORT_INSTANCE',
                 'SORT_FETCH_ZERO_INSTANCE', 'UNION_MERGE', 'UNION_REMOVE', 'UNION_TO_DISTINCT',
                 'UNION_PULL_UP_CONSTANTS', 'UNION_INSTANCE', 'INTERSECT_INSTANCE', 'MINUS_INSTANCE']

def run_get_rewrite_span(connector_type, benchmark, query):
    query_path = f'benchmark/queries/{benchmark}/{query}'
    logger.info('Approximate rewrite span for query: %s', query_path)
    sql = storage.read_sql_file(query_path)
    storage.register_query(query_path, sql)

    rewrite_span = approximate_rewrite_span(connector_type, sql, call_rewriter)

    # Serialize the approximated query span in the database
    for rewrite_rule in rewrite_span:  # pylint: disable=not-an-iterable
        logger.info('Found new rewrite-set: %s', rewrite_rule)
        storage.register_rewrite_rule(query_path, ','.join(sorted(rewrite_rule.knobs)), rewrite_rule.required)

def approximate_rewrite_span(connector_type, sql_query: str, call_rewriter) -> list:
    # Create singleton hint-sets
    knobs = np.array(rule_list)
    hint_sets = np.array([HintSet({knob}, None) for knob in knobs])
    # To speed up the query span approximation, we can submit multiple queries in parallel
    with Pool(N_THREADS) as thread_pool:
        args = [(REWRITE_SCHEMA, sql_query, knob) for knob in hint_sets]
        results = np.array(list(thread_pool.map(call_rewriter, args)))
        spark_connector = connector_type()
        default_plan_hash = int(hash_query_plan(str(spark_connector.explain(sql_query))), 16) & ((1 << 31) - 1)
        for i in range(len(results)):
            if results[i].plan == sql_query:
                results[i].plan = 'NA'
            else:
                try:
                    plan_hash = int(hash_query_plan(str(spark_connector.explain(results[i].plan))), 16) & ((1 << 31) - 1)
                    if plan_hash == default_plan_hash:
                        logger.info(f'Rewrite rule {results[i].get_all_knobs()[0]} take effect, but has no impact to the query plan.')
                        results[i].plan = 'NA'
                except:
                    results[i].plan = 'NA'
        rewrite_span: list[HintSet] = []
        result_rewrite = np.array([i.plan for i in results])
        effective_knobs_indexes = np.where((result_rewrite != 'NA'))
        logger.info('There are %s alternative rewrites', effective_knobs_indexes[0].size)

        new_effective_knobs = queue.Queue()
        for optimizer in results[effective_knobs_indexes]:
            new_effective_knobs.put(optimizer)

        # Note that indices change after delete
        while not new_effective_knobs.empty():
            new_effective_optimizer = new_effective_knobs.get()
            rewrite_span.append(new_effective_optimizer)
    return rewrite_span

class RewriteSpan:
    """A wrapper class for query spans which are reconstructed from (serialized) storage."""

    def __init__(self, query_path=None):
        if query_path is not None:
            self.query_path = query_path
            self.effective_rewrite_rules = storage.get_effective_rewrite_rules(self.query_path)

    def get_tunable_knobs(self):
        return sorted(list(set(self.effective_rewrite_rules)))