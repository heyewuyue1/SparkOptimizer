--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS benchmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    name VARCHAR(256) UNIQUE NOT NULL
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    benchmark_id INTEGER REFERENCES benchmarks NOT NULL, 
    query_path varchar(256) NOT NULL,
    sql TEXT NOT NULL,
    result_fingerprint INTEGER DEFAULT 0
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_required_optimizers (
    query_id INTEGER REFERENCES queries NOT NULL,
    optimizer TEXT NOT NULL,
    PRIMARY KEY (query_id, optimizer)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_effective_optimizers (
    query_id INTEGER REFERENCES queries NOT NULL,
    optimizer TEXT NOT NULL,
    PRIMARY KEY (query_id, optimizer)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_effective_rewrite_rules (
    query_id INTEGER REFERENCES queries NOT NULL,
    rewrite_rule TEXT NOT NULL,
    PRIMARY KEY (query_id, rewrite_rule)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_effective_optimizers_dependencies (
    query_id INTEGER REFERENCES queries NOT NULL,
    optimizer TEXT NOT NULL,
    dependent_optimizer TEXT NOT NULL,
    -- dependency of 'optimizer'
    PRIMARY KEY (query_id, optimizer, dependent_optimizer)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_optimizer_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    query_id INTEGER REFERENCES queries NOT NULL,
    disabled_rules TEXT NOT NULL,
    -- this column represents exactly one hint-set
    query_plan TEXT NOT NULL,
    num_disabled_rules INTEGER NOT NULL,
    hash INTEGER NOT NULL,
    -- the hash value of the optimizer query plan
    duplicated_plan BOOLEAN DEFAULT FALSE NOT NULL,
    UNIQUE (query_id, disabled_rules)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_rewrite_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    query_id INTEGER REFERENCES queries NOT NULL,
    rewrite_rules TEXT NOT NULL,
    -- this column represents exactly one hint-set
    rewrite_sql TEXT NOT NULL,
    num_rewrite_rules INTEGER NOT NULL,
    hash INTEGER NOT NULL,
    -- the hash value of the optimizer query plan
    duplicated_plan BOOLEAN DEFAULT FALSE NOT NULL,
    UNIQUE (query_id, rewrite_rules)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS measurements (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    query_optimizer_config_id INTEGER REFERENCES query_optimizer_configs NOT NULL,
    walltime INTEGER NOT NULL,
    machine TEXT NOT NULL,
    time TIMESTAMP NOT NULL,
    input_data_size INTEGER NOT NULL,
    num_compute_nodes INTEGER NOT NULL
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rewrite_measurements (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    query_rewrite_config_id INTEGER REFERENCES query_rewrite_configs NOT NULL,
    walltime INTEGER NOT NULL,
    machine TEXT NOT NULL,
    time TIMESTAMP NOT NULL,
    input_data_size INTEGER NOT NULL,
    num_compute_nodes INTEGER NOT NULL
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS create_mv_statement (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    mv_name TEXT NOT NULL,
    subq TEXT NOT NULL
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mv_rewrites (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    query TEXT NOT NULL,
    rewrite_query TEXT NOT NULL
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS predicate_rewrites (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    query TEXT NOT NULL,
    rewrite_query TEXT NOT NULL
);
--------------------------------------------------------------------------------