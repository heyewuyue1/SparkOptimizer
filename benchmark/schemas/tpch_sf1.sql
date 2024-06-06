CREATE TABLE IF NOT EXISTS PART (
	P_PARTKEY		integer,
	P_NAME			string,
	P_MFGR			string,
	P_BRAND			string,
	P_TYPE			string,
	P_SIZE			INTEGER,
	P_CONTAINER		string,
	P_RETAILPRICE	DECIMAL,
	P_COMMENT		string
) USING CSV LOCATION '/home/hejiahao/tpch-kit/sc1/part.csv';

CREATE TABLE IF NOT EXISTS SUPPLIER (
	S_SUPPKEY		integer,
	S_NAME			string,
	S_ADDRESS		string,
	S_NATIONKEY		integer NOT NULL, -- references N_NATIONKEY
	S_PHONE			string,
	S_ACCTBAL		DECIMAL,
	S_COMMENT		string
) USING CSV LOCATION '/home/hejiahao/tpch-kit/sc1/supplier.csv' ;

CREATE TABLE IF NOT EXISTS PARTSUPP (
	PS_PARTKEY		integer NOT NULL, -- references P_PARTKEY
	PS_SUPPKEY		integer NOT NULL, -- references S_SUPPKEY
	PS_AVAILQTY		INTEGER,
	PS_SUPPLYCOST	DECIMAL,
	PS_COMMENT		string
) USING CSV LOCATION '/home/hejiahao/tpch-kit/sc1/partsupp.csv';

CREATE TABLE IF NOT EXISTS CUSTOMER (
	C_CUSTKEY		integer,
	C_NAME			string,
	C_ADDRESS		string,
	C_NATIONKEY		integer NOT NULL, -- references N_NATIONKEY
	C_PHONE			string,
	C_ACCTBAL		DECIMAL,
	C_MKTSEGMENT	string,
	C_COMMENT		string
) USING CSV LOCATION '/home/hejiahao/tpch-kit/sc1/customer.csv' ;

CREATE TABLE IF NOT EXISTS ORDERS (
	O_ORDERKEY		integer,
	O_CUSTKEY		integer NOT NULL, -- references C_CUSTKEY
	O_ORDERSTATUS	string,
	O_TOTALPRICE	DECIMAL,
	O_ORDERDATE		DATE,
	O_ORDERPRIORITY	string,
	O_CLERK			string,
	O_SHIPPRIORITY	INTEGER,
	O_COMMENT		string
) USING CSV LOCATION '/home/hejiahao/tpch-kit/sc1/orders.csv' ;

CREATE TABLE IF NOT EXISTS LINEITEM (
	L_ORDERKEY		integer NOT NULL, -- references O_ORDERKEY
	L_PARTKEY		integer NOT NULL, -- references P_PARTKEY (compound fk to PARTSUPP)
	L_SUPPKEY		integer NOT NULL, -- references S_SUPPKEY (compound fk to PARTSUPP)
	L_LINENUMBER	INTEGER,
	L_QUANTITY		DECIMAL,
	L_EXTENDEDPRICE	DECIMAL,
	L_DISCOUNT		DECIMAL,
	L_TAX			DECIMAL,
	L_RETURNFLAG	string,
	L_LINESTATUS	string,
	L_SHIPDATE		DATE,
	L_COMMITDATE	DATE,
	L_RECEIPTDATE	DATE,
	L_SHIPINSTRUCT	string,
	L_SHIPMODE		string,
	L_COMMENT		string
) USING CSV LOCATION '/home/hejiahao/tpch-kit/sc1/lineitem.csv';

CREATE TABLE IF NOT EXISTS NATION (
	N_NATIONKEY		integer,
	N_NAME			string,
	N_REGIONKEY		integer NOT NULL,  -- references R_REGIONKEY
	N_COMMENT		string
) USING CSV LOCATION '/home/hejiahao/tpch-kit/sc1/nation.csv' ;

CREATE TABLE IF NOT EXISTS REGION (
	R_REGIONKEY	integer,
	R_NAME		string,
	R_COMMENT	string
) USING CSV LOCATION '/home/hejiahao/tpch-kit/sc1/region.csv';