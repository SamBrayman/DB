-- This script creates each of the TPCH tables using the SQL 'create table' command.
drop table if exists part;
drop table if exists supplier;
drop table if exists partsupp;
drop table if exists customer;
drop table if exists orders;
drop table if exists lineitem;
drop table if exists nation;
drop table if exists region;

-- Notes:
--   1) Use all lowercase letters for table and column identifiers.
--   2) Use only INTEGER/REAL/TEXT datatypes. Use TEXT for dates.
--   3) Do not specify any integrity contraints (e.g., PRIMARY KEY, FOREIGN KEY).

-- Students should fill in the followins statements:

create table part (
    partkey INTEGER, name TEXT, mfgr TEXT, brand TEXT, type TEXT, size INTEGER, container TEXT, retailprice REAL, comment TEXT 
);

create table supplier (
    suppkey INTEGER, name TEXT, address TEXT, nationkey INTEGER, phone TEXT, acctbal REAL, comment TEXT
);

create table partsupp (
    partkey INTEGER, suppkey INTEGER, availqty INTEGER, supplycost REAL, comment TEXT
);

create table customer (
    custkey INTEGER, name TEXT, address TEXT, nationkey INTEGER, phone TEXT, acctbal REAL, mktsegment TEXT, comment TEXT
);

create table orders (
    orderkey INTEGER, custkey INTEGER, orderstatus TEXT, totalprice REAL, orderdate TEXT, orderpriority TEXT, clerk TEXT, shippriority INTEGER, comment TEXT
);

create table lineitem (
    orderkey INTEGER, partkey INTEGER, suppkey INTEGER, linenumber INTEGER, quantity REAL, extendedprice REAL, discount REAL, tax REAL, returnflag TEXT, linestatus TEXT, shipdate TEXT, commitdate TEXT, receiptdate TEXT, shipinstruct TEXT, shipmode TEXT, comment TEXT 
);

create table nation (
    nationkey INTEGER, name TEXT, regionkey INTEGER, comment TEXT
);

create table region (
    regionkey INTEGER, name TEXT, comment TEXT
);
