-- Set up the defaults
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
USE WAREHOUSE COMPUTE_WH;

DROP DATABASE IF EXISTS ECOM CASCADE;

CREATE DATABASE ECOM;

CREATE SCHEMA IF NOT EXISTS ECOM.STAGE;
CREATE SCHEMA IF NOT EXISTS ECOM.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS ECOM.MART;
CREATE SCHEMA IF NOT EXISTS ECOM.DEV;
CREATE SCHEMA IF NOT EXISTS ECOM.RAW;

grant usage on warehouse COMPUTE_WH to role  TRANSFORM;

 grant CREATE SCHEMA, USAGE
    	on database ecom
    	to role TRANSFORM;


GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE ECOM to ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE ECOM to ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE ECOM to ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA ECOM.DEV to ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOM.DEV to ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA ECOM.STAGE to ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOM.STAGE to ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA ECOM.INTERMEDIATE to ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOM.INTERMEDIATE to ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA ECOM.MART to ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOM.MART to ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA ECOM.RAW to ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOM.RAW to ROLE TRANSFORM;
GRANT SELECT ON ALL VIEWS IN SCHEMA ECOM.RAW to ROLE TRANSFORM;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ECOM.RAW to ROLE TRANSFORM;


GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE REPORTER;
GRANT USAGE ON DATABASE AIRBNB TO ROLE REPORTER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ECOM to ROLE REPORTER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ECOM to ROLE REPORTER;
GRANT SELECT ON ALL TABLES IN SCHEMA ECOM.DEV to ROLE REPORTER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOM.DEV to ROLE REPORTER;


USE DATABASE ECOM;
USE SCHEMA RAW;



----------=======================PART==================-------------

--------------------------------2023
 create or replace view ecom.raw.part_2023 as
  select 
    CASE WHEN P_Partkey in (40003,40005) then 'None' ELSE P_BRAND END P_BRAND, 
    CASE WHEN P_Partkey in (50003,41056) then 'None' ELSE P_COMMENT END P_COMMENT,
    CASE WHEN P_Partkey in (40007,40080) then 'None' ELSE P_MFGR END P_MFGR,
    CASE WHEN P_Partkey in (40013,40004) then 'None' ELSE P_NAME END P_NAME,
    P_SIZE,
    CASE WHEN P_Partkey in (40003,40025) then 'None' ELSE P_TYPE END P_TYPE,
    P_PARTKEY,
    CASE WHEN P_Partkey in (40003,40055) then P_RETAILPRICE+0.06 ELSE P_RETAILPRICE END P_RETAILPRICE,
    CASE WHEN P_Partkey in (40003,40059) then 'None' ELSE P_CONTAINER END P_CONTAINER, 
    '2023-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts from  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;


                -------2024
create or replace view ecom.raw.part_2024 as
  select 
    CASE WHEN P_Partkey in (42003,40005) then 'None' ELSE P_BRAND END P_BRAND, 
    CASE WHEN P_Partkey in (50103,41056) then 'None' ELSE P_COMMENT END P_COMMENT,
    CASE WHEN P_Partkey in (40017,40030) then 'None' ELSE P_MFGR END P_MFGR,
    CASE WHEN P_Partkey in (40113,40504) then 'None' ELSE P_NAME END P_NAME,
   P_SIZE,
    CASE WHEN P_Partkey in (40203,44025) then 'None' ELSE P_TYPE END P_TYPE,
    P_PARTKEY,
    CASE WHEN P_Partkey in (40103,40055) then P_RETAILPRICE+0.06 ELSE P_RETAILPRICE END P_RETAILPRICE,
    CASE WHEN P_Partkey in (40303,40159) then 'None' ELSE P_CONTAINER END P_CONTAINER, 
    '2024-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts from  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;


                --------2025
create or replace view ecom.raw.part_2025 as
  select 
     P_BRAND, 
   P_COMMENT,
     P_MFGR,
     P_NAME,
    P_SIZE,
    P_TYPE,
    P_PARTKEY,
    P_RETAILPRICE,
    P_CONTAINER, 
    current_timestamp as base_ts from  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;

--select * from ecom.raw.part_2025 limit 1
    
            ----raw view
create or replace view ecom.raw.part
  as 
  select * from ecom.raw.part_2023;
  

----------=======================SUPPLIER==================-------------

            ---2023
create or replace view ecom.raw.supplier_2023 as
  select 
S_PHONE,
S_NATIONKEY,
CASE WHEN S_SUPPKEY in (40205,44625) then 'None' ELSE S_NAME END S_NAME,
CASE WHEN S_SUPPKEY in (42203,44425) then 'None' ELSE S_COMMENT END S_COMMENT,
S_SUPPKEY,
S_ACCTBAL,
CASE WHEN S_SUPPKEY in (40203,44025) then 'None' ELSE S_ADDRESS END S_ADDRESS,
'2023-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts
from snowflake_sample_data.tpch_sf1.SUPPLIER;

            ---2024
create or replace view ecom.raw.supplier_2024 as
  select 
S_PHONE,
S_NATIONKEY,
CASE WHEN S_SUPPKEY in (40205,44625) then 'None' ELSE S_NAME END S_NAME,
CASE WHEN S_SUPPKEY in (42203,44425) then 'None' ELSE S_COMMENT END S_COMMENT,
S_SUPPKEY,
S_ACCTBAL,
CASE WHEN S_SUPPKEY in (40203,44025) then 'None' ELSE S_ADDRESS END S_ADDRESS,
'2024-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts
from snowflake_sample_data.tpch_sf1.SUPPLIER;

            ---2025
create or replace view ecom.raw.supplier_2025 as
  select 
S_PHONE,
S_NATIONKEY,
S_NAME,
S_COMMENT,
S_SUPPKEY,
S_ACCTBAL,
S_ADDRESS,
current_timestamp as base_ts
from snowflake_sample_data.tpch_sf1.SUPPLIER;


            ---raw view
CREATE OR REPLACE VIEW ECOM.RAW.SUPPLIER
as select * from ecom.raw.supplier_2023;


----------=======================NATION==================-------------

    ----2023
create or replace view ecom.raw.NATION_2023 as
  select 
CASE WHEN N_NATIONKEY in (2,4) then 'None' ELSE N_COMMENT END N_COMMENT,
N_REGIONKEY,
N_NAME,
N_NATIONKEY,
'2023-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts
from snowflake_sample_data.tpch_sf1.NATION;

            ---2024
create or replace view ecom.raw.NATION_2024 as
  select 
CASE WHEN N_NATIONKEY in (12,41) then 'None' ELSE N_COMMENT END N_COMMENT,
N_REGIONKEY,
N_NAME,
N_NATIONKEY,
'2024-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts
from snowflake_sample_data.tpch_sf1.NATION;

            ---2025
create or replace view ecom.raw.NATION_2025 as
  select 
N_COMMENT,
N_REGIONKEY,
N_NAME,
N_NATIONKEY,
current_timestamp as base_ts
from snowflake_sample_data.tpch_sf1.NATION;

            ---raw view
create or replace view ecom.raw.NATION as
select * 
from ecom.raw.NATION_2023;


--=======================ORDERS ==========-
--set shift_days = 9935, replace with the value that you want in this script, this is for rolling data from 1998 to 2025;

    ----2023
create or replace view ecom.raw.ORDERS_2023 as
  select 
O_TOTALPRICE,
O_SHIPPRIORITY,
O_CLERK,
O_COMMENT,
timestampadd(day, 9935, O_ORDERDATE) O_ORDERDATE,
O_ORDERPRIORITY,
O_ORDERSTATUS,
O_CUSTKEY,
O_ORDERKEY,
'2023-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts
FROM snowflake_sample_data.tpch_sf1.ORDERS
WHERE O_ORDERDATE<='1996-12-31 23:59:38.661 -0800';

--select * from ecom.raw.ORDERS_1997 limit 10



        --2024
create or replace view ecom.raw.ORDERS_2024 as
  select 
O_TOTALPRICE,
O_SHIPPRIORITY,
O_CLERK,
O_COMMENT,
timestampadd(day, 9935, O_ORDERDATE) O_ORDERDATE,
O_ORDERPRIORITY,
O_ORDERSTATUS,
O_CUSTKEY,
O_ORDERKEY,
'2024-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts
FROM snowflake_sample_data.tpch_sf1.ORDERS
WHERE O_ORDERDATE BETWEEN '1996-12-31 23:59:38.661 -0800' AND '1997-12-31 23:59:38.661 -0800' ;


--2025
create or replace view ecom.raw.ORDERS_2025 as
  select 
O_TOTALPRICE,
O_SHIPPRIORITY,
O_CLERK,
O_COMMENT,
timestampadd(day, 9935, O_ORDERDATE) O_ORDERDATE,
O_ORDERPRIORITY,
O_ORDERSTATUS,
O_CUSTKEY,
O_ORDERKEY,
current_timestamp as base_ts
FROM snowflake_sample_data.tpch_sf1.ORDERS
WHERE O_ORDERDATE > '1997-12-31 23:59:38.661 -0800' ;

        ----------- raw view 
create or replace view ecom.raw.ORDERS as
select * 
from ecom.raw.ORDERS_2023;

--select * from ecom.raw.ORDERS limit 3
------================= LINEITEM ======---------

            -----prep


create or replace view ecom.raw.LINEITEM_prep as
  select
timestampadd(day, 9935, L_RECEIPTDATE) L_RECEIPTDATE,
L_TAX,
L_PARTKEY,
L_EXTENDEDPRICE,
L_LINESTATUS,
L_SHIPMODE,
timestampadd(day, 9935, L_COMMITDATE) L_COMMITDATE,
L_DISCOUNT,
L_SHIPINSTRUCT,
L_RETURNFLAG,
L_ORDERKEY,
L_QUANTITY,
L_LINENUMBER,
L_COMMENT,
timestampadd(day, 9935, L_SHIPDATE) L_SHIPDATE,
L_SUPPKEY
FROM snowflake_sample_data.tpch_sf1.LINEITEM
;

---select * from ecom.raw.LINEITEM_prep limit 10




        ----------- raw view 
create or replace view ecom.raw.LINEITEM as
select s.*, o.base_ts
from ecom.raw.LINEITEM_prep s join ecom.raw.orders o on s.l_orderkey = o.o_orderkey;




-- ==== PARTSUPP
CREATE OR REPLACE view ecom.raw.PARTSUPP as
SELECT *, '2023-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts from snowflake_sample_data.tpch_sf1.PARTSUPP;


--- ===REGION

CREATE OR REPLACE view ecom.raw.REGION as
SELECT *, '2023-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts from snowflake_sample_data.tpch_sf1.REGION;


        --======CUSTOMER
        ---CURRENT_DATE 2023
create or replace view ecom.raw.CUSTOMER_2023 as
  select 
C_COMMENT,
C_CUSTKEY,
C_NAME,
CASE WHEN C_CUSTKEY in (30001, 30006,30008) THEN C_ACCTBAL/2 ELSE C_ACCTBAL END C_ACCTBAL,
C_MKTSEGMENT,
C_PHONE,
C_ADDRESS,
C_NATIONKEY,
'2023-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts
from snowflake_sample_data.tpch_sf1.CUSTOMER



        --2024
create or replace view ecom.raw.CUSTOMER_2024 as
  select 
C_COMMENT,
C_CUSTKEY,
C_NAME,
CASE WHEN C_CUSTKEY in (30005, 30010,3009) THEN C_ACCTBAL/2 ELSE C_ACCTBAL END C_ACCTBAL,
C_MKTSEGMENT,
C_PHONE,
C_ADDRESS,
C_NATIONKEY,
'2024-12-31 23:59:38.661 -0800'::TIMESTAMP_LTZ(9) as base_ts
from snowflake_sample_data.tpch_sf1.CUSTOMER


        ---2025
create or replace view ecom.raw.CUSTOMER_2025 as
  select 
C_COMMENT,
C_CUSTKEY,
C_NAME,
C_ACCTBAL,
C_MKTSEGMENT,
C_PHONE,
C_ADDRESS,
C_NATIONKEY,
current_timestamp as base_ts
from snowflake_sample_data.tpch_sf1.CUSTOMER



        ----------- raw view 
create or replace view ecom.raw.CUSTOMER as
select * 
from ecom.raw.CUSTOMER_2023;

