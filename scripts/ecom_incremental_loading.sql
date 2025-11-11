--PLEASE EXECUTE FOR 2023 FIRST, DURING NEXT DBT RUN -- FOR 2024 AND AFTER THAT - FOR 2025. YOU WILL GET 3 LOADINGS


--=========================== 2023
create or replace view ecom.raw.part
  as 
  select * from ecom.raw.part_2023;


  CREATE OR REPLACE VIEW ECOM.RAW.SUPPLIER
as select * from ecom.raw.supplier_2023;


create or replace view ecom.raw.NATION as
select * 
from ecom.raw.NATION_2023;




create or replace view ecom.raw.ORDERS as
select * 
from ecom.raw.ORDERS_2023;


create or replace view ecom.raw.CUSTOMER as
select * 
from ecom.raw.CUSTOMER_2023;


--=========================== 2024
create or replace view ecom.raw.part
  as 
  select * from ecom.raw.part_2024;


  CREATE OR REPLACE VIEW ECOM.RAW.SUPPLIER
as select * from ecom.raw.supplier_2024;


create or replace view ecom.raw.NATION as
select * 
from ecom.raw.NATION_2024;

create or replace view ecom.raw.ORDERS as
select * 
from ecom.raw.ORDERS_2024;





create or replace view ecom.raw.CUSTOMER as
select * 
from ecom.raw.CUSTOMER_2024;




--=========================== 2025
create or replace view ecom.raw.part
  as 
  select * from ecom.raw.part_2025;


  CREATE OR REPLACE VIEW ECOM.RAW.SUPPLIER
as select * from ecom.raw.supplier_2025;


create or replace view ecom.raw.NATION as
select * 
from ecom.raw.NATION_2025;

create or replace view ecom.raw.ORDERS as
select * 
from ecom.raw.ORDERS_2025;





create or replace view ecom.raw.CUSTOMER as
select * 
from ecom.raw.CUSTOMER_2025;
select * from ecom.raw.nation