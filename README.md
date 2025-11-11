This is the model that was built on the snowflake_sample_data.tpch_sf1 schema. 
The data was splitted into 3 views to simulate 3 incremental transformations. You can find view definitions for dbt source in scripts/ddl.sql file.
Also for date fields were added some rolling values - to pull date ranges from 1990s to 2020s.

Typical date shifts look like timestampadd(day, 9935, O_ORDERDATE) O_ORDERDATE. You can change 9935 in the ddl.sql to the value that you need by replacing in the entire document.



There were also some changes to simulate scd and incremental loading with some text fields. You will see that during next loading all changes were reverted

For load 3 times data please review ecom_incremental_loading.sql file. There you will find source view definitions and you can manage view data for transformations.

There were added some tests, hooks and exposure into the document. In the scripts you can also see the dashboard screen build on Preset
