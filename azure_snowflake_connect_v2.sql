/*
We connect to an Azure Stage 
Then export from shared snowflake_sample_data to stage
Finally import back into Snowflake


*/



use role sysadmin;
create database if not exists util;
use schema util.public;

create warehouse if not exists play_wh with warehouse_size = 'xsmall' auto_suspend = 1 initially_suspended = true;
use warehouse play_wh;

//CREATE STAGE "UTIL"."PUBLIC".azure171_stg URL = 'azure://azure171.blob.core.windows.net/azure171container' CREDENTIALS = (AZURE_SAS_TOKEN = '****************************************************************************************************************************************************************');

ls @util.public.azure171_stg;

show stages;

create schema if not exists tpcds;

    
  --CHANGE THE SOURCE TABLE AS NECESSARY
  --We use the shared dataset in all Snowflake accounts 
  --https://docs.snowflake.com/en/user-guide/sample-data.html
  create or replace view tpcds.source_vw as
  select *
  from snowflake_sample_data.tpcds_sf10tcl.customer;

  select top 3000 * from tpcds.source_vw;
  select count(*) from tpcds.source_vw;

--reset demo by dropping all files in specified stage
  ls @util.public.azure171_stg;
  remove @util.public.azure171_stg;

--create empty destination table
  drop table if exists tpcds.tpcds_target;
  
  --transient table is great for staging & ELT use-cases since we don't need time travel
  create transient table tpcds.tpcds_target as 
      select *
      from tpcds.source_vw limit 0;
      
  select top 300 * from tpcds.tpcds_target;
      

--size up to save time and get more parallel operations
    --xsmall small medium large xlarge x2large x3large x4large
    --notice will create 32 files since a medium is 4 times larger than the 8 threads of an xsmall
      alter warehouse play_wh set warehouse_size = 'medium';


-----------------------------------------------------
--copy into <stage> will UNLOAD from Snowflake
    copy into @util.public.azure171_stg/tpcds/load from 
        (select * from tpcds.source_vw)
        max_file_size = 262144000   //250MB
        overwrite = true
//        file_format = (type = parquet);
        file_format = (type = csv field_optionally_enclosed_by='"');
        
    
    --verify files unloaded @ = stage
    ls @util.public.azure171_stg/tpcds/;


    --we can always peer into a file
    select top 30 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        from @util.public.azure171_stg/tpcds/load_0_0_0.csv.gz;
        
        
    --verify empty and we want to load it back in    
    select top 3000 * from tpcds.tpcds_target;

-----------------------------------------------------
--copy those files back into Snowflake
    --notice inline file_format 
    copy into tpcds.tpcds_target 
    from @util.public.azure171_stg/tpcds/ 
    file_format = (type = csv
        field_optionally_enclosed_by='"'        //double-quote strings 
        replace_invalid_characters = TRUE       //Snowflake supports UTF-8 characters
    );

--size down when done to save credits
    alter warehouse play_wh set warehouse_size = 'xsmall';






-----------------------------------------------------
--verify target table
    select top 3000 * from tpcds.tpcds_target;



--count will match what we unloaded earlier
    select count(*), 'source' location
    from tpcds.source_vw
        union all
    select count(*), 'target' location
    from tpcds.tpcds_target;


