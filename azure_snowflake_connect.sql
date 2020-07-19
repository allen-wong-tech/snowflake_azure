/*
Goal:
    Control Azure Storage Account from Snowflake Integration and Stage
    

Benefit:
    Setup Integration once and no longer need to pass credentials
    Use Snowflake RBAC to control access to Azure Storage Container
    Use Snowflake's Massively Parallel COPY INTO Statement and instant compute power for bulk loading
    Remove Azure Storage Container files after loaded into Snowflake

Snowflake Documentation
    https://docs.snowflake.com/en/user-guide/data-load-azure-config.html#option-1-configuring-a-snowflake-storage-integration

    Elitmind presentation
        https://youtu.be/jTIStJfCbdY?t=241
        
Agenda:
    Prerequisites:
        In Azure, create a storage container and populate some data in it

    List Azure Storage Container from Snowflake
    In Snowflake:
        create STORAGE INTEGRATION, FILE FORMAT, STAGE
    In Azure:
        Azure Active Directory, Storage Access Control (IAM)
    


*/




-----------------------------------------------------
--Test Driven Developement: list an Azure Storage Container from Snowflake
--This will work once this tutorial is complete
list @azure_stage_2;





-----------------------------------------------------
--create database and warehouse
    use role sysadmin;
    create database if not exists playdb;
    create warehouse if not exists playwh with warehouse_size = 'xsmall' auto_suspend = 60 initially_suspended = true;
    use schema playdb.public;










-----------------------------------------------------
--cloud storage INTEGRATION - helps you authorize Snowflake without hardcoding credentials
--To get AZURE_TENANT_ID: Azure Active Directory | Tenant ID   

//escalate to accountadmin since needed to create a integration
    use role accountadmin;

    create storage integration azure_snowflake_integration_171_2
      type = external_stage
      storage_provider = azure
      enabled = true
      azure_tenant_id = 'c3dde62b-7e49-464f-ad42-84476aa3479d'
      storage_allowed_locations = ('*');

    grant usage on integration azure_snowflake_integration_171_2 to sysadmin;






//go to AZURE_CONSENT_URL | [No need to click Consent] | Click Accept
    describe storage integration azure_snowflake_integration_171_2;







    
    
    
    
    
    
    
//Authorize Snowflake in Access Control (IAM)
    //Azure | Storage Account | Access Control (IAM)
    //Add a role assignment | Storage Blob Data Contributor
    //Select | Type in "Snowflake" and select the Snowflake app you just created | Click Save








-----------------------------------------------------
--Setup Snowflake Stage

//De-escalate role
use role sysadmin;




/*file format makes it easy to reload files of a certain type
https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#examples
*/

    create or replace file format my_csv_format_2
      type = csv
      field_delimiter = '|'
      skip_header = 1
      null_if = ('NULL', 'null')
      empty_field_as_null = true
      compression = gzip;





//url = 'azure://<storage_account>.blob.core.windows.net/<container>'
    //replace snowflake171a with your storage account
    //replace datalake with your container
create stage azure_stage_2
  storage_integration = azure_snowflake_integration_171_2
  url = 'azure://snowflake171a.blob.core.windows.net/datalake'
  file_format = my_csv_format_2;
  







//Please note it can take an hour or two for Azure to create the objects necessary for the integration
list @azure_stage_2;



----------------------------------------------------------------------------------------------------------
--CLEANUP IF YOU WANT TO RERUN
use role accountadmin;
drop integration if exists azure_snowflake_integration_171_2;
drop file format if exists my_csv_format_2;
drop stage if exists azure_stage_2;
