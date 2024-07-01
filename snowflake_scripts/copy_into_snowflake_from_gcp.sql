CREATE STORAGE INTEGRATION copy_data_to_snowflake_v2
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://citibike-hist-data2/')

  DESC INTEGRATION copy_data_to_snowflake_v2

GRANT USAGE ON DATABASE CITIBIKE TO ROLE accountadmin;
GRANT USAGE ON SCHEMA CITIBIKE.RAW.stages TO ROLE accountadmin;
GRANT CREATE STAGE ON SCHEMA CITIBIKE.RAW.stages TO ROLE accountadmin;
GRANT USAGE ON INTEGRATION copy_data_to_snowflake TO ROLE accountadmin;


USE SCHEMA mydb.stages;

CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = CSV field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true compression = AUTO error_on_column_count_mismatch=false;

CREATE STAGE my_gcs_stage2
  URL = 'gcs://citibike-hist-data2/'
  STORAGE_INTEGRATION = copy_data_to_snowflake_v2
  FILE_FORMAT = my_csv_format;

  ALTER WAREHOUSE DBT_WH RESUME;

  LIST @my_gcs_stage2;
  
-- Check if any data exists
-- if exists then truncate
TRUNCATE TABLE CITIBIKE.RAW.TRIPS_EXT_LOAD;
SELECT *
FROM CITIBIKE.RAW.TRIPS_EXT_LOAD;


COPY INTO CITIBIKE.RAW.TRIPS_EXT_LOAD
FROM @my_gcs_stage2
PATTERN='.*csv'
ON_ERROR = 'CONTINUE';

-- TEST UPLOADED DATA

-- SELECT *
-- FROM CITIBIKE.RAW.TRIPS_EXT_LOAD
-- LIMIT 100;

-- SELECT COUNT(*)
-- FROM CITIBIKE.RAW.TRIPS_EXT_LOAD;