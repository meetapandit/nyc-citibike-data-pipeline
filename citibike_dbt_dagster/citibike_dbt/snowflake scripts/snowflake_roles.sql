/*--
In this Worksheet we are creating Roles, Warehouses, Users and permissions to roles in Snowflake.

For the User we will provide grants to a defined role and warehouse
and then walk through viewing all other users and roles in our account.

We will setup resource monitoring on the warehouses and further create table structures for data 
to be ingested in them.
--*/

-- Use an admin role to create more roles
USE ROLE ACCOUNTADMIN;

-- Use default Warehouse provided by Snowflake
USE WAREHOUSE compute_wh;

-------------------------------------------------------------------------------------------
    -- Step 1: ROLE creation
-------------------------------------------------------------------------------------------

-- Create the `DBT_ANALYTICS_ADMIN_ROLE` role
CREATE ROLE IF NOT EXISTS DBT_ANALYTICS_ADMIN_ROLE;
-- Encapsulate role to father role in this case 'ACCOUNTADMIN'
GRANT ROLE DBT_ANALYTICS_ADMIN_ROLE TO ROLE ACCOUNTADMIN;

-- Create the `DATA_ADMIN_ROLE` role
CREATE ROLE IF NOT EXISTS DATA_ADMIN_ROLE;
-- Encapsulate role to father role in this case 'ACCOUNTADMIN'
GRANT ROLE DATA_ADMIN_ROLE TO ROLE ACCOUNTADMIN;

-------------------------------------------------------------------------------------------
    -- Step 2: WAREHOUSE creation
-------------------------------------------------------------------------------------------

-- Create the 'ANALYTICS_WH' warehouse and grant usage to 'DBT_ANALYTICS_ADMIN_ROLE' role
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH;
GRANT OPERATE ON WAREHOUSE ANALYTICS_WH TO ROLE DBT_ANALYTICS_ADMIN_ROLE;

-- Create the 'ETL_WH' warehouse and grant usage to 'DATA_ADMIN_ROLE ' role
CREATE WAREHOUSE IF NOT EXISTS ETL_WH;
GRANT OPERATE ON WAREHOUSE ETL_WH TO ROLE DATA_ADMIN_ROLE;

-------------------------------------------------------------------------------------------
    -- Step 3: USER creation
-------------------------------------------------------------------------------------------

-- create user 'MEETA'
CREATE OR REPLACE USER MEETA
PASSWORD = 'Magic2024'
LOGIN_NAME = 'MEETA'
FIRST_NAME = 'Meeta'
LAST_NAME = 'Pandit'
EMAIL = 'magicmeadowss@gmail.com'
MUST_CHANGE_PASSWORD = true
DEFAULT_WAREHOUSE = ETL_WH;

-- assign role to user
GRANT ROLE DBT_ANALYTICS_ADMIN_ROLE TO USER MEETA;
GRANT ROLE DATA_ADMIN_ROLE TO USER MEETA;

-- user ABENIWAL was already created during account setup and has access to all the roles including ACCOUNTADMIN

-------------------------------------------------------------------------------------------
    -- Step 4: DATABASE and SCHEMA creation
-------------------------------------------------------------------------------------------

-- Create databases and schemas
CREATE DATABASE IF NOT EXISTS CITIBIKE;
CREATE SCHEMA IF NOT EXISTS CITIBIKE.RAW;

-- Create Schemas based on Medallion Architecture ( Bronze, Silver, Gold)
CREATE SCHEMA IF NOT EXISTS CITIBIKE.BRONZE;
CREATE SCHEMA IF NOT EXISTS CITIBIKE.SILVER;
CREATE SCHEMA IF NOT EXISTS CITIBIKE.GOLD;

-------------------------------------------------------------------------------------------
    -- Step 5: Permissions creation
-------------------------------------------------------------------------------------------

-- Set up permissions to role `DBT_ANALYTICS_ADMIN_ROLE`
GRANT ALL ON WAREHOUSE ANALYTICS_WH TO ROLE DBT_ANALYTICS_ADMIN_ROLE; 
GRANT ALL ON DATABASE CITIBIKE to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE CITIBIKE to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE CITIBIKE to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.RAW to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.RAW to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.BRONZE to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.BRONZE to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.SILVER to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.SILVER to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.GOLD to ROLE DBT_ANALYTICS_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.GOLD to ROLE DBT_ANALYTICS_ADMIN_ROLE;

-- Set up permissions to role `DATA_ADMIN_ROLE`
GRANT ALL ON WAREHOUSE ETL_WH TO ROLE DATA_ADMIN_ROLE; 
GRANT ALL ON DATABASE CITIBIKE to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE CITIBIKE to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE CITIBIKE to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.RAW to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.RAW to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.BRONZE to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.BRONZE to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.SILVER to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.SILVER to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.GOLD to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.GOLD to ROLE DATA_ADMIN_ROLE;

-------------------------------------------------------------------------------------------
    -- Step 6: RESOURCE MONITOR creation
-------------------------------------------------------------------------------------------

---> Create Resource Monitor
CREATE RESOURCE MONITOR "MONITOR_QUOTA" WITH CREDIT_QUOTA = 2
TRIGGERS
ON 90 PERCENT DO SUSPEND -- notify and suspend once execution is complete
ON 80 PERCENT DO SUSPEND_IMMEDIATE -- notify and suspend immediate without completing current execution
ON 60 PERCENT DO NOTIFY;  -- No action other then sending notification

-- Create Warehouse resource monitor notification on ANALYTICS_WH
ALTER WAREHOUSE "ANALYTICS_WH" SET RESOURCE_MONITOR = "MONITOR_QUOTA";

-- Create Warehouse resource monitor notification on ETL_WH
ALTER WAREHOUSE "ETL_WH" SET RESOURCE_MONITOR = "MONITOR_QUOTA";

-------------------------------------------------------------------------------------------
    -- Step 7: Viewing the users, roles and privileges given to them
-------------------------------------------------------------------------------------------

-- show all users in account
SHOW USERS;

-- show all roles in account
SHOW ROLES;

-- list the users to a role
SHOW GRANTS OF ROLE DBT_ANALYTICS_ADMIN_ROLE;
SHOW GRANTS OF ROLE DATA_ADMIN_ROLE;

-- list the privileges that this role has access to 
SHOW GRANTS TO ROLE DBT_ANALYTICS_ADMIN_ROLE;
SHOW GRANTS TO ROLE DATA_ADMIN_ROLE;

-------------------------------------------------------------------------------------------
    -- Step 8: Create tables in the schema
-------------------------------------------------------------------------------------------

-- Creating table 'PRICING_AND_PLAN'
CREATE OR REPLACE TABLE CITIBIKE.RAW.PRICING_AND_PLAN cluster by (curr_date)(
    IS_TAXABLE BOOLEAN,
    NAME VARCHAR(16777216),
    DESCRIPTION VARCHAR(16777216),
    PRICE FLOAT,
    PRICE_PER_MIN FLOAT,
    PLAN_ID VARCHAR(16777216),
    CURRENCY VARCHAR(16777216),
    CURR_DATE DATE
);


-- Creating table 'TRIPS'
create or replace TABLE CITIBIKE.RAW.TRIPS (
	TRIPDURATION NUMBER(38,0),
	STARTTIME TIMESTAMP_NTZ(9),
	STOPTIME TIMESTAMP_NTZ(9),
	START_STATION_ID NUMBER(38,0),
	START_STATION_NAME VARCHAR(16777216),
	START_STATION_LATITUDE FLOAT,
	START_STATION_LONGITUDE FLOAT,
	END_STATION_ID NUMBER(38,0),
	END_STATION_NAME VARCHAR(16777216),
	END_STATION_LATITUDE FLOAT,
	END_STATION_LONGITUDE FLOAT,
	BIKEID NUMBER(38,0),
	MEMBERSHIP_TYPE VARCHAR(16777216),
	USERTYPE VARCHAR(16777216),
	BIRTH_YEAR NUMBER(38,0),
	GENDER NUMBER(38,0)
);

-- Create table 'BIKE_STATION_INFORMATION'                    
create or replace TABLE CITIBIKE.RAW.BIKE_STATION_INFORMATION cluster by (batch_run_date)(
	WINDOW_START TIMESTAMP_NTZ(9),
	STATION_ID VARCHAR(16777216),
	LONGITUDE FLOAT,
	LATITUDE FLOAT,
	NAME VARCHAR(16777216),
	REGION_ID VARCHAR(16777216),
	CAPACITY NUMBER(38,0),
	LAST_UPDATED_TMP TIMESTAMP_NTZ(9),
	BATCH_RUN_DATE DATE
);

-- Create table 'BIKE_STATION_STATUS' 
create or replace TABLE CITIBIKE.RAW.BIKE_STATION_STATUS cluster by (batch_run_date)(
	WINDOW_START TIMESTAMP_NTZ(9),
	STATION_ID VARCHAR(16777216),
	LEGACY_ID NUMBER(38,0),
	NUM_EBIKES_AVAILABLE NUMBER(38,0),
	NUM_BIKES_AVAILABLE NUMBER(38,0),
	NUM_BIKES_DISABLED NUMBER(38,0),
	IS_INSTALLED NUMBER(38,0),
	IS_RETURNING NUMBER(38,0),
	IS_RENTING NUMBER(38,0),
	EIGHTD_HAS_AVAILABLE_KEYS BOOLEAN,
	NUM_DOCKS_AVAILABLE NUMBER(38,0),
	NUM_DOCKS_DISABLED NUMBER(38,0),
	NUM_SCOOTERS_AVAILABLE NUMBER(38,0),
	NUM_SCOOTERS_UNAVAILABLE NUMBER(38,0),
	MAX_LAST_REPORTED TIMESTAMP_NTZ(9),
	MAX_LAST_UPDATED TIMESTAMP_NTZ(9),
	BATCH_RUN_DATE DATE
);