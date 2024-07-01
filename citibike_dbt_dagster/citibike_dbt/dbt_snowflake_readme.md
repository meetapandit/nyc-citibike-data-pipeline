# Introduction and Environment Setup

## Snowflake Environment Setup Steps:

#### Step 1: Snowflake free account setup
Create Snowflake 30 days Free Trial Account from your personal email [https://signup.snowflake.com/]. For the capstone we have created Business Critical Snowflake and linked it to GCP as the cloud service platform to bring in raw data.

#### Step 2: Roles, Warehouses and Users setup
##### Role Based Access Control (RBAC) in Snowflake:
Snowflake follows roles encapsulation/ hierarchy which means ACCOUNTADMIN encapsulates all the privileges on the roles under it and so on. The role classification for this project is divided into 2 main roles DBT_ANALYTICS_ADMIN_ROLE and DATA_ADMIN_ROLE just to show how different roles can be allocated to different users in an organization. Now depending on each user’s designation in an organization the role is assigned, let’s say for example User A is assigned Data Engineer role based on their designation, similarly User B is assigned Data Analyst role to maintain data governance policies. Note that Snowflake users can hold multiple roles but only one role can be active per session for a user. 

[https://docs.snowflake.com/en/user-guide/security-access-control-overview]


##### Warehouse: 
Snowflake contains options for various warehouse sizes. It also supports default warehouse for users. Therefore, following the best practices we build warehouses based on roles to ensure appropriate access control, monitoring and resource allocation. The ANALYTICS_WH for the DBT_ANALYTICS_ADMIN_ROLE and ETL_WH for DATA_ADMIN_ROLE both the warehouses being x-small in size.

[https://docs.snowflake.com/en/user-guide/warehouses-overview]

##### Resource Monitors: 
Also created warehouse resource monitors notifications to keep a check on both the warehouses.

[https://docs.snowflake.com/user-guide/resource-monitors]


##### Users: 
Created 2 users based on the members in the team for this capstone and assigned them the roles we created earlier. As discussed earlier both the users ‘ABENIWAL’ and ‘MEETA’ have been assigned to both the roles (DATA_ADMIN_ROLE and DBT_ANALYTICS_ROLE) so that both the users can work on both the parts of the capstone project. However, in an ideal scenario it is preferred to keep separate roles for engineers and analysts to rule better data governance through roles and to maintain least privilege principle.
 
[https://docs.snowflake.com/en/user-guide/admin-user-management]

## Snowflake user creation
Copy these SQL statements into a Snowflake Worksheet, select all and execute them (i.e. pressing the play button).

```sql {#snowflake_setup}
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
-- the DEV schema will be created automatically once we run our dbt code
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

-- Set up permissions to role `DATA_ADMIN_ROLE`
GRANT ALL ON WAREHOUSE ETL_WH TO ROLE DATA_ADMIN_ROLE; 
GRANT ALL ON DATABASE CITIBIKE to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE CITIBIKE to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE CITIBIKE to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CITIBIKE.RAW to ROLE DATA_ADMIN_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CITIBIKE.RAW to ROLE DATA_ADMIN_ROLE;

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

```

## Snowflake data tables setup

Copy these SQL statements into a Snowflake Worksheet, select all and execute them (i.e. pressing the play button).

```sql {#snowflake_tables}
-- Set up the defaults
USE WAREHOUSE ETL_WH;
USE DATABASE CITIBIKE;
USE SCHEMA RAW;

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
```

# DBT installation
## Virtualenv setup and DBT installation - Mac
Here are the commands we executed to setup DBT:

```sh
mkdir citibike_dbt
cd citibike_dbt
virtualenv venv
. venv/bin/activate
pip install dbt-snowflake==1.7.1
```

## dbt setup
Initialize the dbt profiles folder on Mac/Linux:
```sh
mkdir ~/.dbt
```
Create a dbt project (all platforms):
```sh
dbt init citibike_dbt
```
To ensure everything runs well run
```sh
dbt debug
```
Install all the packages using the
```sh
dbt deps
```

# Models
So the idea is to extract src trips table, clean it, rename the columns and create dim_station and fct_station out of it. Also, create macro to do the trip price calculations and add the trip amount to fct_trips table using calculation macro.

## Code used to create src, dim and fct models

### SRC Trips 
`models/staging/src_trips.sql`:

```sql
with
    source_trips as (

        select * from {{ source('citibike', 'trips') }}

    )

    , raw_trips as (
        select *
        from source_trips
        where bikeid is not null
        -- trying to remove rows with null values
    )

select
    -- creating surrogate key for unique hash values
    {{
        dbt_utils.generate_surrogate_key([
            'starttime'
            , 'stoptime'
            , 'start_station_id'
            , 'end_station_id'
            , 'bikeid'
            , 'usertype'
            , 'birth_year'
            , 'gender'
        ])
    }} as trip_key
    , tripduration as trip_duration
    , starttime as start_time
    , stoptime as stop_time
    , start_station_id
    , start_station_name
    , start_station_latitude
    , start_station_longitude
    , end_station_id
    , end_station_name
    , end_station_latitude
    , end_station_longitude
    , bikeid as bike_id
    , membership_type
    , usertype as user_type
    , birth_year
    , gender
from
    raw_trips

```

### SRC payment_and_plan
`models/staging/src_payments_and_plan.sql`:

```sql
with
    raw_pricing_and_plan as (
        select *
        from
            {{ source('citibike', 'pricing_and_plan') }}
    )

select
    is_taxable
    , name
    , description
    , price
    , price_per_min
    , plan_id
    , currency
    , curr_date
from
    raw_pricing_and_plan
```

### DIM station 
`models/dim/dim_station.sql`:

```sql
-- creating dim_station by taking the unique start_station_id and end_station_id from trips model

{{
  config(
    materialized = 'table'
    )
}}

with
    src_trips as (

        select * from {{ ref('src_trips') }}

    )

    , unique_start_stations as (
        select
            start_station_id as station_id
            , start_station_name as name
            , start_station_latitude as latitude
            , start_station_longitude as longitude
        from src_trips
        where station_id is not null
        group by
            start_station_id
            , start_station_name
            , start_station_latitude
            , start_station_longitude
    )

    , unique_end_stations as (
        select
            end_station_id as station_id
            , end_station_name as name
            , end_station_latitude as latitude
            , end_station_longitude as longitude
        from src_trips
        where station_id is not null
        group by
            end_station_id
            , end_station_name
            , end_station_latitude
            , end_station_longitude
    )

    , combined_stations as (
        select
            coalesce(s.station_id, e.station_id) as station_id
            , coalesce(s.name, e.name) as name
            , coalesce(s.latitude, e.latitude) as latitude
            , coalesce(s.longitude, e.longitude) as longitude
        from
            unique_start_stations as s
        full outer join
            unique_end_stations as e
            on
            s.station_id = e.station_id
    ),

    aggregated_stations as (
        select
            station_id,
            max(name) as name,
            max(latitude) as latitude,
            max(longitude) as longitude
        from
            combined_stations
        group by
            station_id
    )

select
    station_id
    , name
    , latitude
    , longitude
from aggregated_stations
```

### FCT trips
`models/marts/fct_trips.sql`:

```sql
-- adding the calculated trip amount from macro to this fct_trips model


{{
  config(
    materialized = 'table'
  )
}}

with src_pricing_and_plan as (

    select * from {{ ref('src_pricing_and_plan') }}

),

    src_trips as (

        select * from {{ ref('src_trips') }}

    ),

    cte_src_trips as (
        select
            trip_key
            , trip_duration
            , start_time
            , stop_time
            , start_station_id
            , end_station_id
            , bike_id
            , membership_type
            , user_type
            , birth_year
            , gender
        from src_trips
        ),

    cte_src_pricing_and_plan as (
        select
            is_taxable
            , name
            , description
            , price
            , price_per_min
            , plan_id
            , currency
            , curr_date
        from src_pricing_and_plan
    )

select
    t.*
    , {{ cal_trip_price('p.price', 'p.price_per_min', 't.trip_duration', 2) }} as trip_amount
from
    cte_src_trips t
    cross join cte_src_pricing_and_plan p
```

Making a full-refresh:
```
dbt run --full-refresh
```
Can also run individual models using
```
dbt run -s fct_trips 
dbt run --full-refresh -s fct_trips
```

## Contents of models/sources.yml
```yaml
version: 2

sources:
  - name: citibike
    database: citibike
    schema: raw
    tables:
      - name: trips
      - name: bike_station_information
      - name: bike_station_status
      - name: pricing_and_plan
```

# Snapshots

## Snapshots for pricing_and_plan
The contents of `snapshots/scd_pricing_and_plan.sql`:

```sql
-- Snapshot to handle SCD 2 idempotent transformation for pricing_and_plan

{% snapshot scd_pricing_and_plan %}

{{
   config(
       target_schema='DEV',
       unique_key='plan_id',
       strategy='timestamp',
       updated_at='curr_date',
       invalidate_hard_deletes=True
   )
}}

select * FROM {{ source('citibike', 'pricing_and_plan') }}

{% endsnapshot %}
```

Run the snapshot using
```sh
dbt snapshot
```

# Tests

## Generic Tests
We have applied them by creating seperate yml files for models
for better clarity.
The contents of `models/staging/src_trips.yml`:

```yaml
version: 2

models:
  - name: src_trips
    description: "This table contains information about the bike trips"
    columns:

      - name: trip_key
        data_type: varchar
        description: "Unique hash value generated by creating a surrogate key"
        tests:
          - not_null

      - name: trip_duration
        data_type: number
        description: "The time taken for the entire trip in seconds"
        tests:
          - not_null
          - positive_value

      - name: start_time
        data_type: timestamp_ntz
        description: "Start time and date of the trip"

      - name: stop_time
        data_type: timestamp_ntz
        description: "Stop time and date of the trip"

      - name: start_station_id
        data_type: number
        description: "Unique id of the start station"

      - name: start_station_name
        data_type: varchar
        description: "Name of the start station for the trip"

      - name: start_station_latitude
        data_type: float
        description: "Latitude of the start station"

      - name: start_station_longitude
        data_type: float
        description: "Longitude of the start station"

      - name: end_station_id
        data_type: number
        description: "Unique id of the end station"

      - name: end_station_name
        data_type: varchar
        description: "Name of the end station for the trip"

      - name: end_station_latitude
        data_type: float
        description: "Latitude of the end station"

      - name: end_station_longitude
        data_type: float
        description: "Longitude of the end station"

      - name: bike_id
        data_type: number
        description: "Unique bike id corresponding to the trip"

      - name: membership_type
        data_type: varchar
        description: "Type of membership"

      - name: user_type
        data_type: varchar
        description: "Type of user - Customer = 24-hour pass or 3-day pass user; Subscriber = Annual Member"
        tests:
          - accepted_values:
              values: ['Customer', 'Subscriber']

      - name: birth_year
        data_type: number
        description: "Year of birth of the customer"

      - name: gender
        data_type: number
        description: "Gender of the customer- Zero=unknown; 1=male; 2=female"
        tests:
          - accepted_values:
              values: [0,1,2]
```

The contents of `models/marts/fct_trips.yml`:

```yaml
version: 2

models:
  - name: fct_trips
    description: "Fact table for Citibike Trips along with total trip amoumt"
    columns:

      - name: trip_key
        data_type: varchar
        description: "Unique hash value generated by creating a surrogate key"
        tests:
          - not_null

      - name: trip_duration
        data_type: number
        description: "The time taken for the entire trip in seconds"
        tests:
          - not_null
          - positive_value

      - name: start_time
        data_type: timestamp_ntz
        description: "Start time and date of the trip"

      - name: stop_time
        data_type: timestamp_ntz
        description: "Stop time and date of the trip"

      - name: start_station_id
        data_type: number
        description: "Unique id of the start station"
        tests:
          - relationships:
              to: ref('dim_station')
              field: station_id

      - name: end_station_id
        data_type: number
        description: "Unique id of the end station"
        tests:
          - relationships:
              to: ref('dim_station')
              field: station_id

      - name: bike_id
        data_type: number
        description: "Unique bike id corresponding to the trip"

      - name: membership_type
        data_type: varchar
        description: "Type of membership"

      - name: user_type
        data_type: varchar
        description: "Type of user - Customer = 24-hour pass or 3-day pass user; Subscriber = Annual Member"
        tests:
          - accepted_values:
              values: ['Customer', 'Subscriber']

      - name: birth_year
        data_type: number
        description: "Year of birth of the customer"

      - name: gender
        data_type: number
        description: "Gender of the customer- Zero=unknown; 1=male; 2=female"
        tests:
          - accepted_values:
              values: [0,1,2]

      - name: trip_amount
        data_type: number
        description: "Total cost of the trip in USD generated from trip_price_calc macro. It has price and price_per_min which is coming from src_pricing_and_plan model "
        tests:
          - not_null
          - positive_value
```

### Generic test for null check in all columns using macro

The contents of `macros/no_nulls_in_columns.sql`:

```sql
-- macro to check that there should be no null values in columns

{% macro no_nulls_in_columns(model) %}
    SELECT * FROM {{ model }} WHERE
    {% for col in adapter.get_columns_in_relation(model) -%}
        {{ col.column }} IS NULL OR
    {% endfor %}
    FALSE
{% endmacro %}

```
The contents of `no_nulls_in_dim_station.sql`:

```sql
-- test to check no null in all the columns of a model

{{ no_nulls_in_columns(ref('dim_station')) }}

```

### Restricting test execution to a model
```sh
dbt test --select dim_station
```
or to run test on all the models
```sh
dbt test
```

# Macros and Packages 
## Macros

The contents of `macros/positive_value.sql`
```sql
{% test positive_value(model, column_name) %}
SELECT
    *
FROM
    {{ model }}
WHERE
    {{ column_name}} < 1
{% endtest %}
```

The contents of `macros/trip_price_calc.sql`
```sql
-- macro to calculate trip price

{% macro cal_trip_price(unlock_fee, price_per_min, trip_duration_mins, scale=2) %}
    ( {{ unlock_fee }} + ( {{ price_per_min }} * ( {{ trip_duration_mins }} / 60 )) )::decimal(16, {{ scale }})
{% endmacro %}

```

## Packages
The contents of `packages.yml`:
```yaml
packages:
- package: dbt-labs/dbt_utils
  version: 1.1.1
- package: calogica/dbt_expectations
  version: 0.10.3
- package: dbt-labs/codegen
  version: 0.12.1
- package: calogica/dbt_date
  version: 0.10.1
sha1_hash: 9302591a94f5f8dda50a5c5325ff8e4cd252e569
```

## Documentation
Generate dbt http docs
```sh
dbt docs generate
dbt docs serve
```
this pops up the dbt docs server where everything can be viewed in a systematic UI.
the files that dbt is generating are under target folder
in manifest.json

## Analyses
Contains the codegen files to generate base models, yaml, base model ctes etc
[https://hub.getdbt.com/dbt-labs/codegen/latest/]

The contents of `analyses/codegen_base_model.sql`:
```sql
-- this macro generates the SQL for a base model

{{ codegen.generate_base_model(
    source_name='citibike',
    table_name='pricing_and_plan',
    materialized='view'
) }}
```

The contents of `analyses/codegen_generate_source.sql`:
```sql
-- This macro generates yaml for a source 

{{ codegen.generate_source(
    schema_name = 'raw',
    table_names = ['trips','bike_station_information','bike_station_status'],
    name = 'citibike',
    include_database = True,
    include_schema = True 
) }}
```

The contents of `analyses/codegen_model_import_ctes.sql`:
```sql
-- this macro is used to generate SQL for a given model with all references pulled up into import CTEs

{{ codegen.generate_model_import_ctes(
    model_name = 'fct_trips'
) }}
```

The contents of `analyses/codegen_model_yaml.sql`:
```sql
-- this macro generates yaml for a list of model(s)

{{ codegen.generate_model_yaml(
    model_names=['fct_trips']
) }}

```

they can be run using the 
```sh
dbt compile -s codegen_model_yaml
```

## SQLFluff
Add this file to beautify the ctes and structure the codes.
this can be run using the 
```sh
sqlfluff fix models/staging/src_trips.sql
```

## dbt Orchestration 

### Dagster

#### Set up your environment
Let's create a virtualenv and install dbt and dagster. These packages are located in [requirements.txt](requirements.txt).
```
virutalenv venv -p python3.11
pip install -r requirements.txt
```

#### Create a dagster project
Dagster has a command for creating a dagster project from an existing dbt project: 
```
dagster-dbt project scaffold --project-name dbt_dagster_project --dbt-project-dir=citibike_dbt
```

_Open [schedules.py](dbt_dagster_project/dbt_dagster_project/schedules.py) and uncomment the schedule logic._

#### Start dagster
Now that our project is created, start the Dagster server:

##### On Linux / Mac

```
cd dbt_dagster_project
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev
```

We will continue our work on the dagster UI at [http://localhost:3000/](http://localhost:3000) 

Run the materialize all to run the models.