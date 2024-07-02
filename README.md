[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/1lXY_Wlg)

# Streaming Pipeline for NYC Bike-sharing App and Analysis of User Activity

## Team members: Aayushi Beniwal, Meeta Pandit

## Objective: 
As part of the intensive Data Engineering boot camp at DataExpert.io led by Zach Wilson, we designed a real-time streaming app and historical trend analysis of NYC Citibike's bike-share data for our capstone submission. We are thrilled to present the final product and our approach and design decisions we took along the way to make the design simple and intuitive yet utilizing modern data engineering practices. Please follow along as we walk you through the step-by-step process of achieving our final product.

## Motivation: 
As a nature lover and adventure enthusiast, we strongly believe in sustainable and clean energy resources that are good for us and the planet. Thus the GBFS dataset intrigued us as it is an amazing initiative to make shared mobility data available from more than 50+ countries in open data format. This is a great example of how we can leverage the power of technology to solve real-world problems.
Combining technology and a keen interest in doing my part to reduce carbon footprint by promoting sustainable modes of commute we decided to architect a streaming application to track real-time changes happening at the different stations using the Citibike NYC data feeds. To double down on the carbon footprint reduction we also made conscious design choices like minimizing storage of large-scale datasets and using cloud storage effectively by configuring tight retention policies, optimizing data processing techniques by introducing mico-batch processing, and preventing large-scale processing caused by shuffling of data in Spark executors and by overwhelming executors to process more data by consuming more compute resources.

## Scope: 
- The project comprises 3 parallel processes
    - Real-time Streaming Pipeline using Kafka, Databricks and Snowflake
    - Historical Trend Analysis using DBT and Snowflake
    - CI/CD using Dagster
 
 ![0_FYLioK-JRjvQW3iQ](https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/19ff336a-7c77-4452-9744-99e5b5391156)

                              Fig 1. Data Pyramid showing bottom-up stages of a Data Pipeline (Google images)

The processes have the following stages of data architecture:
- Data Collection
- Data Preparation
- Data Transformation
- Data Validation
- Data Storage
- Data Visualization

**Part I: Real-time Streaming Pipeline using Kafka, Databricks and Snowflake**

- [Medium Article Part 1 Streaming Pipeline using Kafka, PySpark, Snowflake](https://medium.com/@meeta.pandit890/end-to-end-pyspark-databricks-kafka-dbt-snowflake-and-dagster-etl-pipeline-part-i-14dd7d91c4e1)

- [Medium Article Part 2 Historical Trend Analysis using DBT, Dagster and Snowflake](https://medium.com/@aayushi.beniwal/end-to-end-pyspark-databricks-kafka-dbt-snowflake-and-dagster-etl-pipeline-part-ii-fd923bb35e4d)

- [Kaggle dataset](https://www.kaggle.com/datasets/aayushibeniwal/citibike-new-york-data)

## High-level Architecture Diagram

![streaming_data_architecture](https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/2ef7cc19-ee68-4c68-94d2-3d3317d5346c)

## Tech Stack and Config Details

- Data Sources: GBFS data feed for real-time data access
      - Station_Status: [https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json](https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json)
      - Station_Information: [https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json](https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json)

- Data Staging: Apache Kafka for distributed, fault-tolerant message queuing to retain messages even after consumption, unlike others like RabbitMQ which deletes messages immediately after consumption. Kafka is reliable and offers message delivery guarantees and idempotency through config settings. We used a Kafka cluster managed by Confluent (the company founded by the creators of Kafka)
      - Set up Kafka Cluster in Confluent:
  
<img width="791" alt="kafka_cluster" src="https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/9337c393-67ef-4277-9f4d-4077cc1bad91">

      - Create topics:
      
<img width="1428" alt="kafka_topics" src="https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/527d707c-fe2f-4491-a1fa-cde0392dca2d">

- Data Transformation: Pyspark in Databricks hosted on GCP. Databricks is a managed Spark cluster environment that uses compute resources from GCP. The Databricks-managed Spark cluster can achieve higher parallelism using 1 driver and dynamically adding executors from 1 to 8.
      - Create a Databricks account and file systems
  
  <img width="1432" alt="cloud_storage_unity_catalog_buckets" src="https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/89653b3a-3b36-4fda-b1a3-ef0139c928f3">

      - Create Workspace

  <img width="1432" alt="create_workspace" src="https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/562c3356-64c5-4946-b621-c4903fa94ae2">

      - Cluster Setup
  
<img width="1432" alt="cluster_created" src="https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/deffd37d-93fe-4136-9794-30f04780ea7b">

- Data validation: Spark input and output Schema are validated using Chispa framework.
     - It is a Python library that provides fast PySpark test helper methods that output descriptive error messages. It is used for integration testing of various Pyspark modules for generating high- 
       quality Pyspark code.
     - The Chispa library checks for a couple of things, for example,
     - Column equality using chispa.column_comparer package
     - Dataframe equality using from chispa.dataframe_comparer
     - It takes sample input columns and the expected output columns and validates the expected results with the actual results acquired by running the module that is being tested.
     - Please find the script for setting up Chispa and running pytest in Github repo in **pyspark_integration_tests

<img width="1224" alt="tests_using_chispa" src="https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/852e56ea-06c7-40c5-920a-ab8b16ce18d4">

- Data Storage: Selected Snowflake for persistent storage as we wanted interoperability with historical data that was modeled in Snowflake. Secondly, it also has Streamlit integrated for interactive visualizations and AI/ML
     - Created Snowflake Account
     - Created a default Snowflake account with any cloud provider of your choice. Snowflake offers $400 credits for free for personal use
     - Created 2 tables in Snowflake for storing the data flowing from station_status and station_information topics from Kafka
     - The Create table scripts can be found in Databricks-Snowflake scripts in GitHub repo

# Streaming Pipeline Details

![micro_batch_architecture](https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/12166992-a041-4219-83cf-2e42833becdf)

- Data Collection
     - NYC Citibike is part of this standard and they share real-time data about Citibike's locations and stations across NYC in GBFS standard format of JSON feeds. There are more than 10+ feeds GBFS 
       shares but we have referred to 2 which are updated real-time with a time interval of not more than 5 minutes from the last data point from same location and service
     - We referred to the following 2 real-time data feeds which are termed as required for every service sharing the data with GBFS:
      - Station_Status: [https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json](https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json)
      - Station_Information: [https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json](https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json)

- Data Preparation
      - The 2 data feeds shared by NYC Citibike under GBFS standards are JSON API with open data contract collecting data from 1500+ bike stations across NYC every minute.
      - Data Volume Estimates:
           - 1500 * 1440 (total minutes per day) = 2,160,000 rows/ day
           - The estimated row size of station_status is 134 KB. This is calculated taking into account the datatypes of each column and finding the number of blocks of memory required to store each column              in bytes and convert it into KB
           - Multiply total rows * size of each row: 2.16MM * 134 KB = 289.44 MB
           - Each table stores 289 MB of data daily, continuously growing as more stations are launched and more bikes and docks are dispatched
           - To efficiently serve the velocity and volume of data I chose Apache Kafka as an intermediate storage layer and selected Confluent's managed Kafka cluster as a staging layer
           - Created 2 topics one for each API feed station_status and station_information with default 6 partitions with the key as null and value as the incoming message
           - As the key is null, Kafka selects a default partitioner to split the message into topic partitions based on a round-robin approach
           - This way the 2 API feeds are producing data for 2 topics in Kafka

- Data Transformation
      - Kafka acts as a transient storage and it is recommended to have a retention policy of upto 7 to 14 days until the data is moved to a lakehouse where it can be stored as-is without applying any              business logic.
      - Selected Databricks as the lakehouse and transformation layer using Delta tables as lakehouse and spark for data processing. Databricks is founded by the creators of Apache Spark and has native     
        support for Spark
      - Used dataframes with Spark structured streaming for consuming the real-time messages from Kafka for processing in Spark
      - Created a notebook and connected to Kafka cluster and subscribed to the 2 topics
      - Both data feeds were JSON APIs and had nested objects with lists of dictionaries
      - Thus, each record was a dictionary of key-value pairs with keys being the column names and values being the column values
      - The following StructType defined the schema for the messages (values) flowing through Kafka along with Kafka schema

<img width="1155" alt="read_from_kafka_message_schema" src="https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/1e80e0d7-726c-494d-9379-5ff811942671">

      - The transformation was done by exploding the outer stations list which created rows of key-value pairs
      - Now, these rows could be flattened by accessing the keys and creating columns for each key in the map
      - As the incoming rate of events was < 1 min per message from every station there was an opportunity to store only the messages with the greatest timestamp using the last_updated field

<img width="1194" alt="read_from_kafka_explode_struct_type" src="https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/39450eec-671b-4fc7-b6a4-37c5fa4f0016">

      - As the data source was streaming it was important to create micro-batches to prevent overwhelming the executors.
      - Processed the incoming streaming data in micro-batch windows of 10 minutes which forms batches of data before sending data to executors for processing.
      - This way the Spark resource manager gets time between incoming messages and can manage data distribution of work among executors
      - Added batch_run_date which would act as a partition key to optimize retrieval and query performance when filtered against the date field

 - Data Validation
      - Data Quality is the most critical step in any data architecture and it is even more critical and challenging to achieve in a real-time data streaming pipeline as the job is always running and if            important decision decisions about failure recovery, idempotency, load balancing, and data contracts. We ensured the following
        
           - Failure Recovery: is managed by configuring a replication factor of 3 for Lafka brokers and acks which denotes message delivery guarantees are set to all with a trade-off of higher latency vs a guaranteed receipt that the message is delivered to all in-sync replicas.
           - On the Databricks side, failure is handled using checkpointing and late-arriving events are handled by watermark.        

- Idempotent Pipelines:
	    - From Kafka 3.0, idempotence is set to true which supports exactly-once semantics which guarantees that no rows are missing or duplicated in the sink after recovering from failure.
            - This is otherwise a huge concern for streaming pipelines as non-idempotent pipelines create silent failures that are not caught easily during backfilling. These uncaught records can cause 
               cascading failures in downstream pipelines as it does not cause any alarm bells to ring and bad data can reach production without notice
  - Data Contracts:
	    - Kafka guarantees data contracts by storing a unique identifier for each schema in the schema registry
            - This schema id is part of the metadata of the message subscribed to by the consumer application
            - Thus if the schema at the data source changes without warning Kafka compares the new schema with the existing schema, adds a new schema to the schema registry and the records with the 
              changed schema can be handled by the client application using techniques like Dead Letter (DL) queue and the pipeline runs without failure
            - The delta lakehouse architecture which uses unity catalog for file metadata also supports schema evolution
    
    - Data Storage
	    - Selected Snowflake for storage as it also offers support for Spark for analysts to explore the datasets and also has Streamlit integrated into the platform which allows creating interactive web applications with ease
            - Created 2 tables and partitioned them by batch_run_date.
            - Created a private key. Follow the link to configure private key and to set private key in Snowflake follow this Youtube link
       
       <img width="1179" alt="create_private_key_snowflake" src="https://github.com/DataExpert-ZachWilson-V4/capstone-citibike-nyc-data-pipeline/assets/15186489/40b1b189-fb8c-4001-8c2c-80424b6ff06d">
       
            - Created a for_each_batch function to write the micro-batches to the tables
      
      <img width="1179" alt="snowflake_conn_options_and_foreachBatch" src="https://github.com/DataExpert-ZachWilson-V4/capstone-citibike-nyc-data-pipeline/assets/15186489/d21e6701-6bc3-4add-a4fc-cfdcd21d742b">

            - The challenge was without foreachBatch() the micro-batches were not utilized as the writeStream() was not using the micro-batches
            - The for_each_batch function reduced the load on the executors and effectively processed each micro-batch

<img width="1179" alt="write_stream_snowflake" src="https://github.com/DataExpert-ZachWilson-V4/capstone-citibike-nyc-data-pipeline/assets/15186489/01575f89-a11e-4c45-a23b-e393976ed2b5">

- Data Visualization

  <img width="1179" alt="streamlit_viz_map_stations" src="https://github.com/DataExpert-ZachWilson-V4/capstone-citibike-nyc-data-pipeline/assets/15186489/8982365a-fe18-4009-9779-a0cd31aeed5d">

             - Snowflake offers support to Streamlit which is a Python library for interactive web application
             - The audience for this dashboard was intended to manage the distribution of bikes across all the stations and ensure that the most popular bike stations have enough bikes with adequate power 
               in all bikes at the busiest times of the day.

<img width="1402" alt="streamlit_dist_of_ebikes_by_stations" src="https://github.com/DataExpert-ZachWilson-V4/capstone-citibike-nyc-data-pipeline/assets/15186489/66fe1aff-ce98-477c-bea0-cc832337b50c">

             - This operational dashboard can be paired with the analytics dashboard showing descriptive statistics to get insights from historical data on seasonality to increase or reduce bikes at a 
               station when required.

**Part II: Historical trip data**

**Data:** Citi bikes also shares downloadable CSV files of historical trips which goes back to 2013 when they started the bike-share service and collect data.
Historical Citibike trip data : https://citibikenyc.com/system-data

**Objective:** Analyze user activity across all stations to find out interesting insights like 
- What is the highest trip distance taken?
- Avg trip per day/week/month? Depending on who is the audience for the visualization
- What time of the day were the trips taken
- What are the most popular stations?
    
## Conceptual Diagram

![meetapandit_conceptual_modeling drawio](https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/ceb466f5-b424-4c8f-8bef-9acc74212421)

**References:**
Historical Citibike trip data : https://citibikenyc.com/system-data
Realtime citi bike system data published as GBFS format. GBFS feed linked here: https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json

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


### Future Scope
1) It’s considered best practice to follow Medallion Architecture ( Delta Architecture ) to organize data in three layers/schemas — Bronze, Silver, Gold. We can represent these layers in the form of Snowflake Schemas in dev and prod in future.
2. Implement the CI/CD using the Dagster Prod to replicate the production environment.
3. In the interest of time, I used managed services for Kafka, Spark and Snowflake that can be an overhead to manage cloud bills
4. So as a next step, I will be replacing these services with Docker and scaling it to use only a single cloud offering using the Kubernetes cluster
5. As this is a prototype version, I will move away from Databricks managed file system to mounting volume in external storage for more secure access to data and credentials
6. Integrate Real-time and historical data in Streamlit to combine real-time insights with historical trends to provide better customer service and elevate user experience
