[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/1lXY_Wlg)

# Streaming Pipeline for NYC Bike-sharing App and Analysis of User Activity

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



**Part II: Historical trip data**

**Data:** Citi bikes also shares downloadable CSV files of historical trips which goes back to 2013 when they started the bike-share service and collect data.
Historical Citibike trip data : https://citibikenyc.com/system-data

**Objective:** Analyze user activity across all stations to find out interesting insights like 
- What is the highest trip distance taken?
- Avg trip per day/week/month? Depending on who is the audience for the visualization
- What time of the day were the trips taken
- What are the most popular stations?
  
## Tech Stack

- Real-time data analysis
  - Apache Flink for capturing real-time feed from bike stations like station id, number of docks, bikes available, bikes booked and bikes 
  disabled and partitioned by region id
  - Store into a relational database to maintain ACID properties as we don’t want  to show a booked bike as available
  Move to Apache Iceberg for analytics

- Historical Data Analysis and Visualization
  - Convert csv to Parquet files in Apache Iceberg as 
  - Parquet compresses the data and Iceberg offers pointers to last updated file which saves full file scan and time travel
  - Aggregate data for weekly and monthly analysis of rides, popular stations, average distance traveled
  - Create Dash plots for interactive visualization
    
## Conceptual Diagram

![meetapandit_conceptual_modeling drawio](https://github.com/DataExpert-ZachWilson-V4/capstone-project-meeta-p/assets/15186489/ceb466f5-b424-4c8f-8bef-9acc74212421)

**References:**
Historical Citibike trip data : https://citibikenyc.com/system-data
Realtime citi bike system data published as GBFS format. GBFS feed linked here: https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json
