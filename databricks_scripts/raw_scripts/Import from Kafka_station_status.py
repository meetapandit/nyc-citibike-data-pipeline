# Databricks notebook source
# MAGIC %md 
# MAGIC # Read data from Apache Kafka

# COMMAND ----------


import json
from pyspark.sql.functions import col, explode, split, struct, array, lit, udf, from_json, window, to_date, to_timestamp,current_date, array_max, to_timestamp, max, avg, round, count,row_number
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, ArrayType, DateType, BooleanType, BinaryType, LongType

# spark conf set up for optimize and copaction of files in unity catalog
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") 
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.fileSize", "134217728")
spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")

# COMMAND ----------

# MAGIC %md # Create Secrets scope

# COMMAND ----------

confluentClusterName = "de_bootcamp_cluster"
confluentBootstrapServers = "pkc-12576z.us-west2.gcp.confluent.cloud:9092"
confluentTopicName = "bike_station_status"
schemaRegistryUrl = "https://psrc-9jzo5.us-central1.gcp.confluent.cloud"
confluentApiKey = dbutils.secrets.get(scope = "de_capstone_project", key = "ConfluentClusterAPIKey")
confluentSecret = dbutils.secrets.get(scope = "de_capstone_project", key = "ConfluentClusterAPISecret")
schemaRegistryApiKey = dbutils.secrets.get(scope = "de_capstone_project", key = "ConfluentSchemaRegistryKey")
schemaRegistrySecret = dbutils.secrets.get(scope = "de_capstone_project", key = "ConfluentSchemaRegistrySecret")
deltaTablePath = "dbfs:/delta/landing/table/station_status_landing"
checkpointPath = "dbfs:/delta/landing/checkpoints/station_status_landing"
checkPointPath_stream = "dbfs:/delta/stream/checkpoints/station_status_stream"

# COMMAND ----------

# MAGIC %md # Get Schema registry id

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient
import ssl
schema_registry_conf = {
    'url': schemaRegistryUrl,
    'basic.auth.user.info': '{}:{}'.format(schemaRegistryApiKey, schemaRegistrySecret)}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# COMMAND ----------

schema = StructType([
    StructField("data", StructType([
        StructField("stations", ArrayType(StructType([
            StructField("legacy_id", StringType(), True),
            StructField("num_docks_available", IntegerType(), True),
            StructField("is_renting", IntegerType(), True),
            StructField("is_installed", IntegerType(), True),
            StructField("num_ebikes_available", IntegerType(), True),
            StructField("num_docks_disabled", IntegerType(), True),
            StructField("last_reported", LongType(), True),
            StructField("eightd_has_available_keys", BooleanType(), True),
            StructField("num_bikes_available", IntegerType(), True),
            StructField("num_bikes_disabled", IntegerType(), True),
            StructField("num_scooters_unavailable", IntegerType(), True),
            StructField("is_returning", IntegerType(), True),
            StructField("station_id", StringType(), True),
            StructField("num_scooters_available", IntegerType(), True)
        ]), True)),
    ]), True),
    StructField("last_updated", LongType(), True),
    StructField("ttl", IntegerType(), True),
    StructField("version", StringType(), True),
])

# COMMAND ----------

# MAGIC %md # Connect to kafka cluster

# COMMAND ----------

import pyspark.sql.functions as fn
from pyspark.sql.types import StringType

kafka_df = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", confluentBootstrapServers)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", confluentTopicName)
  .option("startingOffsets", "latest")
  .option("failOnDataLoss", "false")
  .load()
  .selectExpr("CAST(value AS STRING) as value", "topic", "partition", "offset", "timestamp")
  .select(from_json(col("value"),schema).alias("value"), col("topic"), col("partition"), col("offset"), col("timestamp")) 
  .withWatermark("timestamp", "30 seconds")     
)

# COMMAND ----------

station_df = kafka_df.withColumn("batch_run_date", to_date(current_date(), "yyyy-MM-dd"))\
                     .withColumn("station_exploded", explode(col("value.data.stations")))

explode_df = station_df\
               .withColumn("num_docks_disabled", col("station_exploded.num_docks_disabled"))\
               .withColumn("num_ebikes_available", col("station_exploded.num_ebikes_available"))\
               .withColumn("num_bikes_available", col("station_exploded.num_bikes_available"))\
               .withColumn("station_id", col("station_exploded.station_id"))\
               .withColumn("last_reported", col("station_exploded.last_reported"))\
               .withColumn("num_bikes_disabled", col("station_exploded.num_bikes_disabled"))\
               .withColumn("is_installed", col("station_exploded.is_installed"))\
               .withColumn("is_returning", col("station_exploded.is_returning"))\
               .withColumn("is_renting", col("station_exploded.is_renting"))\
               .withColumn("eightd_has_available_keys", col("station_exploded.eightd_has_available_keys"))\
               .withColumn("legacy_id", col("station_exploded.legacy_id"))\
               .withColumn("num_docks_available", col("station_exploded.num_docks_available"))\
               .withColumn("num_scooters_available", col("station_exploded.num_scooters_available"))\
               .withColumn("num_scooters_unavailable", col("station_exploded.num_scooters_unavailable"))\
               .groupby(window("timestamp", "10 minutes"), "station_id", "legacy_id", "num_ebikes_available",
                    "num_bikes_available", "num_bikes_disabled", "is_installed", "is_returning", "is_renting", "eightd_has_available_keys", "num_docks_available", "num_docks_disabled", "num_scooters_available", "num_scooters_unavailable", "last_reported", "batch_run_date")\
               .agg(max("value.last_updated").alias("max_last_updated"),
                    max("last_reported").alias("max_last_reported"))\
               .select(col("window.start").alias("window_start"), "station_id", "legacy_id", "num_ebikes_available",
                    "num_bikes_available", "num_bikes_disabled", "is_installed", "is_returning", "is_renting", "eightd_has_available_keys", "num_docks_available", "num_docks_disabled", "num_scooters_available", "num_scooters_unavailable", to_timestamp("max_last_reported").alias("max_last_reported"), to_timestamp("max_last_updated").alias("max_last_updated"), "batch_run_date"
                    )

# COMMAND ----------

# MAGIC %md # Connect to Snowflake

# COMMAND ----------

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

private_key_obj = open("/Volumes/de_capstone_project/default/key/rsa_key.p8","r")
private_key=private_key_obj.read()
private_key_obj.close()

key = bytes(private_key, 'utf-8')

p_key = serialization.load_pem_private_key(key, password="databricks123".encode(), backend=default_backend())

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
).replace(b"-----BEGIN PRIVATE KEY-----\n", b"") \
    .replace(b"\n-----END PRIVATE KEY-----", b"") \
        .decode("utf-8")

# COMMAND ----------

# Use dbutils secrets to get Snowflake credentials.
user = dbutils.secrets.get("snowflake-data-warehouse", "snowflakeUsername")
password = dbutils.secrets.get("snowflake-data-warehouse", "snowflakePassword")
 
sfOptions = {
  "sfUrl": "https://RCYNOVC-SX62748.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "CITIBIKE",
  "sfSchema": "RAW",
  "sfWarehouse": "DBT_WH",
  "pem_private_key": pkb, 
  "sfRole": "accountadmin"  # Optional: If you need to specify a role
}

# COMMAND ----------

def foreach_batch_function(df, epoch_id):
    df.write\
      .format("snowflake")\
      .mode("append")\
      .options(**sfOptions)\
      .option("dbtable", "CITIBIKE.RAW.BIKE_STATION_STATUS") \
      .option("streaming_stage", "CITIBIKE.RAW.BIKE_STATION_STATUS_STAGE")\
      .save()

# COMMAND ----------

streaming_query = explode_df.writeStream \
                        .format("snowflake")\
                        .outputMode("append") \
                        .trigger(processingTime="5 seconds")\
                        .option("checkpointLocation", checkPointPath_stream)\
                        .foreachBatch(foreach_batch_function) \
                        .start()
  
streaming_query.awaitTermination()

# COMMAND ----------

# MAGIC %md # Test Connection tyo Snowflake from Databricks

# COMMAND ----------

# test snowflake connectivity from spark
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
 
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
   .options(**sfOptions) \
   .option("query",  "SELECT * FROM CITIBIKE.RAW.TRIPS LIMIT 10") \
   .load()
