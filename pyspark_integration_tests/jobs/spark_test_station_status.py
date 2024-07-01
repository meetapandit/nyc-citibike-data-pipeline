from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(input_table_name: str) -> str:
    query = f"""
    SELECT WINDOW_START
         , STATION_ID
         , NUM_EBIKES_AVAILABLE
         , NUM_BIKES_AVAILABLE
         , NUM_BIKES_DISABLED
         , IS_INSTALLED
         , IS_RETURNING
         , IS_RENTING
         , EIGHTD_HAS_AVAILABLE_KEYS
         , NUM_DOCKS_AVAILABLE
         , NUM_DOCKS_DISABLED
         , NUM_SCOOTERS_AVAILABLE
         , NUM_SCOOTERS_UNAVAILABLE
         , MAX_LAST_REPORTED
         , MAX_LAST_UPDATED
         , BATCH_RUN_DATE
    FROM {input_table_name}
    WHERE BATCH_RUN_DATE = DATE('2024-06-25')
    """
    return query

def job_station_status(spark_session: SparkSession, input_table_name: str) -> Optional[DataFrame]:
  input_df = spark_session.table(input_table_name)
  input_df.createOrReplaceTempView(input_table_name)
  return spark_session.sql(query_2(input_table_name))

def main():
    output_table_name: str = "station_status_output"
    input_table_name = "station_status"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("spark_test_station_status")
        .getOrCreate()
    )
    output_df = job_station_status(spark_session, output_table_name, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
