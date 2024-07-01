from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

import shutil
import os

def query_1(input_table_name: str) -> str:
    query = f"""
        SELECT WINDOW_START
         , STATION_ID
         , LONGITUDE
         , LATITUDE
         , NAME
         , REGION_ID
         , CAPACITY
         , LAST_UPDATED_TMP
         , BATCH_RUN_DATE
    FROM {input_table_name}
    WHERE BATCH_RUN_DATE = DATE('2024-06-24')
    """
    return query


def job_station_information(
    spark_session: SparkSession, input_table_name: str) -> Optional[DataFrame]:
    input_df = spark_session.table(input_table_name)
    input_df.createOrReplaceTempView(input_table_name)
    return spark_session.sql(query_1(input_table_name))


def main():
    input_table_name: str = "bike_station_information"
    output_table_name: str = "output_bike_station_information"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    output_df = job_station_information(spark_session, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
