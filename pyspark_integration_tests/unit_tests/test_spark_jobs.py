from chispa.dataframe_comparer import *
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from ..jobs.spark_test_station_information import job_station_information
from ..jobs.spark_test_station_status import job_station_status
from collections import namedtuple
import shutil
import os

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest-spark").getOrCreate()
    yield spark
    spark.stop()


def test_station_information(spark):
    # define input schema
    station_information_input = namedtuple("station_information_input","WINDOW_START STATION_ID \
         LONGITUDE LATITUDE NAME REGION_ID CAPACITY LAST_UPDATED_TMP BATCH_RUN_DATE")
    
    # define output schema
    station_information_output = namedtuple("station_information_output","WINDOW_START STATION_ID \
         LONGITUDE LATITUDE NAME REGION_ID CAPACITY LAST_UPDATED_TMP BATCH_RUN_DATE")
    
    station_information_records = [
        station_information_input("2024-06-24 23:20:00.000",
                            "1884599469785421928",
                            "-73.9563752",
                            "40.775744",
                            "3 Ave & E 81 St",
                            "-1",
                            "47",
                            "2024-06-24 23:27:47.000",
                            "2024-06-24",
                        ),
    station_information_input("2024-06-24 23:20:00.000",
                        "66dd217d-0aca-11e7-82f6-3863bb44ef7c",
                        "-73.95866",
                        "40.73564",
                        "Franklin St & Dupont St",
                        "71",
                        "27",
                        "2024-06-24 23:27:47.000",
                        "2024-06-24" 
                        )
    ]
    station_information_df = spark.createDataFrame(station_information_records)
    station_information_df.createOrReplaceTempView("station_information_input")

    actual_df = job_station_information(spark, "station_information_input")

    expected_output = [
        station_information_output("2024-06-24 23:20:00.000",
                            "1884599469785421928",
                            "-73.9563752",
                            "40.775744",
                            "3 Ave & E 81 St",
                            "-1",
                            "47",
                            "2024-06-24 23:27:47.000",
                            "2024-06-24",
                        ),
        station_information_output("2024-06-24 23:20:00.000",
                        "66dd217d-0aca-11e7-82f6-3863bb44ef7c",
                        "-73.95866",
                        "40.73564",
                        "Franklin St & Dupont St",
                        "71",
                        "27",
                        "2024-06-24 23:27:47.000",
                        "2024-06-24" 
                        )]

    expected_df = spark.createDataFrame(expected_output)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_station_status(spark):
    # define schema of test input
    station_status = namedtuple("station_status", 
                                "WINDOW_START STATION_ID NUM_EBIKES_AVAILABLE NUM_BIKES_AVAILABLE \
                                NUM_BIKES_DISABLED IS_INSTALLED IS_RETURNING IS_RENTING \
                                EIGHTD_HAS_AVAILABLE_KEYS NUM_DOCKS_AVAILABLE NUM_DOCKS_DISABLED \
                                NUM_SCOOTERS_AVAILABLE NUM_SCOOTERS_UNAVAILABLE \
                                MAX_LAST_REPORTED MAX_LAST_UPDATED BATCH_RUN_DATE")
    station_status_output = namedtuple("station_status_output", 
                                "WINDOW_START STATION_ID NUM_EBIKES_AVAILABLE NUM_BIKES_AVAILABLE \
                                NUM_BIKES_DISABLED IS_INSTALLED IS_RETURNING IS_RENTING \
                                EIGHTD_HAS_AVAILABLE_KEYS NUM_DOCKS_AVAILABLE NUM_DOCKS_DISABLED \
                                NUM_SCOOTERS_AVAILABLE NUM_SCOOTERS_UNAVAILABLE \
                                MAX_LAST_REPORTED MAX_LAST_UPDATED BATCH_RUN_DATE")

    # Add test record to the schema created above
    station_status_input = [
        station_status(
      "2024-06-25 06:50:00.000",
      "1846086665080867236",
       "1",
       "20",
        "0",
        "1",
        "1",
        "1",
        "FALSE",
        "1",
        "0",
        "0",
        "0",
        "2024-06-25 06:54:44.000",	
        "2024-06-25 06:56:44.000",
        "2024-06-25"
        ),
        station_status(
        "2024-06-25 06:50:00.000",
        "66dcf0b3-0aca-11e7-82f6-3863bb44ef7c",
        "3",
        "9",
        "2",
        "1"	,
        "1"	,
        "1"	,
        "FALSE",
        "17",
        "0",
        "0"	,
        "0"	,
        "2024-06-25 06:52:40.000",	
        "2024-06-25 06:54:44.000",	
        "2024-06-25"
    )]
    station_status_df = spark.createDataFrame(station_status_input)
    station_status_df.createOrReplaceTempView("station_status")

    actual_df = job_station_status(spark, "station_status")

    expected_output = [
        station_status_output(
      "2024-06-25 06:50:00.000",
      "1846086665080867236",
       "1",
       "20",
        "0",
        "1",
        "1"	,
        "1" ,
        "FALSE",
        "1" ,
        "0" ,
        "0" ,
        "0"	,
        "2024-06-25 06:54:44.000" ,	
        "2024-06-25 06:56:44.000" ,
        "2024-06-25" 
        ),
        station_status_output(
        "2024-06-25 06:50:00.000",
        "66dcf0b3-0aca-11e7-82f6-3863bb44ef7c",
        "3",
        "9",
        "2",
        "1"	,
        "1"	,
        "1"	,
        "FALSE",
        "17",
        "0",
        "0"	,
        "0"	,
        "2024-06-25 06:52:40.000",	
        "2024-06-25 06:54:44.000",	
        "2024-06-25"
        )]

    expected_df = spark.createDataFrame(expected_output)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)