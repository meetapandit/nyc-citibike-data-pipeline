import pytest
from pyspark.sql import SparkSession

def spark_session_factory(app_name: str) -> SparkSession:
  return (
      SparkSession.builder
      .master("local")
      .appName("chispa")
      .getOrCreate()
  )

@pytest.fixture(scope='session')
def spark_session():
    return spark_session_factory("")