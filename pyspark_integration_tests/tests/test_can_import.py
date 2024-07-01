

def test_can_import_queries():
    from src.jobs.spark_test_station_information import query_1
    assert query_1 is not None