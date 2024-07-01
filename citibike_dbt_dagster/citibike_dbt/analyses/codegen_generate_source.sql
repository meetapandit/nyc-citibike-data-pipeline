-- This macro generates yaml for a source 

{{ codegen.generate_source(
    schema_name = 'raw',
    table_names = ['trips','bike_station_information','bike_station_status'],
    name = 'citibike',
    include_database = True,
    include_schema = True 
) }}