with
    raw_bike_station_information as (
        select *
        from
            {{ source('citibike', 'bike_station_information') }}
    )

select
    window_start
    , station_id
    , longitude
    , latitude
    , name
    , region_id
    , capacity
    , last_updated_tmp
    , batch_run_date 
from
    raw_bike_station_information
