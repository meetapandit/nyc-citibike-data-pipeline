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
