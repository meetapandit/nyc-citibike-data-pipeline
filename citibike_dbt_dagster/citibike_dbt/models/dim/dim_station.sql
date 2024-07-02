-- creating dim_station by taking the unique start_station_id and end_station_id from trips model

{{
  config(
    materialized = 'table'
    )
}}

with
    src_trips as (

        select * from {{ ref('src_trips') }}

    )

    , unique_start_stations as (
        select
            start_station_id as station_id
            , start_station_name as name
            , start_station_latitude as latitude
            , start_station_longitude as longitude
        from src_trips
        where station_id is not null
        group by
            start_station_id
            , start_station_name
            , start_station_latitude
            , start_station_longitude
    )

    , unique_end_stations as (
        select
            end_station_id as station_id
            , end_station_name as name
            , end_station_latitude as latitude
            , end_station_longitude as longitude
        from src_trips
        where station_id is not null
        group by
            end_station_id
            , end_station_name
            , end_station_latitude
            , end_station_longitude
    )

    , combined_stations as (
        select
            coalesce(s.station_id, e.station_id) as station_id
            , coalesce(s.name, e.name) as name
            , coalesce(s.latitude, e.latitude) as latitude
            , coalesce(s.longitude, e.longitude) as longitude
        from
            unique_start_stations as s
        full outer join
            unique_end_stations as e
            on
            s.station_id = e.station_id
    ),

    aggregated_stations as (
        select
            station_id,
            max(name) as name,
            max(latitude) as latitude,
            max(longitude) as longitude
        from
            combined_stations
        group by
            station_id
    )

select
    station_id
    , name
    , latitude
    , longitude
from aggregated_stations
