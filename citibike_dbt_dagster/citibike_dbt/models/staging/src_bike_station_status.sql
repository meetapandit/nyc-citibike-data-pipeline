with
    raw_bike_station_status as (
        select *
        from
            {{ source('citibike', 'bike_station_status') }}
    )

select
    window_start
    , station_id
    , legacy_id
    , num_ebikes_available
    , num_bikes_available
    , num_bikes_disabled
    , is_installed
    , is_returning
    , is_renting
    , eightd_has_available_keys
    , num_docks_available
    , num_docks_disabled
    , num_scooters_available
    , num_scooters_unavailable
    , max_last_reported
    , max_last_updated
    , batch_run_date

from
    raw_bike_station_status
