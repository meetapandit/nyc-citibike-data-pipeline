-- adding the calculated trip amount from macro to this fct_trips model


{{
  config(
    materialized = 'table'
  )
}}

with src_pricing_and_plan as (

    select * from {{ ref('src_pricing_and_plan') }}

),

    src_trips as (

        select * from {{ ref('src_trips') }}

    ),

    cte_src_trips as (
        select
            trip_key
            , trip_duration
            , start_time
            , stop_time
            , start_station_id
            , end_station_id
            , bike_id
            , membership_type
            , user_type
            , birth_year
            , gender
        from src_trips
        ),

    cte_src_pricing_and_plan as (
        select
            is_taxable
            , name
            , description
            , price
            , price_per_min
            , plan_id
            , currency
            , curr_date
        from src_pricing_and_plan
    )

select
    t.*
    , {{ cal_trip_price('p.price', 'p.price_per_min', 't.trip_duration', 2) }} as trip_amount
from
    cte_src_trips t
    cross join cte_src_pricing_and_plan p