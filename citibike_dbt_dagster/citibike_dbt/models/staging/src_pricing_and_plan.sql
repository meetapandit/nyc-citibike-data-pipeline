-- pricing data related to trip

with
    raw_pricing_and_plan as (
        select *
        from
            {{ source('citibike', 'pricing_and_plan') }}
    )

select
    is_taxable
    , name
    , description
    , price
    , price_per_min
    , plan_id
    , currency
    , curr_date
from
    raw_pricing_and_plan
