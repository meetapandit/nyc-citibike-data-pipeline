-- macro to calculate trip price

{% macro cal_trip_price(unlock_fee, price_per_min, trip_duration_mins, scale=2) %}
    ( {{ unlock_fee }} + ( {{ price_per_min }} * ( {{ trip_duration_mins }} / 60 )) )::decimal(16, {{ scale }})
{% endmacro %}
