-- Snapshot to handle SCD 2 idempotent transformation for pricing_and_plan

{% snapshot scd_pricing_and_plan %}

{{
   config(
       target_schema='DEV',
       unique_key='plan_id',
       strategy='timestamp',
       updated_at='curr_date',
       invalidate_hard_deletes=True
   )
}}

select * FROM {{ source('citibike', 'pricing_and_plan') }}

{% endsnapshot %}