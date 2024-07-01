-- this macro generates the SQL for a base model

{{ codegen.generate_base_model(
    source_name='citibike',
    table_name='pricing_and_plan',
    materialized='view'
) }}