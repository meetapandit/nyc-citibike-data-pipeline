-- test to check no null in all the columns of a model

{{ no_nulls_in_columns(ref('dim_station')) }}