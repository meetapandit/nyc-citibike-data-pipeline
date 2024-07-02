-- this macro is used to generate SQL for a given model with all references pulled up into import CTEs

{{ codegen.generate_model_import_ctes(
    model_name = 'fct_trips'
) }}