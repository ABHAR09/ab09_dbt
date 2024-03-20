{{ config(
    materialized = 'incremental'
)
}}

{{job_log_start()}}
{{truncate_macro()}}
SELECT * FROM 
{{source('RAW','SAMPLE_PRODUCT_DATA')}}
