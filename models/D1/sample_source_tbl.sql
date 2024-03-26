/* SAMPLE code for testing abc framework */
{{ config(
    materialized = 'incremental'
) }}
/*CALLING abc macro
AND TRUNCATE THE d1 targets IN ORDER TO have fresh DATA loaded */
{{ job_log_start() }}
{{ truncate_macro() }}
/* sample model */
SELECT
    *
FROM
    {{ source(
        'RAW',
        'SAMPLE_PRODUCT_DATA'
    ) }}
