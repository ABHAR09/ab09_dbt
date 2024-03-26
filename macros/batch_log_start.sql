/* 
Macro name: batch_log_start 
Description: Input paramaeter is the name of the batch, 
will make an insert into batch_run_log based on the previous  batch status
*/
{% macro batch_log_start(batch_nm) %}
/* Extraction of batch_id */
{% set batch_query %}
SELECT
  batch_id
FROM
  {{source('AUDIT','BATCH_CONFIG')}} 
WHERE
  batch_name = '{{batch_nm}}'
 {% endset %}
  {% set results = run_query(batch_query) %}
  {% if execute %}
    {% set batch_id = results.columns [0].values() [0] %}
  {% endif %}
/* Extraction of latest batch_run details */
{% set batch_run_query %}
SELECT
  batch_status
FROM
  {{source('AUDIT','BATCH_RUN_LOG')}} 
WHERE
  batch_id = {{batch_id}}
 {% endset %}
  {% set results_1 = run_query(batch_run_query) %}
  {% if execute %}
    {% set batch_status = results_1.columns [0].values() [0] %}
  {% endif %}

/* Setting up the insert statement into a set variable quary*/
{% set query %}
INSERT INTO
   {{source('AUDIT','BATCH_RUN_LOG')}} 
  (
    batch_id,
    batch_status,
    start_time_ts,
    end_time_ts
  )
VALUES
  ({{ batch_id }},'RUNNING',CURRENT_TIMESTAMP(),null)
{% endset %}

/* check for condition */
{% if batch_status == "COMPLETED" %}
    {% do run_query(query) %}
{% endif %}

{% if batch_status == "RUNNING" %}
    {{ exceptions.raise_compiler_error(
      "ABC_ERROR The batch_status of " + batch_nm + " is OPENED, please check BATCH_CONFIG table "
    ) }}
{% endif %}
/* run the batch query */
{% do run_query(query) %}
{% endmacro %}





