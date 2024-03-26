/* Macro name : batch_log_end
   Description: Used to update the batch_run_log
*/
{% macro batch_log_end(batch_nm) %}
{#/* Extraction of batch_id */#}
{% set batch_query %}
SELECT
  batch_id
FROM
  {{source('AUDIT','BATCH_CONFIG')}} 
WHERE
  batch_name = '{{batch_nm}}' /* Assuming batch name is provided in run macro command */
 {% endset %}
  {% set results = run_query(batch_query) %}
  {% if execute %}
    {% set batch_id = results.columns [0].values() [0] %}
  {% endif %}

/* Extracting batch run info */
{% set batch_run_query %}
SELECT
    batch_run_id,batch_status
FROM
      {{source('AUDIT','BATCH_RUN_LOG')}} 
WHERE
    batch_id = {{batch_id}}
    and end_time_ts is null
ORDER BY
    batch_run_id DESC {% endset %}
{% set results_1 = run_query(batch_run_query) %}
    {% if execute %}
        {% set batch_run_id = results_1.columns [0].values() [0] %}
        {% set batch_status = results_1.columns [1].values() [0] %}
    {% endif %}
/* Updating batch run log table */
{% if batch_status == 'RUNNING' %}
        {% set update_status_upd %}
    UPDATE
         {{source('AUDIT','BATCH_RUN_LOG')}} 
        set batch_status = 'COMPLETED',
        end_time_ts = CURRENT_TIMESTAMP()
    WHERE
        batch_id = {{ batch_id }}
        AND batch_run_id = {{ batch_run_id }}
        {% endset %}
        {% do run_query(update_status_upd) %}
{% endif %}
{% endmacro %}