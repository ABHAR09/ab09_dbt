{% macro job_log_start() %}
/*job info extraction*/ 
{% set job_info %}
    select job_id, job_name, batch_id from
    {{source('AUDIT','JOB_CONFIG')}} 
    WHERE lower(job_name) = '{{this.name}}' 
{% endset %}
{% set results = run_query(job_info) %}
{% if execute %}
    {% set job_id = results.columns[0].values()[0] %}
    {% set job_name = results.columns[1].values()[0] %}
    {% set batch_id = results.columns[2].values()[0] %}
{% endif %}
/*batch info extraction*/
{% set batch_info %}
    select batch_run_id from
    {{source('AUDIT','BATCH_RUN_LOG')}} 
    WHERE batch_id  = {{batch_id}}
    order by batch_run_id desc
{% endset %}
{% set results = run_query(batch_info) %}
{% if execute %}
    {% set batch_run_id = results.columns[0].values()[0] %}
{% endif %}

/*Insert query for insert into job_run_log*/ 
{% set insert_query %}
INSERT INTO
    {{ source('AUDIT', 'JOB_RUN_LOG') }}
    (
        job_id,
        batch_run_id,
        source_count,
        rows_affected,
        job_status,
        error_msg,
        start_time_ts,
        end_time_ts
    )
    values 
    ( 
        {{job_id}},
        {{batch_run_id}}, 
        0,
        0,
        'STARTED',
        null,
        CURRENT_TIMESTAMP(),
        null
    )
    {% endset %}
 {% do run_query(insert_query) %}
{% endmacro %}
