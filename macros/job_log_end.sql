/* 
Macro name: Job_log_end
Description: Used to update the job_run_log table once the model run is done
*/
{% macro job_log_end(results) %}
    {% if execute %}
        {% for res in results %}
        {%set run_dict = res.to_dict()%}
        /* Feching job status */
        {% set job_status = run_dict.get('status') %}
        {% if "SUCCESS" not in res.message %}
                {% set log_message = res.message %}
        {% else %}
                {% set log_message = 'null' %}
        {% endif %}
        /* Feching model name*/
        {%set model_nm = run_dict.get('node').get('name')%}
        /* Feching source count*/
        {% set source_data %}
        select count(*) from {{source(run_dict.node.sources[3][0],run_dict.node.sources[3][1])}};
        {% endset %}
        {% set source_results = run_query(source_data) %}
        {% if execute %}
        {% set source_count = source_results.columns[0].values()[0] %}
        {% endif %}
        /* Feching job info */
        {%set job_info %}
        select job_run_id, job_id,batch_run_id from {{source('AUDIT','JOB_RUN_LOG')}}
        WHERE JOB_ID = (
        select job_id from {{source('AUDIT','JOB_CONFIG')}} where lower(job_name) = '{{model_nm}}')
        ORDER BY JOB_RUN_ID DESC
        {%endset%}
        {% set run_job_info = run_query(job_info) %}
        {% if execute %}
        {% set job_run_id = run_job_info.columns[0].values()[0] %}
        {% set job_id = run_job_info.columns[1].values()[0] %}
        {% set batch_run_id = run_job_info.columns[2].values()[0] %}
        {% endif %}
        /* updating the job log run table */
        {% set update_query %}
        update {{ source('AUDIT', 'JOB_RUN_LOG') }} 
        set 
        source_count = {{source_count}},
        rows_affected = {{ res.adapter_response.get('rows_affected', 0) if res.adapter_response is defined else 0 }},
        job_status = '{{job_status}}',
        error_msg =  '{{ log_message|replace("'","''") }}'/*"*/,
        end_time_ts = current_timestamp()
        where job_id = {{job_id}} and job_run_id  = {{job_run_id}} 
        {% endset %}
        {% do run_query(update_query) %}
        /* Updating the batch run log table if the model gets failed */
        {% set quary_upd_failed %}
            UPDATE
               {{source('AUDIT','BATCH_RUN_LOG')}}
                set batch_status = 'FAILED',
                end_time_ts = CURRENT_TIMESTAMP()
            WHERE
                batch_run_id = {{ batch_run_id }}
        {% endset %}
        {% if job_status == 'failure' or job_status == 'error' %}
            {% do run_query(quary_upd_failed) %}
        {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

