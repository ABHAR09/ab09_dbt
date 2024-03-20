{% macro job_log_end(results) %}
    {% if execute %}
        {% for res in results %}
        {%set run_dict = res.to_dict()%}
        {% set line %}
        rows_affected = {{ res.adapter_response.get('rows_affected', 0) if res.adapter_response is defined else 0 }}
        {% endset %}
        {{ log(line, info=True) }}
        {% set job_status = run_dict.get('status') %}
        {% if "SUCCESS" not in res.message %}
                {% set log_message = res.message %}
        {% else %}
                {% set log_message = 'null' %}
        {% endif %}
        {%set model_nm = run_dict.get('node').get('name')%}
        {% set source_data %}
        select count(*) from {{source(run_dict.node.sources[2][0],run_dict.node.sources[2][1])}};
        {% endset %}
        {% set source_results = run_query(source_data) %}
        {% if execute %}
        {% set source_count = source_results.columns[0].values()[0] %}
        {% endif %}
        {%set job_info %}
        select job_run_id, job_id from {{source('AUDIT','JOB_RUN_LOG')}}
        WHERE JOB_ID = (
        select job_id from {{source('AUDIT','JOB_CONFIG')}} where lower(job_name) = '{{model_nm}}')
        ORDER BY JOB_RUN_ID DESC
         --assuming model_nm and job_name are same
        {%endset%}
        {% set run_job_info = run_query(job_info) %}
        {% if execute %}
        {% set job_run_id = run_job_info.columns[0].values()[0] %}
        {% set job_id = run_job_info.columns[1].values()[0] %}
        {% endif %}
        {{print("log_message:" ~ log_message)}}
        {{print("model_nm:" ~ model_nm)}}
        {{print("status:" ~ job_status)}}
        {{print("source_count :" ~ source_count)}}
        {% set update_query %}
        update {{ source('AUDIT', 'JOB_RUN_LOG') }} 
        set 
        source_count = {{source_count}},
        rows_affected = {{ res.adapter_response.get('rows_affected', 0) if res.adapter_response is defined else 0 }},
        job_status = '{{job_status}}',
        error_msg =  '{{ log_message|replace("'","''") }}',
        end_time_ts = current_timestamp()
        where job_id = {{job_id}} and job_run_id  = {{job_run_id}} 
        {% endset %}
        {% do run_query(update_query) %}
        {% endfor %}
    {% endif %}
{% endmacro %}

