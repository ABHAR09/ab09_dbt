{% macro truncate_macro() %}
{% set truncate_query %}
Truncate table {{this}};
{% endset %}
{% do run_query(truncate_query) %}
{% endmacro %}