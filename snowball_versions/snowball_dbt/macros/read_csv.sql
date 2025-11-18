-- Macro to read CSV and return specific columns based on filter criteria
{%- macro get_dimension(filter_value=None, index=-1, exclude_list=[]) -%}
    {%- set results = get_seed_table_data(filter_value=filter_value, index=index, exclude_list=exclude_list) -%}
    {%- for column_name in results -%}
        {%- if index == -1 -%}
            {%- if loop.first -%}
                {{- column_name[0] ~ ' AS ' ~ column_name[1] -}}
            {% else %}
                {{- '\n   , ' ~ column_name[0] ~ ' AS ' ~ column_name[1] -}}
            {% endif %}
        {% else %}
            {% if loop.first %}
                {{- column_name -}}
            {% else %}
                {{- '\n   , ' ~ column_name -}}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}

--Helper function that actually reads seed file and return specific columns based on filter criteria for get_dimension macro
{%- macro get_seed_table_data(filter_value=None, index=-1, exclude_list=[]) -%}
    {%- set site_array -%}
        SELECT * FROM {{ ref('column_mapping') }} 
        {% if filter_value is not none -%}
           WHERE dimension = '{{ filter_value | lower }}'
        {%- endif -%}
    {%- endset -%}

    {%- if execute -%}
        {%- set query_results = run_query(site_array) -%}
        {%- set col_value = query_results.columns[index].values() if index != -1 else query_results.rows -%}
        {%- set results = [] -%}
        {%- for ele in col_value -%}
            {%- if ele not in exclude_list -%}
                {%- do results.append(ele) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do return(results) -%}
    {%- else -%}
        {%- do return([]) -%}
    {%- endif -%}
{%- endmacro -%}

-- macros/pre_run_setup.sql
-- To create temporary tables or perform setup tasks before main operations
{% macro pre_run_setup(db_name, schema_name, table_name) %}
  {{ log("Running pre-run setup...", info=true) }}
    {{ snowball_revenue_temp_table(db_name, schema_name, table_name) }}
  {{ log("Pre-run setup completed", info=true) }}
{% endmacro %}