{%- macro databricks__generate_hash_key(filter_value=None, index=-1, exclude_list=[]) -%}
    {%- set results = get_seed_table_data(filter_value, index=index, exclude_list=exclude_list) -%}
    MD5(CONCAT(
        {%- for column_name in results -%}
            COALESCE({{ column_name }}, ''){%- if not loop.last -%},{{ '\n        ' }}{%- endif -%}
        {%- endfor -%}))
{%- endmacro -%}

{%- macro databricks__generate_series() -%}
    (SELECT row_number() OVER (ORDER BY (SELECT NULL)) AS Number FROM (SELECT * FROM range(1000)))
{%- endmacro -%}

{%- macro databricks__format_date(date_column, format) -%}
    DATE_FORMAT({{ date_column }}, '{{ format | replace("MON", "MMM") | replace("YY", "yy") }}')
{%- endmacro -%}

{%- macro databricks__cast_revenue_type(column_name) -%}
    CAST({{ column_name }} AS STRING)
{%- endmacro -%}

{%- macro databricks__format_mmm_yy(column_name) -%}
    UPPER(SUBSTRING(DATE_FORMAT({{ column_name }}, 'MMM'), 1, 3)) || '-' || RIGHT(CAST(YEAR({{ column_name }}) AS STRING), 2)
{%- endmacro -%}

{%- macro databricks__extract_date_part(part, date_expr) -%}
    EXTRACT({{ part | upper }} FROM {{ date_expr }})
{%- endmacro -%}

{%- macro databricks__get_quarter_string(date_col) -%}
    CONCAT('Q', QUARTER({{ date_col }}))
{%- endmacro -%}

{%- macro databricks__get_month(date_col) -%}
    DATE_FORMAT({{ date_col }}, 'yyyy-MM-dd')
{%- endmacro -%}

{%- macro databricks__snowball_revenue_temp_table(db_name, schema_name, table_name) -%}
    {% set create_stmt %}
        CREATE OR REPLACE TABLE {{ db_name }}.{{ schema_name }}.snowball_revenue AS
        SELECT *
        FROM {{ db_name }}.{{ schema_name }}.{{ table_name }}
        LIMIT 0
    {% endset %}

    {% do run_query(create_stmt) %}
{%- endmacro -%}

{%- macro databricks__select_snowball_revenue_temp_table() -%}
    SELECT
        {{ get_dimension() }}
    FROM {{ var('my_database') }}.{{ var('my_schema') }}.snowball_revenue
{%- endmacro -%}

{%- macro databricks__unpivot_kpis(model_ref, columns) -%}
    {%- set col_exprs = [] %}
    {%- for col in columns %}
        {%- set expr = "SELECT '" ~ col ~ "' AS kpi, CAST(" ~ col ~ " AS DECIMAL(18,2)) AS kpi_value FROM " ~ model_ref %}
        {%- do col_exprs.append(expr) %}
    {%- endfor %}
    {{ col_exprs | join('\nUNION ALL\n') }}
{%- endmacro -%}
