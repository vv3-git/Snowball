{%- macro sqlserver__generate_hash_key(filter_value=None, index=-1, exclude_list=[]) -%}
    {%- set results = get_seed_table_data(filter_value, index=index, exclude_list=exclude_list) -%}
    {%- if results | length == 1 -%}
        {%- do results.append("''") -%}
    {%- endif -%}
    LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5',
        CONCAT(
            {%- for column_name in results -%}
                COALESCE(CAST({{ column_name }} AS VARCHAR), ''){% if not loop.last %},{{ '\n        ' }}{% endif %}
            {%- endfor -%}
        )
    ), 2))
{%- endmacro -%}

{%- macro sqlserver__generate_series() -%}
(
    SELECT TOP (1000)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Number
    FROM sys.all_objects
) AS gen
{%- endmacro -%}

{%- macro sqlserver__format_date(date_column, format) -%}
    FORMAT({{ date_column }}, '{{ format }}')
{%- endmacro -%}

{%- macro sqlserver__cast_revenue_type(column_name) -%}
    CAST({{ column_name }} AS VARCHAR)
{%- endmacro -%}

{%- macro sqlserver__format_mmm_yy(column_name) -%}
    FORMAT({{ column_name }}, 'MMM-yy')
{%- endmacro -%}

{%- macro sqlserver__extract_date_part(part, date_expr) -%}
    DATEPART({{ part | upper }}, {{ date_expr }})
{%- endmacro -%}

{%- macro sqlserver__get_quarter_string(date_col) -%}
    'Q' + CAST(DATEPART(QUARTER, {{ date_col }}) AS VARCHAR)
{%- endmacro -%}

{%- macro sqlserver__get_month(date_col) -%}
    CAST(CONVERT(DATE, {{ date_col }}, 105) AS DATE)
{%- endmacro -%}

{%- macro sqlserver__snowball_revenue_temp_table(db_name, schema_name, table_name) -%}
    {% set create_temp_table %}
        DROP TABLE IF EXISTS "{{ db_name }}"."{{ schema_name }}"."snowball_revenue";
        SELECT TOP 0 *
        INTO "{{ db_name }}"."{{ schema_name }}"."snowball_revenue"
        FROM "{{ db_name }}"."{{ schema_name }}"."{{ table_name }}";
    {% endset %}

    {% do run_query(create_temp_table) %}
{%- endmacro -%}

{%- macro sqlserver__select_snowball_revenue_temp_table() -%}
    SELECT
        {{ get_dimension() }}
    FROM "{{ var('my_database') }}"."{{ var('my_schema') }}".snowball_revenue
{%- endmacro -%}

{%- macro sqlserver__unpivot_kpis(model_ref, columns) -%}
    SELECT *
    FROM (
        SELECT
            {% for col in columns %}
              CAST({{ col }} AS DECIMAL(18, 2)) AS {{ col }}{% if not loop.last %},{% endif %}
            {% endfor %}
        FROM {{ model_ref }}
    ) AS src
    UNPIVOT (
        kpi_value FOR kpi IN (
            {{ columns | join(', ') }}
        )
    ) AS unpvt
{%- endmacro -%}
