{%- macro snowflake__generate_hash_key(filter_value=None, index=-1, exclude_list=[]) -%}
    {%- set results = get_seed_table_data(filter_value, index=index, exclude_list=exclude_list) -%}
    MD5(CONCAT(
        {%- for column_name in results -%}
            COALESCE({{ column_name }}, ''){%- if not loop.last -%},{{ '\n        ' }}{%- endif -%}
        {%- endfor -%}))
{%- endmacro -%}

{%- macro snowflake__generate_series() -%}
  (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS Number FROM TABLE(GENERATOR(ROWCOUNT => 1000)))
{%- endmacro -%}

{%- macro snowflake__format_date(date_column, format) -%}
  TO_CHAR({{ date_column }}, '{{ format }}')
{%- endmacro -%}

{%- macro snowflake__cast_revenue_type(column_name) -%}
  CAST({{ column_name }} AS VARCHAR)
{%- endmacro -%}

{%- macro snowflake__format_mmm_yy(column_name) -%}
  TO_VARCHAR({{ column_name }}, 'MON-YY')
{%- endmacro -%}

{%- macro snowflake__extract_date_part(part, date_expr) -%}
  EXTRACT({{ part | upper }} FROM {{ date_expr }})
{%- endmacro -%}

{%- macro snowflake__get_quarter_string(date_col) -%}
    CONCAT('Q', QUARTER({{ date_col }}))
{%- endmacro -%}

{%- macro snowflake__get_month(date_col) -%}
    TO_DATE({{ date_col }}, 'DD-MM-YYYY')
{%- endmacro -%}

{%- macro snowflake__snowball_revenue_temp_table(db_name, schema_name, table_name) -%}
    {% set create_temp_table %}
        CREATE OR REPLACE TABLE {{ db_name }}.{{ schema_name }}.snowball_revenue AS
        SELECT *
        FROM {{ db_name }}.{{ schema_name }}.{{ table_name }}
        LIMIT 0;
    {% endset %}

    {% do run_query(create_temp_table) %}
{%- endmacro -%}

{%- macro snowflake__select_snowball_revenue_temp_table() -%}
    SELECT
        {{ get_dimension() }}
    FROM {{ var('my_database') }}.{{ var('my_schema') }}.snowball_revenue
{%- endmacro -%}

{%- macro snowflake__unpivot_kpis(model_ref, columns) -%}
    {%- set col_exprs = [] %}
    {%- for col in columns %}
        {%- set expr = "SELECT '" ~ col ~ "' AS kpi, CAST(" ~ col ~ " AS DECIMAL(18,2)) AS kpi_value FROM " ~ model_ref %}
        {%- do col_exprs.append(expr) %}
    {%- endfor %}
    {{ col_exprs | join('\nUNION ALL\n') }}
{%- endmacro -%}
