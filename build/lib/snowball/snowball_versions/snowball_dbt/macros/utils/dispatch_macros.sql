-- This file defines cross-platform dispatch wrappers that resolve to
-- adapter-specific implementations like snowflake__*, sqlserver__*, etc.

-- used in ssot/calendar.sql
{%- macro generate_series() -%}
  {% set macro = adapter.dispatch('generate_series') %}
  {{ return(macro()) }}
{%- endmacro -%}

-- used in data_mart/dim_calendar.sql
{%- macro format_date(date_column, format) -%}
  {% set macro = adapter.dispatch('format_date') %}
  {{ return(macro(date_column, format)) }}
{%- endmacro -%}

-- used in ssot/revenue.sql
{%- macro cast_revenue_type(column_name) -%}
  {% set macro = adapter.dispatch('cast_revenue_type') %}
  {{ return(macro(column_name)) }}
{%- endmacro -%}

-- used in data_mart/dim_calendar.sql
{%- macro format_mmm_yy(column_name) -%}
  {% set macro = adapter.dispatch('format_mmm_yy') %}
  {{ return(macro(column_name)) }}
{%- endmacro -%}

-- revenue.sql
{%- macro generate_hash_key(filter_value=None, index=-1, exclude_list=[]) -%}
  {% set macro = adapter.dispatch('generate_hash_key') %}
  {{ return(macro(filter_value=filter_value, index=index, exclude_list=exclude_list)) }}
{%- endmacro -%}

-- monthly_revenue.sql 
{%- macro extract_date_part(part, date_expr) -%}
  {% set macro = adapter.dispatch('extract_date_part') %}
  {{ return(macro(part, date_expr)) }}
{%- endmacro -%}

{%- macro get_quarter_string(date_col) -%}
  {% set macro = adapter.dispatch('get_quarter_string') %}
  {{ return(macro(date_col)) }}
{%- endmacro -%}

{%- macro get_month(date_col) -%}
  {% set macro = adapter.dispatch('get_month') %}
  {{ return(macro(date_col)) }}
{%- endmacro -%}

{%- macro snowball_revenue_temp_table(db_name, schema_name, table_name) -%}
  {% set macro = adapter.dispatch('snowball_revenue_temp_table') %}
  {{ return(macro(db_name, schema_name, table_name)) }}
{%- endmacro -%}

{%- macro select_snowball_revenue_temp_table() -%}
  {% set macro = adapter.dispatch('select_snowball_revenue_temp_table') %}
  {{ return(macro()) }}
{%- endmacro -%}

{%- macro unpivot_kpis(model_ref, columns) -%}
  {% set macro = adapter.dispatch('unpivot_kpis') %}
  {{ return(macro(model_ref, columns)) }}
{%- endmacro -%}
