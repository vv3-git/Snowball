--  Macro to dynamically select columns for the models. It can be chosen by giving any of the 4 parameters as per requirement.
--  The 4 params are: model name, match key word, table prefix name, exclusion list
{%- macro get_dimension_from_table(model_name, keyword, alias=None, exclude_list=[]) -%}
    {%- set relation = ref(model_name) -%}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}

    {%- set filtered_columns = [] -%}
    {%- for col in columns -%}
        {%- if keyword | lower in col.name | lower and col.name | lower not in (exclude_list | map('lower') | list) -%}
            {%- if alias -%}
                {%- do filtered_columns.append(alias ~ '.' ~ col.name) -%}
            {%- else -%}
                {%- do filtered_columns.append(col.name) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {%- for col in filtered_columns -%}
        {%- if loop.first -%}
            {{- col -}}
        {%- else -%}
            {{- '\n    , ' ~ col -}}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}

-- NOTE:
-- {{ get_dimension_from_table('monthly_revenue', 'product', 'p', ['PRODUCT_KEY']) }}
-- Will return all product columns from monthly_revenue prefixed with p., excluding PRODUCT_KEY.
-- 
-- {{ get_dimension_from_table('monthly_revenue', 'customer') }}

----------------------------------------------------------------------------------------------------------------
--  Macro for coalesced usage in models
{%- macro coalesce_columns_with_alias(model_name, keyword, alias=None, exclude_list=[]) -%}
    {%- set relation = ref(model_name) -%}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}

    {%- set coalesced_columns = [] -%}
    {%- for col in columns -%}
        {%- if keyword | lower in col.name | lower and col.name not in exclude_list -%}
            {%- set col_expr = "COALESCE(" ~ (alias ~ '.' if alias else '') ~ col.name ~ ", '')" -%}
            {%- do coalesced_columns.append(col_expr ~ " AS " ~ col.name) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- for col in coalesced_columns -%}
        {%- if loop.first -%}
            {{- col -}}
        {%- else -%}
            {{- '\n   , ' ~ col -}}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}

-----------------------------------------------------------------------------------------------------------------
--  Macro to generate join conditions based on a keyword and aliases
--  It can be used to create join conditions dynamically based on the columns present in the model
{%- macro get_join_conditions(model_name, keyword, left_alias, right_alias, exclude_list=[]) -%}
    {%- set relation = ref(model_name) -%}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- set exclude_list_lower = exclude_list | map('lower') | list -%}

    {%- set join_conditions = [] -%}
    {%- for col in columns -%}
        {%- set col_name = col.name -%}
        {%- if keyword | lower in col_name | lower and col_name | lower not in exclude_list_lower -%}
            {%- do join_conditions.append(left_alias ~ '.' ~ col_name ~ ' = ' ~ right_alias ~ '.' ~ col_name) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- for condition in join_conditions -%}
        {%- if loop.first -%}
            {{- condition -}}
        {%- else -%}
            {{- '\n   AND ' ~ condition -}}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}