{{ 
    config(
        tags=['datamart']
        ) 
}}

SELECT

    {{ get_dimension_from_table('revenue', 'product') }}
    
FROM {{ ref('revenue') }}

GROUP BY
    {{ get_dimension_from_table('revenue', 'product') }}