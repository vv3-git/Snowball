{{ 
    config(
        tags=['datamart']
        ) 
}}

SELECT

    other_key
    , {{ get_dimension('other',1) }}
    
FROM {{ ref('revenue') }}

GROUP BY
    other_key
    , {{ get_dimension('other',1) }}