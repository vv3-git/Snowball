{{ 
    config(
        tags=['datamart']
        ) 
}}

SELECT

    customer_key
    , {{ get_dimension('customer', 1)}}

FROM {{ ref('revenue') }}

GROUP BY
    customer_key
    , {{ get_dimension('customer',1) }}