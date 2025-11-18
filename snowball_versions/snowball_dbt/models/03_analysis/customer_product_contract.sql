{{ 
    config(
        tags=['analysis']
        ) 
}}
 
 /* This stored procedure calculates the start, end, and anticipated churn months for each customer-product pair based on recurring revenue data and product details*/

WITH get_product_start_end_month AS (

    SELECT
        {{ get_dimension_from_table('revenue', 'customer_level') }} 
        , {{ get_dimension_from_table('revenue', 'product_level') }}   
        , MIN(month) OVER (PARTITION BY  {{ get_dimension_from_table('revenue', 'customer_level') }} , {{ get_dimension_from_table('revenue', 'product_level') }} )                 AS product_start_month
        , MAX(month) OVER (PARTITION BY  {{ get_dimension_from_table('revenue', 'customer_level') }} , {{ get_dimension_from_table('revenue', 'product_level') }} )                 AS product_end_month
        , DATEADD(MONTH, 1, MAX(month) OVER (PARTITION BY customer_level_1, product_level_1))                                                                                       AS product_churn_month
    
    FROM {{ ref('revenue') }} AS r
    WHERE
        mrr <> 0
        
)

SELECT
    
    {{ get_dimension_from_table('revenue', 'customer_level') }} 
    , {{ get_dimension_from_table('revenue', 'product_level') }}   
    , product_start_month
    , product_end_month
    , product_churn_month

FROM 
    get_product_start_end_month

GROUP BY
    {{ get_dimension_from_table('revenue', 'customer_level') }} 
    , {{ get_dimension_from_table('revenue', 'product_level') }}   
    , product_start_month
    , product_end_month
    , product_churn_month