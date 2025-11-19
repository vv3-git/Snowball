{{ 
    config(
        tags=['analysis']
        ) 
}}
 
/* This Stored Procedure calculates the join month, end month, and churn month for each customer based on their revenue records*/

SELECT
    
    {{ get_dimension_from_table('revenue', 'customer_level') }} 
    , MIN(month)                                                 AS customer_join_month
    , MAX(month)                                                 AS customer_end_month
    , DATEADD(MONTH, 1, MAX(month))                              AS customer_churn_month

FROM {{ ref('revenue') }}

WHERE
    mrr <> 0.0
GROUP BY 
    {{ get_dimension_from_table('revenue', 'customer_level') }} 

