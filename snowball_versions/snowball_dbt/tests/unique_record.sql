-- Identify duplicate revenue records 

WITH ranked_revenue AS (
    SELECT *,
        ROW_NUMBER() over(
            PARTITION BY customer_key, product_key, month, revenue_type 
            ORDER BY month 
        ) as rn

    FROM {{ref('revenue')}}
)

select 
   *
FROM 
   ranked_revenue
where 
   rn > 1
