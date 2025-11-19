-- Identify duplicate revenue records 

WITH ranked_revenue AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY customer_key, product_key, month, revenue_type
            ORDER BY month
        ) AS rn

    FROM "arr_sandbox"."core"."revenue"
)

SELECT *
FROM
    ranked_revenue
WHERE
    rn > 1
