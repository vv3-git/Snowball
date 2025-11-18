/*
 This test identifies mismatches between total revenue (from 'revenue' table)
 and monthly revenue (from 'monthly_revenue' table) for each customer.
 It checks the aggregated revenue across all products per customer 
*/


WITH revenue_summary AS (
    SELECT
        customer_key,
        SUM(mrr) AS revenue
    FROM {{ ref('revenue') }}
    GROUP BY customer_key
),

monthly_summary AS (
    SELECT
        customer_key,
        SUM(mrr) AS monthly_revenue
    FROM {{ ref('monthly_revenue') }}
    GROUP BY customer_key
)

SELECT
    r.customer_key,
      r.revenue,
    m.monthly_revenue
FROM revenue_summary r
LEFT JOIN monthly_summary m
    ON r.customer_key = m.customer_key
WHERE 
    r.revenue != m.monthly_revenue
