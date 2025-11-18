/*
-- This test identifies mismatches between total revenue (from 'revenue' table)
 and monthly revenue (from 'monthly_revenue' table) for each customer-product pair.
 It checks the aggregated revenue for each customer-product combination consistent across both table
 */


WITH revenue_summary AS (
    SELECT 
        customer_key,
        product_key,
        SUM(mrr) AS revenue
    FROM 
        {{ ref('revenue') }}
    GROUP BY 
        customer_key,
        product_key
),

monthly_summary AS (
    SELECT 
        customer_key,
        product_key,
        SUM(mrr) AS monthly_revenue
    FROM 
        {{ ref('monthly_revenue') }}
    GROUP BY 
        customer_key,
        product_key
)

SELECT 
    r.customer_key,
    r.product_key,
    r.revenue,
    m.monthly_revenue
FROM 
    revenue_summary r
LEFT JOIN 
    monthly_summary m
ON 
    r.customer_key = m.customer_key 
    AND r.product_key = m.product_key
WHERE 
    r.revenue != m.monthly_revenue
