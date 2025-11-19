/* Test checks if 'monthly_revenue' has complete monthly coverage
   for each customer-product pair based on revenue duration.
*/

-- Calculate the expected number of revenue months
WITH revenue_month AS (
    SELECT
        customer_key
        , product_key
        , min(month)                                                    AS min_month
        , max(month)                                                    AS max_month
        , datediff(MONTH, min_month, dateadd(MONTH, 12, max_month)) + 1 AS expected_months
    FROM
        "arr_sandbox"."core"."revenue"
    WHERE revenue <> 0 AND revenue_type = 1
    GROUP BY
        customer_key
        , product_key

)

--Count how many distinct months are actually present in monthly_revenue
, month_counts AS (
    SELECT
        customer_key
        , product_key

        , count(DISTINCT month_roll) AS actual_month
    FROM
        "arr_sandbox"."analysis"."monthly_revenue"
    GROUP BY
        customer_key
        , product_key
)

SELECT
    r.customer_key
    , r.product_key
    , r.min_month
    , r.max_month
    , r.expected_months
    , m.actual_month
FROM revenue_month r
LEFT JOIN month_counts m
    ON
        r.customer_key = m.customer_key
        AND r.product_key = m.product_key
WHERE
    m.actual_month <> r.expected_months
