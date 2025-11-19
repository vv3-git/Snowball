-- Write your own SQL object definition here, and it'll be included in your package.

WITH test AS (

    SELECT

        snowball_key
        , s.customer_key
        , c.customer_level_1
        , p.product_level_1
        , month_roll
        , period_type
        , bop_arr
        , customer_churn
        , product_churn
        , downsell
        , grr
        , upsell
        , cross_sell
        , nrr
        , new_customer
        , eop_arr
        , SUM(bop_arr) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll)        AS net_bop
        , SUM(downsell) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll)       AS net_downsell
        , SUM(upsell) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll)         AS net_upsell
        , SUM(cross_sell) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll)     AS net_cross_sell
        , SUM(nrr) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll)            AS net_nrr
        , SUM(new_customer) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll)   AS net_new_customer
        , SUM(customer_churn) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll) AS net_customer_churn
        , SUM(product_churn) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll)  AS net_product_churn
        , SUM(eop_arr) OVER (PARTITION BY period_type, c.customer_level_1, p.product_level_1, month_roll)        AS net_eop
    FROM "arr_sandbox"."reporting"."rpt_revenue_bridge" s
    INNER JOIN "arr_sandbox"."datamart"."dim_customer" c
        ON s.customer_key = c.customer_key
    INNER JOIN "arr_sandbox"."datamart"."dim_product" p
        ON s.product_key = p.product_key
)

SELECT *
FROM
    test
WHERE
    ROUND(
        net_bop
        + net_customer_churn
        + net_product_churn
        + net_downsell
        + net_upsell
        + net_cross_sell
        + net_new_customer, 0
    )
    - ROUND(net_eop, 0) <> 0
