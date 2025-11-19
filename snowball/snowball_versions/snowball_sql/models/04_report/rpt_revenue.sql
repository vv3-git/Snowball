CREATE OR ALTER PROCEDURE report.sp_rpt_revenue
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS report.rpt_revenue; 
    END;

WITH

revenue AS (

    SELECT
        monthly_revenue_key AS revenue_key
        , customer_key
        , product_key
        -- , entity_key
        , other_key
        , revenue_type
        , month_roll
        , arr               AS ltm_revenue
        , mrr
        , volume
        , CASE
            WHEN month_roll <= customer_churn_month
                THEN SUM(mrr) OVER (
                    PARTITION BY r.customer_level_1
                    ORDER BY month_roll
                )
            ELSE 0
        END                 AS cltv
        , CASE
            WHEN month_roll = customer_join_month
                THEN 1
            ELSE 0
        END                 AS is_customer_new
        , CASE
            WHEN month_roll = customer_churn_month
                THEN 1
            ELSE 0
        END                 AS is_customer_churn

    FROM
        "arr_sandbox"."analysis"."monthly_revenue" r
    LEFT JOIN
        "arr_sandbox"."analysis"."customer_contract" c
        ON r.customer_level_1 = c.customer_level_1
)

SELECT * INTO report.rpt_revenue
FROM revenue
END;