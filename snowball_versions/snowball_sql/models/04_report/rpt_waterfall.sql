CREATE OR ALTER PROCEDURE report.sp_rpt_waterfall
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS report.rpt_waterfall; 
    END;

SELECT *
FROM (
    SELECT *
    FROM (
        SELECT

            CAST(eop_arr AS DECIMAL(18, 2))          AS eop_arr

            , CAST(bop_arr AS DECIMAL(18, 2))        AS bop_arr

            , CAST(customer_churn AS DECIMAL(18, 2)) AS customer_churn

            , CAST(new_customer AS DECIMAL(18, 2))   AS new_customer

            , CAST(cross_sell AS DECIMAL(18, 2))     AS cross_sell

            , CAST(product_churn AS DECIMAL(18, 2))  AS product_churn

            , CAST(upsell AS DECIMAL(18, 2))         AS upsell

            , CAST(downsell AS DECIMAL(18, 2))       AS downsell

            , CAST(grr AS DECIMAL(18, 2))            AS grr

            , CAST(nrr AS DECIMAL(18, 2))            AS nrr

        INTO report.rpt_waterfall
FROM "arr_sandbox"."reporting"."rpt_revenue_bridge"
    ) src
    UNPIVOT (
        kpi_value FOR kpi IN (
            eop_arr, bop_arr, customer_churn, new_customer, cross_sell, product_churn, upsell, downsell, grr, nrr
        )
    ) AS unpvt
) v
WHERE
    v.kpi_value <> 0
    OR v.kpi = 'eop_arr'
END;