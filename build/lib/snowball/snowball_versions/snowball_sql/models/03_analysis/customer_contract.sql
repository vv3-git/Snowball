CREATE OR ALTER PROCEDURE analysis.sp_customer_contract
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS analysis.customer_contract; 
    END;

/* This Stored Procedure calculates the join month, end month, and churn month for each customer based on their revenue records*/

SELECT

    customer_level_1
    , customer_level_2
    , customer_level_3
    , customer_level_4
    , customer_level_5
    , customer_level_6
    , customer_level_7
    , customer_level_8
    , customer_level_9
    , MIN(month)                    AS customer_join_month
    , MAX(month)                    AS customer_end_month
    , DATEADD(MONTH, 1, MAX(month)) AS customer_churn_month

INTO analysis.customer_contract
FROM "arr_sandbox"."core"."revenue"

WHERE
    mrr <> 0.0
GROUP BY
    customer_level_1
    , customer_level_2
    , customer_level_3
    , customer_level_4
    , customer_level_5
    , customer_level_6
    , customer_level_7
    , customer_level_8
    , customer_level_9
END;