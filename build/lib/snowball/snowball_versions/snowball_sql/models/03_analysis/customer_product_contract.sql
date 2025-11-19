CREATE OR ALTER PROCEDURE analysis.sp_customer_product_contract
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS analysis.customer_product_contract; 
    END;

/* This stored procedure calculates the start, end, and anticipated churn months for each customer-product pair based on recurring revenue data and product details*/

WITH get_product_start_end_month AS (

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
        , product_level_1
        , product_level_2
        , MIN(month) OVER (
            PARTITION BY customer_level_1
            , customer_level_2
            , customer_level_3
            , customer_level_4
            , customer_level_5
            , customer_level_6
            , customer_level_7
            , customer_level_8
            , customer_level_9, product_level_1
            , product_level_2
        )                                                                                     AS product_start_month
        , MAX(month) OVER (
            PARTITION BY customer_level_1
            , customer_level_2
            , customer_level_3
            , customer_level_4
            , customer_level_5
            , customer_level_6
            , customer_level_7
            , customer_level_8
            , customer_level_9, product_level_1
            , product_level_2
        )                                                                                     AS product_end_month
        , DATEADD(MONTH, 1, MAX(month) OVER (PARTITION BY customer_level_1, product_level_1)) AS product_churn_month

    FROM "arr_sandbox"."core"."revenue"
    WHERE
        mrr <> 0
)

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
    , product_level_1
    , product_level_2
    , product_start_month
    , product_end_month
    , product_churn_month

INTO analysis.customer_product_contract
FROM
    get_product_start_end_month

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
    , product_level_1
    , product_level_2
    , product_start_month
    , product_end_month
    , product_churn_month
END;