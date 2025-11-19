CREATE OR ALTER PROCEDURE datamart.sp_dim_customer
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS datamart.dim_customer; 
    END;

SELECT

    customer_key
    , customer_level_1
    , customer_level_2
    , customer_level_3
    , customer_level_4
    , customer_level_5
    , customer_level_6
    , customer_level_7
    , customer_level_8
    , customer_level_9
    , customer_name
    , customer_region

INTO datamart.dim_customer
FROM "arr_sandbox"."core"."revenue"

GROUP BY
    customer_key
    , customer_level_1
    , customer_level_2
    , customer_level_3
    , customer_level_4
    , customer_level_5
    , customer_level_6
    , customer_level_7
    , customer_level_8
    , customer_level_9
    , customer_name
    , customer_region
END;