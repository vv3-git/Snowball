CREATE OR ALTER PROCEDURE datamart.sp_dim_product
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS datamart.dim_product; 
    END;

SELECT

    product_key
    , product_level_1
    , product_level_2

INTO datamart.dim_product
FROM "arr_sandbox"."core"."revenue"

GROUP BY
    product_key
    , product_level_1
    , product_level_2
END;