CREATE OR ALTER PROCEDURE datamart.sp_fact_revenue
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS datamart.fact_revenue; 
    END;

SELECT

    revenue_key
    , customer_key
    , product_key
    , other_key
    , volume
    , month
    , revenue_type
    , revenue
    , mrr

INTO datamart.fact_revenue
FROM "arr_sandbox"."core"."revenue"
END;