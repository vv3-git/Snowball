CREATE OR ALTER PROCEDURE datamart.sp_dim_other
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS datamart.dim_other; 
    END;

SELECT

    other_key
    , other_dim_1
    , other_dim_2

INTO datamart.dim_other
FROM "arr_sandbox"."core"."revenue"

GROUP BY
    other_key
    , other_dim_1
    , other_dim_2
END;