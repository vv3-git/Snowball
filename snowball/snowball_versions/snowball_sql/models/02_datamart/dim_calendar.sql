CREATE OR ALTER PROCEDURE datamart.sp_dim_calendar
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS datamart.dim_calendar; 
    END;

SELECT

    *
    , YEAR(month_roll)                                     AS year
    , MONTH(month_roll)                                    AS month_no
    , FORMAT(month_roll, 'MON')                            AS month_name
    , 'Q' + CAST(DATEPART(QUARTER, month_roll) AS VARCHAR) AS quarter
    , FORMAT(month_roll, 'MMM-yy')                         AS mmm_yy

INTO datamart.dim_calendar
FROM "arr_sandbox"."core"."calendar"
END;