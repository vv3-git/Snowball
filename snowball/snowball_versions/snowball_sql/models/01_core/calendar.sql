CREATE OR ALTER PROCEDURE core.sp_calendar
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS core.calendar; 
    END;

-- Get the start and end dates from the revenue table

WITH date_bounds AS (

    SELECT

        MIN(month)                       AS start_date
        , DATEADD(MONTH, 12, MAX(month)) AS end_date

    FROM "arr_sandbox"."core"."revenue"
)

-- Generate a series of numbers to represent months
, numbers AS (
    SELECT * FROM (
        SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS number
        FROM sys.all_objects
    ) gen
)

-- Create the rolling calendar
, calendar AS (

    SELECT DATEADD(MONTH, number - 1, start_date) AS month_roll

    FROM date_bounds
    INNER JOIN numbers
        ON
            DATEADD(MONTH, number - 1, start_date) <= end_date
)

SELECT month_roll

INTO core.calendar
FROM calendar
END;