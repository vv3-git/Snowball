{{ 
    config(
        tags=['core']
        ) 
}}

-- Get the start and end dates from the revenue table

WITH date_bounds AS (

    SELECT 

        MIN(month)                              AS StartDate
        , DATEADD(MONTH, 12, MAX(month))        AS EndDate

    FROM {{ ref('revenue') }}
),

-- Generate a series of numbers to represent months
numbers AS (
    SELECT * FROM {{ generate_series() }}
),

-- Create the rolling calendar
calendar AS (

    SELECT

        DATEADD(MONTH, Number - 1, StartDate)   AS month_roll

    FROM date_bounds
    JOIN numbers
    ON
        DATEADD(MONTH, Number - 1, StartDate) <= EndDate
)

SELECT 

    month_roll

FROM calendar