{{ 
    config(
        tags=['analysis']
        ) 
}}

/* This model processes revenue data by joining it with customer contract information to calculate and aggregate ARR (Annual Recurring Revenue) across different months.*/

WITH date_joins AS (

    SELECT
    
        -- Joining Customer and Revenue Data
        r.revenue_key
        , r.revenue_type
        , r.month
        , r.revenue
        , r.mrr
        , r.volume
        , MIN(month) OVER (PARTITION BY revenue_key)        AS segment_start_month
        , MAX(month) OVER (PARTITION BY revenue_key)        AS segment_end_month
        , c.*
        , p.*
        , o.*

    FROM {{ ref('fact_revenue') }} AS r
    LEFT JOIN 
        {{ ref('dim_customer') }} AS c
    ON r.customer_key = c.customer_key
    LEFT JOIN 
        {{ ref('dim_product') }} AS p 
    ON r.product_key = p.product_key
    LEFT JOIN 
        {{ ref('dim_other') }} AS o
    ON r.other_key = o.other_key
    WHERE
        r.revenue <> 0.00

)

-- Filling in the gaps for each customer with 0 revenue whenever a record of revenue for a customer on a month is not available
, date_scaffolding AS (

    SELECT

        revenue_key
        , revenue_type
        , {{ get_dimension_from_table('revenue', 'customer') }} 
        , {{ get_dimension_from_table('revenue', 'product') }} 
        , {{ get_dimension_from_table('revenue', 'other') }} 
        , c.month_roll
        , CASE
            WHEN
                c.month_roll > d.month
                OR c.month_roll <> d.month
                THEN 0
            ELSE d.volume
        END AS volume
        , CASE
            WHEN
                c.month_roll > d.month
                OR c.month_roll <> d.month
                THEN 0
            ELSE d.mrr
        END AS mrr

    FROM {{ ref('dim_calendar') }} AS c

    INNER JOIN date_joins AS d
        ON c.month_roll <= DATEADD(MONTH,12, d.segment_end_month) 
        AND c.month_roll >= d.segment_start_month

)

-- Create monthly_revenue table
, aggregated_revenue AS (

    SELECT

        revenue_key                                                                         AS monthly_revenue_key
        , revenue_type
        , {{ get_dimension_from_table('revenue', 'customer') }} 
        , {{ get_dimension_from_table('revenue', 'product') }} 
        , {{ get_dimension_from_table('revenue', 'other') }} 
        , month_roll
        , SUM(mrr)                  AS mrr
        , SUM(volume)               AS volume
        -- Add 1 back to YTD year start here so YTD start aligns with month selected i.e. 4 = start in April
        , {{ extract_date_part("MONTH", "DATEADD(MONTH, -" ~ var('ytd_year_start') ~ " + 1, month_roll)") }} AS ytd_helper
    FROM 
        date_scaffolding
    GROUP BY
        revenue_key
        , {{ get_dimension_from_table('revenue', 'customer') }} 
        , {{ get_dimension_from_table('revenue', 'product') }} 
        , {{ get_dimension_from_table('revenue', 'other') }} 
        , month_roll
        , revenue_type

)

, churn_month AS (

    SELECT
        customer_key
        , product_key
        , MAX(month_roll)   AS product_churn_month
    FROM
        aggregated_revenue
    WHERE
        mrr <> 0.0
    GROUP BY
        customer_key
        , product_key
        
)

-- Create monthly_revenue table

SELECT
    a.*
    , CASE 
        WHEN a.revenue_type = 1 OR a.revenue_type = 'Recurring' THEN mrr * 12
        WHEN month_roll <= product_churn_month THEN SUM(mrr) OVER (
                PARTITION BY monthly_revenue_key
                ORDER BY month_roll 
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
        ELSE 0
    END AS arr

FROM
    aggregated_revenue a
LEFT JOIN
    churn_month c
    ON a.customer_key = c.customer_key 
    AND a.product_key = c.product_key