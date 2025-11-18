CREATE OR ALTER PROCEDURE analysis.sp_customer_lifecycle_events
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS analysis.customer_lifecycle_events; 
    END;

/* This stored procedure calculates lifecycle flags for customers based on their join and churn months, producing monthly, quarterly, yearly, and year-to-date indicators for new, churned, and existing customers*/

WITH get_month_difference AS (

    SELECT

        m.monthly_revenue_key
        , m.customer_level_1
        , m.customer_level_2
        , m.customer_level_3
        , m.customer_level_4
        , m.customer_level_5
        , m.customer_level_6
        , m.customer_level_7
        , m.customer_level_8
        , m.customer_level_9
        , m.month_roll
        , m.ytd_helper
        , c.customer_join_month
        , c.customer_end_month
        , c.customer_churn_month
        , DATEDIFF(MONTH, c.customer_join_month, m.month_roll)  AS customer_join_month_difference
        , DATEDIFF(MONTH, c.customer_churn_month, m.month_roll) AS customer_churn_month_difference

    FROM "arr_sandbox"."analysis"."monthly_revenue" m

    INNER JOIN "arr_sandbox"."analysis"."customer_contract" c
        ON
            m.customer_level_1 = c.customer_level_1
            AND m.customer_level_2 = c.customer_level_2
            AND m.customer_level_3 = c.customer_level_3
            AND m.customer_level_4 = c.customer_level_4
            AND m.customer_level_5 = c.customer_level_5
            AND m.customer_level_6 = c.customer_level_6
            AND m.customer_level_7 = c.customer_level_7
            AND m.customer_level_8 = c.customer_level_8
            AND m.customer_level_9 = c.customer_level_9
)

-- Calculating the flags based on customer join, end, and  churn month.

, customer_lifecycle_flags AS (

    SELECT

        monthly_revenue_key
        , customer_level_1
        , customer_level_2
        , customer_level_3
        , customer_level_4
        , customer_level_5
        , customer_level_6
        , customer_level_7
        , customer_level_8
        , customer_level_9
        , month_roll

        -- MONTHLY FLAGS
        , CASE
            WHEN month_roll = customer_join_month
                THEN 1
            ELSE 0
        END AS lm_customer_new_flag
        , CASE
            WHEN customer_churn_month = month_roll
                THEN 1
            ELSE 0
        END AS lm_customer_churn_flag
        , CASE
            WHEN
                month_roll > customer_join_month
                AND month_roll < customer_churn_month
                THEN 1
            ELSE 0
        END AS lm_customer_existing_flag

        -- QUARTERLY FLAGS
        , CASE
            WHEN customer_join_month_difference < 3
                THEN 1
            ELSE 0
        END AS l_3_m_customer_new_flag
        , CASE
            WHEN
                customer_churn_month_difference < 3
                AND customer_churn_month_difference >= 0
                THEN 1
            ELSE 0
        END AS l_3_m_customer_churn_flag
        , CASE
            WHEN
                customer_join_month_difference >= 3
                AND month_roll < customer_churn_month
                THEN 1
            ELSE 0
        END AS l_3_m_customer_existing_flag

        -- YEARLY FLAGS
        , CASE
            WHEN customer_join_month_difference < 12
                THEN 1
            ELSE 0
        END AS ltm_customer_new_flag
        , CASE
            WHEN
                customer_churn_month_difference < 12
                AND customer_churn_month_difference >= 0
                THEN 1
            ELSE 0
        END AS ltm_customer_churn_flag
        , CASE
            WHEN
                customer_join_month_difference >= 12
                AND month_roll < customer_churn_month
                THEN 1
            ELSE 0
        END AS ltm_customer_existing_flag

        -- YTD FLAGS
        , CASE
            WHEN customer_join_month_difference < ytd_helper
                THEN 1
            ELSE 0
        END AS ytd_customer_new_flag
        , CASE
            WHEN
                customer_churn_month_difference < ytd_helper
                AND customer_churn_month_difference >= 0
                THEN 1
            ELSE 0
        END AS ytd_customer_churn_flag
        , CASE
            WHEN
                customer_join_month_difference >= ytd_helper
                AND month_roll < customer_churn_month
                THEN 1
            ELSE 0
        END AS ytd_customer_existing_flag

    FROM get_month_difference
)

SELECT

    monthly_revenue_key AS customer_lifecycle_events_key
    , customer_level_1
    , customer_level_2
    , customer_level_3
    , customer_level_4
    , customer_level_5
    , customer_level_6
    , customer_level_7
    , customer_level_8
    , customer_level_9
    , month_roll
    , lm_customer_new_flag
    , lm_customer_churn_flag
    , lm_customer_existing_flag

    , l_3_m_customer_new_flag
    , l_3_m_customer_churn_flag
    , l_3_m_customer_existing_flag

    , ltm_customer_new_flag
    , ltm_customer_churn_flag
    , ltm_customer_existing_flag

    , ytd_customer_new_flag
    , ytd_customer_churn_flag
    , ytd_customer_existing_flag

INTO analysis.customer_lifecycle_events
FROM
    customer_lifecycle_flags
END;