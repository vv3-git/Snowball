CREATE OR ALTER PROCEDURE analysis.sp_customer_product_lifecycle_events
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS analysis.customer_product_lifecycle_events; 
    END;

/* This stored procedure calculates product lifecycle flags by evaluating churn and existing status across different periods (monthly, quarterly, yearly, and year-to-date), based on revenue and customer lifecycle data. */

WITH get_churn_month_difference AS (
    -- Churn Month Difference

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
        , m.product_level_1
        , m.product_level_2
        , m.month_roll
        , m.ytd_helper
        , p.product_start_month
        , p.product_end_month
        , p.product_churn_month
        , c.lm_customer_existing_flag
        , c.l_3_m_customer_existing_flag
        , c.ltm_customer_existing_flag
        , c.ytd_customer_existing_flag
        , DATEDIFF(MONTH, p.product_start_month, m.month_roll) AS product_start_month_difference
        , DATEDIFF(MONTH, p.product_churn_month, m.month_roll) AS product_churn_month_difference

    FROM
        "arr_sandbox"."analysis"."monthly_revenue" m

    INNER JOIN
        "arr_sandbox"."analysis"."customer_product_contract" p
        ON
            m.customer_level_1 = p.customer_level_1
            AND m.customer_level_2 = p.customer_level_2
            AND m.customer_level_3 = p.customer_level_3
            AND m.customer_level_4 = p.customer_level_4
            AND m.customer_level_5 = p.customer_level_5
            AND m.customer_level_6 = p.customer_level_6
            AND m.customer_level_7 = p.customer_level_7
            AND m.customer_level_8 = p.customer_level_8
            AND m.customer_level_9 = p.customer_level_9
            AND
            m.product_level_1 = p.product_level_1
            AND m.product_level_2 = p.product_level_2

    INNER JOIN
        "arr_sandbox"."analysis"."customer_lifecycle_events" c
        ON
            m.monthly_revenue_key = c.customer_lifecycle_events_key
            AND m.month_roll = c.month_roll
            AND
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

, product_lifecycle_flags AS (

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
        , product_level_1
        , product_level_2
        , month_roll

        -- Monthly flags
        , CASE
            WHEN
                lm_customer_existing_flag = 1
                AND month_roll > product_start_month
                AND month_roll < product_churn_month
                THEN 1
            ELSE 0
        END AS lm_product_existing_flag
        , CASE
            WHEN
                lm_customer_existing_flag = 1
                AND month_roll = product_churn_month
                THEN 1
            ELSE 0
        END AS lm_product_churn_flag

        -- Quarterly flags
        , CASE
            WHEN
                l_3_m_customer_existing_flag = 1
                AND product_start_month_difference >= 3
                AND month_roll < product_churn_month
                THEN 1
            ELSE 0
        END AS l_3_m_product_existing_flag
        , CASE
            WHEN
                l_3_m_customer_existing_flag = 1
                AND product_churn_month_difference < 3
                AND product_churn_month_difference >= 0
                THEN 1
            ELSE 0
        END AS l_3_m_product_churn_flag

        -- Yearly flags
        , CASE
            WHEN
                ltm_customer_existing_flag = 1
                AND product_start_month_difference >= 12
                AND month_roll < product_churn_month
                THEN 1
            ELSE 0
        END AS ltm_product_existing_flag
        , CASE
            WHEN
                ltm_customer_existing_flag = 1
                AND product_churn_month_difference < 12
                AND product_churn_month_difference >= 0
                THEN 1
            ELSE 0
        END AS ltm_product_churn_flag

        -- YTD flags
        , CASE
            WHEN
                ytd_customer_existing_flag = 1
                AND product_start_month_difference >= ytd_helper
                AND month_roll < product_churn_month
                THEN 1
            ELSE 0
        END AS ytd_product_existing_flag
        , CASE
            WHEN
                ytd_customer_existing_flag = 1
                AND product_churn_month_difference < ytd_helper
                AND product_churn_month_difference >= 0
                THEN 1
            ELSE 0
        END AS ytd_product_churn_flag

    FROM get_churn_month_difference
)

SELECT

    monthly_revenue_key AS customer_product_lifecycle_events_key
    , customer_level_1
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
    , month_roll
    , lm_product_churn_flag
    , lm_product_existing_flag

    , l_3_m_product_churn_flag
    , l_3_m_product_existing_flag

    , ltm_product_churn_flag
    , ltm_product_existing_flag

    , ytd_product_churn_flag
    , ytd_product_existing_flag

INTO analysis.customer_product_lifecycle_events
FROM
    product_lifecycle_flags
END;