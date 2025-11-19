{{ 
    config(
        tags=['analysis']
        ) 
}}

/* This stored procedure calculates lifecycle flags for customers based on their join and churn months, producing monthly, quarterly, yearly, and year-to-date indicators for new, churned, and existing customers*/
    
WITH get_month_difference AS (

    SELECT

        m.monthly_revenue_key
        , {{ get_dimension_from_table('monthly_revenue', 'customer_level', 'm') }} 
        , m.month_roll
        , m.ytd_helper
        , c.customer_join_month
        , c.customer_end_month
        , c.customer_churn_month
        , DATEDIFF(month, c.customer_join_month, m.month_roll)       AS customer_join_month_difference
        , DATEDIFF(month, c.customer_churn_month, m.month_roll)      AS customer_churn_month_difference
    
    FROM {{ ref('monthly_revenue') }} AS m

    INNER JOIN {{ ref('customer_contract') }} AS c
        ON
            {{ get_join_conditions('monthly_revenue', 'customer_level', 'm', 'c') }} 

)

-- Calculating the flags based on customer join, end, and  churn month.

, customer_lifecycle_flags AS (

    SELECT

        monthly_revenue_key
        , {{ get_dimension_from_table('monthly_revenue', 'customer_level') }} 
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
            WHEN month_roll > customer_join_month
            AND month_roll < customer_churn_month
                THEN 1 
            ELSE 0 
        END AS lm_customer_existing_flag

        -- QUARTERLY FLAGS
        , CASE  
            WHEN customer_join_month_difference < 3
                THEN 1
            ELSE 0
        END AS l3m_customer_new_flag
        , CASE 
            WHEN customer_churn_month_difference < 3
            AND customer_churn_month_difference >= 0
                THEN 1 
            ELSE 0 
        END AS l3m_customer_churn_flag
        , CASE 
            WHEN customer_join_month_difference >= 3
            AND month_roll < customer_churn_month
                THEN 1 
            ELSE 0 
        END AS l3m_customer_existing_flag

        -- YEARLY FLAGS
        , CASE  
            WHEN customer_join_month_difference < 12
                THEN 1 
            ELSE 0 
        END AS ltm_customer_new_flag
        , CASE 
            WHEN customer_churn_month_difference < 12
            AND customer_churn_month_difference >= 0
                THEN 1 
            ELSE 0 
        END AS ltm_customer_churn_flag
        , CASE 
            WHEN customer_join_month_difference >= 12
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
            WHEN customer_churn_month_difference < ytd_helper
            AND customer_churn_month_difference >= 0
                THEN 1 
            ELSE 0 
        END AS ytd_customer_churn_flag
        , CASE 
            WHEN customer_join_month_difference >= ytd_helper
            AND month_roll < customer_churn_month
                THEN 1 
            ELSE 0 
        END AS ytd_customer_existing_flag
    
    FROM get_month_difference
    
)

SELECT

    monthly_revenue_key         AS customer_lifecycle_events_key
    , {{ get_dimension_from_table('monthly_revenue', 'customer_level') }} 
    , month_roll
    , lm_customer_new_flag
    , lm_customer_churn_flag
    , lm_customer_existing_flag

    , l3m_customer_new_flag
    , l3m_customer_churn_flag
    , l3m_customer_existing_flag
    
    , ltm_customer_new_flag
    , ltm_customer_churn_flag
    , ltm_customer_existing_flag
    
    , ytd_customer_new_flag
    , ytd_customer_churn_flag
    , ytd_customer_existing_flag

FROM 
    customer_lifecycle_flags