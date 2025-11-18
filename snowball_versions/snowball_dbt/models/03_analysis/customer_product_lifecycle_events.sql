{{ 
    config(
        tags=['analysis']
        ) 
}}

/* This stored procedure calculates product lifecycle flags by evaluating churn and existing status across different periods (monthly, quarterly, yearly, and year-to-date), based on revenue and customer lifecycle data. */

WITH get_churn_month_difference AS (
    -- Churn Month Difference

    SELECT

        m.monthly_revenue_key
        , {{ get_dimension_from_table('period_revenue', 'customer_level', 'm') }} 
        , {{ get_dimension_from_table('period_revenue', 'product_level', 'm') }} 
        , m.month_roll
        , m.ytd_helper
        , p.product_start_month
        , p.product_end_month
        , p.product_churn_month
        , c.lm_customer_existing_flag
        , c.l3m_customer_existing_flag
        , c.ltm_customer_existing_flag
        , c.ytd_customer_existing_flag
        , DATEDIFF(MONTH, p.product_start_month, m.month_roll)       AS product_start_month_difference
        , DATEDIFF(MONTH, p.product_churn_month, m.month_roll)       AS product_churn_month_difference

    FROM 
        {{ ref('monthly_revenue') }} AS m

    INNER JOIN 
        {{ ref('customer_product_contract') }} AS p
        ON
        {{ get_join_conditions('customer_product_contract', 'customer_level', 'm', 'p') }}
        AND 
        {{ get_join_conditions('customer_product_contract', 'product_level', 'm', 'p') }}

    INNER JOIN 
        {{ ref('customer_lifecycle_events') }} AS c
        ON m.monthly_revenue_key = c.customer_lifecycle_events_key
        AND m.month_roll = c.month_roll
        AND 
        {{ get_join_conditions('customer_product_contract', 'customer_level', 'm', 'c') }} 
)

, product_lifecycle_flags AS (

    SELECT

        monthly_revenue_key
        , {{ get_dimension_from_table('period_revenue', 'customer_level') }} 
        , {{ get_dimension_from_table('period_revenue', 'product_level') }}         
        , month_roll      

        -- Monthly flags
        , CASE 
            WHEN lm_customer_existing_flag = 1
                    AND month_roll > product_start_month 
                    AND month_roll < product_churn_month
            THEN 1 
            ELSE 0 
        END AS lm_product_existing_flag        
        , CASE  
            WHEN lm_customer_existing_flag = 1
                    AND month_roll = product_churn_month
            THEN 1 
            ELSE 0 
        END AS lm_product_churn_flag      

        -- Quarterly flags
        , CASE 
            WHEN l3m_customer_existing_flag = 1 
                    AND product_start_month_difference >= 3
                    AND month_roll < product_churn_month
            THEN 1 
            ELSE 0 
        END AS l3m_product_existing_flag        
        , CASE 
            WHEN l3m_customer_existing_flag = 1 
                    AND product_churn_month_difference < 3
                    AND product_churn_month_difference >= 0
            THEN 1 
            ELSE 0 
        END AS l3m_product_churn_flag    

        -- Yearly flags
        , CASE 
            WHEN ltm_customer_existing_flag = 1 
                    AND product_start_month_difference >= 12
                    AND month_roll < product_churn_month
            THEN 1 
            ELSE 0 
        END AS ltm_product_existing_flag       
        , CASE 
            WHEN ltm_customer_existing_flag = 1 
                    AND product_churn_month_difference < 12
                    AND product_churn_month_difference >= 0
            THEN 1 
            ELSE 0 
        END AS ltm_product_churn_flag      
          
        -- YTD flags
        , CASE 
            WHEN ytd_customer_existing_flag = 1 
                    AND product_start_month_difference >= ytd_helper
                    AND month_roll < product_churn_month
            THEN 1 
            ELSE 0 
        END AS ytd_product_existing_flag        
        , CASE 
            WHEN ytd_customer_existing_flag = 1 
                    AND product_churn_month_difference < ytd_helper
                    AND product_churn_month_difference >= 0
            THEN 1 
            ELSE 0 
        END AS ytd_product_churn_flag

    FROM get_churn_month_difference
    
)

SELECT

    monthly_revenue_key         AS customer_product_lifecycle_events_key
    , {{ get_dimension_from_table('period_revenue', 'customer_level') }} 
    , {{ get_dimension_from_table('period_revenue', 'product_level') }}     
    , month_roll
    , lm_product_churn_flag
    , lm_product_existing_flag

    , l3m_product_churn_flag
    , l3m_product_existing_flag

    , ltm_product_churn_flag
    , ltm_product_existing_flag
    
    , ytd_product_churn_flag
    , ytd_product_existing_flag

FROM 
    product_lifecycle_flags