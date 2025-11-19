CREATE OR ALTER PROCEDURE analysis.sp_delta_revenue
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS analysis.delta_revenue; 
    END;

/* This stored procedure calculates revenue deltas by applying flags for changes like acquisition, churn, cross-sell, upsell, and downsell over various time periods(monthly, quarterly, last 12 months, and year-to-date), using joins between revenue data and customer and product lifecycle tables.*/

WITH get_arr_business_flag AS (

    SELECT

        p_1.period_revenue_key
        , p_1.customer_key
        , p_1.product_key
        , p_1.other_key
        , p_1.customer_level_1
        , p_1.customer_level_2
        , p_1.customer_level_3
        , p_1.customer_level_4
        , p_1.customer_level_5
        , p_1.customer_level_6
        , p_1.customer_level_7
        , p_1.customer_level_8
        , p_1.customer_level_9
        , p_1.product_level_1
        , p_1.product_level_2
        , p_1.month_roll
        , p_1.arr
        , p_1.arr_lm
        , p_1.arr_l_3_m
        , p_1.arr_ltm
        , p_1.arr_ytd
        , p_1.arr_lm_delta
        , p_1.arr_l_3_m_delta
        , p_1.arr_ltm_delta
        , p_1.arr_ytd_delta

        -- PRICE VOLUME 
        -- uncomment below fields to find the price volume difference

        -- , p1.percentage_price_change_lm
        -- , p1.percentage_price_change_l3m
        -- , p1.percentage_price_change_ltm
        -- , p1.percentage_price_change_ytd

        , c.lm_customer_new_flag
        , c.l_3_m_customer_new_flag
        , c.ltm_customer_new_flag
        , c.ytd_customer_new_flag

        , c.lm_customer_churn_flag
        , c.l_3_m_customer_churn_flag
        , c.ltm_customer_churn_flag
        , c.ytd_customer_churn_flag

        -- , c.lm_customer_existing_flag
        -- , c.l3m_customer_existing_flag
        -- , c.ltm_customer_existing_flag
        -- , c.ytd_customer_existing_flag

        , p_2.lm_product_churn_flag
        , p_2.l_3_m_product_churn_flag
        , p_2.ltm_product_churn_flag
        , p_2.ytd_product_churn_flag

        -- , p2.lm_product_existing_flag
        -- , p2.l3m_product_existing_flag
        -- , p2.ltm_product_existing_flag
        -- , p2.ytd_product_existing_flag

        , b.winback_helper
        , b.deactivation_helper
        , b.reactivation_helper
        , b.intermittent_churn_helper
        , b.lm_cross_sell_flag
        , b.lm_upsell_flag
        , b.lm_downsell_flag

        , b.l_3_m_winback_helper
        , b.l_3_m_reactivation_helper
        , b.l_3_m_cross_sell_flag
        , b.l_3_m_upsell_flag
        , b.l_3_m_downsell_flag

        , b.ltm_winback_helper
        , b.ltm_reactivation_helper
        , b.ltm_cross_sell_flag
        , b.ltm_upsell_flag
        , b.ltm_downsell_flag

        , ytd_winback_helper
        , ytd_reactivation_helper
        , b.ytd_cross_sell_flag
        , b.ytd_upsell_flag
        , b.ytd_downsell_flag

    FROM
        "arr_sandbox"."analysis"."period_revenue" p_1

    INNER JOIN "arr_sandbox"."analysis"."customer_lifecycle_events" c
        ON
            p_1.period_revenue_key = c.customer_lifecycle_events_key
            AND p_1.month_roll = c.month_roll
    INNER JOIN "arr_sandbox"."analysis"."customer_product_lifecycle_events" p_2
        ON
            p_1.period_revenue_key = p_2.customer_product_lifecycle_events_key
            AND p_1.month_roll = p_2.month_roll
    INNER JOIN "arr_sandbox"."analysis"."customer_product_revenue_events" b
        ON
            p_1.period_revenue_key = b.customer_product_revenue_events_key
            AND p_1.month_roll = b.month_roll
)

, filling_delta AS (

    SELECT

        period_revenue_key
        , customer_key
        , product_key
        , other_key
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

        -- MONTHLY DELTAS
        , CASE
            WHEN lm_customer_new_flag = 1
                THEN arr
            ELSE 0
        END AS lm_delta_customer_new

        , CASE
            WHEN lm_customer_churn_flag = 1
                THEN -arr_lm
            ELSE 0
        END AS lm_delta_customer_churn

        , CASE
            WHEN deactivation_helper = 1
                THEN -arr_lm
            ELSE 0
        END AS lm_deactivation

        , CASE
            WHEN reactivation_helper = 1
                THEN arr
            ELSE 0
        END AS lm_reactivation

        , CASE
            WHEN intermittent_churn_helper = 1
                THEN -arr_lm
            ELSE 0
        END AS lm_intermittent_churn

        , CASE
            WHEN winback_helper = 1
                THEN arr
            ELSE 0
        END AS lm_winback

        , CASE
            WHEN lm_cross_sell_flag = 1
                THEN arr
            ELSE 0
        END AS lm_delta_cross_sell

        , CASE
            WHEN
                deactivation_helper = 0
                AND intermittent_churn_helper = 0
                AND lm_product_churn_flag = 1
                THEN -arr_lm
            ELSE 0
        END AS lm_delta_downgrade

        , CASE
            WHEN lm_upsell_flag = 1
                THEN arr_lm_delta
            ELSE 0
        END AS lm_delta_upsell

        , CASE
            WHEN lm_downsell_flag = 1
                THEN arr_lm_delta
            ELSE 0
        END AS lm_delta_downsell

        -- QUARTERLY DELTAS
        , CASE
            WHEN l_3_m_customer_new_flag = 1
                THEN arr
            ELSE 0
        END AS l_3_m_delta_customer_new

        , CASE
            WHEN l_3_m_customer_churn_flag = 1
                THEN -arr_l_3_m
            ELSE 0
        END AS l_3_m_delta_customer_churn

        , CASE
            WHEN deactivation_helper = 1
                THEN -arr_l_3_m
            ELSE 0
        END AS l_3_m_deactivation

        , CASE
            WHEN intermittent_churn_helper = 1
                THEN -arr_l_3_m
            ELSE 0
        END AS l_3_m_intermittent_churn

        , CASE
            WHEN l_3_m_reactivation_helper = 1
                THEN arr
            ELSE 0
        END AS l_3_m_reactivation

        , CASE
            WHEN l_3_m_winback_helper = 1
                THEN arr
            ELSE 0
        END AS l_3_m_winback

        , CASE
            WHEN l_3_m_cross_sell_flag = 1
                THEN arr
            ELSE 0
        END AS l_3_m_delta_cross_sell

        , CASE
            WHEN
                deactivation_helper = 0
                AND intermittent_churn_helper = 0 AND l_3_m_product_churn_flag = 1
                THEN -arr_l_3_m
            ELSE 0
        END AS l_3_m_delta_downgrade

        , CASE
            WHEN l_3_m_upsell_flag = 1
                THEN arr_l_3_m_delta
            ELSE 0
        END AS l_3_m_delta_upsell

        , CASE
            WHEN l_3_m_downsell_flag = 1
                THEN arr_l_3_m_delta
            ELSE 0
        END AS l_3_m_delta_downsell

        -- YEARLY DELTAS
        , CASE
            WHEN ltm_customer_new_flag = 1
                THEN arr
            ELSE 0
        END AS ltm_delta_customer_new

        , CASE
            WHEN ltm_customer_churn_flag = 1
                THEN -arr_ltm
            ELSE 0
        END AS ltm_delta_customer_churn

        , CASE
            WHEN deactivation_helper = 1
                THEN -arr_ltm
            ELSE 0
        END AS ltm_deactivation

        , CASE
            WHEN intermittent_churn_helper = 1
                THEN -arr_ltm
            ELSE 0
        END AS ltm_intermittent_churn

        , CASE
            WHEN ltm_reactivation_helper = 1
                THEN arr
            ELSE 0
        END AS ltm_reactivation

        , CASE
            WHEN ltm_winback_helper = 1
                THEN arr
            ELSE 0
        END AS ltm_winback

        , CASE
            WHEN ltm_cross_sell_flag = 1
                THEN arr
            ELSE 0
        END AS ltm_delta_cross_sell

        , CASE
            WHEN
                deactivation_helper = 0
                AND intermittent_churn_helper = 0 AND ltm_product_churn_flag = 1
                THEN -arr_ltm
            ELSE 0
        END AS ltm_delta_downgrade

        , CASE
            WHEN ltm_upsell_flag = 1
                THEN arr_ltm_delta
            ELSE 0
        END AS ltm_delta_upsell

        , CASE
            WHEN ltm_downsell_flag = 1
                THEN arr_ltm_delta
            ELSE 0
        END AS ltm_delta_downsell

        -- YTD DELTAS
        , CASE
            WHEN ytd_customer_new_flag = 1
                THEN arr
            ELSE 0
        END AS ytd_delta_customer_new

        , CASE
            WHEN deactivation_helper = 1
                THEN -arr_ytd
            ELSE 0
        END AS ytd_deactivation

        , CASE
            WHEN ytd_customer_churn_flag = 1
                THEN -arr_ytd
            ELSE 0
        END AS ytd_delta_customer_churn

        , CASE
            WHEN intermittent_churn_helper = 1
                THEN -arr_ytd
            ELSE 0
        END AS ytd_intermittent_churn

        , CASE
            WHEN ytd_reactivation_helper = 1
                THEN arr
            ELSE 0
        END AS ytd_reactivation

        , CASE
            WHEN ytd_winback_helper = 1
                THEN arr
            ELSE 0
        END AS ytd_winback

        , CASE
            WHEN ytd_cross_sell_flag = 1
                THEN arr
            ELSE 0
        END AS ytd_delta_cross_sell

        , CASE
            WHEN
                deactivation_helper = 0
                AND intermittent_churn_helper = 0 AND ytd_product_churn_flag = 1
                THEN -arr_ytd
            ELSE 0
        END AS ytd_delta_downgrade

        , CASE
            WHEN ytd_upsell_flag = 1
                THEN arr_ytd_delta
            ELSE 0
        END AS ytd_delta_upsell

        , CASE
            WHEN ytd_downsell_flag = 1
                THEN arr_ytd_delta
            ELSE 0
        END AS ytd_delta_downsell

        -- PRICE VOLUME
    -- uncomment below CASE STATEMENTS to find the price volume difference

    -- MONTHLY
    -- , CASE  
    --     WHEN lm_upsell_flag = 1
    --     THEN arr_lm_delta * percentage_price_change_lm
    --     ELSE 0
    -- END AS lm_delta_price_upsell

    -- , CASE  
    --     WHEN lm_upsell_flag = 1
    --     THEN arr_lm_delta * (1 - percentage_price_change_lm)
    --     ELSE 0
    -- END AS lm_delta_volume_upsell

    -- , CASE  
    --     WHEN lm_downsell_flag = 1
    --     THEN arr_lm_delta * percentage_price_change_lm
    --     ELSE 0
    -- END AS lm_delta_price_downsell

    -- , CASE  
    --     WHEN lm_downsell_flag = 1
    --     THEN arr_lm_delta * (1 - percentage_price_change_lm)
    --     ELSE 0
    -- END AS lm_delta_volume_downsell

    -- -- QUARTERLY
    -- , CASE  
    --     WHEN l3m_upsell_flag = 1
    --     THEN arr_l3m_delta * percentage_price_change_l3m
    --     ELSE 0
    -- END AS l3m_delta_price_upsell

    -- , CASE  
    --     WHEN l3m_upsell_flag = 1
    --     THEN arr_l3m_delta * (1 - percentage_price_change_l3m)
    --     ELSE 0
    -- END AS l3m_delta_volume_upsell

    -- , CASE  
    --     WHEN l3m_downsell_flag = 1
    --     THEN arr_l3m_delta * percentage_price_change_l3m
    --     ELSE 0
    -- END AS l3m_delta_price_downsell

    -- , CASE  
    --     WHEN l3m_downsell_flag = 1
    --     THEN arr_l3m_delta * (1 - percentage_price_change_l3m)
    --     ELSE 0
    -- END AS l3m_delta_volume_downsell

    -- -- YEARLY

    -- , CASE  
    --     WHEN ltm_upsell_flag = 1
    --     THEN arr_ltm_delta * percentage_price_change_ltm
    --     ELSE 0
    -- END AS ltm_delta_price_upsell

    -- , CASE  
    --     WHEN ltm_upsell_flag = 1
    --     THEN arr_ltm_delta * (1 - percentage_price_change_ltm)
    --     ELSE 0
    -- END AS ltm_delta_volume_upsell

    -- , CASE  
    --     WHEN ltm_downsell_flag = 1
    --     THEN arr_ltm_delta * percentage_price_change_ltm
    --     ELSE 0
    -- END AS ltm_delta_price_downsell

    -- , CASE  
    --     WHEN ltm_downsell_flag = 1
    --     THEN arr_ltm_delta * (1 - percentage_price_change_ltm)
    --     ELSE 0
    -- END AS ltm_delta_volume_downsell

    -- -- YTD    

    -- , CASE  
    --     WHEN ytd_upsell_flag = 1
    --     THEN arr_ytd_delta * percentage_price_change_ytd
    --     ELSE 0
    -- END AS ytd_delta_price_upsell

    -- , CASE  
    --     WHEN ytd_upsell_flag = 1
    --     THEN arr_ytd_delta * (1 - percentage_price_change_ytd)
    --     ELSE 0
    -- END AS ytd_delta_volume_upsell

    -- , CASE  
    --     WHEN ytd_downsell_flag = 1
    --     THEN arr_ytd_delta * percentage_price_change_ytd
    --     ELSE 0
    -- END AS ytd_delta_price_downsell

    -- , CASE  
    --     WHEN ytd_downsell_flag=1
    --     THEN arr_ytd_delta * (1 - percentage_price_change_ytd)
    --     ELSE 0
    -- END AS ytd_delta_volume_downsell

    FROM get_arr_business_flag
)

SELECT

    period_revenue_key AS delta_revenue_key
    , month_roll
    , customer_key
    , product_key
    , other_key
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

    , lm_delta_customer_new
    , lm_delta_customer_churn
    , lm_deactivation
    , lm_reactivation
    , lm_intermittent_churn
    , lm_winback
    , lm_delta_cross_sell
    , lm_delta_downgrade
    , lm_delta_upsell
    -- , lm_delta_price_upsell
    -- , lm_delta_volume_upsell
    , lm_delta_downsell
    -- , lm_delta_price_downsell
    -- , lm_delta_volume_downsell

    , l_3_m_delta_customer_new
    , l_3_m_delta_customer_churn
    , l_3_m_deactivation
    , l_3_m_reactivation
    , l_3_m_intermittent_churn
    , l_3_m_winback
    , l_3_m_delta_cross_sell
    , l_3_m_delta_downgrade
    , l_3_m_delta_upsell
    -- , l3m_delta_price_upsell
    -- , l3m_delta_volume_upsell
    , l_3_m_delta_downsell
    -- , l3m_delta_price_downsell
    -- , l3m_delta_volume_downsell

    , ltm_delta_customer_new
    , ltm_delta_customer_churn
    , ltm_deactivation
    , ltm_reactivation
    , ltm_intermittent_churn
    , ltm_winback
    , ltm_delta_cross_sell
    , ltm_delta_downgrade
    , ltm_delta_upsell
    -- , ltm_delta_price_upsell
    -- , ltm_delta_volume_upsell
    , ltm_delta_downsell
    -- , ltm_delta_price_downsell
    -- , ltm_delta_volume_downsell

    , ytd_delta_customer_new
    , ytd_delta_customer_churn
    , ytd_reactivation
    , ytd_deactivation
    , ytd_intermittent_churn
    , ytd_winback
    , ytd_delta_cross_sell
    , ytd_delta_downgrade
    , ytd_delta_upsell
    -- , ytd_delta_price_upsell
    -- , ytd_delta_volume_upsell
    , ytd_delta_downsell
    -- , ytd_delta_price_downsell
    -- , ytd_delta_volume_downsell

INTO analysis.delta_revenue
FROM filling_delta
END;