CREATE OR ALTER PROCEDURE analysis.sp_customer_product_revenue_events
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS analysis.customer_product_revenue_events; 
    END;

/* This model calculates flags for revenue events by assessing product growth and decline across various periods (monthly, quarterly, yearly, and year-to-date), joining revenue data with customer and product lifecycle information to identify cross-sell, upsell, and downsell activities. */

WITH product_grew AS (

    SELECT

        period_revenue_key
        , customer_key
        , customer_level_1
        , customer_level_2
        , customer_level_3
        , customer_level_4
        , customer_level_5
        , customer_level_6
        , customer_level_7
        , customer_level_8
        , customer_level_9
        , customer_name
        , customer_region
        , product_key
        , product_level_1
        , product_level_2
        , other_key
        , month_roll
        , arr
        , arr_lm
        , arr_l_3_m
        , arr_ltm
        , arr_ytd

        --MONTHLY GREW
        , CASE
            WHEN sum_arr_lm_delta > 0 THEN 1
            ELSE 0
        END AS product_grew_monthly
        , CASE
            WHEN sum_arr_lm_delta < 0 THEN 1
            ELSE 0
        END AS product_declined_monthly
        --QUARTERLY GREW
        , CASE
            WHEN sum_arr_l_3_m_delta > 0 THEN 1
            ELSE 0
        END AS product_grew_quarterly
        , CASE
            WHEN sum_arr_l_3_m_delta < 0 THEN 1
            ELSE 0
        END AS product_declined_quarterly
        --YEARLY GREW
        , CASE
            WHEN sum_arr_ltm_delta > 0 THEN 1
            ELSE 0
        END AS product_grew_yearly
        , CASE
            WHEN sum_arr_ltm_delta < 0 THEN 1
            ELSE 0
        END AS product_declined_yearly
        --ytd GREW
        , CASE
            WHEN sum_arr_ytd_delta > 0 THEN 1
            ELSE 0
        END AS product_grew_ytd
        , CASE
            WHEN sum_arr_ytd_delta < 0 THEN 1
            ELSE 0
        END AS product_declined_ytd

    FROM "arr_sandbox"."analysis"."period_revenue"
)

, ranked_product AS (

    SELECT

        p_1.period_revenue_key
        , p_1.customer_key
        , p_1.customer_level_1
        , p_1.customer_level_2
        , p_1.customer_level_3
        , p_1.customer_level_4
        , p_1.customer_level_5
        , p_1.customer_level_6
        , p_1.customer_level_7
        , p_1.customer_level_8
        , p_1.customer_level_9
        , p_1.customer_name
        , p_1.customer_region
        , p_1.product_key
        , p_1.product_level_1
        , p_1.product_level_2
        , p_1.other_key
        , p_1.arr
        , p_1.arr_lm
        , p_1.arr_l_3_m
        , p_1.arr_ltm
        , p_1.arr_ytd
        , p_1.month_roll
        , c.lm_customer_new_flag
        , c.l_3_m_customer_new_flag
        , c.ltm_customer_new_flag
        , c.ytd_customer_new_flag
        , c.lm_customer_churn_flag
        , c.l_3_m_customer_churn_flag
        , c.ltm_customer_churn_flag
        , c.ytd_customer_churn_flag

        -- MONTHLY FLAGS
        , CASE
            WHEN
                lm_customer_existing_flag = 1
                AND product_start_month = p_1.month_roll
                THEN 1
            ELSE 0
        END AS lm_cross_sell_flag
        , CASE
            WHEN
                product_grew_monthly = 1
                AND lm_product_existing_flag = 1
                THEN 1
            ELSE 0
        END AS lm_upsell_flag
        , CASE
            WHEN
                product_declined_monthly = 1
                AND lm_product_existing_flag = 1
                THEN 1
            ELSE 0
        END AS lm_downsell_flag

        -- QUARTERLY FLAGS
        , CASE
            WHEN
                l_3_m_customer_existing_flag = 1
                AND l_3_m_product_existing_flag = 0
                THEN 1
            ELSE 0
        END AS l_3_m_cross_sell_flag
        , CASE
            WHEN
                product_grew_quarterly = 1
                AND l_3_m_product_existing_flag = 1
                THEN 1
            ELSE 0
        END AS l_3_m_upsell_flag
        , CASE
            WHEN
                product_declined_quarterly = 1
                AND l_3_m_product_existing_flag = 1
                THEN 1
            ELSE 0
        END AS l_3_m_downsell_flag

        -- YEARLY FLAGS
        , CASE
            WHEN
                ltm_customer_existing_flag = 1
                AND ltm_product_existing_flag = 0
                THEN 1
            ELSE 0
        END AS ltm_cross_sell_flag
        , CASE
            WHEN
                product_grew_yearly = 1
                AND ltm_product_existing_flag = 1
                THEN 1
            ELSE 0
        END AS ltm_upsell_flag
        , CASE
            WHEN
                product_declined_yearly = 1
                AND ltm_product_existing_flag = 1
                THEN 1
            ELSE 0
        END AS ltm_downsell_flag

        -- ytd FLAGS
        , CASE
            WHEN
                ytd_customer_existing_flag = 1
                AND ytd_product_existing_flag = 0
                THEN 1
            ELSE 0
        END AS ytd_cross_sell_flag
        , CASE
            WHEN
                product_grew_ytd = 1
                AND ytd_product_existing_flag = 1
                THEN 1
            ELSE 0
        END AS ytd_upsell_flag
        , CASE
            WHEN
                product_declined_ytd = 1
                AND ytd_product_existing_flag = 1
                THEN 1
            ELSE 0
        END AS ytd_downsell_flag

    FROM product_grew p_1

    INNER JOIN "arr_sandbox"."analysis"."customer_lifecycle_events" c
        ON
            p_1.period_revenue_key = c.customer_lifecycle_events_key
            AND p_1.month_roll = c.month_roll
    INNER JOIN "arr_sandbox"."analysis"."customer_product_lifecycle_events" p_2
        ON
            p_1.period_revenue_key = p_2.customer_product_lifecycle_events_key
            AND p_1.month_roll = p_2.month_roll
    INNER JOIN "arr_sandbox"."analysis"."customer_product_contract" p_3
        ON
            p_1.customer_level_1 = p_3.customer_level_1
            AND p_1.customer_level_2 = p_3.customer_level_2
            AND p_1.customer_level_3 = p_3.customer_level_3
            AND p_1.customer_level_4 = p_3.customer_level_4
            AND p_1.customer_level_5 = p_3.customer_level_5
            AND p_1.customer_level_6 = p_3.customer_level_6
            AND p_1.customer_level_7 = p_3.customer_level_7
            AND p_1.customer_level_8 = p_3.customer_level_8
            AND p_1.customer_level_9 = p_3.customer_level_9
            AND
            p_1.product_level_1 = p_3.product_level_1
            AND p_1.product_level_2 = p_3.product_level_2
)

-- Logic for Intermittent_churn , Winback , deactivation, reactivaiton helper columns

, find_next_nonzero_month AS (

    SELECT
        current_plan.customer_level_1
        , current_plan.month_roll
        , MIN(next_plan.month_roll) AS next_nonzero_month -- finding next non zero month, so can get possible winback dates

    FROM
        ranked_product current_plan

    LEFT JOIN
        ranked_product next_plan
        ON
            current_plan.customer_level_1 = next_plan.customer_level_1
            AND next_plan.arr != 0
            AND current_plan.month_roll < next_plan.month_roll
    WHERE
        current_plan.lm_customer_new_flag = 0
    GROUP BY
        current_plan.customer_level_1
        , current_plan.month_roll
)

, find_prev_nonzero_month AS (

    SELECT DISTINCT

        current_plan.customer_level_1
        , current_plan.month_roll
        , MAX(prev_plan.month_roll) AS prev_nonzero_month  -- will get possible intermittent churn dates

    FROM
        ranked_product current_plan
    LEFT JOIN
        ranked_product prev_plan
        ON
            current_plan.customer_level_1 = prev_plan.customer_level_1
            AND prev_plan.arr != 0
            AND current_plan.month_roll > prev_plan.month_roll
    GROUP BY
        current_plan.customer_level_1
        , current_plan.month_roll
)

, find_next_nonzero_month_l_3_m AS (

    SELECT

        current_plan.customer_level_1
        , current_plan.month_roll
        , MIN(next_plan.month_roll) AS next_nonzero_month_l_3_m -- finding next non zero month, so can get possible winback dates
    FROM
        ranked_product current_plan
    LEFT JOIN
        ranked_product next_plan
        ON
            current_plan.customer_level_1 = next_plan.customer_level_1
            AND next_plan.arr_l_3_m != 0
            AND current_plan.month_roll < next_plan.month_roll
    WHERE
        current_plan.l_3_m_customer_new_flag = 0
    GROUP BY
        current_plan.customer_level_1
        , current_plan.month_roll
)

, find_prev_nonzero_month_l_3_m AS (

    SELECT DISTINCT

        current_plan.customer_level_1
        , current_plan.month_roll
        , MAX(prev_plan.month_roll) AS prev_nonzero_month_l_3_m  -- will get possible intermittent churn dates

    FROM ranked_product current_plan

    LEFT JOIN
        ranked_product prev_plan
        ON
            current_plan.customer_level_1 = prev_plan.customer_level_1
            AND prev_plan.arr_l_3_m != 0
            AND current_plan.month_roll > prev_plan.month_roll

    GROUP BY
        current_plan.customer_level_1
        , current_plan.month_roll
)

, find_next_nonzero_month_ltm AS (

    SELECT

        current_plan.customer_level_1
        , current_plan.month_roll
        , MIN(next_plan.month_roll) AS next_nonzero_month_ltm -- finding next non zero month, so can get possible winback dates

    FROM ranked_product current_plan

    LEFT JOIN
        ranked_product next_plan
        ON
            current_plan.customer_level_1 = next_plan.customer_level_1
            AND next_plan.arr_ltm != 0
            AND current_plan.month_roll < next_plan.month_roll

    WHERE current_plan.ltm_customer_new_flag = 0

    GROUP BY
        current_plan.customer_level_1
        , current_plan.month_roll
)

, find_prev_nonzero_month_ltm AS (

    SELECT DISTINCT

        current_plan.customer_level_1
        , current_plan.month_roll
        , MAX(prev_plan.month_roll) AS prev_nonzero_month_ltm  -- will get possible intermittent churn dates

    FROM
        ranked_product current_plan
    LEFT JOIN
        ranked_product prev_plan
        ON
            current_plan.customer_level_1 = prev_plan.customer_level_1
            AND prev_plan.arr_ltm != 0
            AND current_plan.month_roll > prev_plan.month_roll
    GROUP BY
        current_plan.customer_level_1
        , current_plan.month_roll
)

, find_next_nonzero_month_ytd AS (

    SELECT

        current_plan.customer_level_1
        , current_plan.month_roll
        , MIN(next_plan.month_roll) AS next_nonzero_month_ytd -- finding next non zero month, so can get possible winback dates

    FROM
        ranked_product current_plan
    LEFT JOIN
        ranked_product next_plan
        ON
            current_plan.customer_level_1 = next_plan.customer_level_1
            AND next_plan.arr_ytd != 0
            AND current_plan.month_roll < next_plan.month_roll
    WHERE
        current_plan.ytd_customer_new_flag = 0
    GROUP BY
        current_plan.customer_level_1
        , current_plan.month_roll
)

, find_prev_nonzero_month_ytd AS (

    SELECT DISTINCT

        current_plan.customer_level_1
        , current_plan.month_roll
        , MAX(prev_plan.month_roll) AS prev_nonzero_month_ytd  -- will get possible intermittent churn dates

    FROM
        ranked_product current_plan
    LEFT JOIN
        ranked_product prev_plan
        ON
            current_plan.customer_level_1 = prev_plan.customer_level_1
            AND prev_plan.arr_ytd != 0
            AND current_plan.month_roll > prev_plan.month_roll
    GROUP BY
        current_plan.customer_level_1
        , current_plan.month_roll
)

, pre_final AS (

    SELECT
        rp.*
        , fn.next_nonzero_month
        , fp.prev_nonzero_month
        , fn_3_m.next_nonzero_month_l_3_m
        , fp_3_m.prev_nonzero_month_l_3_m
        , fnm.next_nonzero_month_ltm
        , fpm.prev_nonzero_month_ltm
        , fntd.next_nonzero_month_ytd
        , fptd.prev_nonzero_month_ytd

    FROM
        ranked_product rp                                    -- will get possible dates of churn winback in one table
    LEFT JOIN
        find_next_nonzero_month fn
        ON
            rp.customer_level_1 = fn.customer_level_1
            AND rp.month_roll = fn.month_roll
    LEFT JOIN
        find_prev_nonzero_month fp
        ON
            rp.customer_level_1 = fp.customer_level_1
            AND rp.month_roll = fp.month_roll
    LEFT JOIN
        find_next_nonzero_month_l_3_m fn_3_m
        ON
            rp.customer_level_1 = fn_3_m.customer_level_1
            AND rp.month_roll = fn_3_m.month_roll
    LEFT JOIN
        find_prev_nonzero_month_l_3_m fp_3_m
        ON
            rp.customer_level_1 = fp_3_m.customer_level_1
            AND rp.month_roll = fp_3_m.month_roll
    LEFT JOIN
        find_next_nonzero_month_ltm fnm
        ON
            rp.customer_level_1 = fnm.customer_level_1
            AND rp.month_roll = fnm.month_roll
    LEFT JOIN
        find_prev_nonzero_month_ltm fpm
        ON
            rp.customer_level_1 = fpm.customer_level_1
            AND rp.month_roll = fpm.month_roll

    LEFT JOIN
        find_next_nonzero_month_ytd fntd
        ON
            rp.customer_level_1 = fntd.customer_level_1
            AND rp.month_roll = fntd.month_roll

    LEFT JOIN
        find_prev_nonzero_month_ytd fptd
        ON
            rp.customer_level_1 = fptd.customer_level_1
            AND rp.month_roll = fptd.month_roll
)

, customer_product_revenue_events AS (

    SELECT
        *
        --creating a flag column bsaed on conditions with date difference
        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) = 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month AS DATE), CAST(next_nonzero_month AS DATE)) - 2 = 1
                THEN 1
            ELSE 0
        END AS deactivation_helper
        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) != 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month AS DATE), CAST(month_roll AS DATE)) - 2 = 1
                THEN 1
            ELSE 0
        END AS reactivation_helper
        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) = 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month AS DATE), CAST(next_nonzero_month AS DATE)) - 1 > 3
                THEN 1
            ELSE 0
        END AS intermittent_churn_helper
        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) != 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month AS DATE), CAST(month_roll AS DATE)) - 1 > 3
                THEN 1
            ELSE 0
        END AS winback_helper
        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) != 0
                AND (SUM(arr_l_3_m) OVER (PARTITION BY customer_level_1, month_roll)) = 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month_l_3_m AS DATE), CAST(next_nonzero_month_l_3_m AS DATE)) - 1 > 3
                THEN 1
            ELSE 0
        END AS l_3_m_winback_helper
        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) != 0
                AND (SUM(arr_l_3_m) OVER (PARTITION BY customer_level_1, month_roll)) = 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month_l_3_m AS DATE), CAST(next_nonzero_month_l_3_m AS DATE)) - 2 = 1
                THEN 1
            ELSE 0
        END AS l_3_m_reactivation_helper

        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) != 0
                AND (SUM(arr_ltm) OVER (PARTITION BY customer_level_1, month_roll)) = 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month_ltm AS DATE), CAST(next_nonzero_month_ltm AS DATE)) - 1 > 3
                THEN 1
            ELSE 0
        END AS ltm_winback_helper

        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) != 0
                AND (SUM(arr_ltm) OVER (PARTITION BY customer_level_1, month_roll)) = 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month_ltm AS DATE), CAST(next_nonzero_month_ltm AS DATE)) - 2 = 1
                THEN 1
            ELSE 0
        END AS ltm_reactivation_helper

        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) != 0
                AND (SUM(arr_ytd) OVER (PARTITION BY customer_level_1, month_roll)) = 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month_ytd AS DATE), CAST(next_nonzero_month_ytd AS DATE)) - 1 > 3
                THEN 1
            ELSE 0
        END AS ytd_winback_helper

        , CASE
            WHEN
                (SUM(arr) OVER (PARTITION BY customer_level_1, month_roll)) != 0
                AND (SUM(arr_ytd) OVER (PARTITION BY customer_level_1, month_roll)) = 0
                AND DATEDIFF(MONTH, CAST(prev_nonzero_month_ytd AS DATE), CAST(next_nonzero_month_ytd AS DATE)) - 2 = 1
                THEN 1
            ELSE 0
        END AS ytd_reactivation_helper

    FROM pre_final

)

SELECT

    period_revenue_key AS customer_product_revenue_events_key
    , customer_key
    , customer_level_1
    , customer_level_2
    , customer_level_3
    , customer_level_4
    , customer_level_5
    , customer_level_6
    , customer_level_7
    , customer_level_8
    , customer_level_9
    , customer_name
    , customer_region
    , product_key
    , product_level_1
    , product_level_2
    , other_key
    , month_roll
    , winback_helper
    , deactivation_helper
    , reactivation_helper
    , intermittent_churn_helper

    , l_3_m_reactivation_helper
    , l_3_m_winback_helper
    , ltm_reactivation_helper
    , ltm_winback_helper
    , ytd_reactivation_helper
    , ytd_winback_helper
    , CASE
        WHEN
            winback_helper = 0 AND deactivation_helper = 0
            AND reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN lm_cross_sell_flag
        ELSE 0
    END                AS lm_cross_sell_flag
    , CASE
        WHEN
            winback_helper = 0 AND deactivation_helper = 0
            AND reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN lm_upsell_flag
        ELSE 0
    END                AS lm_upsell_flag
    , CASE
        WHEN
            winback_helper = 0 AND deactivation_helper = 0
            AND reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN lm_downsell_flag
        ELSE 0
    END                AS lm_downsell_flag
    , CASE
        WHEN
            l_3_m_winback_helper = 0 AND deactivation_helper = 0
            AND l_3_m_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN l_3_m_cross_sell_flag
        ELSE 0
    END                AS l_3_m_cross_sell_flag
    , CASE
        WHEN
            l_3_m_winback_helper = 0 AND deactivation_helper = 0
            AND l_3_m_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN l_3_m_upsell_flag
        ELSE 0
    END                AS l_3_m_upsell_flag
    , CASE
        WHEN
            l_3_m_winback_helper = 0 AND deactivation_helper = 0
            AND l_3_m_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN l_3_m_downsell_flag
        ELSE 0
    END                AS l_3_m_downsell_flag
    , CASE
        WHEN
            ltm_winback_helper = 0 AND deactivation_helper = 0
            AND ltm_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN ltm_cross_sell_flag
        ELSE 0
    END                AS ltm_cross_sell_flag
    , CASE
        WHEN
            ltm_winback_helper = 0 AND deactivation_helper = 0
            AND ltm_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN ltm_upsell_flag
        ELSE 0
    END                AS ltm_upsell_flag
    , CASE
        WHEN
            ltm_winback_helper = 0 AND deactivation_helper = 0
            AND ltm_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN ltm_downsell_flag
        ELSE 0
    END                AS ltm_downsell_flag
    , CASE
        WHEN
            ytd_winback_helper = 0 AND deactivation_helper = 0
            AND ytd_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN ytd_cross_sell_flag
        ELSE 0
    END                AS ytd_cross_sell_flag
    , CASE
        WHEN
            ytd_winback_helper = 0 AND deactivation_helper = 0
            AND ytd_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN ytd_upsell_flag
        ELSE 0
    END                AS ytd_upsell_flag
    , CASE
        WHEN
            ytd_winback_helper = 0 AND deactivation_helper = 0
            AND ytd_reactivation_helper = 0 AND intermittent_churn_helper = 0
            THEN ytd_downsell_flag
        ELSE 0
    END                AS ytd_downsell_flag

INTO analysis.customer_product_revenue_events
FROM
    customer_product_revenue_events
END;