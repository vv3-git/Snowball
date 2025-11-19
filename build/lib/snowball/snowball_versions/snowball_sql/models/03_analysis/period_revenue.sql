CREATE OR ALTER PROCEDURE analysis.sp_period_revenue
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS analysis.period_revenue; 
    END;

/* This stored procedure calculates ARR changes over different periods (monthly, quarterly, yearly, and year-to-date) and provides insights into how revenue evolves over time.*/

WITH get_ytd_start AS (

    SELECT

        m.monthly_revenue_key
        , m.customer_key
        , m.customer_level_1
        , m.customer_level_2
        , m.customer_level_3
        , m.customer_level_4
        , m.customer_level_5
        , m.customer_level_6
        , m.customer_level_7
        , m.customer_level_8
        , m.customer_level_9
        , m.customer_name
        , m.customer_region
        , m.product_key
        , m.product_level_1
        , m.product_level_2
        , m.other_key
        , m.other_dim_1
        , m.other_dim_2
        , m.month_roll
        , m.arr
        , m.mrr
        , m.volume
        , m.ytd_helper
        , m.revenue_type
    FROM
        "arr_sandbox"."analysis"."monthly_revenue" m

)

-- Calculate revenue lags based on the above declared variables for monthly, yearly, and quarterly.

, get_revenue_lags AS (

    SELECT

        a.monthly_revenue_key
        , a.customer_key
        , a.customer_level_1
        , a.customer_level_2
        , a.customer_level_3
        , a.customer_level_4
        , a.customer_level_5
        , a.customer_level_6
        , a.customer_level_7
        , a.customer_level_8
        , a.customer_level_9
        , a.customer_name
        , a.customer_region
        , a.product_key
        , a.product_level_1
        , a.product_level_2
        , a.other_key
        , a.other_dim_1
        , a.other_dim_2
        , a.month_roll
        , a.arr
        , a.mrr
        , a.volume
        , a.revenue_type
        , COALESCE(LAG(a.arr, 1) OVER (PARTITION BY a.monthly_revenue_key ORDER BY a.month_roll), 0)  AS arr_lm   -- Monthly Revenue Lag
        , COALESCE(LAG(a.arr, 3) OVER (PARTITION BY a.monthly_revenue_key ORDER BY a.month_roll), 0)  AS arr_l_3_m  -- Quarterly Revenue Lag
        , COALESCE(LAG(a.arr, 12) OVER (PARTITION BY a.monthly_revenue_key ORDER BY a.month_roll), 0) AS arr_ltm  -- Yearly Revenue Lag
        , COALESCE(b.arr, 0)                                                                          AS arr_ytd
        -- Uncommend the below lines to get the volume lags
        -- , LAG(volume, 1, 0)    OVER (PARTITION BY monthly_revenue_key ORDER BY month_roll)     AS volume_lm,  -- Monthly Volume Lag
        -- , LAG(volume, 3, 0)  OVER (PARTITION BY monthly_revenue_key ORDER BY month_roll)     AS volume_l3m, -- Quarterly Volume Lag
        -- , LAG(volume, 12, 0)     OVER (PARTITION BY monthly_revenue_key ORDER BY month_roll)     AS volume_ltm, -- Yearly Volume Lag
        -- , LAG(volume, ytd_helper, 0)  OVER (PARTITION BY monthly_revenue_key ORDER BY month_roll)               AS volume_ytd  -- Ytd Volume Lag
    FROM get_ytd_start a

    LEFT JOIN
        get_ytd_start b
        ON
            a.customer_key = b.customer_key
            AND a.product_key = b.product_key
            AND a.month_roll = DATEADD(MONTH, a.ytd_helper, b.month_roll)
)

, get_delta_revenue AS (

    SELECT

        monthly_revenue_key
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
        , other_dim_1
        , other_dim_2
        , month_roll
        , revenue_type
        , mrr
        , arr
        , volume
        , arr_lm
        , arr_l_3_m
        , arr_ltm
        , arr_ytd
        -- Uncommand the lines to get the  volume lags
        -- volume_lm,
        -- volume_l3m,
        -- volume_ltm,
        -- volume_ytd,
        , arr - arr_lm    AS arr_lm_delta
        , arr - arr_l_3_m AS arr_l_3_m_delta
        , arr - arr_ltm   AS arr_ltm_delta
        , arr - arr_ytd   AS arr_ytd_delta

    FROM
        get_revenue_lags
)

, find_price_volume_deltas AS (
    SELECT
        customer_level_1
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
        , revenue_type
        -- Master Product level Revenue
        , SUM(arr_lm_delta)    AS sum_arr_lm_delta
        , SUM(arr_l_3_m_delta) AS sum_arr_l_3_m_delta
        , SUM(arr_ltm_delta)   AS sum_arr_ltm_delta
        , SUM(arr_ytd_delta)   AS sum_arr_ytd_delta

    -- Uncomment the below lines to get the price volume increases
    -- -- PRICE DELTAS
    -- , CASE
    --     -- Check if both current and last month's volumes are non-zero to avoid division by zero
    --     WHEN SUM(volume) <> 0 AND SUM(volume_lm) <> 0 THEN 
    --         -- Calculate the price change (ARR per unit volume) between current and last month
    --         ((SUM(arr) / SUM(volume)) - (SUM(arr_lm) / SUM(volume_lm))) * SUM(volume)
    --     ELSE 
    --         -- If either volume is zero, set the price delta to 0
    --         0 
    -- END AS abs_price_lm_delta

    -- , CASE
    --     WHEN SUM(volume) <> 0 AND SUM(volume_l3m) <> 0 THEN 
    --         ((SUM(arr) / SUM(volume)) - (SUM(arr_l3m) / SUM(volume_l3m))) * SUM(volume)
    --     ELSE 
    --         0 
    -- END AS abs_price_l3m_delta

    -- , CASE
    --     WHEN SUM(volume) <> 0 AND SUM(volume_ltm) <> 0 THEN 
    --         ((SUM(arr) / SUM(volume)) - (SUM(arr_ltm) / SUM(volume_ltm))) * SUM(volume)
    --     ELSE 
    --         0 
    -- END AS abs_price_ltm_delta

    -- , CASE
    --     WHEN SUM(volume) <> 0 AND SUM(volume_ytd) <> 0 THEN 
    --         ((SUM(arr) / SUM(volume)) - (SUM(arr_ytd) / SUM(volume_ytd))) * SUM(volume)
    --     ELSE 
    --         0 
    -- END AS abs_price_ytd_delta

    -- -- VOLUME DELTAS
    -- , CASE
    --     WHEN SUM(volume) <> 0 AND SUM(volume_lm) <> 0 THEN 
    --         (SUM(volume) - SUM(volume_lm)) * SUM(arr_lm) / SUM(volume_lm)
    --     ELSE 
    --         SUM(arr_lm_delta) 
    -- END AS abs_volume_lm_delta

    -- , CASE
    --     WHEN SUM(volume) <> 0 AND SUM(volume_l3m) <> 0 THEN 
    --         (SUM(volume) - SUM(volume_l3m)) * SUM(arr_l3m) / SUM(volume_l3m)
    --     ELSE 
    --         SUM(arr_l3m_delta) 
    -- END AS abs_volume_l3m_delta

    -- , CASE
    --     WHEN SUM(volume) <> 0 AND SUM(volume_ltm) <> 0 THEN 
    --         (SUM(volume) - SUM(volume_ltm)) * SUM(arr_ltm) / SUM(volume_ltm)
    --     ELSE 
    --         SUM(arr_ltm_delta) 
    -- END AS abs_volume_ltm_delta

    -- , CASE
    --     WHEN SUM(volume) <> 0 AND SUM(volume_ytd) <> 0 THEN 
    --         (SUM(volume) - SUM(volume_ytd)) * SUM(arr_ytd) / SUM(volume_ytd)
    --     ELSE 
    --         SUM(arr_ytd_delta) 
    -- END AS abs_volume_ytd_delta

    FROM
        get_delta_revenue
    GROUP BY
        customer_level_1
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
        , revenue_type
)

, get_percentage_change AS (
    SELECT
        customer_level_1
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
        , revenue_type
        , sum_arr_lm_delta
        , sum_arr_l_3_m_delta
        , sum_arr_ltm_delta
        , sum_arr_ytd_delta

    -- Uncomment the lines to get the price volume increases
    -- MONTHLY
    -- , CASE 
    --     WHEN abs_price_lm_delta <> 0 THEN 1 
    --     ELSE 0 
    -- END AS price_change_lm

    -- , CASE 
    --     WHEN abs_volume_lm_delta <> 0 THEN 1 
    --     ELSE 0 
    -- END AS volume_change_lm

    -- , CASE 
    --     WHEN sum_arr_lm_delta <> 0 THEN abs_price_lm_delta / sum_arr_lm_delta 
    -- END AS percentage_price_change_lm

    -- -- QUARTERLY
    -- , CASE 
    --     WHEN abs_price_l3m_delta <> 0 THEN 1 
    --     ELSE 0 
    -- END AS price_change_l3m

    -- , CASE 
    --     WHEN abs_volume_l3m_delta <> 0 THEN 1 
    --     ELSE 0 
    -- END AS volume_change_l3m

    -- , CASE 
    --     WHEN sum_arr_l3m_delta <> 0 THEN abs_price_l3m_delta / sum_arr_l3m_delta 
    -- END AS percentage_price_change_l3m

    -- -- YEARLY
    -- , CASE 
    --     WHEN abs_price_ltm_delta <> 0 THEN 1 
    --     ELSE 0 
    -- END AS price_change_ltm

    -- , CASE 
    --     WHEN abs_volume_ltm_delta <> 0 THEN 1 
    --     ELSE 0 
    -- END AS volume_change_ltm

    -- , CASE 
    --     WHEN sum_arr_ltm_delta <> 0 THEN abs_price_ltm_delta / sum_arr_ltm_delta 
    -- END AS percentage_price_change_ltm

    -- -- YTD
    -- , CASE 
    --     WHEN abs_price_ytd_delta <> 0 THEN 1 
    --     ELSE 0 
    -- END AS price_change_ytd

    -- , CASE 
    --     WHEN abs_volume_ytd_delta <> 0 THEN 1 
    --     ELSE 0 
    -- END AS volume_change_ytd

    -- , CASE 
    --     WHEN sum_arr_ytd_delta <> 0 THEN abs_price_ytd_delta / sum_arr_ytd_delta 
    -- END AS percentage_price_change_ytd

    FROM
        find_price_volume_deltas
)

SELECT

    r.monthly_revenue_key AS period_revenue_key
    , r.customer_key
    , r.customer_level_1
    , r.customer_level_2
    , r.customer_level_3
    , r.customer_level_4
    , r.customer_level_5
    , r.customer_level_6
    , r.customer_level_7
    , r.customer_level_8
    , r.customer_level_9
    , r.customer_name
    , r.customer_region
    , r.product_key
    , r.product_level_1
    , r.product_level_2
    , r.other_key
    , r.other_dim_1
    , r.other_dim_2
    , r.month_roll
    , r.mrr
    , r.arr
    , r.volume
    , r.arr_lm
    , r.arr_l_3_m
    , r.arr_ltm
    , r.arr_ytd
    , r.arr_lm_delta
    , r.arr_l_3_m_delta
    , r.arr_ltm_delta
    , r.arr_ytd_delta
    , p.sum_arr_lm_delta
    , p.sum_arr_l_3_m_delta
    , p.sum_arr_ltm_delta
    , p.sum_arr_ytd_delta
-- Uncomment the lines to get the price volume increases
-- , p.abs_price_lm_delta
-- , p.percentage_price_change_lm
-- , p.percentage_price_change_l3m
-- , p.percentage_price_change_ltm
-- , p.percentage_price_change_ytd

INTO analysis.period_revenue
FROM get_delta_revenue r
LEFT JOIN
    get_percentage_change p
    ON
        r.customer_level_1 = p.customer_level_1
        AND r.customer_level_2 = p.customer_level_2
        AND r.customer_level_3 = p.customer_level_3
        AND r.customer_level_4 = p.customer_level_4
        AND r.customer_level_5 = p.customer_level_5
        AND r.customer_level_6 = p.customer_level_6
        AND r.customer_level_7 = p.customer_level_7
        AND r.customer_level_8 = p.customer_level_8
        AND r.customer_level_9 = p.customer_level_9
        AND r.product_level_1 = p.product_level_1
        AND r.product_level_2 = p.product_level_2
        AND r.month_roll = p.month_roll
        AND r.revenue_type = p.revenue_type
END;