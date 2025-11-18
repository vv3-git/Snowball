{{ 
    config(
        tags=['reporting']
        ) 
}}

/* This stored procedure combines all types of period revenues into one, enabling slicing and dicing between the period types in BI. */

WITH arr_join AS (

    SELECT

        a.*
        , m.arr
        , m.volume
        , p.arr_lm
        , p.arr_l3m
        , p.arr_ltm
        , p.arr_ytd

    FROM {{ ref('delta_revenue') }} a

    INNER JOIN {{ ref('monthly_revenue') }} m
        ON a.delta_revenue_key = m.monthly_revenue_key
        AND a.month_roll = m.month_roll
    INNER JOIN {{ ref('period_revenue') }} p
        ON a.delta_revenue_key = p.period_revenue_key
        AND a.month_roll = p.month_roll

)

, lm_prep AS (

    SELECT

        delta_revenue_key                       AS snowball_key
        , customer_key
        , product_key
        , other_key
        , month_roll
        , 'lm'                                   AS period_type
        , arr_lm                                 AS bop_arr
        , lm_delta_customer_churn                AS customer_churn
        , lm_delta_downgrade                     AS product_churn
        , lm_delta_downsell                      AS downsell

        -- uncomment below fields to find the price volume downsell
        -- , lm_delta_price_downsell             AS downsell_price
        -- , lm_delta_volume_downsell            AS downsell_volume

        , lm_delta_upsell                        AS upsell

        -- uncomment below fields to find the price volume upsell
        -- , lm_delta_price_upsell               AS upsell_price
        -- , lm_delta_volume_upsell              AS upsell_volume

        , lm_delta_cross_sell                    AS cross_sell
        , lm_delta_customer_new                  AS new_customer
        , arr                                    AS eop_arr
        , volume                                 AS volume
    
    FROM 
        arr_join

)

, l3m_prep AS (

    SELECT

        delta_revenue_key                      AS snowball_key
        , customer_key
        , product_key
        , other_key
        , month_roll
        , 'l3m'                                  AS period_type
        , arr_l3m                                AS bop_arr
        , l3m_delta_customer_churn               AS customer_churn
        , l3m_delta_downgrade                    AS product_churn
        , l3m_delta_downsell                     AS downsell

        -- , l3m_delta_price_downsell            AS downsell_price
        -- , l3m_delta_volume_downsell           AS downsell_volume

        , l3m_delta_upsell                       AS upsell

        -- , l3m_delta_price_upsell              AS upsell_price
        -- , l3m_delta_volume_upsell             AS upsell_volume

        , l3m_delta_cross_sell                   AS cross_sell
        , l3m_delta_customer_new                 AS new_customer
        , arr                                    AS eop_arr
        , volume                                 AS volume
    
    FROM 
        arr_join

)

-- CTE for ltm_prep
, ltm_prep AS (

    SELECT
    
        delta_revenue_key                        AS snowball_ke
        , customer_key
        , product_key
        , other_key
        , month_roll
        , 'ltm'                                  AS period_type
        , arr_ltm                                AS bop_arr
        , ltm_delta_customer_churn               AS customer_churn
        , ltm_delta_downgrade                    AS product_churn
        , ltm_delta_downsell                     AS downsell

        -- , ltm_delta_price_downsell            AS downsell_price
        -- , ltm_delta_volume_downsell           AS downsell_volume

        , ltm_delta_upsell                       AS upsell

        -- , ltm_delta_price_upsell              AS upsell_price
        -- , ltm_delta_volume_upsell             AS upsell_volume

        , ltm_delta_cross_sell                   AS cross_sell
        , ltm_delta_customer_new                 AS new_customer
        , arr                                    AS eop_arr
        , volume                                 AS volume
    
    FROM 
        arr_join

)

, ytd_prep AS (

    SELECT

        delta_revenue_key                        AS snowball_key
        , customer_key
        , product_key
        , other_key
        , month_roll
        , 'ytd'                                  AS period_type
        , arr_ytd                                AS bop_arr
        , ytd_delta_customer_churn               AS customer_churn
        , ytd_delta_downgrade                    AS product_churn
        , ytd_delta_downsell                     AS downsell

        -- , ytd_delta_price_downsell            AS downsell_price
        -- , ytd_delta_volume_downsell           AS downsell_volume

        , ytd_delta_upsell                       AS upsell

        -- , ytd_delta_price_upsell              AS upsell_price
        -- , ytd_delta_volume_upsell             AS upsell_volume

        , ytd_delta_cross_sell                   AS cross_sell
        , ytd_delta_customer_new                 AS new_customer
        , arr                                    AS eop_arr
        , volume                                 AS volume
    
    FROM arr_join

)

, combined_period_type AS (

    SELECT * FROM lm_prep

    UNION ALL

    SELECT * FROM l3m_prep

    UNION ALL

    SELECT * FROM ltm_prep

    UNION ALL

    SELECT * FROM ytd_prep
    
)

SELECT

    snowball_key
    , customer_key
    , product_key
    , other_key
    , month_roll
    , period_type
    , bop_arr
    , customer_churn
    , product_churn
    , downsell
    , bop_arr + customer_churn + product_churn + downsell                                            AS grr
    , upsell
    , cross_sell
    , bop_arr + customer_churn + product_churn + downsell + upsell + cross_sell                      AS nrr
    , new_customer
    , eop_arr
    , volume

FROM combined_period_type