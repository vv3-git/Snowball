{{ 
    config(
        tags=['core']
        ) 
}}

-- depends_on: {{ ref('column_mapping') }}

/* This stored procedure consolidates and organizes revenue data by customer, product, and region, focusing on tracking recurring revenue. It generates a unique revenue key by combining various dimension fields to ensure accurate and structured data analysis. */
/* USERS SHOULD UPDATE SNOWBALL INPUT DIMENSIONS IN THIS SP ONLY */

/* UPDATE ALL REQUIRED INPUT FIELDS IN THE CTE BELOW */
/* Contact SQL snowball owner if you want to add dimensions that do not exist below or need to adjust logic */

WITH revenue_columns AS (

    {{ select_snowball_revenue_temp_table() }}

)

, revenue_input AS (

    SELECT
        -- Customer dimensions       
        -- REQUIRED FIELDS
        {{ get_dimension('customer',1) }} 
        
        -- Product levels        
        -- REQUIRED FIELDS
        , {{ cast_revenue_type('revenue_type') }}           AS revenue_type  -- Options for this field are 'Recurring' 'Re-occurring' and 'Non-recurring' OR 1/0
        , {{ get_dimension('product',1) }}
        , {{ get_dimension('other', 1)}}

        -- Month spine
        -- REQUIRED FIELDS
        , {{ get_month('month') }}                          AS month           -- Note each subscription / contract / invoice should have one row per month of billing. Adjustment might be need pre this script to transform data into right format
        
        -- Revenue
        -- REQUIRED FIELD
        , revenue
        , CASE 
            WHEN  {{ cast_revenue_type('revenue_type') }}  = '1' 
            OR  {{ cast_revenue_type('revenue_type') }} = 'Recurring' 
            THEN revenue 
            ELSE 0 
        END                                                 AS mrr           -- Use calculation or MRR directly if available
        
        -- OPTIONAL FIELDS
        , 1                                                 AS volume         -- Required for price / volume snowball

    -- Update your source here
    FROM revenue_columns

)

-- Group by all dimensions and filter out 0 / NULL value rows
, revenue_prep AS (

    SELECT
        
        {{ get_dimension(index = 1, exclude_list = ['revenue', 'volume']) }}
        , SUM(revenue)                                      AS revenue
        , SUM(mrr)                                          AS mrr
        , SUM(volume)                                       AS volume
    
    FROM revenue_input
    WHERE revenue IS NOT NULL 
        AND revenue <> 0.00
    GROUP BY
        {{ get_dimension(index = 1, exclude_list = ['revenue' , 'volume']) }}

)

SELECT 

    {{ generate_hash_key(index = 1, exclude_list = ['revenue', 'month']) }} AS revenue_key
    , {{ generate_hash_key('customer',1) }}                                 AS customer_key
    , {{ generate_hash_key('product',1) }}                                  AS product_key
    , {{generate_hash_key('other', 1)}}                                     AS other_key
    , {{ get_dimension(index = 1, exclude_list = ['volume']) }}
    , mrr
    , volume

FROM revenue_prep