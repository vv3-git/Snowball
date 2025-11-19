CREATE OR ALTER PROCEDURE core.sp_revenue
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN
        DROP TABLE IF EXISTS core.revenue; 
    END;

-- depends_on: "arr_sandbox"."dbo"."column_mapping"

/* This stored procedure consolidates and organizes revenue data by customer, product, and region, focusing on tracking recurring revenue. It generates a unique revenue key by combining various dimension fields to ensure accurate and structured data analysis. */
/* USERS SHOULD UPDATE SNOWBALL INPUT DIMENSIONS IN THIS SP ONLY */

/* UPDATE ALL REQUIRED INPUT FIELDS IN THE CTE BELOW */
/* Contact SQL snowball owner if you want to add dimensions that do not exist below or need to adjust logic */

WITH revenue_columns AS (

    SELECT

        customer_name          AS customer_level_1
        , customer_sector      AS customer_level_2
        , customer_size        AS customer_level_3
        , customer_type        AS customer_level_4
        , is_organic           AS customer_level_5
        , anon_customer_size   AS customer_level_6
        , anon_sic_section     AS customer_level_7
        , client_business_type AS customer_level_8
        , company_size         AS customer_level_9
        , customer_name
        , region               AS customer_region
        , is_recurring         AS revenue_type
        , service_line         AS product_level_1
        , subservice           AS product_level_2
        , month
        , revenue
        , entity_id            AS other_dim_1
        , entity               AS other_dim_2

    FROM "arr_sandbox"."dbo".sample_arr_dataset

)

, revenue_input AS (

    SELECT
        -- Customer dimensions       
        -- REQUIRED FIELDS

        customer_level_1
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

        -- Product levels        
        -- REQUIRED FIELDS
        , product_level_1  -- Options for this field are 'Recurring' 'Re-occurring' and 'Non-recurring' OR 1/0
        , product_level_2
        , other_dim_1
        , other_dim_2
        , revenue

        -- Month spine
        , CAST(CONVERT(DATE, month, 105) AS DATE) AS month 
        -- Note each subscription / contract / invoice should have one row per month of billing. Adjustment might be need pre this script to transform data into right format
        , 1                                       AS volume -- Required for price / volume snowball

        -- Revenue
        -- REQUIRED FIELD
        , CAST(revenue_type AS VARCHAR)           AS revenue_type
        
        , CASE
            WHEN
                CAST(revenue_type AS VARCHAR) = '1'
                OR CAST(revenue_type AS VARCHAR) = 'Recurring'
                THEN revenue
            ELSE 0
        END                                       AS mrr -- Use calculation or MRR directly if available   

    -- Update your source here
    FROM revenue_columns

)

-- Group by all dimensions and filter out 0 / NULL value rows
, revenue_prep AS (

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
        , customer_name
        , customer_region
        , revenue_type
        , product_level_1
        , product_level_2
        , month
        , other_dim_1
        , other_dim_2
        , SUM(revenue) AS revenue
        , SUM(mrr)     AS mrr
        , SUM(volume)  AS volume

    FROM revenue_input
    WHERE
        revenue IS NOT NULL
        AND revenue <> 0.00
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
        , customer_name
        , customer_region
        , revenue_type
        , product_level_1
        , product_level_2
        , month
        , other_dim_1
        , other_dim_2

)

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
    , customer_name
    , customer_region
    , revenue_type
    , product_level_1
    , product_level_2
    , month
    , revenue
    , other_dim_1
    , other_dim_2
    , mrr
    , volume
    , LOWER(CONVERT(VARCHAR(32), HASHBYTES(
        'MD5'
        , CONCAT(
            COALESCE(CAST(customer_level_1 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_2 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_3 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_4 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_5 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_6 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_7 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_8 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_9 AS VARCHAR), '')
            , COALESCE(CAST(customer_name AS VARCHAR), '')
            , COALESCE(CAST(customer_region AS VARCHAR), '')
            , COALESCE(CAST(revenue_type AS VARCHAR), '')
            , COALESCE(CAST(product_level_1 AS VARCHAR), '')
            , COALESCE(CAST(product_level_2 AS VARCHAR), '')
            , COALESCE(CAST(other_dim_1 AS VARCHAR), '')
            , COALESCE(CAST(other_dim_2 AS VARCHAR), '')
        )
    ), 2)) AS revenue_key

    , LOWER(CONVERT(VARCHAR(32), HASHBYTES(
        'MD5'
        , CONCAT(
            COALESCE(CAST(customer_level_1 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_2 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_3 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_4 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_5 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_6 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_7 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_8 AS VARCHAR), '')
            , COALESCE(CAST(customer_level_9 AS VARCHAR), '')
            , COALESCE(CAST(customer_name AS VARCHAR), '')
            , COALESCE(CAST(customer_region AS VARCHAR), '')
        )
    ), 2)) AS customer_key

    , LOWER(CONVERT(VARCHAR(32), HASHBYTES(
        'MD5'
        , CONCAT(
            COALESCE(CAST(product_level_1 AS VARCHAR), '')
            , COALESCE(CAST(product_level_2 AS VARCHAR), '')
        )
    ), 2)) AS product_key
    , LOWER(CONVERT(VARCHAR(32), HASHBYTES(
        'MD5'
        , CONCAT(
            COALESCE(CAST(other_dim_1 AS VARCHAR), '')
            , COALESCE(CAST(other_dim_2 AS VARCHAR), '')
        )
    ), 2)) AS other_key

INTO core.revenue
FROM revenue_prep
END;