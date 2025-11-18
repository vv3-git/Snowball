{{ 
    config(
        tags=['datamart']
        ) 
}}

SELECT

      revenue_key,
      customer_key,
      product_key,
      other_key,
      volume,
      month,
      revenue_type,
      revenue,
      mrr

FROM {{ ref('revenue') }}