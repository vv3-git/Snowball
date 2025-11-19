{{ 
    config(
        tags=['reporting']
    ) 
}}

SELECT *
FROM (
    {{ unpivot_kpis(ref('rpt_revenue_bridge'), [
        'eop_arr',
        'bop_arr',
        'customer_churn',
        'new_customer',
        'cross_sell',
        'product_churn',
        'upsell',
        'downsell',
        'grr',
        'nrr'
    ]) }}
) AS v
WHERE
    v.kpi_value <> 0
    OR v.kpi = 'eop_arr'