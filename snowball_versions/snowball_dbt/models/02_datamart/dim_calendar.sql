{{ 
    config(
        tags=['datamart']
        ) 
}}

SELECT

    *,
    YEAR(month_roll)                            AS year,
    MONTH(month_roll)                           AS month_no,
    {{ format_date('month_roll', 'MON') }}      AS month_name,
    {{ get_quarter_string('month_roll') }}      AS quarter, 
    {{ format_mmm_yy('month_roll') }}           AS mmm_yy

FROM {{ ref('calendar') }}
