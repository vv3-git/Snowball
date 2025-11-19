/* Test checks if 'monthly_revenue' has complete monthly coverage 
   for each customer-product pair based on revenue duration.
*/

-- Calculate the expected number of revenue months
with revenue_month as (
    select 
        customer_key,
        product_key,
        min(month) as min_month,
        max(month) as max_month,
        datediff(month, min_month, dateadd(month, 12, max_month)) + 1 as expected_months
    from 
        {{ref('revenue')}}
      where revenue <> 0 and revenue_type = 1
    group by 
        customer_key,
        product_key
  
),
--Count how many distinct months are actually present in monthly_revenue
month_counts as (
    select 
        customer_key,
        product_key,

        count(distinct month_roll) as actual_month
    from 
        {{ ref('monthly_revenue')}}
    Group by 
        customer_key, 
        product_key
)
select 
    r.customer_key,
    r.product_key,
    r.min_month,
    r.max_month,
    r.expected_months,
    m.actual_month
from revenue_month r 
left join month_counts m 
    on r.customer_key = m.customer_key
    and r.product_key = m.product_key
where 
    m.actual_month != r.expected_months

