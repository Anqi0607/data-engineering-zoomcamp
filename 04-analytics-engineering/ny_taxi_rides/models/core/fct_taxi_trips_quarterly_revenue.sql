-- Compute the Quarterly Revenues for each year for based on total_amount
with
    quarterly_revenue as (
        select
            year,
            quarter,
            service_type,
            sum(total_amount) as total_revenue,
            row_number() over (partition by service_type, quarter order by year) as rn,
            lag(sum(total_amount)) over (
                partition by service_type, quarter order by year
            ) as last_year_revenue
        from {{ ref("fact_trips") }}
        where year between 2019 and 2020
        group by 1, 2, 3
        order by 1, 2, 3
    )
select 
    *, 
    100 * (total_revenue - last_year_revenue) / last_year_revenue as growth
from 
    quarterly_revenue
order by 
    service_type, 
    growth
