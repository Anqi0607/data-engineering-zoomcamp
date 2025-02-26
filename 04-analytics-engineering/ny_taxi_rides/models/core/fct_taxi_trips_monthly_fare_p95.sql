with
    valid_trips as (
        select *
        from {{ ref("fact_trips") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and payment_type_description in ('Cash', 'Credit card')

    ),
quantiles as (
select
    service_type,
    year,
    month,
    approx_quantiles(fare_amount, 100)[offset(97)] as percentile_97,
    approx_quantiles(fare_amount, 100)[offset(95)] as percentile_95,
    approx_quantiles(fare_amount, 100)[offset(90)] as percentile_90
from valid_trips
group by 1,2,3
)
select * from quantiles
where year = 2020 and month = 4
