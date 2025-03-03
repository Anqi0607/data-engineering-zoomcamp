with tripdata as (
    select 
        *,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
),
percentile as (
select
    *,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) AS percentile_90
from tripdata
),
ranked as (
select 
    year,
    month,
    pickup_zone,
    dropoff_zone,
    percentile_90,
    dense_rank()over(partition by year, month, pickup_locationid order by percentile_90 desc) as rn
from percentile
where year = 2019
    and month = 11
    and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
)
select 
    year,
    month,
    pickup_zone,
    dropoff_zone,
    percentile_90,
    rn
from ranked
where rn = 2
