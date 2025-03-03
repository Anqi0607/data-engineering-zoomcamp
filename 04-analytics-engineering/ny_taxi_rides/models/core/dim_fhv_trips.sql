with tripdata as (
    select *
    from {{ ref('stg_fhv_trips') }}
),
dim_zones as (
    select *
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    t.*,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,
    EXTRACT(YEAR from pickup_datetime) as year,
    EXTRACT(MONTH from pickup_datetime) as month 
from tripdata t
inner join dim_zones pz
    on t.pickup_locationid = pz.locationid
inner join dim_zones dz
    on t.dropoff_locationid = dz.locationid
