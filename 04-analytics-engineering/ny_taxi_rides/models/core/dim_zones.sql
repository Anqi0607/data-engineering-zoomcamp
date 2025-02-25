{{ config(materialized='table') }}

select 
    locationid, 
    borough, 
    zone, 
    -- all the Boro zones are where the green taxis go
    replace(service_zone,'Boro','Green') as service_zone 
from {{ ref('taxi_zone_lookup') }}

