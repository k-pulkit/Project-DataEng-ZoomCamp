{{ config(
    materialized="table",
    partition_by={
        "field": "pickup_datetime",
        "data_type": "timestamp",
        "granularity": "month"
    }
) 
}}

WITH trips_unioned AS (
    select *
    from {{ ref("stg_green_tripdata") }}

    UNION ALL

    select *
    from {{ ref("stg_yellow_tripdata") }}
),
pickup_zone AS (
    select *
    from {{ ref("dim_zones") }}
    where borough != "Unknown"
)
Select 
    trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.taxitype as service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    -- trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_desc, 
    trips_unioned.congestion_surcharge
from trips_unioned
INNER JOIN pickup_zone as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
INNER JOIN pickup_zone as dropoff_zone
on trips_unioned.pickup_locationid = dropoff_zone.locationid
