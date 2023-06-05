{{ config(materialized="table") }}

select 
    pickup_zone as revenue_zone,
    date_trunc(pickup_datetime, month) as revenue_month,
    service_type,
    -- Revenue calculation
    {{ sum_of_fields(["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge"], "monthly_revenue") }},
    -- Other calc
    count(tripid) as total_monthly_trips,
    avg(passenger_count) as avg_passenger_count,
    avg(trip_distance) as avg_trip_distance

from {{ ref("fact_trips") }}
group by 1,2,3