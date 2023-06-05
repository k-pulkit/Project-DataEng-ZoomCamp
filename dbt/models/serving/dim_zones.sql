{{ config(materialized="table") }}

select locationid,
        borough,
        zone,
        replace(service_zone, "Bore", "Green") as service_zone
from {{ ref("taxi_zone_lookup") }}