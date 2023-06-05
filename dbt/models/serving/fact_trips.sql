{{ config(
    materialized="table",
    partition_by={
        "field": "pickup_datetime",
        "data_type": "timestamp",
        "granularity": "month"
    }
) 
}}

select *
from {{ ref("stg_green_tripdata") }}

UNION ALL

select *
from {{ ref("stg_yellow_tripdata") }}
