{{ config(materialized='table') }}

select
    container_id,
    date_trunc('hour', event_timestamp) as telemetry_hour,
    count(*) as ping_count,
    round(avg(temperature_celsius), 2) as avg_temp_c,
    round(max(temperature_celsius), 2) as peak_temp_c,
    round(min(temperature_celsius), 2) as min_temp_c,
    round(avg(humidity_pct), 2) as avg_humidity_pct,
    round(max(vibration_g), 3) as peak_vibration_g
from {{ ref('stg_telemetry') }}
group by 1, 2