{{ config(materialized='view') }}

with raw_records as (
    select
        container_id,
        temperature,
        humidity,
        vibration,
        event_timestamp,
        ingested_at
    from {{ source('raw_iot', 'IOT_TELEMETRY') }}
)

select
    container_id,
    round(temperature, 2) as temperature_celsius,
    round((temperature * 9/5) + 32, 2) as temperature_fahrenheit,
    round(humidity, 2) as humidity_pct,
    round(vibration, 3) as vibration_g,
    event_timestamp,
    ingested_at
from raw_records