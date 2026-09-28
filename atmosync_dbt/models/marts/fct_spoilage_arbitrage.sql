{{ config(materialized='table') }}

with latest_telemetry as (
    select
        container_id,
        avg_temp_c,
        peak_temp_c,
        avg_humidity_pct,
        peak_vibration_g,
        telemetry_hour,
        row_number() over (partition by container_id order by telemetry_hour desc) as rn
    from {{ ref('fct_hourly_metrics') }}
),

routes as (
    select * from {{ ref('commodity_routes') }}
),

calculated_metrics as (
    select
        t.container_id,
        r.commodity_type,
        r.original_destination,
        r.dist_to_orig_km,
        r.orig_market_price_per_kg,
        r.reroute_destination,
        r.dist_to_reroute_km,
        r.reroute_market_price_per_kg,
        t.avg_temp_c,
        r.max_safe_temp_c,
        r.shelf_life_hours,
        -- Spoilage acceleration: 4 hours lost per 1°C over safety threshold
        case 
            when t.avg_temp_c > r.max_safe_temp_c 
            then round(greatest(0, r.shelf_life_hours - ((t.avg_temp_c - r.max_safe_temp_c) * 4)), 1)
            else r.shelf_life_hours
        end as estimated_remaining_shelf_life_hrs,
        -- Truck travel duration at 70 km/h baseline
        round(r.dist_to_orig_km / 70.0, 1) as transit_time_orig_hrs,
        round(r.dist_to_reroute_km / 70.0, 1) as transit_time_reroute_hrs
    from latest_telemetry t
    join routes r on t.container_id = r.container_id
    where t.rn = 1
)

select
    *,
    case
        when estimated_remaining_shelf_life_hrs < transit_time_orig_hrs
             and estimated_remaining_shelf_life_hrs >= transit_time_reroute_hrs
        then 'REROUTE_RECOMMENDED'
        when estimated_remaining_shelf_life_hrs < transit_time_orig_hrs
        then 'CRITICAL_SPOILAGE_IMMINENT'
        else 'ON_SCHEDULE'
    end as arbitrage_action
from calculated_metrics