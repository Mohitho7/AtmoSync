{{ config(alias='MART_CONTAINER_RISK') }}

with sensor_health as (
    select *
    from {{ ref('mart_sensor_health') }}
),

contributions as (
    select
        *,
        iff(temperature_status = 'ABNORMAL', {{ var('risk_temperature_points') }}, 0)
            as temperature_risk_points,
        iff(humidity_status = 'ABNORMAL', {{ var('risk_humidity_points') }}, 0)
            as humidity_risk_points,
        iff(vibration_status = 'ABNORMAL', {{ var('risk_vibration_points') }}, 0)
            as vibration_risk_points
    from sensor_health
),

scored as (
    select
        *,
        case
            when data_quality_status = 'COMPLETE'
                then temperature_risk_points + humidity_risk_points + vibration_risk_points
            else null
        end as risk_score
    from contributions
)

select
    event_id,
    shipment_id,
    container_id,
    sensor_id,
    event_timestamp,
    temperature_c,
    humidity_pct,
    vibration_g,
    temperature_status,
    humidity_status,
    vibration_status,
    data_quality_status,
    temperature_risk_points,
    humidity_risk_points,
    vibration_risk_points,
    risk_score,
    case
        when risk_score is null then 'NOT_ASSESSABLE'
        when risk_score >= {{ var('risk_high_min_score') }} then 'HIGH'
        when risk_score >= {{ var('risk_medium_min_score') }} then 'MEDIUM'
        else 'LOW'
    end as risk_classification,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_source_key
from scored
