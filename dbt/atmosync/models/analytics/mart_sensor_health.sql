{{ config(alias='MART_SENSOR_HEALTH') }}

with staged as (
    select *
    from {{ ref('stg_sensor_events') }}
),

raw_presence as (
    select
        kafka_topic,
        kafka_partition,
        kafka_offset,
        payload:"readings":"temperature_c" is not null as temperature_present,
        payload:"readings":"humidity_pct" is not null as humidity_present,
        payload:"readings":"vibration_g" is not null as vibration_present
    from {{ source('atmosync_raw', 'sensor_events') }}
),

readings as (
    select
        staged.*,
        raw_presence.temperature_present,
        raw_presence.humidity_present,
        raw_presence.vibration_present
    from staged
    inner join raw_presence
        on raw_presence.kafka_topic = staged.kafka_topic
        and raw_presence.kafka_partition = staged.kafka_partition
        and raw_presence.kafka_offset = staged.kafka_offset
),

classified as (
    select
        *,
        case
            when not temperature_present then 'MISSING'
            when temperature_c is null then 'INVALID_NUMERIC'
            when temperature_c < {{ var('demo_temperature_min_c') }}
              or temperature_c > {{ var('demo_temperature_max_c') }}
                then 'ABNORMAL'
            else 'NORMAL'
        end as temperature_status,
        case
            when not humidity_present then 'MISSING'
            when humidity_pct is null or humidity_pct < 0 or humidity_pct > 100
                then 'INVALID'
            when humidity_pct < {{ var('demo_humidity_min_pct') }}
              or humidity_pct > {{ var('demo_humidity_max_pct') }}
                then 'ABNORMAL'
            else 'NORMAL'
        end as humidity_status,
        case
            when not vibration_present then 'MISSING'
            when vibration_g is null or vibration_g < 0 then 'INVALID'
            when vibration_g > {{ var('demo_vibration_max_g') }}
                then 'ABNORMAL'
            else 'NORMAL'
        end as vibration_status
    from readings
),

scored as (
    select
        *,
        (
            iff(temperature_status in ('NORMAL', 'ABNORMAL'), 1, 0)
            + iff(humidity_status in ('NORMAL', 'ABNORMAL'), 1, 0)
            + iff(vibration_status in ('NORMAL', 'ABNORMAL'), 1, 0)
        ) * 100.0 / 3 as sensor_health_score,
        case
            when temperature_status = 'MISSING'
              or humidity_status = 'MISSING'
              or vibration_status = 'MISSING'
                then 'MISSING_REQUIRED_READING'
            when temperature_status = 'INVALID_NUMERIC'
              or humidity_status = 'INVALID'
              or vibration_status = 'INVALID'
                then 'INVALID_READING'
            else 'COMPLETE'
        end as data_quality_status
    from classified
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
    latitude,
    longitude,
    temperature_status,
    humidity_status,
    vibration_status,
    data_quality_status,
    sensor_health_score,
    case
        when data_quality_status <> 'COMPLETE' then 'DATA_QUALITY_ISSUE'
        when temperature_status = 'ABNORMAL'
          or humidity_status = 'ABNORMAL'
          or vibration_status = 'ABNORMAL' then 'ABNORMAL_READING'
        else 'OBSERVATION_OK'
    end as sensor_health_status,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_source_key
from scored
