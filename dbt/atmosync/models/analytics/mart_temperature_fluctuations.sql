{{ config(alias='MART_TEMPERATURE_FLUCTUATIONS') }}

with sensor_events as (
    select *
    from {{ ref('stg_sensor_events') }}
),

observed_temperatures as (
    select
        shipment_id,
        container_id,
        sensor_id,
        kafka_source_key,
        lag(temperature_c) over (
            partition by shipment_id, container_id, sensor_id
            order by event_timestamp, kafka_topic, kafka_partition, kafka_offset
        ) as previous_temperature_c
    from sensor_events
    where temperature_c is not null
),

with_previous as (
    select
        sensor_events.*,
        observed_temperatures.previous_temperature_c,
        sensor_events.temperature_c - observed_temperatures.previous_temperature_c
            as temperature_change_c
    from sensor_events
    left join observed_temperatures
        on observed_temperatures.kafka_source_key = sensor_events.kafka_source_key
)

select
    event_id,
    shipment_id,
    container_id,
    sensor_id,
    event_timestamp,
    temperature_c,
    previous_temperature_c,
    temperature_change_c,
    case
        when temperature_c is null then 'NO_VALID_TEMPERATURE'
        when previous_temperature_c is null then 'FIRST_OBSERVATION'
        when abs(temperature_change_c) > {{ var('demo_temperature_change_threshold_c') }}
            then 'LARGE_CHANGE'
        else 'WITHIN_THRESHOLD'
    end as temperature_change_status,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_source_key
from with_previous
