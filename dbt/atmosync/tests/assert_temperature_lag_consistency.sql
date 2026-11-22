with observed_temperatures as (
    select
        kafka_source_key,
        shipment_id,
        container_id,
        sensor_id,
        temperature_c,
        lag(temperature_c) over (
            partition by shipment_id, container_id, sensor_id
            order by event_timestamp, kafka_topic, kafka_partition, kafka_offset
        ) as expected_previous_temperature_c
    from {{ ref('stg_sensor_events') }}
    where temperature_c is not null
),

expected as (
    select
        kafka_source_key,
        expected_previous_temperature_c,
        temperature_c - expected_previous_temperature_c as expected_change_c
    from observed_temperatures
)

select actual.kafka_source_key
from {{ ref('mart_temperature_fluctuations') }} as actual
left join expected
    on expected.kafka_source_key = actual.kafka_source_key
where
    (
        actual.temperature_c is not null
        and expected.kafka_source_key is null
    )
    or actual.previous_temperature_c is distinct from expected.expected_previous_temperature_c
    or actual.temperature_change_c is distinct from expected.expected_change_c
    or actual.temperature_change_status is distinct from (
        case
            when actual.temperature_c is null then 'NO_VALID_TEMPERATURE'
            when expected.expected_previous_temperature_c is null then 'FIRST_OBSERVATION'
            when abs(expected.expected_change_c) > {{ var('demo_temperature_change_threshold_c') }}
                then 'LARGE_CHANGE'
            else 'WITHIN_THRESHOLD'
        end
    )
    or (
        actual.temperature_c is not null
        and
        expected.expected_previous_temperature_c is null
        and actual.temperature_change_status <> 'FIRST_OBSERVATION'
    )
