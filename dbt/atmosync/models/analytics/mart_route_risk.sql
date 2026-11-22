{{ config(alias='MART_ROUTE_RISK') }}

with locations as (
    select *
    from {{ ref('stg_sensor_events') }}
)

select
    event_id,
    shipment_id,
    container_id,
    sensor_id,
    event_timestamp,
    latitude,
    longitude,
    row_number() over (
        partition by shipment_id, container_id
        order by event_timestamp, kafka_topic, kafka_partition, kafka_offset
    ) as location_observation_number,
    cast(null as varchar) as route_id,
    cast(null as number(5, 2)) as route_risk_score,
    case
        when latitude is null or longitude is null then 'LOCATION_UNAVAILABLE'
        when latitude < -90 or latitude > 90 or longitude < -180 or longitude > 180
            then 'INVALID_COORDINATES'
        else 'POINT_LOCATION_ONLY'
    end as route_data_status,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_source_key
from locations
