{{ config(alias='STG_SENSOR_EVENTS') }}

with source as (
    select *
    from {{ source('atmosync_raw', 'sensor_events') }}
),

cleaned as (
    select
        event_id::varchar(36) as event_id,
        shipment_id::varchar(256) as shipment_id,
        container_id::varchar(256) as container_id,
        sensor_id::varchar(256) as sensor_id,
        payload:"schema_version"::varchar as schema_version,
        event_timestamp::timestamp_tz as event_timestamp,
        payload:"commodity"::varchar as commodity,
        lower(trim(payload:"commodity"::varchar)) as commodity_key,
        try_to_double(payload:"location":"latitude"::varchar) as latitude,
        try_to_double(payload:"location":"longitude"::varchar) as longitude,
        try_to_double(payload:"readings":"temperature_c"::varchar) as temperature_c,
        try_to_double(payload:"readings":"humidity_pct"::varchar) as humidity_pct,
        try_to_double(payload:"readings":"vibration_g"::varchar) as vibration_g,
        kafka_topic::varchar(256) as kafka_topic,
        kafka_partition::number(10, 0) as kafka_partition,
        kafka_offset::number(38, 0) as kafka_offset,
        ingested_at::timestamp_tz as ingested_at,
        payload_sha256::varchar(64) as payload_sha256,
        kafka_topic || ':' || kafka_partition::varchar || ':' || kafka_offset::varchar
            as kafka_source_key
    from source
)

select * from cleaned
