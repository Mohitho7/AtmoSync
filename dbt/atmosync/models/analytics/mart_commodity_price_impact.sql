{{ config(alias='MART_COMMODITY_PRICE_IMPACT') }}

select
    event_id,
    shipment_id,
    container_id,
    sensor_id,
    event_timestamp,
    commodity,
    commodity_key,
    commodity_price_matched,
    price_usd_per_unit,
    currency,
    effective_date as price_effective_date,
    cast(null as varchar) as price_unit,
    cast(null as number(38, 6)) as price_exposure_amount,
    'SYNTHETIC_MOCK' as pricing_data_origin,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_source_key
from {{ ref('fact_sensor_events') }}
