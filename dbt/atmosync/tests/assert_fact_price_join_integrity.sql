select kafka_source_key
from {{ ref('fact_sensor_events') }}
where
    (commodity_price_matched and
        (commodity_key is null or price_usd_per_unit is null or currency is null))
    or (not commodity_price_matched and
        (price_usd_per_unit is not null or currency is not null))
