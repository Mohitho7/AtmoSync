select kafka_source_key
from {{ ref('stg_sensor_events') }}
where
    (latitude is not null and (latitude < -90 or latitude > 90))
    or (longitude is not null and (longitude < -180 or longitude > 180))
    or (humidity_pct is not null and (humidity_pct < 0 or humidity_pct > 100))
    or (vibration_g is not null and vibration_g < 0)
