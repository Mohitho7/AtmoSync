select kafka_source_key
from {{ ref('mart_sensor_health') }}
where
    sensor_health_score < 0
    or sensor_health_score > 100
    or sensor_health_score is distinct from (
        (
            iff(temperature_status in ('NORMAL', 'ABNORMAL'), 1, 0)
            + iff(humidity_status in ('NORMAL', 'ABNORMAL'), 1, 0)
            + iff(vibration_status in ('NORMAL', 'ABNORMAL'), 1, 0)
        ) * 100.0 / 3
    )
