with expected as (
    select
        risk.shipment_id,
        risk.container_id,
        count(*) as total_events,
        count(distinct risk.shipment_id) as distinct_shipments,
        count(distinct risk.container_id) as distinct_containers,
        count(distinct risk.sensor_id) as distinct_sensors,
        count_if(health.temperature_status = 'ABNORMAL') as abnormal_temperature_events,
        count_if(health.humidity_status = 'ABNORMAL') as abnormal_humidity_events,
        count_if(health.vibration_status = 'ABNORMAL') as abnormal_vibration_events,
        count_if(risk.risk_classification = 'HIGH') as high_risk_events,
        count_if(risk.risk_classification = 'MEDIUM') as medium_risk_events,
        count_if(risk.risk_classification = 'LOW') as low_risk_events,
        count_if(health.data_quality_status <> 'COMPLETE') as data_quality_issue_events,
        count_if(pricing.commodity_price_matched) as commodity_price_matched_events,
        count_if(not pricing.commodity_price_matched) as commodity_price_unmatched_events
    from {{ ref('mart_container_risk') }} as risk
    inner join {{ ref('mart_sensor_health') }} as health
        on health.kafka_source_key = risk.kafka_source_key
    inner join {{ ref('mart_commodity_price_impact') }} as pricing
        on pricing.kafka_source_key = risk.kafka_source_key
    group by risk.shipment_id, risk.container_id
),

comparisons as (
    select
        expected.*,
        actual.total_events as actual_total_events,
        actual.distinct_shipments as actual_distinct_shipments,
        actual.distinct_containers as actual_distinct_containers,
        actual.distinct_sensors as actual_distinct_sensors,
        actual.abnormal_temperature_events as actual_abnormal_temperature_events,
        actual.abnormal_humidity_events as actual_abnormal_humidity_events,
        actual.abnormal_vibration_events as actual_abnormal_vibration_events,
        actual.high_risk_events as actual_high_risk_events,
        actual.medium_risk_events as actual_medium_risk_events,
        actual.low_risk_events as actual_low_risk_events,
        actual.data_quality_issue_events as actual_data_quality_issue_events,
        actual.commodity_price_matched_events as actual_commodity_price_matched_events,
        actual.commodity_price_unmatched_events as actual_commodity_price_unmatched_events
    from expected
    full outer join {{ ref('mart_shipment_risk_summary') }} as actual
        on actual.shipment_id = expected.shipment_id
        and actual.container_id = expected.container_id
)

select *
from comparisons
where
    total_events is distinct from actual_total_events
    or distinct_shipments is distinct from actual_distinct_shipments
    or distinct_containers is distinct from actual_distinct_containers
    or distinct_sensors is distinct from actual_distinct_sensors
    or abnormal_temperature_events is distinct from actual_abnormal_temperature_events
    or abnormal_humidity_events is distinct from actual_abnormal_humidity_events
    or abnormal_vibration_events is distinct from actual_abnormal_vibration_events
    or high_risk_events is distinct from actual_high_risk_events
    or medium_risk_events is distinct from actual_medium_risk_events
    or low_risk_events is distinct from actual_low_risk_events
    or data_quality_issue_events is distinct from actual_data_quality_issue_events
    or commodity_price_matched_events is distinct from actual_commodity_price_matched_events
    or commodity_price_unmatched_events is distinct from actual_commodity_price_unmatched_events
