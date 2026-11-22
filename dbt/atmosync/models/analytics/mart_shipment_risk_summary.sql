{{ config(alias='MART_SHIPMENT_RISK_SUMMARY') }}

with event_details as (
    select
        risk.shipment_id,
        risk.container_id,
        risk.sensor_id,
        risk.kafka_source_key,
        health.temperature_status,
        health.humidity_status,
        health.vibration_status,
        health.data_quality_status,
        risk.risk_classification,
        pricing.commodity_price_matched
    from {{ ref('mart_container_risk') }} as risk
    inner join {{ ref('mart_sensor_health') }} as health
        on health.kafka_source_key = risk.kafka_source_key
    inner join {{ ref('mart_temperature_fluctuations') }} as temperature
        on temperature.kafka_source_key = risk.kafka_source_key
    inner join {{ ref('mart_route_risk') }} as route
        on route.kafka_source_key = risk.kafka_source_key
    inner join {{ ref('mart_commodity_price_impact') }} as pricing
        on pricing.kafka_source_key = risk.kafka_source_key
),

shipment_container_metrics as (
    select
        shipment_id,
        container_id,
        count(*) as total_events,
        count(distinct shipment_id) as distinct_shipments,
        count(distinct container_id) as distinct_containers,
        count(distinct sensor_id) as distinct_sensors,
        count_if(temperature_status = 'ABNORMAL') as abnormal_temperature_events,
        count_if(humidity_status = 'ABNORMAL') as abnormal_humidity_events,
        count_if(vibration_status = 'ABNORMAL') as abnormal_vibration_events,
        count_if(risk_classification = 'HIGH') as high_risk_events,
        count_if(risk_classification = 'MEDIUM') as medium_risk_events,
        count_if(risk_classification = 'LOW') as low_risk_events,
        count_if(data_quality_status <> 'COMPLETE') as data_quality_issue_events,
        count_if(commodity_price_matched) as commodity_price_matched_events,
        count_if(not commodity_price_matched) as commodity_price_unmatched_events
    from event_details
    group by shipment_id, container_id
)

select * from shipment_container_metrics
