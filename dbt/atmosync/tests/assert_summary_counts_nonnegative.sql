select shipment_id, container_id
from {{ ref('mart_shipment_risk_summary') }}
where
    total_events < 0
    or distinct_shipments < 0
    or distinct_containers < 0
    or distinct_sensors < 0
    or abnormal_temperature_events < 0
    or abnormal_humidity_events < 0
    or abnormal_vibration_events < 0
    or high_risk_events < 0
    or medium_risk_events < 0
    or low_risk_events < 0
    or data_quality_issue_events < 0
    or commodity_price_matched_events < 0
    or commodity_price_unmatched_events < 0
