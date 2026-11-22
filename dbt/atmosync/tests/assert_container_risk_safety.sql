select kafka_source_key
from {{ ref('mart_container_risk') }}
where
    (
        data_quality_status <> 'COMPLETE'
        and (
            risk_score is not null
            or risk_classification is distinct from 'NOT_ASSESSABLE'
        )
    )
    or (
        data_quality_status = 'COMPLETE'
        and (
            risk_score is null
            or risk_score < 0
            or risk_score > 100
            or risk_score is distinct from (
                temperature_risk_points + humidity_risk_points + vibration_risk_points
            )
            or risk_classification is distinct from (
                case
                    when risk_score >= {{ var('risk_high_min_score') }} then 'HIGH'
                    when risk_score >= {{ var('risk_medium_min_score') }} then 'MEDIUM'
                    else 'LOW'
                end
            )
        )
    )
    or temperature_risk_points < 0
    or humidity_risk_points < 0
    or vibration_risk_points < 0
