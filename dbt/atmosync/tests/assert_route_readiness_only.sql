select kafka_source_key
from {{ ref('mart_route_risk') }}
where
    route_id is not null
    or route_risk_score is not null
    or (
        route_data_status = 'POINT_LOCATION_ONLY'
        and (
            latitude is null
            or longitude is null
            or latitude < -90
            or latitude > 90
            or longitude < -180
            or longitude > 180
        )
    )
    or (
        route_data_status = 'LOCATION_UNAVAILABLE'
        and latitude is not null
        and longitude is not null
    )
    or (
        route_data_status = 'INVALID_COORDINATES'
        and (
            latitude is null
            or longitude is null
            or (latitude between -90 and 90 and longitude between -180 and 180)
        )
    )
