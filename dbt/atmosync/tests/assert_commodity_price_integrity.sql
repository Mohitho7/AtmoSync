select kafka_source_key
from {{ ref('mart_commodity_price_impact') }}
where
    (
        commodity_price_matched
        and (
            price_usd_per_unit is null
            or currency is null
            or price_effective_date is null
        )
    )
    or (
        not commodity_price_matched
        and (
            price_usd_per_unit is not null
            or currency is not null
            or price_effective_date is not null
        )
    )
    or price_unit is not null
    or price_exposure_amount is not null
