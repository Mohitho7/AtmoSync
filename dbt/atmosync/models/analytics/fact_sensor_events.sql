{{ config(alias='FACT_SENSOR_EVENTS') }}

with sensor_events as (
    select *
    from {{ ref('stg_sensor_events') }}
),

commodity_prices as (
    select
        lower(trim(commodity)) as commodity_key,
        price_usd_per_unit,
        currency,
        effective_date
    from {{ ref('commodity_prices') }}
),

joined as (
    select
        sensor_events.*,
        commodity_prices.price_usd_per_unit,
        commodity_prices.currency,
        commodity_prices.effective_date,
        commodity_prices.commodity_key is not null as commodity_price_matched
    from sensor_events
    left join commodity_prices
        on commodity_prices.commodity_key = sensor_events.commodity_key
        and sensor_events.commodity_key is not null
)

select * from joined
