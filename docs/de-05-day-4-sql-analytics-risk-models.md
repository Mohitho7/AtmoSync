# DE-05 Day 4 - SQL Analytics and Risk Models

## Architecture

```text
ATMOSYNC.RAW.SENSOR_EVENTS
    -> ATMOSYNC.STAGING.STG_SENSOR_EVENTS
    -> event-grain health, temperature, condition-risk, location,
       and commodity-pricing marts
    -> ATMOSYNC.ANALYTICS.MART_SHIPMENT_RISK_SUMMARY
    -> Apache Superset (future Day 5 integration)
```

The dbt project is `dbt/atmosync`. Analytics models are tables to provide
reusable relations for reporting and Superset. Models remain event-grain until
the shipment/container summary, which is exactly one row per
`(SHIPMENT_ID, CONTAINER_ID)`. Source Kafka coordinates are retained in every
event-grain mart as `KAFKA_TOPIC`, `KAFKA_PARTITION`, `KAFKA_OFFSET`, and the
composite `KAFKA_SOURCE_KEY`.

## Live-data limits observed during Day 4

The read-only Snowflake audit found **2 RAW events**, **2 staging rows**, and
**2 existing fact rows**. The RAW records span approximately 16 minutes and
both contain `synthetic-test-only` as their commodity. This is not sufficient
historical depth for statistical claims. The existing price seed has no
matching rows for these events. Day 4 does not add fixtures to RAW or claim a
live matched-price analysis.

The `sensor_event` contract contains point latitude/longitude values, but no
route ID, planned path, origin/destination, route segment, or travel-time
input. Other contract types describe route assessments, but no such Snowflake
source relation is present in this pipeline. Consequently, the route mart is
for location readiness/monitoring only, not route-risk scoring or deviation.

## Models, grains, and calculations

### `ATMOSYNC.STAGING.STG_SENSOR_EVENTS`

**Grain:** one row per Kafka source coordinate.

Reads `ATMOSYNC.RAW.SENSOR_EVENTS`. Extracts contract payload fields:
`schema_version`, `commodity`, `location.latitude`,
`location.longitude`, `readings.temperature_c`,
`readings.humidity_pct`, and `readings.vibration_g`. Numeric fields use
`TRY_TO_DOUBLE`. It preserves top-level identifiers, event/ingest timestamps,
payload hash, and Kafka coordinates. The health mart reads payload-key
presence directly from RAW so the existing staging relation need not be
rewritten for Day 4.

### `ATMOSYNC.ANALYTICS.MART_SENSOR_HEALTH`

**Grain:** one row per Kafka source coordinate.

Classifies each reading as missing, invalid, abnormal, or normal. An abnormal
but parseable reading is distinct from a missing/invalid value. `DATA_QUALITY_STATUS`
is `MISSING_REQUIRED_READING`, `INVALID_READING`, or `COMPLETE`.
`SENSOR_HEALTH_SCORE` is the percent of the three readings that are present,
numeric, and within the sensor contract's validity rules; abnormal numeric
readings still count as valid observations. `SENSOR_HEALTH_STATUS` is
`DATA_QUALITY_ISSUE`, `ABNORMAL_READING`, or `OBSERVATION_OK`. These describe
the observation, not confirmed device failure. There is no heartbeat or
expected-interval input, so this model does not detect offline sensors.

### `ATMOSYNC.ANALYTICS.MART_TEMPERATURE_FLUCTUATIONS`

**Grain:** one row per Kafka source coordinate.

Computes `LAG(TEMPERATURE_C)` over
`(SHIPMENT_ID, CONTAINER_ID, SENSOR_ID)`, ordered by event timestamp and
Kafka coordinates for deterministic ties. It calculates signed current minus
previous temperature only for valid observed temperatures. Missing
temperatures do not become zero. The first valid observation has no prior
value and is labeled `FIRST_OBSERVATION`; a difference greater than the
configurable absolute-change threshold is `LARGE_CHANGE`. This is a change
between observations, not proof of a cold-chain violation.

### `ATMOSYNC.ANALYTICS.MART_CONTAINER_RISK`

**Grain:** one row per Kafka source coordinate (an event-level condition
assessment, not a container-level historical aggregate).

Rule-based contributions are summed for complete valid readings:

```text
temperature contribution = 40 when outside demo range, otherwise 0
humidity contribution    = 30 when outside demo band, otherwise 0
vibration contribution   = 30 when above demo threshold, otherwise 0
risk score               = sum of contributions (range 0..100)
```

Defaults are configurable through dbt vars in `dbt_project.yml`:

| Variable | Default |
|---|---:|
| `demo_temperature_min_c` | -20 |
| `demo_temperature_max_c` | 40 |
| `demo_humidity_min_pct` | 20 |
| `demo_humidity_max_pct` | 90 |
| `demo_vibration_max_g` | 5 |
| `demo_temperature_change_threshold_c` | 5 |
| `risk_temperature_points` | 40 |
| `risk_humidity_points` | 30 |
| `risk_vibration_points` | 30 |
| `risk_medium_min_score` | 30 |
| `risk_high_min_score` | 60 |

These are provisional monitoring/demo thresholds, not cargo-specific or
certified food-safety limits. The sensor contract separately defines valid
humidity as 0-100% and vibration as non-negative; humidity/vibration values
outside those contract constraints are invalid data, not physical-risk
contributions. Complete events score `HIGH` at 60+, `MEDIUM` at 30+, otherwise
`LOW`. Incomplete or invalid events have a NULL score and
`NOT_ASSESSABLE`, never a low/safe score. This is a transparent rules engine,
not an ML prediction.

### `ATMOSYNC.ANALYTICS.MART_ROUTE_RISK`

**Grain:** one row per Kafka source coordinate.

Preserves shipment/container point coordinates and numbers observations
within a shipment/container. `ROUTE_ID` and `ROUTE_RISK_SCORE` are explicitly
NULL because route-path inputs are unavailable. `ROUTE_DATA_STATUS` indicates
`POINT_LOCATION_ONLY`, `LOCATION_UNAVAILABLE`, or `INVALID_COORDINATES`.
No route deviation, distance, duration, or risk score is fabricated.

### `ATMOSYNC.ANALYTICS.MART_COMMODITY_PRICE_IMPACT`

**Grain:** one row per Kafka source coordinate.

Uses the existing normalized exact commodity-key join in
`ATMOSYNC.ANALYTICS.FACT_SENSOR_EVENTS`. All events remain reportable;
unmatched price, currency, and date fields stay NULL. `PRICING_DATA_ORIGIN`
labels this source `SYNTHETIC_MOCK`. The seed has price-per-unit values but
does not define the unit, and sensor events contain no shipment quantity.
Therefore `PRICE_UNIT` and `PRICE_EXPOSURE_AMOUNT` are deliberately NULL.
No monetary exposure is estimated.

### `ATMOSYNC.ANALYTICS.MART_SHIPMENT_RISK_SUMMARY`

**Grain:** one row per shipment/container.

Aggregates event counts and distinct sensor counts plus abnormal measurement,
high/medium/low assessable risk, data-quality, and price-match counts. It
joins event-grain marts by the unique Kafka source key before grouping, so
different model grains cannot multiply events. `DISTINCT_SHIPMENTS` and
`DISTINCT_CONTAINERS` are included for explicit reconciliation and equal one
for each output row.

## Tests

Generic dbt tests cover not-null identity/coordinate fields, source-coordinate
uniqueness, accepted statuses, and required summary counts. Singular SQL tests
check:

- Reading-derived health score formula and 0-100 range.
- Temperature lag/delta calculation under the same shipment/container/sensor
  window partition.
- Incomplete risk assessments remain NULL/`NOT_ASSESSABLE`; complete scores
  equal their contribution sum and stay within 0-100.
- Route IDs/scores remain NULL and location readiness agrees with coordinates.
- Matched pricing fields exist; unmatched pricing and unsupported unit/
  exposure remain NULL.
- Shipment/container summary counts reconcile to event-level marts and are
  non-negative.
- Summary has exactly one row per shipment/container grain.

## Reproducibility

Run from the repository root with Python 3.11 in `.venv` and the existing
Snowflake environment variables:

```powershell
.\.venv\Scripts\dbt.exe debug --project-dir dbt\atmosync --profiles-dir dbt\atmosync
.\.venv\Scripts\dbt.exe parse --project-dir dbt\atmosync --profiles-dir dbt\atmosync
.\.venv\Scripts\dbt.exe build --project-dir dbt\atmosync --profiles-dir dbt\atmosync --select mart_sensor_health mart_temperature_fluctuations mart_container_risk mart_route_risk mart_commodity_price_impact mart_shipment_risk_summary assert_sensor_health_score assert_temperature_lag_consistency assert_container_risk_safety assert_route_readiness_only assert_commodity_price_integrity assert_shipment_summary_reconciles assert_summary_counts_nonnegative assert_shipment_summary_grain
```

The selected `build` materializes only the six Day 4 ANALYTICS marts and runs
their tests. It deliberately does not rebuild the existing Day 3 staging
table, fact table, or seed, and does not update Kafka or RAW. Do not remove the
selection when preserving existing relations/data is required. The Day 3
`commodity_prices` seed is already in Snowflake; use `dbt seed` only when
intentionally refreshing that mock seed.

## Validation and known limitations

Day 4 validation completed with dbt Core 1.12.7 and dbt-snowflake 1.12.5:

- `dbt debug`: live connection passed; credential/account details were not
  included in the report.
- `dbt parse`: passed.
- Selected `dbt build`: 6 analytics tables and 54 data tests passed (60 total
  selected nodes including the 6 models); no RAW, existing staging, fact, or
  seed relation was rebuilt.
- Existing Python suite: 128 passed, 11 skipped, 49 subtests passed.
- `pip check`, Python compile validation, and `git diff --check`: passed.
- Live relations returned 2 rows each. Their event keys remained distinct;
  per-container summary counts reconciled.
- Both events are synthetic and both commodities are unmatched. No price
  amount was assigned; price unit/exposure and unmatched price fields remain
  NULL.
- Both valid temperature observations are first observations in their
  respective shipment/container/sensor partitions, so previous temperature
  and change are correctly NULL.
- Both condition scores are 0/LOW under the stated provisional ranges;
  this does not establish safety or statistical validity.

The live dataset is very small and synthetic; passing tests prove SQL
invariants for data present, not statistical performance or cargo safety.
Missing/invalid payload behavior is covered by SQL invariants but not
production RAW fixture rows; do not insert fabricated production events to
exercise those cases.

Superset integration is deferred to Day 5. It should connect to the
`ATMOSYNC.ANALYTICS` schema and use the shipment/container summary or detailed
marts with their documented grains and limitations.
