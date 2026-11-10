# AtmoSync Data Contracts — DE-02

This document defines the v1.0 payload contracts, default monitoring settings,
and validation boundaries for AtmoSync. The accompanying Python validators use
the standard library and check payload shape and local invariants. They do not
perform persistence, deduplication, authentication, route discovery, market
selection, food-safety certification, or spoilage prediction.

All JSON examples in `data/sample_*.json` are illustrative. In particular,
market offers and predictions are synthetic examples, not live integrations
or scientifically validated outputs.

## Shared conventions

- Timestamps are ISO 8601 strings with an explicit UTC timezone (`Z` or
  `+00:00`). Naive timestamps and non-UTC offsets are rejected.
- JSON measurements and financial amounts are numbers, not numeric strings.
  Validators reject booleans, NaN, and infinity where a finite number is
  required. Producers should retain decimal precision appropriate to the
  source and unit.
- Required fields must be present. `null` is accepted only where this
  document explicitly identifies a field as nullable; a missing measurement
  must not be silently replaced with zero.
- IDs other than `sensor_event.event_id` are non-empty strings in DE-02. Their
  generation and uniqueness strategy belongs to the producing system.
- Percentages use the range 0–100 inclusive. Confidence scores use 0–1
  inclusive.
- Additional owner-specific settings must not override mandatory food-safety
  constraints.

## Contract 1 — `sensor_event`

One event contains one sensor reading set for one container at one event time.
It intentionally contains no market, prediction, recommendation, or owner
configuration fields.

| Field | Type | Constraints |
|---|---|---|
| `event_id` | string | Canonical UUID format. |
| `schema_version` | string | Non-empty contract version. |
| `event_timestamp` | UTC timestamp | Time of the reading. |
| `shipment_id` | string | Non-empty shipment identifier. |
| `container_id` | string | Non-empty container identifier. |
| `sensor_id` | string | Non-empty sensor identifier. |
| `commodity` | string | Non-empty commodity name/code. |
| `location.latitude` | number | Finite; -90 through 90 inclusive. |
| `location.longitude` | number | Finite; -180 through 180 inclusive. |
| `readings.temperature_c` | number | Finite Celsius reading. |
| `readings.humidity_pct` | number | Finite percentage from 0 through 100. |
| `readings.vibration_g` | number | Finite and non-negative. |

The DE-02 validator rejects unknown top-level, `location`, and `readings`
fields. Event-ID generation and duplicate delivery handling are not stateful
validator responsibilities: ingestion should use `event_id` as its
idempotency key and define its duplicate/retention policy at the storage or
stream-processing layer.

Example: [`data/sample_sensor_event.json`](../data/sample_sensor_event.json).

## Contract 2 — `hourly_shipment_summary`

One record summarizes sensor readings for a shipment/container time window.
`window_start` is inclusive and `window_end` is exclusive by convention.

| Field | Type | Constraints |
|---|---|---|
| `summary_id` | string | Non-empty identifier. |
| `shipment_id`, `container_id`, `commodity` | string | Non-empty. |
| `window_start`, `window_end`, `created_at` | UTC timestamp | End must be later than start. |
| `reading_count` | integer | Positive; a metric-bearing summary cannot contain zero readings. Booleans are not integers for this contract. |
| `temperature_avg_c`, `temperature_min_c`, `temperature_max_c` | number | Finite; minimum <= average <= maximum. |
| `humidity_avg_pct`, `humidity_min_pct`, `humidity_max_pct` | number | Finite, each 0–100; minimum <= average <= maximum. |
| `vibration_avg_g`, `vibration_max_g` | number | Finite and non-negative; average <= maximum. |
| `time_outside_operating_range_seconds` | number | Finite, non-negative, and no greater than the window duration. |

All summary metrics are required and must be finite. A null or absent metric is
invalid; do not coalesce missing measurements to zero. A metric-bearing
summary with `reading_count` zero is invalid. If there are no usable readings,
do not emit this summary; define a separate empty-window contract if that
behavior is needed.

For idempotent writes, the logical uniqueness key is
`(shipment_id, container_id, window_start, window_end)`. A warehouse or
summary writer should upsert/reconcile on that key (or a stable `summary_id`)
rather than create duplicate summaries. DE-02 defines the key but does not
implement storage or deduplication.

The 15-minute default retention applies only to the live raw-data buffer.
Historical hourly summaries are not subject to that live-buffer expiry and
must follow their separately governed historical retention policy.

Example: [`data/sample_hourly_shipment_summary.json`](../data/sample_hourly_shipment_summary.json).

## Contract 3 — `shipment_prediction`

One record stores a model-produced estimate. `predicted_spoilage_pct` is an
estimate, not evidence that spoilage has been physically confirmed.

| Field | Type | Constraints |
|---|---|---|
| `prediction_id`, `shipment_id`, `container_id` | string | Non-empty identifiers. |
| `prediction_timestamp`, `forecast_start`, `forecast_end` | UTC timestamp | Forecast end must be later than start. |
| `forecast_horizon_minutes` | positive integer | Must match the forecast interval. In DYNAMIC mode, the producer determines the horizon. |
| `estimated_temperature_c` | number | Finite Celsius estimate. |
| `estimated_humidity_pct` | number | Finite and 0–100. |
| `estimated_vibration_g` | number | Finite and non-negative. |
| `estimated_hours_to_spoilage` | number or null | When present, finite and non-negative; null means unavailable, not zero. |
| `predicted_spoilage_pct` | number | Finite and 0–100; model estimate only. |
| `risk_stage` | string | `EARLY_WARNING`, `CRITICAL_RISK`, or `SPOILAGE_THRESHOLD_REACHED`. |
| `confidence_score` | number | Finite and 0–1. |
| `model_version`, `configuration_version` | string | Non-empty provenance versions. |

The validator does not determine stage thresholds, calculate a forecast, or
set a low-confidence cutoff. Consumers must propagate insufficient or
unreliable data as uncertainty and trigger the applicable human-review/safety
workflow; they must not invent certainty or treat a threshold crossing as
physical confirmation. Every prediction records the configuration version
used so later changes do not rewrite its provenance.

No scientifically validated spoilage model is supplied by DE-02.

Example: [`data/sample_shipment_prediction.json`](../data/sample_shipment_prediction.json).

## Contract 4 — `market_offer`

An offer is a time-bounded price observation. Sample values are fictional.

| Field | Type | Constraints |
|---|---|---|
| `offer_id`, `market_id`, `market_name`, `commodity` | string | Non-empty. |
| `shipment_id` | string or absent/null | Optional association. |
| `quoted_price` | number | Finite and greater than zero. |
| `currency` | string | Three uppercase letters (for example `USD`). |
| `price_unit` | string | Required, non-empty explicit basis (for example `USD/kg` or `USD/container`). |
| `available_quantity` | number or absent/null | Optional; when present, finite and non-negative. |
| `market_latitude`, `market_longitude` | number | Finite; latitude -90..90 and longitude -180..180. |
| `offer_timestamp` | UTC timestamp | Observation time. |
| `offer_expiry` | UTC timestamp or absent/null | Optional; when present, later than offer time. |
| `source` | string | Non-empty provenance label. |

Before comparing offers, normalize pricing units and apply shipment quantities
consistently. Offers with incompatible commodities are ineligible. A consumer
must check expiry at recommendation time; the Python validator can do this
when called with an explicit UTC `as_of` timestamp. Offers without an expiry
still require a freshness policy at the integration layer. DE-02 does not
connect to a market API or normalize prices.

Example: [`data/sample_market_offer.json`](../data/sample_market_offer.json).

## Contract 5 — `route_risk_assessment`

One assessment connects a shipment, candidate market, route estimate, and
prediction. Coordinates are not fields in this contract; location validity is
checked in the sensor and market-offer inputs used by a future route service.

| Field | Type | Constraints |
|---|---|---|
| `assessment_id`, `shipment_id`, `market_id`, `prediction_id` | string | Non-empty. |
| `route_distance_km` | number | Finite and non-negative. |
| `estimated_travel_minutes` | number | Finite and non-negative. |
| `estimated_arrival_time`, `assessment_timestamp` | UTC timestamp | Arrival must not precede assessment time. |
| `predicted_spoilage_pct` | number | Finite and 0–100. |
| `estimated_saleable_pct` | number | Finite and 0–100; must equal 100 minus predicted spoilage. |
| `additional_transport_cost` | number | Finite and non-negative. |
| `currency` | string | Three uppercase letters. |

The validator checks payload values, not route feasibility, coordinate
provenance, travel-time estimation quality, or the correctness of the linked
prediction.

Example: [`data/sample_route_risk_assessment.json`](../data/sample_route_risk_assessment.json).

## Contract 6 — `intervention_recommendation`

One record captures a decision and its provenance. `offer_id` and
`assessment_id` identify the exact market offer and route assessment used.
`gross_expected_revenue` means expected sale revenue after applying the
spoilage adjustment and normalizing the offer price unit and shipment quantity
to compatible units. `additional_costs` includes the referenced route's
transport cost and all other applicable incremental costs. Every financial
value uses the declared `currency`.

| Field | Type | Constraints |
|---|---|---|
| `recommendation_id`, `shipment_id`, `decision_reason`, `configuration_version`, `prediction_id` | string | Non-empty. |
| `recommended_market_id` | string or null | Required field; null only for `MITIGATE` or `HUMAN_REVIEW`. |
| `offer_id`, `assessment_id` | string or null | Required fields; identify the source offer and route assessment when a market is recommended. Both are null when there is no eligible market. |
| `recommendation_timestamp` | UTC timestamp | Required. |
| `predicted_spoilage_pct`, `maximum_allowed_spoilage_pct` | number | Finite and 0–100. |
| `gross_expected_revenue`, `additional_costs`, `expected_net_revenue` | number or null | For a market recommendation, all are required, finite, and in `currency`; gross and costs are non-negative, and net equals gross minus costs. When no eligible market exists, all three are null. |
| `currency` | string or null | Three-letter uppercase currency for all financial values; required for a market recommendation and null when its financial values are null. |
| `recommendation_action` | string | `REROUTE`, `CONTINUE`, `MITIGATE`, or `HUMAN_REVIEW`. |

For a market recommendation, `recommended_market_id`, `offer_id`, and
`assessment_id` must all identify the selected market and the exact records
used. The referenced offer and assessment, recommendation, and active owner
configuration must agree on their linked shipment/market, currency,
prediction/configuration version, and spoilage threshold. `configuration_version`
and `maximum_allowed_spoilage_pct` are decision provenance, not trusted client
inputs. For `MITIGATE` or `HUMAN_REVIEW` when no eligible market exists,
`recommended_market_id`, `offer_id`, `assessment_id`, all three financial
amounts, and `currency` must be null; other required decision, prediction,
threshold, reason, and configuration-provenance fields remain populated.

Recommended market selection behavior, when implemented:

1. Discover nearby markets that accept the commodity.
2. Calculate route distance and travel time.
3. Estimate spoilage at arrival.
4. Exclude candidates above the configured maximum predicted spoilage.
   Never silently relax that limit.
5. Normalize the offer's explicit price unit and the shipment's quantity to
   compatible units before calculating revenue. Calculate gross expected
   revenue as `normalized unit price × saleable shipment quantity`, where
   saleable quantity reflects `(1 - predicted spoilage percentage / 100)`.
   Calculate expected net revenue as gross expected revenue minus all
   applicable additional costs, including route transport cost, in the same
   currency.
6. By default, select the eligible market with the highest expected net
   revenue. Other supported owner priorities are specified in the owner
   configuration contract.
7. If no candidate satisfies the constraints, choose `MITIGATE` or
   `HUMAN_REVIEW`; do not manufacture an eligible market.

Every selected market must satisfy the active owner's configured spoilage cap,
regardless of recommendation action. `MITIGATE` and `HUMAN_REVIEW` do not
bypass market eligibility when they name a market. When no market is selected,
those actions may retain predicted spoilage above the cap and must use the
documented null values for market/source/financial fields.

The DE-02 validator checks financial arithmetic, source-record identity and
currency consistency, route-cost inclusion, and that the recorded
configuration version and spoilage limit match the supplied active owner
configuration. The caller must obtain that configuration from a trusted
service after authorizing the owner; the validator does not authenticate or
authorize users. A future recommendation engine must retrieve the authorized
owner's actual active configuration and must never trust a client-supplied
spoilage limit. DE-02 does not normalize units, calculate/recompute gross
revenue, or rank markets; this requires shipment quantity/unit data and
commodity-specific conversion rules not defined in this contract. A
predicted spoilage percentage is not a food-safety certification.

Example: [`data/sample_intervention_recommendation.json`](../data/sample_intervention_recommendation.json).

## Contract 7 — `owner_configuration`

This is the versioned owner-specific record. It is separate from the
system-wide defaults template in
[`data/default_owner_configuration.json`](../data/default_owner_configuration.json).
The defaults file intentionally contains only the supplied settings and
`is_default`; owner IDs, audit timestamps, and activation metadata belong to
an instantiated owner record.

| Field | Type | Constraints |
|---|---|---|
| `configuration_id`, `owner_id`, `configuration_version`, `updated_by` | string | Non-empty. |
| `sensor_interval_minutes` | positive integer | Positive; a deployment-specific supported-interval allowlist is not defined in DE-02. |
| `live_raw_retention_minutes` | positive integer | Applies only to live raw sensor data. |
| `prediction_horizon_mode` | string | `DYNAMIC` or `FIXED`. |
| `fixed_prediction_horizon_minutes` | positive integer or null | Required and positive in FIXED mode; null in DYNAMIC mode. |
| `early_warning_hours` | number | Finite and greater than `critical_risk_hours`. |
| `critical_risk_hours` | number | Finite and greater than zero. |
| `max_predicted_spoilage_pct` | number | Finite and 0–100. The default 5% is a business goodwill rule, not food-safety certification. |
| `market_selection_priority` | string | `MAX_EXPECTED_NET_REVENUE`, `MIN_PREDICTED_SPOILAGE`, or `FASTEST_ELIGIBLE_ROUTE`. |
| `historical_summary_frequency` | string | Non-empty; the default is `HOURLY`. |
| `created_at`, `updated_at` | UTC timestamp | Updated time must not precede creation time. |
| `is_active` | boolean | Required. |

System defaults (not copied into every contract):

| Setting | Default |
|---|---:|
| `configuration_version` | `"1.0"` |
| `sensor_interval_minutes` | `5` |
| `live_raw_retention_minutes` | `15` |
| `prediction_horizon_mode` | `"DYNAMIC"` |
| `fixed_prediction_horizon_minutes` | `null` |
| `early_warning_hours` | `10` |
| `critical_risk_hours` | `5` |
| `max_predicted_spoilage_pct` | `5` |
| `market_selection_priority` | `"MAX_EXPECTED_NET_REVENUE"` |
| `historical_summary_frequency` | `"HOURLY"` |

The 15-minute value is exclusively live raw-buffer retention. Hourly summaries,
predictions, market comparisons, and intervention outcomes must be retained
under their applicable historical retention policy, not deleted after 15
minutes.

The validator can compare `owner_id` to an expected owner ID as a consistency
check, but this is not authentication or authorization. DE-02 does not
implement authorized updates, durable version history, or audit storage.
Configuration changes must be versioned and auditable in the future
configuration service, and stored predictions and recommendations must retain
the version used. A future recommendation engine must obtain the authorized
owner's actual active configuration server-side; it must not treat a
client-supplied `maximum_allowed_spoilage_pct` as authoritative. The
configurable limit (5% by default) remains a business goodwill constraint and
cannot override mandatory food-safety requirements.

Example: [`data/sample_owner_configuration.json`](../data/sample_owner_configuration.json).

## Mandatory food-safety constraints

These are system-level requirements and cannot be disabled by an owner setting:

1. Applicable food-safety and legal requirements are always enforced.
2. Confirmed spoilage or unsafe-product flags cannot be hidden.
3. Unsafe shipments cannot be presented as normally eligible for sale.
4. Predicted threshold crossing is distinct from physically confirmed spoilage.
5. Insufficient or unreliable data produces an uncertainty or human-review flag.
6. Mandatory safety alerts cannot be disabled.
7. The 5% spoilage limit is a business goodwill policy, not food-safety
   certification.

DE-02 documents these constraints; it does not implement a legal rule engine,
safety assessment, alerting path, or sale-eligibility decision. Consumers must
apply those controls independently of owner preferences and commercial
recommendations.

## Validation and testing plan

Implemented standard-library checks are in
[`src/atmo_sync/contracts.py`](../src/atmo_sync/contracts.py), with
[`tests/test_contracts.py`](../tests/test_contracts.py). Run them from the
repository root in PowerShell:

```powershell
$env:PYTHONPATH = (Join-Path (Get-Location).Path 'src')
python -m unittest discover -s tests -v
```

| Area | DE-02 automated payload checks | Integration/persistence checks still required |
|---|---|---|
| Sensor events | Valid sample; required fields; UUID; UTC timestamp; coordinate ranges; humidity range; finite temperature/vibration; non-negative vibration; sensor-only fields. | Duplicate event idempotency, broker delivery/replay behavior. |
| Historical summaries | Required finite metrics; window order; positive reading count; extrema/average consistency; humidity range; outside-range duration bounded by window; null does not become zero. | Recompute averages/min/max from source readings; duplicate-window upsert; live raw retention does not delete historical records. |
| Predictions | Forecast interval and horizon consistency; percentage ranges; supported risk stage; required configuration version; confidence range. Low-confidence values are preserved rather than rejected using an invented cutoff. | Model quality, uncertainty/human-review workflow, dynamic horizon calculation, version provenance persistence, physical confirmation distinction. |
| Market offers | Positive price; explicit unit; currency shape; coordinates; optional quantity; expiry ordering and expiry at supplied `as_of`. | Commodity compatibility, price-unit/quantity normalization, freshness for non-expiring offers, live feed behavior. |
| Route assessment | Non-negative distance/travel/cost; valid timestamps; percentage ranges and spoilage/saleable complement. | Route-input coordinate validation, route estimation, arrival-time quality, prediction linkage. Coordinates are not fields of this contract. |
| Recommendations | Action enum; source IDs and market nullability; no-market null rules; active configuration version/limit consistency; source offer/assessment identity and currency consistency; route-cost inclusion; expected net revenue equals gross less additional costs. | Authenticate owner and retrieve actual active config; normalize price units and shipment quantities before calculating gross; select highest eligible net revenue; exclude over-limit markets; no eligible market leads to mitigation/human review; safety restrictions cannot be overridden. |
| Owner configuration | Default values; positive intervals/retention; DYNAMIC/FIXED rules; threshold relationship; percentage bounds; priority enum; UTC audit-time ordering; optional expected-owner match. | Supported sensor interval allowlist; authenticated authorization; version history/audit persistence; mandatory safety controls remain enforced. |

The checks in the right column require stateful or external behavior and are
not claimed as implemented by these schema-level validators.
