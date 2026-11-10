"""Tests for the DE-02 contract validators."""

import copy
import json
import unittest
from pathlib import Path

from atmo_sync.contracts import (
    validate_default_configuration,
    validate_hourly_shipment_summary,
    validate_intervention_recommendation,
    validate_market_offer,
    validate_owner_configuration,
    validate_route_risk_assessment,
    validate_sensor_event,
    validate_shipment_prediction,
)


ROOT = Path(__file__).resolve().parents[1]


def load_sample(filename):
    with (ROOT / "data" / filename).open(encoding="utf-8") as sample_file:
        return json.load(sample_file)


class ContractValidatorTests(unittest.TestCase):
    def assert_invalid(self, validator, payload, message, **kwargs):
        with self.assertRaisesRegex(ValueError, message):
            validator(payload, **kwargs)

    def test_all_sample_contracts_are_valid(self):
        cases = (
            (validate_default_configuration, "default_owner_configuration.json"),
            (validate_sensor_event, "sample_sensor_event.json"),
            (validate_hourly_shipment_summary, "sample_hourly_shipment_summary.json"),
            (validate_shipment_prediction, "sample_shipment_prediction.json"),
            (validate_market_offer, "sample_market_offer.json"),
            (validate_route_risk_assessment, "sample_route_risk_assessment.json"),
            (
                validate_intervention_recommendation,
                "sample_intervention_recommendation.json",
            ),
            (validate_owner_configuration, "sample_owner_configuration.json"),
        )
        for validator, filename in cases:
            with self.subTest(filename=filename):
                payload = load_sample(filename)
                if filename == "sample_intervention_recommendation.json":
                    validator(
                        payload,
                        owner_configuration=load_sample(
                            "sample_owner_configuration.json"
                        ),
                        market_offer=load_sample("sample_market_offer.json"),
                        route_assessment=load_sample(
                            "sample_route_risk_assessment.json"
                        ),
                    )
                else:
                    validator(payload)

    def test_sensor_event_rejects_missing_fields_invalid_uuid_and_non_utc_time(self):
        event = load_sample("sample_sensor_event.json")
        missing = copy.deepcopy(event)
        del missing["readings"]
        self.assert_invalid(validate_sensor_event, missing, "missing required fields")

        invalid_uuid = copy.deepcopy(event)
        invalid_uuid["event_id"] = "not-a-uuid"
        self.assert_invalid(validate_sensor_event, invalid_uuid, "event_id must be a UUID")

        invalid_timestamp = copy.deepcopy(event)
        invalid_timestamp["event_timestamp"] = "2026-11-10T06:00:00+05:30"
        self.assert_invalid(validate_sensor_event, invalid_timestamp, "UTC timezone")

    def test_sensor_event_rejects_invalid_coordinates_and_measurements(self):
        event = load_sample("sample_sensor_event.json")
        for field, invalid in (("latitude", 91), ("longitude", -181)):
            payload = copy.deepcopy(event)
            payload["location"][field] = invalid
            with self.subTest(field=field):
                self.assert_invalid(validate_sensor_event, payload, "between")

        for humidity in (-0.1, 100.1):
            payload = copy.deepcopy(event)
            payload["readings"]["humidity_pct"] = humidity
            with self.subTest(humidity=humidity):
                self.assert_invalid(validate_sensor_event, payload, "between 0 and 100")

        for vibration in (-0.01, float("nan"), float("inf"), float("-inf")):
            payload = copy.deepcopy(event)
            payload["readings"]["vibration_g"] = vibration
            with self.subTest(vibration=vibration):
                self.assert_invalid(validate_sensor_event, payload, "finite|not be negative")

        for temperature in (float("nan"), float("inf"), float("-inf")):
            payload = copy.deepcopy(event)
            payload["readings"]["temperature_c"] = temperature
            with self.subTest(temperature=temperature):
                self.assert_invalid(validate_sensor_event, payload, "finite")

    def test_sensor_event_rejects_nulls_and_non_sensor_fields(self):
        event = load_sample("sample_sensor_event.json")
        payload = copy.deepcopy(event)
        payload["readings"]["temperature_c"] = None
        self.assert_invalid(validate_sensor_event, payload, "JSON number")

        payload = copy.deepcopy(event)
        payload["market_id"] = "MARKET-1"
        self.assert_invalid(validate_sensor_event, payload, "unsupported fields")

    def test_summary_rejects_bad_windows_missing_values_and_inconsistent_metrics(self):
        summary = load_sample("sample_hourly_shipment_summary.json")

        payload = copy.deepcopy(summary)
        payload["window_end"] = payload["window_start"]
        self.assert_invalid(validate_hourly_shipment_summary, payload, "later than")

        payload = copy.deepcopy(summary)
        payload["reading_count"] = -1
        self.assert_invalid(validate_hourly_shipment_summary, payload, "at least 1")

        payload = copy.deepcopy(summary)
        payload["reading_count"] = 0
        self.assert_invalid(validate_hourly_shipment_summary, payload, "at least 1")

        payload = copy.deepcopy(summary)
        payload["temperature_avg_c"] = None
        self.assert_invalid(validate_hourly_shipment_summary, payload, "JSON number")

        payload = copy.deepcopy(summary)
        payload["temperature_min_c"] = 13
        self.assert_invalid(
            validate_hourly_shipment_summary,
            payload,
            "temperature_min_c must not exceed",
        )

        payload = copy.deepcopy(summary)
        payload["humidity_max_pct"] = 100.1
        self.assert_invalid(validate_hourly_shipment_summary, payload, "between 0 and 100")

        payload = copy.deepcopy(summary)
        payload["time_outside_operating_range_seconds"] = 3601
        self.assert_invalid(validate_hourly_shipment_summary, payload, "must not exceed")

    def test_prediction_validates_interval_risk_spoilage_and_confidence(self):
        prediction = load_sample("sample_shipment_prediction.json")

        payload = copy.deepcopy(prediction)
        payload["forecast_end"] = payload["forecast_start"]
        self.assert_invalid(validate_shipment_prediction, payload, "later than")

        payload = copy.deepcopy(prediction)
        payload["forecast_horizon_minutes"] = 30
        self.assert_invalid(validate_shipment_prediction, payload, "match the forecast interval")

        payload = copy.deepcopy(prediction)
        payload["predicted_spoilage_pct"] = 101
        self.assert_invalid(validate_shipment_prediction, payload, "between 0 and 100")

        payload = copy.deepcopy(prediction)
        payload["risk_stage"] = "CONFIRMED_SPOILAGE"
        self.assert_invalid(validate_shipment_prediction, payload, "risk_stage must be one")

        payload = copy.deepcopy(prediction)
        del payload["configuration_version"]
        self.assert_invalid(validate_shipment_prediction, payload, "missing required fields")

        payload = copy.deepcopy(prediction)
        payload["confidence_score"] = 0.01
        validate_shipment_prediction(payload)

    def test_market_offer_checks_price_units_expiry_and_location(self):
        offer = load_sample("sample_market_offer.json")
        validate_market_offer(offer, as_of="2026-11-10T06:00:00Z")
        self.assert_invalid(
            validate_market_offer,
            offer,
            "has expired",
            as_of="2026-11-10T07:00:00Z",
        )

        optional_fields_removed = copy.deepcopy(offer)
        for field in ("shipment_id", "available_quantity", "offer_expiry"):
            optional_fields_removed.pop(field)
        validate_market_offer(optional_fields_removed)

        payload = copy.deepcopy(offer)
        payload["offer_expiry"] = None
        self.assert_invalid(
            validate_market_offer,
            payload,
            "ISO 8601 timestamp",
            as_of="not-a-timestamp",
        )

        for price in (0, -1):
            payload = copy.deepcopy(offer)
            payload["quoted_price"] = price
            with self.subTest(price=price):
                self.assert_invalid(validate_market_offer, payload, "greater than zero")

        payload = copy.deepcopy(offer)
        payload["price_unit"] = ""
        self.assert_invalid(validate_market_offer, payload, "price_unit")

        payload = copy.deepcopy(offer)
        payload["currency"] = "usd"
        self.assert_invalid(validate_market_offer, payload, "currency")

        payload = copy.deepcopy(offer)
        payload["market_latitude"] = 91
        self.assert_invalid(validate_market_offer, payload, "between -90 and 90")

        payload = copy.deepcopy(offer)
        payload["offer_expiry"] = payload["offer_timestamp"]
        self.assert_invalid(validate_market_offer, payload, "later than offer_timestamp")

    def test_route_assessment_checks_duration_percentages_cost_and_times(self):
        assessment = load_sample("sample_route_risk_assessment.json")

        payload = copy.deepcopy(assessment)
        payload["estimated_travel_minutes"] = -1
        self.assert_invalid(validate_route_risk_assessment, payload, "must not be negative")

        payload = copy.deepcopy(assessment)
        payload["route_distance_km"] = float("inf")
        self.assert_invalid(validate_route_risk_assessment, payload, "finite")

        payload = copy.deepcopy(assessment)
        payload["estimated_saleable_pct"] = 95
        self.assert_invalid(validate_route_risk_assessment, payload, "must equal 100 minus")

        payload = copy.deepcopy(assessment)
        payload["additional_transport_cost"] = -1
        self.assert_invalid(validate_route_risk_assessment, payload, "must not be negative")

        payload = copy.deepcopy(assessment)
        payload["estimated_arrival_time"] = "2026-11-10T06:00:00Z"
        self.assert_invalid(validate_route_risk_assessment, payload, "must not precede")

    def test_recommendation_enforces_threshold_market_and_revenue_math(self):
        recommendation = load_sample("sample_intervention_recommendation.json")
        owner = load_sample("sample_owner_configuration.json")
        offer = load_sample("sample_market_offer.json")
        assessment = load_sample("sample_route_risk_assessment.json")
        sources = {
            "owner_configuration": owner,
            "market_offer": offer,
            "route_assessment": assessment,
        }
        validate_intervention_recommendation(recommendation, **sources)

        payload = copy.deepcopy(recommendation)
        payload["predicted_spoilage_pct"] = 5.1
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "cannot exceed maximum_allowed_spoilage_pct",
            **sources,
        )

        payload = copy.deepcopy(recommendation)
        payload["recommendation_action"] = "CONTINUE"
        payload["predicted_spoilage_pct"] = 5.1
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "cannot exceed maximum_allowed_spoilage_pct",
            **sources,
        )

        payload = copy.deepcopy(recommendation)
        payload["additional_costs"] = 200
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "must equal",
            **sources,
        )

        payload = copy.deepcopy(recommendation)
        payload["currency"] = "EUR"
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "currency must match the market offer",
            **sources,
        )

        payload = copy.deepcopy(recommendation)
        inconsistent_route = copy.deepcopy(assessment)
        inconsistent_route["currency"] = "EUR"
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "currency must match the route assessment",
            owner_configuration=owner,
            market_offer=offer,
            route_assessment=inconsistent_route,
        )

        payload = copy.deepcopy(recommendation)
        payload["offer_id"] = "OFFER-OTHER"
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "offer_id must match",
            **sources,
        )

        payload = copy.deepcopy(recommendation)
        payload["assessment_id"] = "ROUTE-OTHER"
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "assessment_id must match",
            **sources,
        )

        payload = copy.deepcopy(recommendation)
        payload["additional_costs"] = 100
        payload["expected_net_revenue"] = 9580
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "must include the route assessment transport cost",
            **sources,
        )

        payload = copy.deepcopy(recommendation)
        payload["maximum_allowed_spoilage_pct"] = 10
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "must match the active owner configuration",
            **sources,
        )

        payload = copy.deepcopy(recommendation)
        payload["configuration_version"] = "0.9"
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "must match the active owner configuration",
            **sources,
        )

        configurable_owner = copy.deepcopy(owner)
        configurable_owner["configuration_version"] = "1.1"
        configurable_owner["max_predicted_spoilage_pct"] = 8
        configurable_recommendation = copy.deepcopy(recommendation)
        configurable_recommendation["configuration_version"] = "1.1"
        configurable_recommendation["maximum_allowed_spoilage_pct"] = 8
        validate_intervention_recommendation(
            configurable_recommendation,
            owner_configuration=configurable_owner,
            market_offer=offer,
            route_assessment=assessment,
        )

        payload = copy.deepcopy(recommendation)
        payload["recommendation_action"] = "REROUTE"
        payload["recommended_market_id"] = None
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "may be null only",
            owner_configuration=owner,
        )

        for action in ("MITIGATE", "HUMAN_REVIEW"):
            payload = copy.deepcopy(recommendation)
            payload["recommendation_action"] = action
            payload["recommended_market_id"] = None
            payload["offer_id"] = None
            payload["assessment_id"] = None
            payload["gross_expected_revenue"] = None
            payload["additional_costs"] = None
            payload["expected_net_revenue"] = None
            payload["currency"] = None
            payload["predicted_spoilage_pct"] = 20
            with self.subTest(action=action):
                validate_intervention_recommendation(
                    payload,
                    owner_configuration=owner,
                )

        payload["expected_net_revenue"] = 0
        self.assert_invalid(
            validate_intervention_recommendation,
            payload,
            "must be null when no market is recommended",
            owner_configuration=owner,
        )

    def test_selected_market_spoilage_cap_applies_to_all_actions(self):
        recommendation = load_sample("sample_intervention_recommendation.json")
        sources = {
            "owner_configuration": load_sample("sample_owner_configuration.json"),
            "market_offer": load_sample("sample_market_offer.json"),
            "route_assessment": load_sample("sample_route_risk_assessment.json"),
        }
        for action in ("MITIGATE", "HUMAN_REVIEW"):
            payload = copy.deepcopy(recommendation)
            payload["recommendation_action"] = action
            payload["predicted_spoilage_pct"] = 5.1
            with self.subTest(action=action):
                self.assert_invalid(
                    validate_intervention_recommendation,
                    payload,
                    "cannot exceed maximum_allowed_spoilage_pct",
                    **sources,
                )

    def test_no_market_mitigation_actions_allow_spoilage_above_cap(self):
        recommendation = load_sample("sample_intervention_recommendation.json")
        owner = load_sample("sample_owner_configuration.json")
        for action in ("MITIGATE", "HUMAN_REVIEW"):
            payload = copy.deepcopy(recommendation)
            payload["recommendation_action"] = action
            payload["recommended_market_id"] = None
            payload["offer_id"] = None
            payload["assessment_id"] = None
            payload["gross_expected_revenue"] = None
            payload["additional_costs"] = None
            payload["expected_net_revenue"] = None
            payload["currency"] = None
            payload["predicted_spoilage_pct"] = 20
            with self.subTest(action=action):
                validate_intervention_recommendation(
                    payload,
                    owner_configuration=owner,
                )

    def test_default_and_owner_configuration_constraints(self):
        default = load_sample("default_owner_configuration.json")
        validate_default_configuration(default)
        self.assertEqual(default["sensor_interval_minutes"], 5)
        self.assertEqual(default["live_raw_retention_minutes"], 15)
        self.assertEqual(default["prediction_horizon_mode"], "DYNAMIC")
        self.assertEqual(default["early_warning_hours"], 10)
        self.assertEqual(default["critical_risk_hours"], 5)
        self.assertEqual(default["max_predicted_spoilage_pct"], 5)
        self.assertEqual(default["historical_summary_frequency"], "HOURLY")

        owner = load_sample("sample_owner_configuration.json")
        validate_owner_configuration(owner, expected_owner_id="ORG-EXAMPLE-001")

        fixed = copy.deepcopy(owner)
        fixed["prediction_horizon_mode"] = "FIXED"
        fixed["fixed_prediction_horizon_minutes"] = 90
        validate_owner_configuration(fixed)

        invalid = copy.deepcopy(owner)
        invalid["early_warning_hours"] = 5
        self.assert_invalid(validate_owner_configuration, invalid, "greater than critical")

        invalid = copy.deepcopy(owner)
        invalid["market_selection_priority"] = "LOWEST_PRICE"
        self.assert_invalid(validate_owner_configuration, invalid, "not a supported")

        invalid = copy.deepcopy(owner)
        invalid["prediction_horizon_mode"] = "DYNAMIC"
        invalid["fixed_prediction_horizon_minutes"] = 30
        self.assert_invalid(validate_owner_configuration, invalid, "must be null")

        self.assert_invalid(
            validate_owner_configuration,
            owner,
            "does not match",
            expected_owner_id="ORG-OTHER-001",
        )

        invalid = copy.deepcopy(owner)
        invalid["updated_at"] = "2026-11-10T05:59:00Z"
        self.assert_invalid(validate_owner_configuration, invalid, "must not precede")


if __name__ == "__main__":
    unittest.main()
