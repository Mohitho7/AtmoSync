"""Validation helpers for the AtmoSync DE-02 data contracts.

These validators check payload shape and contract-level invariants. They do
not implement persistence, authorization, prediction, routing, or food-safety
decisions.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


RISK_STAGES = frozenset(
    {"EARLY_WARNING", "CRITICAL_RISK", "SPOILAGE_THRESHOLD_REACHED"}
)
RECOMMENDATION_ACTIONS = frozenset(
    {"REROUTE", "CONTINUE", "MITIGATE", "HUMAN_REVIEW"}
)
MARKET_SELECTION_PRIORITIES = frozenset(
    {
        "MAX_EXPECTED_NET_REVENUE",
        "MIN_PREDICTED_SPOILAGE",
        "FASTEST_ELIGIBLE_ROUTE",
    }
)
PREDICTION_HORIZON_MODES = frozenset({"DYNAMIC", "FIXED"})
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")


def _record(value: Any, contract: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{contract} must be a JSON object")
    return value


def _required(
    record: Mapping[str, Any], fields: tuple[str, ...], contract: str
) -> None:
    missing = [field for field in fields if field not in record]
    if missing:
        raise ValueError(f"{contract} is missing required fields: {', '.join(missing)}")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"{field} must be a JSON number")
    try:
        result = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError(f"{field} must be a finite number") from error
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite number")
    return result


def _integer(value: Any, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field} must be at least {minimum}")
    return value


def _utc_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or "T" not in value:
        raise ValueError(f"{field} must be an ISO 8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO 8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or value.endswith(
        "-00:00"
    ):
        raise ValueError(f"{field} must include a UTC timezone")
    return parsed


def _percentage(value: Any, field: str) -> Decimal:
    result = _decimal(value, field)
    if not Decimal(0) <= result <= Decimal(100):
        raise ValueError(f"{field} must be between 0 and 100")
    return result


def _currency(value: Any, field: str = "currency") -> str:
    result = _text(value, field)
    if not _CURRENCY_PATTERN.fullmatch(result):
        raise ValueError(f"{field} must be a three-letter uppercase currency code")
    return result


def _coordinates(latitude: Any, longitude: Any, prefix: str) -> None:
    latitude_value = _decimal(latitude, f"{prefix}.latitude")
    longitude_value = _decimal(longitude, f"{prefix}.longitude")
    if not Decimal(-90) <= latitude_value <= Decimal(90):
        raise ValueError(f"{prefix}.latitude must be between -90 and 90")
    if not Decimal(-180) <= longitude_value <= Decimal(180):
        raise ValueError(f"{prefix}.longitude must be between -180 and 180")


def _uuid(value: Any, field: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a UUID string")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as error:
        raise ValueError(f"{field} must be a UUID string") from error
    if str(parsed) != value.lower():
        raise ValueError(f"{field} must use the canonical UUID format")


def _nonnegative(value: Any, field: str) -> Decimal:
    result = _decimal(value, field)
    if result < 0:
        raise ValueError(f"{field} must not be negative")
    return result


def validate_sensor_event(value: Any) -> None:
    """Validate one sensor reading event; raises ValueError on invalid input."""
    contract = "sensor_event"
    event = _record(value, contract)
    fields = (
        "event_id",
        "schema_version",
        "event_timestamp",
        "shipment_id",
        "container_id",
        "sensor_id",
        "commodity",
        "location",
        "readings",
    )
    _required(event, fields, contract)
    unknown = set(event) - set(fields)
    if unknown:
        raise ValueError(
            f"{contract} contains unsupported fields: {', '.join(sorted(unknown))}"
        )
    _uuid(event["event_id"], "event_id")
    for field in (
        "schema_version",
        "shipment_id",
        "container_id",
        "sensor_id",
        "commodity",
    ):
        _text(event[field], field)
    _utc_datetime(event["event_timestamp"], "event_timestamp")

    location = _record(event["location"], "location")
    _required(location, ("latitude", "longitude"), "location")
    if set(location) != {"latitude", "longitude"}:
        raise ValueError("location must contain only latitude and longitude")
    _coordinates(location["latitude"], location["longitude"], "location")

    readings = _record(event["readings"], "readings")
    reading_fields = ("temperature_c", "humidity_pct", "vibration_g")
    _required(readings, reading_fields, "readings")
    if set(readings) != set(reading_fields):
        raise ValueError("readings must contain only temperature_c, humidity_pct, and vibration_g")
    _decimal(readings["temperature_c"], "readings.temperature_c")
    _percentage(readings["humidity_pct"], "readings.humidity_pct")
    _nonnegative(readings["vibration_g"], "readings.vibration_g")


def validate_hourly_shipment_summary(value: Any) -> None:
    """Validate an hourly shipment summary payload."""
    contract = "hourly_shipment_summary"
    summary = _record(value, contract)
    fields = (
        "summary_id",
        "shipment_id",
        "container_id",
        "commodity",
        "window_start",
        "window_end",
        "reading_count",
        "temperature_avg_c",
        "temperature_min_c",
        "temperature_max_c",
        "humidity_avg_pct",
        "humidity_min_pct",
        "humidity_max_pct",
        "vibration_avg_g",
        "vibration_max_g",
        "time_outside_operating_range_seconds",
        "created_at",
    )
    _required(summary, fields, contract)
    for field in ("summary_id", "shipment_id", "container_id", "commodity"):
        _text(summary[field], field)
    start = _utc_datetime(summary["window_start"], "window_start")
    end = _utc_datetime(summary["window_end"], "window_end")
    _utc_datetime(summary["created_at"], "created_at")
    if end <= start:
        raise ValueError("window_end must be later than window_start")
    _integer(summary["reading_count"], "reading_count", minimum=1)

    temperature_min = _decimal(summary["temperature_min_c"], "temperature_min_c")
    temperature_avg = _decimal(summary["temperature_avg_c"], "temperature_avg_c")
    temperature_max = _decimal(summary["temperature_max_c"], "temperature_max_c")
    humidity_min = _percentage(summary["humidity_min_pct"], "humidity_min_pct")
    humidity_avg = _percentage(summary["humidity_avg_pct"], "humidity_avg_pct")
    humidity_max = _percentage(summary["humidity_max_pct"], "humidity_max_pct")
    vibration_avg = _nonnegative(summary["vibration_avg_g"], "vibration_avg_g")
    vibration_max = _nonnegative(summary["vibration_max_g"], "vibration_max_g")
    if temperature_min > temperature_max:
        raise ValueError("temperature_min_c must not exceed temperature_max_c")
    if not temperature_min <= temperature_avg <= temperature_max:
        raise ValueError("temperature_avg_c must be between its minimum and maximum")
    if humidity_min > humidity_max:
        raise ValueError("humidity_min_pct must not exceed humidity_max_pct")
    if not humidity_min <= humidity_avg <= humidity_max:
        raise ValueError("humidity_avg_pct must be between its minimum and maximum")
    if vibration_avg > vibration_max:
        raise ValueError("vibration_avg_g must not exceed vibration_max_g")

    outside_seconds = _nonnegative(
        summary["time_outside_operating_range_seconds"],
        "time_outside_operating_range_seconds",
    )
    window_seconds = Decimal(str((end - start).total_seconds()))
    if outside_seconds > window_seconds:
        raise ValueError("time outside operating range must not exceed the window")


def validate_shipment_prediction(value: Any) -> None:
    """Validate a prediction record without asserting physical spoilage."""
    contract = "shipment_prediction"
    prediction = _record(value, contract)
    fields = (
        "prediction_id",
        "shipment_id",
        "container_id",
        "prediction_timestamp",
        "forecast_start",
        "forecast_end",
        "forecast_horizon_minutes",
        "estimated_temperature_c",
        "estimated_humidity_pct",
        "estimated_vibration_g",
        "estimated_hours_to_spoilage",
        "predicted_spoilage_pct",
        "risk_stage",
        "confidence_score",
        "model_version",
        "configuration_version",
    )
    _required(prediction, fields, contract)
    for field in ("prediction_id", "shipment_id", "container_id", "model_version", "configuration_version"):
        _text(prediction[field], field)
    _utc_datetime(prediction["prediction_timestamp"], "prediction_timestamp")
    forecast_start = _utc_datetime(prediction["forecast_start"], "forecast_start")
    forecast_end = _utc_datetime(prediction["forecast_end"], "forecast_end")
    if forecast_end <= forecast_start:
        raise ValueError("forecast_end must be later than forecast_start")
    horizon_minutes = _integer(
        prediction["forecast_horizon_minutes"],
        "forecast_horizon_minutes",
        minimum=1,
    )
    if (forecast_end - forecast_start).total_seconds() != horizon_minutes * 60:
        raise ValueError("forecast_horizon_minutes must match the forecast interval")
    _decimal(prediction["estimated_temperature_c"], "estimated_temperature_c")
    _percentage(prediction["estimated_humidity_pct"], "estimated_humidity_pct")
    _nonnegative(prediction["estimated_vibration_g"], "estimated_vibration_g")
    hours = prediction["estimated_hours_to_spoilage"]
    if hours is not None:
        _nonnegative(hours, "estimated_hours_to_spoilage")
    _percentage(prediction["predicted_spoilage_pct"], "predicted_spoilage_pct")
    risk_stage = _text(prediction["risk_stage"], "risk_stage")
    if risk_stage not in RISK_STAGES:
        raise ValueError(f"risk_stage must be one of: {', '.join(sorted(RISK_STAGES))}")
    confidence = _decimal(prediction["confidence_score"], "confidence_score")
    if not Decimal(0) <= confidence <= Decimal(1):
        raise ValueError("confidence_score must be between 0 and 1")


def validate_market_offer(value: Any, *, as_of: str | None = None) -> None:
    """Validate an offer; pass UTC ``as_of`` to reject offers already expired."""
    contract = "market_offer"
    offer = _record(value, contract)
    fields = (
        "offer_id",
        "market_id",
        "market_name",
        "commodity",
        "quoted_price",
        "currency",
        "price_unit",
        "market_latitude",
        "market_longitude",
        "offer_timestamp",
        "source",
    )
    _required(offer, fields, contract)
    for field in ("offer_id", "market_id", "market_name", "commodity", "price_unit", "source"):
        _text(offer[field], field)
    if "shipment_id" in offer and offer["shipment_id"] is not None:
        _text(offer["shipment_id"], "shipment_id")
    price = _decimal(offer["quoted_price"], "quoted_price")
    if price <= 0:
        raise ValueError("quoted_price must be greater than zero")
    _currency(offer["currency"])
    quantity = offer.get("available_quantity")
    if quantity is not None:
        _nonnegative(quantity, "available_quantity")
    _coordinates(
        offer["market_latitude"], offer["market_longitude"], "market"
    )
    offered_at = _utc_datetime(offer["offer_timestamp"], "offer_timestamp")
    expiry_value = offer.get("offer_expiry")
    expiry = None
    if expiry_value is not None:
        expiry = _utc_datetime(expiry_value, "offer_expiry")
        if expiry <= offered_at:
            raise ValueError("offer_expiry must be later than offer_timestamp")
    checked_at = _utc_datetime(as_of, "as_of") if as_of is not None else None
    if checked_at is not None and expiry is not None:
        if expiry <= checked_at:
            raise ValueError("market offer has expired")


def validate_route_risk_assessment(value: Any) -> None:
    """Validate route, spoilage, saleable-percentage, and cost fields."""
    contract = "route_risk_assessment"
    assessment = _record(value, contract)
    fields = (
        "assessment_id",
        "shipment_id",
        "market_id",
        "route_distance_km",
        "estimated_travel_minutes",
        "estimated_arrival_time",
        "predicted_spoilage_pct",
        "estimated_saleable_pct",
        "additional_transport_cost",
        "currency",
        "prediction_id",
        "assessment_timestamp",
    )
    _required(assessment, fields, contract)
    for field in ("assessment_id", "shipment_id", "market_id", "prediction_id"):
        _text(assessment[field], field)
    _nonnegative(assessment["route_distance_km"], "route_distance_km")
    _nonnegative(
        assessment["estimated_travel_minutes"], "estimated_travel_minutes"
    )
    arrival = _utc_datetime(assessment["estimated_arrival_time"], "estimated_arrival_time")
    assessed_at = _utc_datetime(
        assessment["assessment_timestamp"], "assessment_timestamp"
    )
    if arrival < assessed_at:
        raise ValueError("estimated_arrival_time must not precede assessment_timestamp")
    spoilage = _percentage(
        assessment["predicted_spoilage_pct"], "predicted_spoilage_pct"
    )
    saleable = _percentage(
        assessment["estimated_saleable_pct"], "estimated_saleable_pct"
    )
    if spoilage + saleable != Decimal(100):
        raise ValueError("estimated_saleable_pct must equal 100 minus predicted_spoilage_pct")
    _nonnegative(
        assessment["additional_transport_cost"], "additional_transport_cost"
    )
    _currency(assessment["currency"])


def validate_intervention_recommendation(
    value: Any,
    *,
    owner_configuration: Any,
    market_offer: Any | None = None,
    route_assessment: Any | None = None,
) -> None:
    """Validate recommendation values against their configuration and source records.

    The caller must supply the trusted, active owner configuration selected
    after authorization. This function does not authenticate that caller.
    """
    contract = "intervention_recommendation"
    recommendation = _record(value, contract)
    fields = (
        "recommendation_id",
        "shipment_id",
        "recommended_market_id",
        "offer_id",
        "assessment_id",
        "recommendation_timestamp",
        "predicted_spoilage_pct",
        "maximum_allowed_spoilage_pct",
        "gross_expected_revenue",
        "additional_costs",
        "expected_net_revenue",
        "currency",
        "recommendation_action",
        "decision_reason",
        "configuration_version",
        "prediction_id",
    )
    _required(recommendation, fields, contract)
    owner = _record(owner_configuration, "owner_configuration")
    validate_owner_configuration(owner)
    if not owner["is_active"]:
        raise ValueError("owner_configuration must be active")
    for field in (
        "recommendation_id",
        "shipment_id",
        "decision_reason",
        "configuration_version",
        "prediction_id",
    ):
        _text(recommendation[field], field)
    _utc_datetime(recommendation["recommendation_timestamp"], "recommendation_timestamp")
    if recommendation["configuration_version"] != owner["configuration_version"]:
        raise ValueError("configuration_version must match the active owner configuration")
    action = _text(recommendation["recommendation_action"], "recommendation_action")
    if action not in RECOMMENDATION_ACTIONS:
        raise ValueError(
            f"recommendation_action must be one of: {', '.join(sorted(RECOMMENDATION_ACTIONS))}"
        )
    market_id = recommendation["recommended_market_id"]
    if market_id is None:
        if action not in {"MITIGATE", "HUMAN_REVIEW"}:
            raise ValueError(
                "recommended_market_id may be null only for MITIGATE or HUMAN_REVIEW"
            )
    else:
        _text(market_id, "recommended_market_id")
    spoilage = _percentage(
        recommendation["predicted_spoilage_pct"], "predicted_spoilage_pct"
    )
    maximum = _percentage(
        recommendation["maximum_allowed_spoilage_pct"],
        "maximum_allowed_spoilage_pct",
    )
    configured_maximum = _percentage(
        owner["max_predicted_spoilage_pct"],
        "owner_configuration.max_predicted_spoilage_pct",
    )
    if maximum != configured_maximum:
        raise ValueError(
            "maximum_allowed_spoilage_pct must match the active owner configuration"
        )
    if market_id is not None and spoilage > maximum:
        raise ValueError(
            "predicted_spoilage_pct cannot exceed maximum_allowed_spoilage_pct "
            "for a selected market"
        )
    source_fields = ("offer_id", "assessment_id")
    financial_fields = (
        "gross_expected_revenue",
        "additional_costs",
        "expected_net_revenue",
        "currency",
    )
    if market_id is None:
        if any(recommendation[field] is not None for field in source_fields):
            raise ValueError(
                "offer_id and assessment_id must be null when no market is recommended"
            )
        if any(recommendation[field] is not None for field in financial_fields):
            raise ValueError(
                "financial values and currency must be null when no market is recommended"
            )
        if market_offer is not None or route_assessment is not None:
            raise ValueError(
                "market_offer and route_assessment must be omitted when no market is recommended"
            )
        return

    for field in source_fields:
        _text(recommendation[field], field)
    if any(recommendation[field] is None for field in financial_fields):
        raise ValueError(
            "financial values and currency are required when a market is recommended"
        )
    if market_offer is None or route_assessment is None:
        raise ValueError(
            "market_offer and route_assessment records are required for a market recommendation"
        )

    currency = _currency(recommendation["currency"])
    validate_market_offer(
        market_offer,
        as_of=recommendation["recommendation_timestamp"],
    )
    offer = _record(market_offer, "market_offer")
    validate_route_risk_assessment(route_assessment)
    assessment = _record(route_assessment, "route_risk_assessment")
    if recommendation["offer_id"] != offer["offer_id"]:
        raise ValueError("offer_id must match the market offer used")
    if recommendation["assessment_id"] != assessment["assessment_id"]:
        raise ValueError("assessment_id must match the route assessment used")
    if offer["market_id"] != market_id or assessment["market_id"] != market_id:
        raise ValueError("market IDs must match the recommendation and source records")
    if (
        offer.get("shipment_id") is not None
        and offer["shipment_id"] != recommendation["shipment_id"]
    ):
        raise ValueError("market offer shipment_id must match the recommendation")
    if assessment["shipment_id"] != recommendation["shipment_id"]:
        raise ValueError("route assessment shipment_id must match the recommendation")
    if assessment["prediction_id"] != recommendation["prediction_id"]:
        raise ValueError("route assessment prediction_id must match the recommendation")
    if _percentage(
        assessment["predicted_spoilage_pct"],
        "route_assessment.predicted_spoilage_pct",
    ) != spoilage:
        raise ValueError("predicted_spoilage_pct must match the route assessment")
    if currency != _currency(offer["currency"]):
        raise ValueError("recommendation currency must match the market offer currency")
    if currency != _currency(assessment["currency"]):
        raise ValueError("recommendation currency must match the route assessment currency")

    gross = _nonnegative(
        recommendation["gross_expected_revenue"], "gross_expected_revenue"
    )
    costs = _nonnegative(recommendation["additional_costs"], "additional_costs")
    route_cost = _nonnegative(
        assessment["additional_transport_cost"],
        "route_assessment.additional_transport_cost",
    )
    if costs < route_cost:
        raise ValueError(
            "additional_costs must include the route assessment transport cost"
        )
    net = _decimal(recommendation["expected_net_revenue"], "expected_net_revenue")
    if gross - costs != net:
        raise ValueError(
            "expected_net_revenue must equal gross_expected_revenue minus additional_costs"
        )


def _validate_owner_settings(configuration: Mapping[str, Any]) -> None:
    for field in ("configuration_version", "historical_summary_frequency"):
        _text(configuration[field], field)
    _integer(configuration["sensor_interval_minutes"], "sensor_interval_minutes", minimum=1)
    _integer(
        configuration["live_raw_retention_minutes"],
        "live_raw_retention_minutes",
        minimum=1,
    )
    mode = _text(configuration["prediction_horizon_mode"], "prediction_horizon_mode")
    if mode not in PREDICTION_HORIZON_MODES:
        raise ValueError(
            f"prediction_horizon_mode must be one of: {', '.join(sorted(PREDICTION_HORIZON_MODES))}"
        )
    fixed_horizon = configuration["fixed_prediction_horizon_minutes"]
    if mode == "FIXED":
        _integer(fixed_horizon, "fixed_prediction_horizon_minutes", minimum=1)
    elif fixed_horizon is not None:
        raise ValueError("fixed_prediction_horizon_minutes must be null in DYNAMIC mode")
    early = _decimal(configuration["early_warning_hours"], "early_warning_hours")
    critical = _decimal(configuration["critical_risk_hours"], "critical_risk_hours")
    if critical <= 0:
        raise ValueError("critical_risk_hours must be greater than zero")
    if early <= critical:
        raise ValueError("early_warning_hours must be greater than critical_risk_hours")
    _percentage(
        configuration["max_predicted_spoilage_pct"],
        "max_predicted_spoilage_pct",
    )
    priority = _text(
        configuration["market_selection_priority"], "market_selection_priority"
    )
    if priority not in MARKET_SELECTION_PRIORITIES:
        raise ValueError(
            "market_selection_priority is not a supported market-selection priority"
        )


def validate_owner_configuration(
    value: Any, *, expected_owner_id: str | None = None
) -> None:
    """Validate owner configuration and optionally check its owner identifier.

    The owner comparison is a consistency check, not an authorization system.
    """
    contract = "owner_configuration"
    configuration = _record(value, contract)
    fields = (
        "configuration_id",
        "owner_id",
        "configuration_version",
        "sensor_interval_minutes",
        "live_raw_retention_minutes",
        "prediction_horizon_mode",
        "fixed_prediction_horizon_minutes",
        "early_warning_hours",
        "critical_risk_hours",
        "max_predicted_spoilage_pct",
        "market_selection_priority",
        "historical_summary_frequency",
        "created_at",
        "updated_at",
        "updated_by",
        "is_active",
    )
    _required(configuration, fields, contract)
    for field in ("configuration_id", "owner_id", "updated_by"):
        _text(configuration[field], field)
    created_at = _utc_datetime(configuration["created_at"], "created_at")
    updated_at = _utc_datetime(configuration["updated_at"], "updated_at")
    if updated_at < created_at:
        raise ValueError("updated_at must not precede created_at")
    if not isinstance(configuration["is_active"], bool):
        raise ValueError("is_active must be a boolean")
    _validate_owner_settings(configuration)
    if expected_owner_id is not None and configuration["owner_id"] != expected_owner_id:
        raise ValueError("configuration owner_id does not match the expected owner")


def validate_default_configuration(value: Any) -> None:
    """Validate the system-default settings template (not an owner record)."""
    contract = "default_owner_configuration"
    configuration = _record(value, contract)
    fields = (
        "configuration_version",
        "sensor_interval_minutes",
        "live_raw_retention_minutes",
        "prediction_horizon_mode",
        "fixed_prediction_horizon_minutes",
        "early_warning_hours",
        "critical_risk_hours",
        "max_predicted_spoilage_pct",
        "market_selection_priority",
        "historical_summary_frequency",
        "is_default",
    )
    _required(configuration, fields, contract)
    if configuration["is_default"] is not True:
        raise ValueError("is_default must be true for the system-default template")
    _validate_owner_settings(configuration)
