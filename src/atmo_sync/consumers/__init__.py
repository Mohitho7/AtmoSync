"""Independent Kafka consumer applications for the validated event stream."""

__all__ = [
    "AnalyticsAggregationConsumer",
    "OperationalMetricsConsumer",
    "ValidatedSensorConsumer",
]


def __getattr__(name):
    if name == "AnalyticsAggregationConsumer":
        from .analytics import AnalyticsAggregationConsumer

        return AnalyticsAggregationConsumer
    if name == "OperationalMetricsConsumer":
        from .monitoring import OperationalMetricsConsumer

        return OperationalMetricsConsumer
    if name == "ValidatedSensorConsumer":
        from .validated import ValidatedSensorConsumer

        return ValidatedSensorConsumer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
