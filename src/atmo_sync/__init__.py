"""AtmoSync package."""

__all__ = ["SensorSimulator", "load_simulator_config", "validate_simulator_config"]


def __getattr__(name):
    if name in __all__:
        from .simulator import SensorSimulator, load_simulator_config, validate_simulator_config

        return {
            "SensorSimulator": SensorSimulator,
            "load_simulator_config": load_simulator_config,
            "validate_simulator_config": validate_simulator_config,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
