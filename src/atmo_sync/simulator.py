from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib import resources
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Mapping

from .contracts import validate_sensor_event

DEFAULT_CONFIG_PATH = Path(__file__).with_name("simulator_config.json")


def _ensure_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    return value


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        result = float(value)
    except OverflowError as error:
        raise ValueError(f"{field_name} must be a finite number") from error
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be a finite number")
    return result


def _parse_utc_timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or "T" not in value:
        raise ValueError(f"{field_name} must be a UTC ISO 8601 timestamp")
    timestamp = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError as exc:  # pragma: no cover - defensive path
        raise ValueError(f"{field_name} must be a UTC ISO 8601 timestamp") from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != timedelta(0)
        or value.endswith("-00:00")
    ):
        raise ValueError(f"{field_name} must include a UTC timezone")
    return parsed.astimezone(timezone.utc)


def _format_utc_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_location(value: Any, container_id: str) -> tuple[float, float]:
    location = _ensure_mapping(value, f"container {container_id} location")
    if set(location) != {"latitude", "longitude"}:
        raise ValueError(
            f"container {container_id} location must contain only latitude and longitude"
        )
    latitude = _require_number(location.get("latitude"), f"container {container_id} latitude")
    longitude = _require_number(location.get("longitude"), f"container {container_id} longitude")
    if not -90 <= latitude <= 90:
        raise ValueError(f"container {container_id} latitude must be between -90 and 90")
    if not -180 <= longitude <= 180:
        raise ValueError(f"container {container_id} longitude must be between -180 and 180")
    return latitude, longitude


def _read_initials(container: Mapping[str, Any], container_id: str) -> tuple[float, float, float]:
    if "initial_readings" in container:
        readings = _ensure_mapping(
            container["initial_readings"], f"container {container_id} initial_readings"
        )
        if set(readings) != {"temperature_c", "humidity_pct", "vibration_g"}:
            raise ValueError(
                f"container {container_id} initial_readings must contain only "
                "temperature_c, humidity_pct, and vibration_g"
            )
        temperature = readings.get("temperature_c")
        humidity = readings.get("humidity_pct")
        vibration = readings.get("vibration_g")
    else:
        temperature = container.get("initial_temperature_c")
        humidity = container.get("initial_humidity_pct")
        vibration = container.get("initial_vibration_g")

    temperature_value = _require_number(temperature, f"container {container_id} initial_temperature_c")
    humidity_value = _require_number(humidity, f"container {container_id} initial_humidity_pct")
    vibration_value = _require_number(vibration, f"container {container_id} initial_vibration_g")

    if not 0 <= humidity_value <= 100:
        raise ValueError(f"container {container_id} initial_humidity_pct must be between 0 and 100")
    if vibration_value < 0:
        raise ValueError(f"container {container_id} initial_vibration_g must be non-negative")
    return temperature_value, humidity_value, vibration_value


def _read_variation(container: Mapping[str, Any], container_id: str) -> dict[str, float]:
    variation = container.get("variation")
    if "variation" not in container:
        variation = {}
    if not isinstance(variation, Mapping):
        raise ValueError(f"container {container_id} variation must be a JSON object")
    unknown_variation_fields = set(variation) - {
        "temperature_c",
        "humidity_pct",
        "vibration_g",
    }
    if unknown_variation_fields:
        raise ValueError(
            f"container {container_id} variation contains unsupported fields: "
            + ", ".join(sorted(unknown_variation_fields))
        )

    def read_value(metric_name: str, fallback_name: str, *, minimum: float = 0.0) -> float:
        value = variation.get(metric_name)
        if metric_name not in variation:
            value = container.get(fallback_name)
        if value is None and metric_name not in variation:
            raise ValueError(f"container {container_id} {metric_name} variation is required")
        parsed = _require_number(value, f"container {container_id} {metric_name} variation")
        if parsed < minimum:
            raise ValueError(f"container {container_id} {metric_name} variation must be >= {minimum}")
        return parsed

    drift = container.get("drift")
    if "drift" not in container:
        drift = {}
    if not isinstance(drift, Mapping):
        raise ValueError(f"container {container_id} drift must be a JSON object")
    unknown_drift_fields = set(drift) - {
        "temperature_c",
        "humidity_pct",
        "vibration_g",
    }
    if unknown_drift_fields:
        raise ValueError(
            f"container {container_id} drift contains unsupported fields: "
            + ", ".join(sorted(unknown_drift_fields))
        )

    def read_drift(metric_name: str, fallback_name: str) -> float:
        value = drift.get(metric_name) if isinstance(drift, Mapping) else None
        if isinstance(drift, Mapping) and metric_name in drift:
            return _require_number(
                value, f"container {container_id} {metric_name} drift"
            )
        if fallback_name in container:
            return _require_number(
                container[fallback_name],
                f"container {container_id} {metric_name} drift",
            )
        return 0.0

    return {
        "temperature_variation_c": read_value("temperature_c", "temperature_variation_c"),
        "humidity_variation_pct": read_value("humidity_pct", "humidity_variation_pct"),
        "vibration_variation_g": read_value("vibration_g", "vibration_variation_g"),
        "temperature_drift_c": read_drift("temperature_c", "temperature_drift_c"),
        "humidity_drift_pct": read_drift("humidity_pct", "humidity_drift_pct"),
        "vibration_drift_g": read_drift("vibration_g", "vibration_drift_g"),
    }


def validate_simulator_config(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("simulator configuration must be a JSON object")

    supported_fields = {
        "schema_version",
        "sensor_interval_minutes",
        "random_seed",
        "simulation_start_utc",
        "containers",
    }
    unknown_fields = set(value) - supported_fields
    if unknown_fields:
        raise ValueError(
            "simulator configuration contains unsupported fields: "
            + ", ".join(sorted(unknown_fields))
        )
    required_global_fields = ("schema_version", "sensor_interval_minutes")
    for field_name in required_global_fields:
        if field_name not in value:
            raise ValueError(f"simulator configuration is missing required field: {field_name}")

    schema_version = _require_text(value["schema_version"], "schema_version")
    if schema_version != "1.0":
        raise ValueError("schema_version must be '1.0'")
    sensor_interval_minutes = value["sensor_interval_minutes"]
    if isinstance(sensor_interval_minutes, bool) or not isinstance(sensor_interval_minutes, int):
        raise ValueError("sensor_interval_minutes must be an integer")
    if sensor_interval_minutes <= 0:
        raise ValueError("sensor_interval_minutes must be greater than zero")

    seed = value.get("random_seed")
    if seed is not None:
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise ValueError("random_seed must be an integer when provided")
    start_utc = value.get("simulation_start_utc", _format_utc_timestamp(datetime.now(timezone.utc)))
    _parse_utc_timestamp(start_utc, "simulation_start_utc")

    containers = value.get("containers")
    if not isinstance(containers, list) or not containers:
        raise ValueError("simulator configuration must include at least one container")

    normalized_containers: list[dict[str, Any]] = []
    seen_container_ids: set[str] = set()
    seen_sensor_ids: set[str] = set()

    for index, container in enumerate(containers):
        container_map = _ensure_mapping(container, f"container[{index}]")
        supported_container_fields = {
            "shipment_id",
            "container_id",
            "sensor_id",
            "commodity",
            "location",
            "initial_temperature_c",
            "initial_humidity_pct",
            "initial_vibration_g",
            "initial_readings",
            "variation",
            "temperature_variation_c",
            "humidity_variation_pct",
            "vibration_variation_g",
            "drift",
            "temperature_drift_c",
            "humidity_drift_pct",
            "vibration_drift_g",
        }
        unknown_container_fields = set(container_map) - supported_container_fields
        if unknown_container_fields:
            raise ValueError(
                f"container[{index}] contains unsupported fields: "
                + ", ".join(sorted(unknown_container_fields))
            )
        if "variation" in container_map and any(
            field in container_map
            for field in (
                "temperature_variation_c",
                "humidity_variation_pct",
                "vibration_variation_g",
            )
        ):
            raise ValueError(
                f"container[{index}] must use either variation or individual "
                "variation fields"
            )
        if "drift" in container_map and any(
            field in container_map
            for field in (
                "temperature_drift_c",
                "humidity_drift_pct",
                "vibration_drift_g",
            )
        ):
            raise ValueError(
                f"container[{index}] must use either drift or individual drift fields"
            )
        shipment_id = _require_text(container_map.get("shipment_id"), f"container[{index}].shipment_id")
        container_id = _require_text(container_map.get("container_id"), f"container[{index}].container_id")
        sensor_id = _require_text(container_map.get("sensor_id"), f"container[{index}].sensor_id")
        commodity = _require_text(container_map.get("commodity"), f"container[{index}].commodity")
        if container_id in seen_container_ids:
            raise ValueError(f"container_id must be unique: {container_id}")
        if sensor_id in seen_sensor_ids:
            raise ValueError(f"sensor_id must be unique: {sensor_id}")
        if not container_id.strip() or not sensor_id.strip():
            raise ValueError("container_id and sensor_id must be non-empty")
        seen_container_ids.add(container_id)
        seen_sensor_ids.add(sensor_id)

        if "initial_readings" in container_map and {
            "initial_temperature_c",
            "initial_humidity_pct",
            "initial_vibration_g",
        } & set(container_map):
            raise ValueError(
                f"container {container_id} must use either initial_readings or "
                "the individual initial_* fields, not both"
            )
        latitude, longitude = _normalize_location(container_map.get("location"), container_id)
        temperature, humidity, vibration = _read_initials(container_map, container_id)
        variation = _read_variation(container_map, container_id)
        normalized_containers.append(
            {
                "shipment_id": shipment_id,
                "container_id": container_id,
                "sensor_id": sensor_id,
                "commodity": commodity,
                "location": {"latitude": latitude, "longitude": longitude},
                "initial_temperature_c": temperature,
                "initial_humidity_pct": humidity,
                "initial_vibration_g": vibration,
                **variation,
            }
        )

    return {
        "schema_version": schema_version,
        "sensor_interval_minutes": sensor_interval_minutes,
        "random_seed": seed,
        "simulation_start_utc": start_utc,
        "containers": normalized_containers,
    }


def load_simulator_config(path: str | Path | None = None) -> dict[str, Any]:
    if path is None:
        with resources.files("atmo_sync").joinpath("simulator_config.json").open(
            "r", encoding="utf-8"
        ) as handle:
            payload = json.load(handle)
    else:
        with Path(path).open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    return validate_simulator_config(payload)


@dataclass
class ContainerState:
    shipment_id: str
    container_id: str
    sensor_id: str
    commodity: str
    latitude: float
    longitude: float
    temperature_c: float
    humidity_pct: float
    vibration_g: float
    temperature_variation_c: float
    humidity_variation_pct: float
    vibration_variation_g: float
    temperature_drift_c: float
    humidity_drift_pct: float
    vibration_drift_g: float

    def advance(self, rng: random.Random) -> tuple[float, float, float]:
        next_temperature = self.temperature_c + self.temperature_drift_c + rng.uniform(
            -self.temperature_variation_c, self.temperature_variation_c
        )
        next_humidity = self.humidity_pct + self.humidity_drift_pct + rng.uniform(
            -self.humidity_variation_pct, self.humidity_variation_pct
        )
        next_vibration = self.vibration_g + self.vibration_drift_g + rng.uniform(
            -self.vibration_variation_g, self.vibration_variation_g
        )

        next_humidity = max(0.0, min(100.0, next_humidity))
        next_vibration = max(0.0, next_vibration)

        self.temperature_c = next_temperature
        self.humidity_pct = next_humidity
        self.vibration_g = next_vibration
        return next_temperature, next_humidity, next_vibration


class SensorSimulator:
    def __init__(
        self,
        config: Mapping[str, Any] | str | Path | None = None,
        *,
        seed: int | None = None,
    ):
        if config is None:
            config_mapping = load_simulator_config()
        elif isinstance(config, (str, Path)):
            config_mapping = load_simulator_config(config)
        else:
            config_mapping = validate_simulator_config(config)
        self.config = config_mapping
        if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
            raise ValueError("seed must be an integer")
        self._seed = self.config.get("random_seed") if seed is None else seed
        self._rngs = {
            container["container_id"]: random.Random(
                None
                if self._seed is None
                else f"{self._seed}:{container['container_id']}"
            )
            for container in self.config["containers"]
        }
        self._current_timestamp = _parse_utc_timestamp(
            self.config.get("simulation_start_utc"), "simulation_start_utc"
        )
        self._containers = {
            container["container_id"]: ContainerState(
                shipment_id=container["shipment_id"],
                container_id=container["container_id"],
                sensor_id=container["sensor_id"],
                commodity=container["commodity"],
                latitude=container["location"]["latitude"],
                longitude=container["location"]["longitude"],
                temperature_c=container["initial_temperature_c"],
                humidity_pct=container["initial_humidity_pct"],
                vibration_g=container["initial_vibration_g"],
                temperature_variation_c=container["temperature_variation_c"],
                humidity_variation_pct=container["humidity_variation_pct"],
                vibration_variation_g=container["vibration_variation_g"],
                temperature_drift_c=container["temperature_drift_c"],
                humidity_drift_pct=container["humidity_drift_pct"],
                vibration_drift_g=container["vibration_drift_g"],
            )
            for container in self.config["containers"]
        }

    @classmethod
    def from_file(
        cls, path: str | Path | None = None, *, seed: int | None = None
    ) -> "SensorSimulator":
        return cls(load_simulator_config(path), seed=seed)

    def _next_timestamp(self) -> datetime:
        self._current_timestamp = self._current_timestamp + timedelta(
            minutes=self.config["sensor_interval_minutes"]
        )
        return self._current_timestamp

    def _build_event(self, container: ContainerState, timestamp: datetime) -> dict[str, Any]:
        temperature, humidity, vibration = container.advance(
            self._rngs[container.container_id]
        )
        event = {
            "event_id": str(uuid.uuid4()),
            "schema_version": self.config["schema_version"],
            "event_timestamp": _format_utc_timestamp(timestamp),
            "shipment_id": container.shipment_id,
            "container_id": container.container_id,
            "sensor_id": container.sensor_id,
            "commodity": container.commodity,
            "location": {"latitude": container.latitude, "longitude": container.longitude},
            "readings": {
                "temperature_c": round(temperature, 3),
                "humidity_pct": round(humidity, 3),
                "vibration_g": round(vibration, 3),
            },
        }
        validate_sensor_event(event)
        return event

    def generate_events(self, steps: int = 1) -> list[dict[str, Any]]:
        if isinstance(steps, bool) or not isinstance(steps, int) or steps < 0:
            raise ValueError("steps must be a non-negative integer")
        events: list[dict[str, Any]] = []
        for _ in range(steps):
            timestamp = self._next_timestamp()
            for container in self._containers.values():
                events.append(self._build_event(container, timestamp))
        return events

    def stream(self) -> Iterator[dict[str, Any]]:
        while True:
            timestamp = self._next_timestamp()
            for container in self._containers.values():
                yield self._build_event(container, timestamp)
            time.sleep(self.config["sensor_interval_minutes"] * 60)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic sensor readings for AtmoSync shipments.")
    parser.add_argument(
        "--config",
        help="Path to a simulator configuration JSON (default: bundled configuration).",
    )
    parser.add_argument("--steps", type=int, help="Number of simulation intervals to generate (finite mode).")
    parser.add_argument("--continuous", action="store_true", help="Continuously emit events until interrupted.")
    parser.add_argument("--seed", type=int, help="Override the configured random seed.")
    parser.add_argument("--output", type=Path, help="Write events as JSONL to this path.")
    output_mode = parser.add_mutually_exclusive_group()
    output_mode.add_argument("--append", action="store_true", help="Append JSONL records to an existing output file.")
    output_mode.add_argument("--overwrite", action="store_true", help="Replace an existing output file.")
    args = parser.parse_args(argv)
    if args.continuous and args.steps is not None:
        parser.error("--steps cannot be used with --continuous")
    if not args.continuous and args.steps is not None and args.steps < 0:
        parser.error("--steps must be non-negative")
    if (args.append or args.overwrite) and args.output is None:
        parser.error("--append and --overwrite require --output")
    return args


def _open_output(path: Path, *, append: bool, overwrite: bool) -> BinaryIO:
    if path.is_dir():
        raise ValueError(f"output path is a directory: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if overwrite:
        return path.open("wb")
    if append:
        output = path.open("a+b")
        output.seek(0, 2)
        if output.tell() > 0:
            output.seek(-1, 2)
            if output.read(1) != b"\n":
                output.seek(0, 2)
                output.write(b"\n")
                output.flush()
        return output
    try:
        return path.open("xb")
    except FileExistsError as error:
        raise ValueError(
            f"output file already exists: {path}; choose --append or --overwrite"
        ) from error


def _write_jsonl_event(event: Mapping[str, Any], output: BinaryIO) -> None:
    record = json.dumps(event, allow_nan=False, separators=(",", ":")) + "\n"
    output.write(record.encode("utf-8"))
    output.flush()


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    try:
        simulator = SensorSimulator(args.config, seed=args.seed)
        if args.output is None:
            if args.continuous:
                try:
                    for event in simulator.stream():
                        print(
                            json.dumps(event, allow_nan=False, separators=(",", ":")),
                            flush=True,
                        )
                except KeyboardInterrupt:
                    return
            else:
                results = simulator.generate_events(
                    steps=1 if args.steps is None else args.steps
                )
                print(json.dumps(results, allow_nan=False, indent=2))
            return

        if not args.continuous:
            events = simulator.generate_events(
                steps=1 if args.steps is None else args.steps
            )
            with _open_output(
                args.output, append=args.append, overwrite=args.overwrite
            ) as output:
                for event in events:
                    _write_jsonl_event(event, output)
            return

        with _open_output(
            args.output, append=args.append, overwrite=args.overwrite
        ) as output:
            try:
                for event in simulator.stream():
                    _write_jsonl_event(event, output)
            except KeyboardInterrupt:
                return
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()
