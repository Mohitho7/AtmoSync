"""Tests for the DE-03 synthetic sensor simulator."""

from __future__ import annotations

import copy
import io
import json
import math
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from importlib import resources
from pathlib import Path
from unittest.mock import patch

from atmo_sync.contracts import validate_sensor_event
from atmo_sync.simulator import (
    DEFAULT_CONFIG_PATH,
    SensorSimulator,
    load_simulator_config,
    main,
    validate_simulator_config,
)


EXPECTED_EVENT_FIELDS = {
    "event_id",
    "schema_version",
    "event_timestamp",
    "shipment_id",
    "container_id",
    "sensor_id",
    "commodity",
    "location",
    "readings",
}
EXPECTED_LOCATION_FIELDS = {"latitude", "longitude"}
EXPECTED_READING_FIELDS = {"temperature_c", "humidity_pct", "vibration_g"}


def read_config() -> dict:
    with DEFAULT_CONFIG_PATH.open(encoding="utf-8") as config_file:
        return json.load(config_file)


class SensorSimulatorTests(unittest.TestCase):
    def test_events_have_exact_contract_structure(self):
        events = SensorSimulator(DEFAULT_CONFIG_PATH).generate_events(1)
        self.assertEqual(len(events), 3)
        for event in events:
            self.assertEqual(set(event), EXPECTED_EVENT_FIELDS)
            self.assertEqual(set(event["location"]), EXPECTED_LOCATION_FIELDS)
            self.assertEqual(set(event["readings"]), EXPECTED_READING_FIELDS)
            validate_sensor_event(event)

    def test_generates_events_for_multiple_containers(self):
        events = SensorSimulator(DEFAULT_CONFIG_PATH).generate_events(1)
        self.assertEqual(
            {event["container_id"] for event in events},
            {"CONT-001", "CONT-002", "CONT-003"},
        )

    def test_event_count_is_steps_times_containers(self):
        simulator = SensorSimulator(DEFAULT_CONFIG_PATH)
        self.assertEqual(len(simulator.generate_events(steps=4)), 12)

    def test_event_ids_are_unique_canonical_uuids(self):
        import uuid

        events = SensorSimulator(DEFAULT_CONFIG_PATH).generate_events(steps=4)
        event_ids = [event["event_id"] for event in events]
        self.assertEqual(len(event_ids), len(set(event_ids)))
        for event_id in event_ids:
            self.assertEqual(str(uuid.UUID(event_id)), event_id)

    def test_timestamps_are_utc_and_advance_by_configured_interval(self):
        events = SensorSimulator(DEFAULT_CONFIG_PATH).generate_events(steps=2)
        self.assertTrue(all(event["event_timestamp"].endswith("Z") for event in events))
        timestamps = {
            event["event_timestamp"]
            for event in events
        }
        self.assertEqual(
            timestamps,
            {"2026-11-10T06:05:00Z", "2026-11-10T06:10:00Z"},
        )
        for event in events:
            self.assertEqual(datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00")).utcoffset().total_seconds(), 0)

    def test_readings_are_finite_and_within_contract_bounds(self):
        events = SensorSimulator(DEFAULT_CONFIG_PATH).generate_events(steps=20)
        for event in events:
            readings = event["readings"]
            for value in readings.values():
                self.assertTrue(math.isfinite(value))
            self.assertGreaterEqual(readings["humidity_pct"], 0)
            self.assertLessEqual(readings["humidity_pct"], 100)
            self.assertGreaterEqual(readings["vibration_g"], 0)
            validate_sensor_event(event)

    def test_coordinates_are_within_contract_bounds(self):
        events = SensorSimulator(DEFAULT_CONFIG_PATH).generate_events(steps=1)
        for event in events:
            self.assertGreaterEqual(event["location"]["latitude"], -90)
            self.assertLessEqual(event["location"]["latitude"], 90)
            self.assertGreaterEqual(event["location"]["longitude"], -180)
            self.assertLessEqual(event["location"]["longitude"], 180)

    def test_same_seed_reproduces_measurements(self):
        first = SensorSimulator(DEFAULT_CONFIG_PATH, seed=753).generate_events(steps=3)
        second = SensorSimulator(DEFAULT_CONFIG_PATH, seed=753).generate_events(steps=3)
        self.assertEqual(
            [event["readings"] for event in first],
            [event["readings"] for event in second],
        )

    def test_different_seeds_produce_different_measurements(self):
        first = SensorSimulator(DEFAULT_CONFIG_PATH, seed=753).generate_events(steps=1)
        second = SensorSimulator(DEFAULT_CONFIG_PATH, seed=754).generate_events(steps=1)
        self.assertNotEqual(
            [event["readings"] for event in first],
            [event["readings"] for event in second],
        )

    def test_container_random_state_is_independent_of_other_containers(self):
        config = read_config()
        target_config = copy.deepcopy(config)
        target_config["containers"] = [
            container
            for container in target_config["containers"]
            if container["container_id"] == "CONT-002"
        ]
        all_events = SensorSimulator(config, seed=11).generate_events(steps=4)
        target_events = SensorSimulator(target_config, seed=11).generate_events(steps=4)
        self.assertEqual(
            [
                event["readings"]
                for event in all_events
                if event["container_id"] == "CONT-002"
            ],
            [event["readings"] for event in target_events],
        )

    def test_invalid_or_malformed_configuration_is_rejected(self):
        config = read_config()
        missing_version = copy.deepcopy(config)
        del missing_version["schema_version"]
        with self.assertRaisesRegex(ValueError, "schema_version"):
            validate_simulator_config(missing_version)

        unsupported = copy.deepcopy(config)
        unsupported["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "unsupported fields"):
            validate_simulator_config(unsupported)

        invalid_start = copy.deepcopy(config)
        invalid_start["simulation_start_utc"] = "2026-11-10T06:00:00-00:00"
        with self.assertRaisesRegex(ValueError, "UTC timezone"):
            validate_simulator_config(invalid_start)

    def test_duplicate_container_and_sensor_ids_are_rejected(self):
        for field in ("container_id", "sensor_id"):
            config = read_config()
            config["containers"][1][field] = config["containers"][0][field]
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "must be unique"):
                    validate_simulator_config(config)

    def test_invalid_numeric_configuration_values_are_rejected(self):
        invalid_cases = (
            ("initial_temperature_c", float("nan")),
            ("initial_humidity_pct", 100.1),
            ("initial_vibration_g", -0.01),
            ("temperature_variation_c", float("inf")),
            ("humidity_variation_pct", -1),
            ("vibration_drift_g", True),
        )
        for field, invalid_value in invalid_cases:
            config = read_config()
            config["containers"][0][field] = invalid_value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    validate_simulator_config(config)

        for field, invalid_value in (("latitude", 91), ("longitude", -181)):
            config = read_config()
            config["containers"][0]["location"][field] = invalid_value
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "between"):
                    validate_simulator_config(config)

    def test_finite_generation_does_not_wait(self):
        simulator = SensorSimulator(DEFAULT_CONFIG_PATH)
        with patch("atmo_sync.simulator.time.sleep") as sleep:
            events = simulator.generate_events(steps=2)
        sleep.assert_not_called()
        self.assertEqual(len(events), 6)

    def test_jsonl_creates_parent_directories_and_valid_records(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_path = Path(temporary_directory) / "nested" / "events.jsonl"
            main(["--steps", "2", "--output", str(output_path)])
            records = [
                json.loads(line)
                for line in output_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(records), 6)
            for record in records:
                validate_sensor_event(record)

    def test_jsonl_append_and_explicit_overwrite_behavior(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_path = Path(temporary_directory) / "events.jsonl"
            main(["--steps", "1", "--output", str(output_path)])
            initial_ids = [
                json.loads(line)["event_id"]
                for line in output_path.read_text(encoding="utf-8").splitlines()
            ]
            original_contents = output_path.read_bytes()
            with self.assertRaises(SystemExit):
                main(["--steps", "1", "--output", str(output_path)])
            self.assertEqual(output_path.read_bytes(), original_contents)

            main(["--steps", "1", "--output", str(output_path), "--append"])
            appended_ids = [
                json.loads(line)["event_id"]
                for line in output_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(appended_ids), 6)
            self.assertEqual(appended_ids[:3], initial_ids)

            main(["--steps", "1", "--output", str(output_path), "--overwrite"])
            overwritten = output_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(overwritten), 3)
            self.assertFalse(
                set(initial_ids)
                & {json.loads(line)["event_id"] for line in overwritten}
            )

    def test_jsonl_append_separates_existing_record_without_final_newline(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_path = Path(temporary_directory) / "events.jsonl"
            original_event = SensorSimulator(DEFAULT_CONFIG_PATH).generate_events(1)[0]
            original_record = json.dumps(original_event, separators=(",", ":"))
            output_path.write_text(original_record, encoding="utf-8")

            main(["--steps", "1", "--output", str(output_path), "--append"])

            records = output_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(records), 4)
            self.assertEqual(json.loads(records[0]), original_event)
            for record in records:
                validate_sensor_event(json.loads(record))

    def test_jsonl_default_creation_uses_exclusive_file_creation(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_path = Path(temporary_directory) / "events.jsonl"

            original_open = Path.open
            opened_modes = []

            def track_open(path, mode="r", *args, **kwargs):
                opened_modes.append(mode)
                return original_open(path, mode, *args, **kwargs)

            with patch.object(Path, "open", new=track_open):
                main(["--steps", "1", "--output", str(output_path)])
            self.assertTrue(output_path.is_file())
            self.assertIn("xb", opened_modes)

    def test_cli_rejects_invalid_and_incompatible_arguments(self):
        invalid_arguments = (
            ["--steps", "-1"],
            ["--continuous", "--steps", "2"],
            ["--append"],
            ["--append", "--overwrite", "--output", "unused.jsonl"],
        )
        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments):
                with self.assertRaises(SystemExit) as result:
                    main(arguments)
                self.assertEqual(result.exception.code, 2)

    def test_continuous_mode_stops_gracefully_on_keyboard_interrupt(self):
        output = io.StringIO()
        with (
            patch("atmo_sync.simulator.time.sleep", side_effect=KeyboardInterrupt),
            redirect_stdout(output),
        ):
            main(["--continuous"])
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 3)
        for line in lines:
            validate_sensor_event(json.loads(line))

    def test_each_emitted_event_is_checked_by_de02_validator(self):
        from atmo_sync import simulator as simulator_module

        original_validator = simulator_module.validate_sensor_event
        with patch(
            "atmo_sync.simulator.validate_sensor_event",
            wraps=original_validator,
        ) as validator:
            events = SensorSimulator(DEFAULT_CONFIG_PATH).generate_events(steps=2)
        self.assertEqual(validator.call_count, len(events))

    def test_null_drift_is_a_field_specific_validation_error(self):
        config = read_config()
        container = config["containers"][0]
        container.pop("temperature_drift_c")
        container.pop("humidity_drift_pct")
        container.pop("vibration_drift_g")
        container["drift"] = None
        with self.assertRaisesRegex(ValueError, "CONT-001 drift must be a JSON object"):
            validate_simulator_config(config)

    def test_cli_reports_null_drift_without_traceback(self):
        config = read_config()
        container = config["containers"][0]
        for field in ("temperature_drift_c", "humidity_drift_pct", "vibration_drift_g"):
            container.pop(field)
        container["drift"] = None
        with tempfile.TemporaryDirectory() as temporary_directory:
            config_path = Path(temporary_directory) / "invalid-config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr), self.assertRaises(SystemExit) as result:
                main(["--config", str(config_path), "--steps", "1"])
        self.assertEqual(result.exception.code, 2)
        self.assertIn("CONT-001 drift must be a JSON object", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_default_configuration_is_available_as_a_package_resource(self):
        resource = resources.files("atmo_sync").joinpath("simulator_config.json")
        self.assertTrue(resource.is_file())
        bundled = load_simulator_config()
        with DEFAULT_CONFIG_PATH.open(encoding="utf-8") as config_file:
            source_copy = validate_simulator_config(json.load(config_file))
        self.assertEqual(bundled, source_copy)

    def test_cli_uses_bundled_configuration_without_config_argument(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            main(["--steps", "1"])
        events = json.loads(stdout.getvalue())
        self.assertEqual(len(events), 3)
        for event in events:
            validate_sensor_event(event)

    def test_configured_five_minute_interval_and_seed_override(self):
        config = validate_simulator_config(read_config())
        self.assertEqual(config["sensor_interval_minutes"], 5)
        config["random_seed"] = 1
        configured = SensorSimulator(config).generate_events(steps=1)
        overridden = SensorSimulator(config, seed=2).generate_events(steps=1)
        self.assertNotEqual(
            [event["readings"] for event in configured],
            [event["readings"] for event in overridden],
        )

    def test_invalid_step_values_are_rejected(self):
        simulator = SensorSimulator(DEFAULT_CONFIG_PATH)
        for steps in (-1, True, 1.5):
            with self.subTest(steps=steps):
                with self.assertRaisesRegex(ValueError, "steps must"):
                    simulator.generate_events(steps=steps)


if __name__ == "__main__":
    unittest.main()
