# DE-03 — Synthetic Sensor Simulator

## Objective and scope

The simulator generates synthetic temperature, humidity, and vibration events
for configured shipping containers. Each event is checked with the existing
DE-02 `validate_sensor_event()` validator before it is returned or written.
This implementation produces synthetic data only: it does not connect to
physical sensors, infer spoilage, classify risk, or publish to Kafka.

## Architecture and data flow

1. The bundled `atmo_sync/simulator_config.json` resource supplies the
   interval, optional random seed, optional simulation start time, and
   container definitions. The repository copy at
   `data/simulator_config.json` is provided for inspection and customization.
2. `validate_simulator_config()` rejects invalid fields, identifiers,
   coordinates, readings, and variations before simulation starts.
3. `SensorSimulator` maintains reading state and a local random generator for
   each container.
4. Each interval advances the simulation clock and produces one event per
   configured container.
5. The event is validated by `validate_sensor_event()` and then returned,
   printed as JSON, or serialized as one JSONL record.

## Configuration

The default configuration is bundled with the package; the editable repository
copy is [../data/simulator_config.json](../data/simulator_config.json). Running
without `--config` loads the bundled resource, including after a regular wheel
installation. Pass `--config` to load a custom JSON file. The configuration
contains:

- `schema_version`: must be `"1.0"`.
- `sensor_interval_minutes`: positive integer; the example uses five minutes.
- `random_seed`: optional integer for reproducible measurements.
- `simulation_start_utc`: optional ISO 8601 UTC timestamp. If omitted, the
  current UTC time is used as the simulation clock's starting point.
- `containers`: non-empty list. Each container requires non-empty
  `shipment_id`, `container_id`, `sensor_id`, and `commodity`, a `location`
  with numeric `latitude` and `longitude`, initial readings, and non-negative
  variation parameters.

Each container's initial values use `initial_temperature_c`,
`initial_humidity_pct`, and `initial_vibration_g`. Its variation settings
`temperature_variation_c`, `humidity_variation_pct`, and
`vibration_variation_g` bound the random change per interval. Optional
`temperature_drift_c`, `humidity_drift_pct`, and `vibration_drift_g` add a
gradual per-interval drift. Drift values may be positive or negative.

Example:

```json
{
  "schema_version": "1.0",
  "sensor_interval_minutes": 5,
  "random_seed": 42,
  "simulation_start_utc": "2026-11-10T06:00:00Z",
  "containers": [
    {
      "shipment_id": "SHIP-001",
      "container_id": "CONT-001",
      "sensor_id": "SENSOR-001",
      "commodity": "mango",
      "location": { "latitude": 19.076, "longitude": 72.8777 },
      "initial_temperature_c": 12.4,
      "initial_humidity_pct": 86.5,
      "initial_vibration_g": 0.08,
      "temperature_variation_c": 0.6,
      "humidity_variation_pct": 3.5,
      "vibration_variation_g": 0.02,
      "temperature_drift_c": 0.1,
      "humidity_drift_pct": 0.2,
      "vibration_drift_g": 0.005
    }
  ]
}
```

Container IDs and sensor IDs must each be unique. Humidity must be between
zero and 100, vibration must be non-negative, and coordinates must be within
the geographic limits. Unknown configuration fields and invalid numeric
values are rejected.

## Sensor event schema

Each emitted event has exactly the DE-02 `sensor_event` structure:

```json
{
  "event_id": "unique canonical UUID",
  "schema_version": "1.0",
  "event_timestamp": "UTC ISO 8601 timestamp",
  "shipment_id": "SHIP-001",
  "container_id": "CONT-001",
  "sensor_id": "SENSOR-001",
  "commodity": "mango",
  "location": { "latitude": 19.076, "longitude": 72.8777 },
  "readings": {
    "temperature_c": 12.4,
    "humidity_pct": 86.5,
    "vibration_g": 0.08
  }
}
```

No producer-specific fields are added. Event IDs are new UUIDs for every
emission, including when a random seed is set.

## Synthetic data behavior

Measurements evolve from each container's own preceding state. At every
interval, the configured drift is combined with a bounded random change.
Humidity is kept in the inclusive 0–100 range and vibration is clamped at
zero; temperature remains a finite Celsius value. Events that fail DE-02
validation raise an error instead of being emitted.

Measurements are synthetic and are not actual IoT readings. The simulator
does not apply commodity-specific safety ranges, predict food spoilage, or
assign risk levels.

## Finite simulation

Finite runs advance the simulation clock without waiting in real time. If
`--steps` is omitted, one interval is generated.

```powershell
python -m atmo_sync.simulator --steps 5
```

Finite output without `--output` is a JSON array on standard output.

## Continuous simulation

Continuous mode waits for the configured interval between generated
intervals. Each event is written to standard output as one compact JSON
object per line. Stop it with Ctrl+C; already-written lines remain complete.

```powershell
python -m atmo_sync.simulator --continuous
```

## JSONL output

Use `--output` to write one event object per line. Missing parent directories
are created. If the file exists, the command refuses to proceed unless
`--append` or `--overwrite` is explicitly supplied.

```powershell
python -m atmo_sync.simulator --steps 3 --output data/generated/test_events.jsonl
python -m atmo_sync.simulator --steps 3 --output data/generated/test_events.jsonl --append
python -m atmo_sync.simulator --steps 3 --output data/generated/test_events.jsonl --overwrite
```

`--append` preserves existing records and adds new ones. `--overwrite`
replaces the file. `--append` and `--overwrite` are mutually exclusive and
require `--output`. Diagnostics are written to standard error, not mixed into
JSON or JSONL output. Files under `data/generated/` are excluded from Git.

## Random seed behavior

Set `random_seed` in the configuration or override it with `--seed`:

```powershell
python -m atmo_sync.simulator --steps 3 --seed 42
```

The seed makes measurement sequences reproducible for a given configuration.
Each container has an independent random stream. UUID event IDs are
intentionally not reproducible.

## Testing

Run the simulator and contract test suites with:

```powershell
python -m pytest -q
python -m unittest discover -s tests -v
```

The simulator tests cover configuration validation, event shape and
constraints, repeatable measurements, finite and continuous execution,
JSONL output modes, and use of the DE-02 validator.

## Limitations and DE-05 integration

This simulator has no Kafka or Snowflake dependency, persistence layer, or
physical sensor integration. Its values are illustrative synthetic telemetry,
not evidence of environmental conditions or product safety.

DE-05 can read each JSONL line as a UTF-8 JSON object, validate it with
`validate_sensor_event()`, and publish the validated object to a Kafka topic.
The Kafka producer should preserve `event_id` for downstream deduplication
and should define its own delivery, retry, and error-handling policy.
