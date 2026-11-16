# AtmoSync — Micro-Climate Arbitrage Analytics

## 1. Project Overview

AtmoSync is a data engineering and analytics project designed to monitor environmental conditions inside agricultural shipping containers.

The project uses simulated IoT sensor data, real-time streaming, data transformation, and analytics to identify potential spoilage risks and support informed shipment rerouting decisions.

### Problem Statement

Changes in temperature, humidity, and vibration during transportation can indicate that agricultural commodities are at risk of spoilage.

AtmoSync aims to collect, process, and analyze these environmental signals alongside shipment information to identify risks and provide actionable operational insights.

## 2. Project Objectives

- Simulate environmental sensor readings from agricultural shipping containers.
- Ingest and stream sensor events in real time.
- Store incoming sensor data in a cloud data warehouse.
- Clean, transform, and model data for analytical use.
- Identify potential spoilage risks and shipment anomalies.
- Generate insights that support shipment rerouting decisions.
- Build dashboards for monitoring shipment conditions and operational performance.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python | IoT sensor simulation and data ingestion |
| Apache Kafka | Real-time event streaming |
| Snowflake | Cloud data warehouse |
| dbt Core | Data transformation and modelling |
| SQL | Data analysis and analytical calculations |
| Apache Superset | Dashboard development and visualization |
| Git & GitHub | Version control and collaboration |

## 4. My Role — Data Engineering

**Contributor:** Mohith  
**Branch:** `MOHITH`  
**Workstream:** Data Engineering

My responsibility is to build the data ingestion pipeline, beginning with simulated IoT sensor events and progressing toward real-time streaming and warehouse integration.

### Assigned Responsibilities

- Set up the Python development environment.
- Define the sensor event data structure.
- Develop a Python-based IoT sensor simulator.
- Implement sensor data validation.
- Build a Kafka producer to publish sensor events.
- Coordinate Kafka integration with the data engineering team.
- Support the pipeline's integration with Snowflake.

## 5. Development Progress Tracker

### Phase 1 — Python Environment and Sensor Simulation

| ID | Task | Status |
|---|---|---|
| DE-01 | Python environment setup | Completed |
| DE-02 | Data contracts and validation | Completed |
| DE-03 | Implement IoT sensor simulator | Completed |

### Phase 2 — Kafka Infrastructure, Ingestion, and Processing

| ID | Task | Status |
|---|---|---|
| DE-04A | Kafka infrastructure (Docker Compose/KRaft) | Completed; runtime verified |
| DE-04B | Python producer integration with the existing sensor simulator | Completed; Kafka delivery verified |
| DE-04C | Transactional validation and event-ID deduplication | Completed; Kafka integration verified |
| DE-04D | Retry handling and Dead Letter Queue processing | Implemented and verified by unit and live Kafka tests |
| DE-04E | Downstream consumer groups and independent processing pipelines | Implemented and verified by unit tests and live Kafka integration |
| DE-04F | Reliability and performance testing | Local test and measurement coverage present; not production certification |
| DE-04G | Architecture documentation and final audit | Documentation present; remaining validation limitations recorded |

### Phase 3 — Data Transformation and Analytics

- Create staging models using dbt.
- Clean and standardize incoming sensor data.
- Build analytical models for shipment monitoring.
- Calculate relevant environmental and shipment metrics.
- Support spoilage-risk and shipment anomaly analysis.

### Phase 4 — Dashboard and Operational Insights

- Connect analytical datasets to Apache Superset.
- Develop shipment monitoring dashboards.
- Visualize temperature, humidity, and vibration trends.
- Present potential spoilage risks and actionable insights.
- Support shipment rerouting analysis.

*Phases 3 and 4 represent the broader project roadmap, not completed work in this branch.*

## 6. DE-01 — Python Environment Setup

<details>
<summary>Implementation details</summary>

The initial development environment has been configured successfully.

**Completed work:**

- [x] Verified Git installation.
- [x] Verified Python 3.11.9 installation.
- [x] Created the project directory structure.
- [x] Created a Python virtual environment using Python 3.11.9.
- [x] Activated the virtual environment in PowerShell.
- [x] Upgraded pip.
- [x] Created `requirements.txt`.
- [x] Created `.gitignore`.
- [x] Created `.env.example`.
- [x] Initialized the local Git repository.
- [x] Created the initial local commit.
- [x] Verified the Python package environment using `pip check`.

**Project structure:**

```text
AtmoSync/
├── src/
│   └── atmo_sync/
│       └── __init__.py
├── tests/
│   └── __init__.py
├── data/
├── docs/
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

**Current result:**

The local Python development environment is ready for the next implementation task.

The virtual environment is excluded from version control.

</details>

## 7. Current Development Status

**Latest completed implementation milestone:** DE-04E downstream consumer
groups, built on DE-04D retry/DLQ processing. DE-04F local reliability tests
and DE-04G documentation are present, with validation limitations explicitly
noted in the milestone reports.

| Area | Current status |
|---|---|
| DE-01 Python environment | Completed |
| DE-02 data contracts and validation | Completed |
| DE-03 sensor simulator | Completed |
| DE-04A Kafka infrastructure | Completed; Docker/Kafka runtime verified |
| DE-04B Python producer integration | Completed; unit and Kafka integration tests passed |
| DE-04C transactional validation and deduplication | Completed; unit and Kafka integration tests passed |
| DE-04D retry and DLQ handling | Implemented; unit and live Kafka integration coverage present |
| DE-04E downstream consumers and consumer groups | Implemented; unit and live Kafka integration coverage present |
| DE-04F performance and reliability testing | Local tests and actual small-workload measurements documented; incomplete as production-style validation |
| DE-04G architecture and verification documentation | Complete documentation set present; limitations and evidence recorded |
| Project structure | Created |
| Git initialization | Completed |
| GitHub branch synchronization | MOHITH branch synchronized through the active working tree |

The DE-04 documentation set includes infrastructure, producer, transactional
processor, retry/DLQ, downstream consumers, testing/performance, and final
audit reports. The audit does not claim production certification: the
broker is single-node, consumer side effects are process-local, and
performance/group-rebalance evidence has stated limits. See the [DE-04
documentation index](#de-04-documentation).

## 8. Upcoming Priorities

**Next priorities:** Expand multi-broker fault testing and sustained
benchmark methodology before making stronger reliability or
production-readiness claims.

Bounded retry handling and DLQ routing are implemented and tested. A
dedicated DLQ consumer, remediation, or replay application is not implemented.
Broader Snowflake, dbt, and dashboard work remains outside the scope of the
current code and documentation in this branch.

## 9. Local Development Setup

### Requirements

- Python 3.11
- Git
- Visual Studio Code (recommended)

### Setup Commands

```powershell
git clone -b MOHITH https://github.com/Mohitho7/AtmoSync.git
cd AtmoSync

py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

If PowerShell blocks virtual environment activation:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\Activate.ps1
```

## 10. Repository Information

- **Repository:** [AtmoSync](https://github.com/Mohitho7/AtmoSync)
- **Development branch:** `MOHITH`
- **Workstream:** Data Engineering
- **Latest implemented milestone:** DE-04E — downstream consumer groups

---

*This README tracks the implementation progress of the AtmoSync Data Engineering workstream in the `MOHITH` branch. Task statuses should be updated as work is implemented, tested, and committed.*

## DE-04 Documentation

- [DE-04A — Kafka infrastructure](docs/de-04-kafka-infrastructure.md)
- [DE-04B — Kafka producer](docs/de-04b-kafka-producer.md)
- [DE-04C — Transactional validation and deduplication](docs/de-04c-transactional-validation-deduplication.md)
- [DE-04D — Retry and DLQ](docs/de-04d-retry-and-dlq.md)
- [DE-04E — Independent consumers](docs/de-04e-independent-consumers.md)
- [DE-04F — Testing and performance](docs/de-04f-testing-and-performance.md)
- [DE-04G — Final architecture and audit](docs/de-04g-final-audit.md)
- [DE-04 reliability investigation](docs/de-04-reliability-investigation.md)
