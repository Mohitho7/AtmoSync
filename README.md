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

### Phase 2 — Kafka Infrastructure and Producer Integration

| ID | Task | Status |
|---|---|---|
| DE-04A | Kafka infrastructure (Docker Compose/KRaft) | Completed; runtime verified |
| DE-04B | Python producer integration with the existing sensor simulator | Next |

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

**Latest completed milestone:** DE-04A Kafka infrastructure, runtime verified
on `MOHITH`.

**Next milestone:** DE-04B Python producer integration with the existing
sensor simulator.

| Area | Current status |
|---|---|
| DE-01 Python environment | Completed |
| DE-02 data contracts and validation | Completed |
| DE-03 sensor simulator | Completed |
| DE-04A Kafka infrastructure | Completed; Docker/Kafka runtime verified |
| Project structure | Created |
| Git initialization | Completed |
| GitHub branch synchronization | MOHITH branch synchronized through DE-03 |
| DE-04B producer integration | Not implemented; next milestone |

DE-04A verification confirmed Docker Engine and Compose, Kafka 3.9.1 broker
health, all four topic configurations, and a test message surviving a
container stop/start. See the [DE-04A verification record](docs/de-04-kafka-infrastructure.md#verification-record).

## 8. Upcoming Priorities

The next milestone is **DE-04B: Python producer integration with the existing
sensor simulator**.

Producer integration is not implemented yet. Transactional validation,
deduplication, retry/DLQ processing, Snowflake integration, dbt models, and
dashboards are also not implemented.

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
python -m pip install -r requirements.txt
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
- **Next milestone:** DE-04B — Python producer integration with the existing sensor simulator

---

*This README tracks the implementation progress of the AtmoSync Data Engineering workstream in the `MOHITH` branch. Task statuses should be updated as work is implemented, tested, and committed.*