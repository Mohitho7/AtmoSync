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
| DE-01 | Python environment setup | Completed locally |
| DE-02 | Define sensor data schema | Pending |
| DE-03 | Implement IoT sensor simulator | Pending |
| DE-04 | Implement sensor data validation | Pending |
| DE-05 | Build Kafka producer | Pending |

### Phase 2 — Streaming and Warehouse Integration

| ID | Task | Status |
|---|---|---|
| DE-06 | Kafka environment setup | Pending |
| DE-07 | Create Kafka topics | Pending |
| DE-08 | Implement Kafka consumer | Pending |
| DE-09 | Create Snowflake raw tables | Pending |
| DE-10 | Integrate Kafka with Snowflake | Pending |

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

**Current milestone:** DE-01 completed locally.

| Area | Current status |
|---|---|
| Python environment | Completed locally |
| Project structure | Created |
| Git initialization | Completed locally |
| GitHub branch synchronization | Pending |
| Sensor data schema | Not started |
| IoT simulator | Not started |
| Sensor validation | Not started |
| Kafka producer | Not started |
| Snowflake integration | Not started |

**Important:** DE-01 has passed local validation. The merge and push to the remote `MOHITH` branch are still pending. This README does not claim that the remote branch has been updated.

## 8. Upcoming Priorities

The immediate development sequence is:

1. Resolve the GitHub branch synchronization and publish DE-01.
2. Complete DE-02 by defining a consistent sensor event schema.
3. Implement DE-03, the Python IoT sensor simulator.
4. Complete DE-04 with input validation and error handling.
5. Implement DE-05 to publish simulated sensor events to Kafka.

The next major milestone is a working Python-based sensor event generation and ingestion pipeline.

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
- **Current task:** DE-01 — Python Environment Setup
- **Next task:** DE-02 — Sensor Data Schema

---

*This README tracks the implementation progress of the AtmoSync Data Engineering workstream in the `MOHITH` branch. Task statuses should be updated as work is implemented, tested, and committed.*