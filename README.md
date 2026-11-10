# AtmoSync – Micro-Climate Arbitrage Analytics

AtmoSync is a data engineering and analytics project for monitoring environmental conditions inside agricultural shipping containers. It will use simulated IoT sensor events to identify potential spoilage risks and support data-informed shipment rerouting decisions.

## Problem statement

Temperature, humidity, and vibration changes during shipment can indicate that sensitive agricultural commodities are at risk. AtmoSync is intended to collect and analyze these signals alongside container, commodity, timestamp, and location data so that risks can be surfaced in time to act.

## Project objectives

- Simulate and ingest container sensor events.
- Stream events for timely processing.
- Transform and model shipment data for analytics.
- Identify potential spoilage risks and support rerouting decisions.
- Present operational insights through dashboards.

## Technology stack

- **Python** – sensor simulation and data ingestion
- **Apache Kafka** – real-time event streaming
- **Snowflake** – cloud data warehouse
- **dbt Core** – data transformation and modelling
- **SQL** – analytical calculations
- **Apache Superset** – dashboards and visualization

Only Python's standard library is needed for the current setup task. Kafka, Snowflake, dbt, and dashboard dependencies will be introduced in their relevant future tasks.

## Project structure

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

The local `.venv/` virtual environment is intentionally excluded from version control.

## Development phase

**Current task: DE-01 – Python Environment Setup.**

DE-01 establishes the project layout, Python virtual environment, dependency manifest, safe environment-variable template, documentation, and Git repository. The sensor schema and application logic have not been implemented yet.

## Development status

- **DE-01:** Current implementation task
- **DE-02 and later:** Not started
- No Kafka, Snowflake, dbt, or Superset services are configured yet.

## Local development

Run these commands from the project root in PowerShell:

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, allow scripts for the current process only, then activate:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\Activate.ps1
```

.
