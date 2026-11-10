# AtmoSync

## Project overview

AtmoSync aims to turn container sensor readings and shipment context into
timely, actionable insight for agricultural logistics. By bringing
environmental, shipment, market, and route information together, it is
designed to help operators recognize potential spoilage risks and make more
informed shipment decisions.

The project is designed around simulated IoT data and a planned streaming,
warehouse, transformation, and visualization stack. Technology names in this
README describe the intended architecture; they do not imply that external
services or integrations are currently operational.

## Problem statement

Temperature, humidity, and vibration conditions can change while agricultural
commodities are in transit. Without timely, connected shipment and market
information, operators may have limited visibility into potential quality
risks or the options available to respond.

AtmoSync aims to organize these signals and related data into an analytics
workflow that can support earlier risk awareness and better-informed
operational decisions. Predictions and analytics are intended as decision
support, not proof of physically confirmed spoilage or a substitute for
applicable food-safety requirements.

## What AtmoSync aims to do

- Simulate environmental readings from agricultural shipping containers.
- Collect readings with shipment, container, commodity, timestamp, and location
  context.
- Stream sensor events for downstream processing.
- Store and transform shipment data for analysis.
- Surface potential environmental and spoilage risks with clear uncertainty.
- Compare eligible shipment options using market, route, spoilage, and cost
  information when those data sources are available.
- Present analytical results through dashboards for operational monitoring.

These are intended project capabilities. The system does not claim that a
scientifically validated spoilage model, live market feed, route optimizer, or
food-safety decision engine is provided.

## How it works

The following diagram shows the intended high-level data flow. It is an
architecture direction, not a claim that the listed integrations are deployed.

```mermaid
flowchart TD
    S[Simulated container sensors] --> P[Python event generation and validation]
    P --> K[Apache Kafka event stream]
    K --> W[Snowflake data warehouse]
    W --> D[dbt transformations]
    D --> A[SQL analytics and risk indicators]
    M[Market and route data<br/>(when available)] --> A
    A --> B[Apache Superset dashboards]
    A --> O[Operational decision support]
```

In the intended workflow, Python produces structured sensor events. Kafka is
the planned event-streaming layer; Snowflake is the planned analytical
warehouse; dbt and SQL are intended to transform and analyze the data; and
Superset is the planned visualization layer. Market and route information
would be additional inputs when suitable sources and integrations are
available.

## Core data signals

AtmoSync is designed to work with:

| Signal | Description |
|---|---|
| Temperature | Container temperature readings in degrees Celsius. |
| Humidity | Relative humidity readings as a percentage. |
| Vibration | Container acceleration readings in g (standard acceleration due to gravity). |
| Container ID | Identifies the monitored shipping container. |
| Shipment ID | Connects readings and analysis to a shipment. |
| Commodity | Identifies the agricultural product being transported. |
| Timestamp | Records when a sensor event or analytical result applies. |
| Location | Latitude and longitude associated with the shipment or reading. |
| Market and route context | Potential offer, destination, travel, and cost information for future comparison workflows. |

## Technology stack

| Technology | Intended role |
|---|---|
| Python | Sensor simulation, event handling, and validation. |
| Apache Kafka | Real-time event streaming. |
| Snowflake | Cloud data warehouse. |
| dbt Core | Data transformation and modelling. |
| SQL | Analytical calculations and queries. |
| Apache Superset | Dashboarding and visualization. |
| Git and GitHub | Source control and collaboration. |

## Getting started

### Prerequisites

- Git
- Python 3.11 (recommended)
- A terminal: PowerShell or Command Prompt on Windows, Terminal on macOS/Linux

VS Code is an optional editor. Kafka, Snowflake, dbt, and Superset are not
required to create the local Python environment described below.

### Clone the repository

```shell
git clone https://github.com/Mohitho7/AtmoSync.git
cd AtmoSync
```

### Create and activate a virtual environment

#### Windows - PowerShell

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, allow scripts for the current process only,
then activate the environment:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\Activate.ps1
```

#### Windows - Command Prompt

```bat
py -3.11 -m venv .venv
.venv\Scripts\activate.bat
```

#### macOS and Linux

```shell
python3.11 -m venv .venv
source .venv/bin/activate
```

### Install Python dependencies

With the virtual environment activated, upgrade pip and install the
dependencies declared by the project:

```shell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The current requirements file does not install Kafka, Snowflake, dbt, or
Superset packages; those are not needed for this local setup.

## Repository

[AtmoSync repository](https://github.com/Mohitho7/AtmoSync)