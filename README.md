# Ubitricity-Ev-Pipeline
Ubitricity EV Charging Network — Data Engineering Pipeline
A production-grade lakehouse pipeline built to simulate the data platform behind a UK EV charging network. Ingests global EV charging station data, applies medallion architecture transformations using PySpark on Databricks, and serves business-ready models via dbt.

Architecture
Raw CSV (Kaggle)
      |
      v
  [Bronze Layer]  — PySpark — Raw ingestion with metadata columns
      |
      v
  [Silver Layer]  — PySpark — Type casting, null handling, deduplication, UK filter
      |
      v
  [Gold Layer]    — PySpark — Aggregated business models written to Delta
      |
      v
  [dbt Models]    — dbt on Databricks — Tested, documented, production-ready marts
All layers persist to Delta Lake on Databricks FileStore. dbt connects to Databricks via the dbt-databricks adapter and runs transformations on top of the Gold Delta tables.

Project Structure
ubitricity-ev-pipeline/
├── README.md
├── .gitignore
│
├── notebooks/
│   ├── 01_bronze_ingestion.py      # Raw CSV -> Bronze Delta
│   ├── 02_silver_cleaning.py       # Bronze -> Silver Delta (cleaned)
│   └── 03_gold_models.py           # Silver -> Gold Delta (aggregated)
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example        # Template — do not store real credentials
│   ├── packages.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_ev_stations.sql
│   │   │   └── stg_ev_stations.yml
│   │   └── marts/
│   │       ├── mart_network_health.sql
│   │       ├── mart_competitor_analysis.sql
│   │       ├── mart_connector_distribution.sql
│   │       ├── mart_coverage_gap.sql
│   │       └── marts.yml
│   │
│   └── tests/
│       └── generic/
│
├── data/
│   └── sample/
│       └── ev_stations_sample.csv  # 100-row sample for reference only
│
└── images/
    └── architecture.png

Tech Stack
LayerToolComputeDatabricks (Serverless)ProcessingPySparkStorageDelta LakeTransformationdbt (dbt-databricks adapter)Source DataKaggle — Global EV Charging Stations DatasetLanguagePython, SQLVersion ControlGit / GitHub

Medallion Layers
Bronze — Raw Ingestion

Reads raw CSV with no transformations
Appends metadata: ingested_at, source_file
Writes to Delta as-is — full historical record preserved
Fail-safe: try/except per file with clear error logging

Silver — Cleaned & Conformed

Casts lat, lon to double; num_connectors to integer
Standardises text columns with trim() and upper()
Fills nulls in categorical columns with "Unknown"
Drops rows with missing coordinates — unusable for geo analysis
Deduplicates exact row matches
Filters to United Kingdom only — Ubitricity's core market
Adds cleaned_at audit timestamp

Gold — Business Aggregations
Four aggregated models written as Delta tables, consumed by dbt:
ModelDescriptionmart_network_healthCharger operational vs faulted rate by regionmart_competitor_analysisUbitricity vs competitor charger counts by regionmart_connector_distributionConnector type breakdown (CCS, Type 2, etc.) by regionmart_coverage_gapUK towns ranked by charger density — flags underserved areas

dbt Models
Staging
stg_ev_stations — thin layer over Silver Delta. Renames columns, casts types, applies basic filters. Single source of truth for all downstream marts.
Marts
mart_network_health
Answers: Which UK regions have the highest charger fault rates?
sqlselect
    state,
    count(*)                                                        as total_chargers,
    sum(case when status = 'OPERATIONAL' then 1 end)               as operational,
    round(sum(case when status = 'FAULTED' then 1 end)
          / count(*) * 100, 2)                                      as fault_rate_pct
from {{ ref('stg_ev_stations') }}
group by state
mart_competitor_analysis
Answers: Where is Ubitricity vs competitors across UK regions?
mart_connector_distribution
Answers: Which connector types dominate and are Ubitricity's chargers compatible?
mart_coverage_gap
Answers: Which UK towns have the fewest active chargers per area — expansion opportunities?

dbt Tests
Every mart includes schema tests defined in marts.yml:
yamlmodels:
  - name: mart_network_health
    columns:
      - name: state
        tests:
          - not_null
          - unique
      - name: fault_rate_pct
        tests:
          - not_null
Run all tests with:
bashdbt test

Setup & Running
Prerequisites

Databricks workspace (Community Edition works)
Python 3.8+
dbt-databricks adapter

1. Get the Data
Download ev_stations_2025.csv from Kaggle:
Global EV Charging Stations Dataset
Upload to Databricks:
Databricks > Catalog > Create Table > Upload File
Path: /FileStore/raw/ev_stations_2025.csv
2. Run the PySpark Notebooks
Run in order inside Databricks:
notebooks/01_bronze_ingestion.py
notebooks/02_silver_cleaning.py
notebooks/03_gold_models.py
3. Install dbt
bashpip install dbt-databricks
4. Configure dbt Profile
Copy the example profile and fill in your Databricks credentials:
bashcp dbt/profiles.yml.example ~/.dbt/profiles.yml
yamlubitricity_ev_pipeline:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <your-databricks-host>
      http_path: <your-cluster-http-path>
      token: <your-personal-access-token>
      schema: gold
5. Run dbt
bashcd dbt
dbt deps
dbt run
dbt test
dbt docs generate
dbt docs serve

Key Business Insights This Pipeline Surfaces

Ubitricity operates lamppost chargers across the UK — this pipeline quantifies their footprint vs competitors like Pod Point, BP Pulse, and ChargePoint
Fault rate analysis by region identifies reliability problems in the network
Coverage gap model surfaces towns with high EV adoption but low charger density — direct input for expansion decisions
Connector type analysis shows whether Ubitricity's hardware is aligned with the dominant standards (CCS, Type 2)


What Is Not Included

Full CSV dataset (Kaggle licensing — download directly from the link above)
Databricks credentials or tokens
Personal API keys


Data Source
Global EV Charging Stations Dataset
Source: Kaggle — vivekattri/global-ev-charging-stations-dataset
Columns used: id, title, address, town, state, postcode, country, lat, lon, num_connectors, connector_types, operator, status
