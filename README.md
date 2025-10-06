# Wind Turbine Pipeline (Databricks Serverless)

This repo contains a **bronze → silver → gold** Delta pipeline for wind turbine telemetry. It is designed for **Databricks serverless** (Jobs or SQL warehouse) and comes with notebooks, a Databricks Workflow spec, tests, and sample data.

**Core tasks** (from the challenge):
- Ingest raw turbine CSVs to a bronze Delta table
- Clean & validate data (missing values, outliers) → silver
- Compute per-turbine summary statistics (min / max / avg over period) → gold
- Detect anomalies (|z| ≥ 2 from mean over the period) → gold
- Store processed data in Unity Catalog Delta tables

> The repo also includes a local test harness (pytest + pandas) so you can run logic outside Spark while still deploying to Databricks for scale.

---

## Quickstart (Local)

1. Create a venv and install deps:
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Explore sample data:
   ```bash
   pytest -q
   ```

3. Inspect notebooks in `notebooks/` for the Spark/Delta implementation used on Databricks.

---

## Databricks Setup (Serverless)

### 1) Repo import
- Push this repo to GitHub, then in Databricks use **Repos** → **Add Repo** (GitHub URL).
- Or directly import the zip into Repos if preferred.

### 2) Unity Catalog objects
Pick (or create) a catalog and schema (e.g., `main.turbine`). The pipeline uses these tables by default (override via `config/pipeline_config.yaml`):

- `main.turbine.bronze_readings`
- `main.turbine.silver_readings`
- `main.turbine.gold_turbine_stats`
- `main.turbine.gold_turbine_anomalies`

Create the catalog & schema if needed:
```sql
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.turbine;
```

### 3) Storage
Point `raw_data_path` in `config/pipeline_config.yaml` to your cloud storage (e.g., `s3://...`, `abfss://...`, or volume path). For demo, the sample CSVs are included under `data/sample`.

### 4) Serverless compute
- Use **Serverless Jobs Compute** (recommended) or a **Serverless SQL Warehouse** for SQL-only tasks.
- The provided `workflows/turbine_pipeline_workflow.json` defines a Databricks Job with a serverless job cluster and three notebook tasks (bronze → silver → gold).

### 5) Run the workflow
- Import `workflows/turbine_pipeline_workflow.json` into **Workflows** → **Create Job** → **Import**.
- Update the `repo` paths (if different) and the `raw_data_path`/tables in `config/pipeline_config.yaml` or via job parameters.
- Click **Run**.

---

## Configuration

Edit `config/pipeline_config.yaml`:
```yaml
catalog: main
schema: turbine
raw_data_path: /Workspace/Repos/<your-user>/wind-turbine-pipeline/data/sample
bronze_table: bronze_readings
silver_table: silver_readings
gold_stats_table: gold_turbine_stats
gold_anomalies_table: gold_turbine_anomalies
expected_turbines_per_group: 5
anomaly_z_threshold: 2.0
```

You can also pass any of these as notebook widgets (Databricks) to override per-run.

---

## Data assumptions

- CSVs contain columns: `timestamp` (ISO8601), `turbine_id` (int/str), `wind_speed` (m/s), `wind_direction` (deg), `power_mw` (float).
- Files are appended daily; each file consistently maps to the same group of turbines.
- Cleaning rules (see code):
  - Drop rows without `turbine_id` or `timestamp`
  - Impute missing numeric columns via group-wise median (per turbine) then global median fallback
  - Remove extreme outliers via IQR (Tukey) in silver (soft-clip) and z-score thresholding in gold-anomaly stage

---

## Tests

`pytest` tests validate the pandas reference implementations mirror the Spark logic (schemas, NA handling, stats, anomaly flags).

---

## Repo layout

See tree below. Notebooks call into `src/` modules so logic stays DRY and testable.

```
wind-turbine-pipeline/
├── README.md
├── requirements.txt
├── config/
│   └── pipeline_config.yaml
├── src/
│   ├── __init__.py
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── ingest_raw_data.py
│   ├── silver/
│   │   ├── __init__.py
│   │   └── clean_and_validate.py
│   └── utils/
│       ├── __init__.py
│       ├── data_quality.py
│       └── schema_definitions.py
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_aggregation.py
├── workflows/
│   └── turbine_pipeline_workflow.json
├── tests/
│   ├── __init__.py
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
└── data/
    └── sample/
        ├── data_group_1.csv
        ├── data_group_2.csv
        └── data_group_3.csv
```
<img width="1582" height="448" alt="image" src="https://github.com/user-attachments/assets/3e2c3187-4fc6-44a3-bd99-343c30a48924" />
