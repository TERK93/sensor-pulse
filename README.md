# sensor-pulse

End-to-end industrial sensor data pipeline built with PySpark and Spark SQL, following the Medallion Architecture (Bronze → Silver → Gold).

The pipeline processes NASA CMAPSS turbofan engine sensor data — 21 sensors per engine cycle measuring temperature, pressure, and rotation speed. This mirrors the type of industrial IoT data found in manufacturing and process industries.

---

## Architecture

```
Raw CSV
   │
   ▼
Bronze  ──  Raw ingestion, append-only, load timestamp added
   │
   ▼
Silver  ──  Validation, status flags, cycle_pct derived column
   │
   ▼
Gold    ──  4 analytics tables + ML feature engineering
```

Each layer is written as **Parquet** — the columnar format used in modern data platforms like Microsoft Fabric and Databricks.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| PySpark | Distributed data transformations |
| Spark SQL | Window functions and analytics queries |
| Parquet | Columnar storage format |
| NASA CMAPSS | Industrial sensor dataset (via Kaggle) |
| Google Colab | Notebook environment |

---

## Pipeline Details

### Bronze — Raw Ingestion
- Reads space-separated CSV with explicit schema (avoids type inference errors)
- Adds `load_timestamp` for data lineage
- Written as Parquet, append-only — data is never modified after insert

### Silver — Validation & Cleaning
- Null check on engine_id, cycle, and key sensors
- Sensor range check — physical sensors cannot return zero or negative values
- Invalid rows are **kept with status flags**, not deleted — lineage is preserved
- Derived columns: `max_cycle`, `cycle_pct` (how far through engine lifetime)
- Gold filters on `WHERE status = 'valid'`

### Gold — Analytics & ML Features
Four tables built with Spark SQL window functions:

| Table | Description |
|-------|-------------|
| `rolling_averages` | 10- and 30-cycle rolling averages per engine — reveals degradation trends |
| `engine_health` | Per-engine summary: total cycles, avg sensor readings, life category |
| `ml_features` | Feature-engineered table for ML: rolling mean, stddev, delta (rate of change), RUL as target variable |
| `sensor_drift` | Early life vs late life sensor comparison — quantifies degradation |

---

## ML Feature Engineering

The `gold_ml_features` table prepares data for a Remaining Useful Life (RUL) prediction model:

- **s02_mean_10** — rolling mean over 10 cycles (signal stability)
- **s02_std_10** — rolling stddev over 10 cycles (instability indicator)
- **s02_delta / s04_delta** — rate of change via LAG (trending up or down?)
- **rul** — target variable: `max_cycle - cycle`

This table is the direct input for anomaly detection and baseline ML models.

---

## Dataset

**NASA CMAPSS Turbofan Engine Degradation Simulation**
- 100 engines, up to 362 cycles per engine
- 21 sensor readings per cycle
- 20,631 rows total, 100% valid after silver validation
- Source: [Kaggle — behrad3d/nasa-cmaps](https://www.kaggle.com/datasets/behrad3d/nasa-cmaps)

---

## Key Design Decisions

**Explicit schema over inference**
Defining the schema manually avoids silent type errors that can propagate through the pipeline undetected.

**Status flags over deletion**
Invalid rows are flagged (`invalid_null`, `invalid_sensor_range`) and retained in Silver. Data lineage is preserved — Gold simply filters on `status = 'valid'`.

**Parquet over CSV**
Columnar format enables predicate pushdown and compression. Same data is ~10x smaller than CSV and significantly faster to query at scale.

**Spark SQL for Gold**
Gold analytics use `createOrReplaceTempView` + Spark SQL — the same pattern used in Microsoft Fabric notebooks and Databricks. Familiar to anyone working in modern data platforms.

---

## How to Run

Open `sensor_pulse_pipeline.ipynb` in Google Colab and run all cells.
No local setup required — PySpark and the dataset are installed automatically.

```python
# All dependencies installed in first cell
!pip install pyspark kagglehub -q
```

---

## Relation to Other Projects

This project focuses on **PySpark and distributed data processing**.
For a SQL-native pipeline using PostgreSQL and pandas, see
[finance-data-pipeline](https://github.com/TERK93/finance-data-pipeline) —
same Medallion Architecture, different execution engine and domain.

---

## Possible Extensions

- **Anomaly detection** — rolling z-score to flag abnormal sensor readings
- **Baseline RUL model** — linear regression or random forest on `gold_ml_features`
- **dbt models** — move Gold transformations into dbt with schema tests
- **Orchestration** — schedule pipeline runs with Apache Airflow# sensor-pulse
Industrial sensor data pipeline built with PySpark and Spark SQL — Medallion Architecture (Bronze → Silver → Gold) on NASA CMAPSS dataset
