# NYC Taxi Pipeline

A production-ready Databricks data pipeline for processing real NYC TLC Yellow Taxi trip data using the medallion architecture (Bronze → Silver → Gold). Built with Spark Declarative Pipelines (SDP) and deployed via Databricks Asset Bundles (DABs).

Source and layer are organized source-first (`src/pipeline/yellow_taxi/{bronze,silver}/`),
with `gold/` staying a separate, cross-source layer - the way a source-owning
team's bronze+silver would be co-located and co-deployed, while a shared
analytics/BI team owns gold across every source. Bronze and silver deploy as
one Lakeflow pipeline resource, `yellow_taxi_pipeline` (same source, same
owning team, same release cadence - splitting them into separate pipelines
would only add orchestration overhead with no real ownership boundary behind
it), while gold is its own separate pipeline resource, `nyc_taxi_gold_pipeline`
(different, cross-source consumers). Each has its own failure notifications.
`verified_trips`/`quarantined_trips` stay pipeline-local temporary views
(no extra storage) inside `yellow_taxi_pipeline` - since gold is a separate
pipeline and can't read another pipeline's views anyway, gold instead
re-applies the same "passed all DQX checks" filter itself
(`gold_transformations.filter_verified_trips`) directly on the one
materialized table both pipelines share, `silver_yellow_tripdata`, rather
than reading a duplicated copy. `orchestration_job` sequences both pipelines
with a quality gate in between
(`ingest -> yellow_taxi (bronze+silver) -> quality_gate -> gold`) for a
single end-to-end run, but each pipeline can also be deployed, run, and
alerted on independently - see
[Monitoring & Notifications](#-monitoring--notifications) for what the gate
does. Adding a second source (e.g. `green_taxi`) means adding
`src/pipeline/green_taxi/{bronze,silver}/` and a matching
`resources/green_taxi_pipeline.pipeline.yml`, without touching yellow_taxi's
or gold's.

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── src/
│   ├── ingest/
│   │   └── yellow_taxi_data_download.py  # Downloads missing monthly TLC files into the landing Volume
│   ├── monitoring/
│   │   └── quality_gate.py               # Fails the job if DQX quarantine rate is too high, blocking gold
│   ├── checks/
│   │   └── silver_yellow_tripdata_checks.yml  # DQX check definitions
│   ├── pipeline/
│   │   ├── yellow_taxi/               # Source-owned: bronze + silver, one deployable pipeline
│   │   │   ├── yellow_taxi_pipeline.py           # Entry point - imports bronze + silver layers
│   │   │   ├── bronze/
│   │   │   │   ├── yellow_taxi_bronze_layer.py   # Auto Loader ingestion from a Volume (DLT registration)
│   │   │   │   └── bronze_transformations.py     # Bronze transform logic (pure functions)
│   │   │   └── silver/
│   │   │       ├── yellow_taxi_silver_layer.py   # DQX checks (_errors/_warnings kept) (DLT registration)
│   │   │       └── silver_transformations.py     # Silver transform logic (pure functions)
│   │   └── gold/                      # Cross-source: aggregates every source's silver output
│   │       ├── nyc_taxi_gold_layer.py    # Business aggregations (DLT registration)
│   │       └── gold_transformations.py   # Gold aggregation logic (pure functions)
│   └── utils/
│       ├── calculations.py           # Metric calculations
│       ├── transformations.py        # Data transformations
│       └── spark_session.py          # Spark session utilities
├── tests/
│   ├── unit/                             # Pure function tests, one file per source module
│   │   ├── test_calculations.py
│   │   ├── test_transformations.py
│   │   ├── test_bronze_transformations.py
│   │   ├── test_silver_transformations.py
│   │   ├── test_gold_transformations.py
│   │   └── test_quality_gate.py
│   └── conftest.py                       # Pytest fixtures & test configuration
├── explorations/
│   └── mock_historical_timeline.py       # One-off script to backfill demo timestamps
├── resources/
│   ├── yellow_taxi_pipeline.pipeline.yml    # Bronze+silver DLT pipeline resource + failure alerts
│   ├── nyc_taxi_gold_pipeline.pipeline.yml  # Gold DLT pipeline resource + failure alerts (cross-source)
│   ├── orchestration_job.job.yml         # ingest -> yellow_taxi -> quality_gate -> gold job, notifications
│   ├── monitoring.yml                    # SQL alert on the data_quality_trend quarantine rate
│   └── test.job.yml                      # Job resource definition
├── .github/
│   └── workflows/
│       └── ci-cd.yml                     # CI/CD automation
├── databricks.yml                        # DABs configuration (3 targets)
├── requirements.txt                      # Python dependencies
├── pyproject.toml                        # Project metadata & tool configuration
└── README.md                             # This file
```

## 🚀 Quick Start

### Prerequisites

* Databricks workspace with Unity Catalog enabled
* Databricks CLI installed (`pip install databricks-cli`)
* Python 3.10 or higher
* A landing Volume for `src/ingest/yellow_taxi_data_download.py` to write into
  (see [Data Source](#-data-source) below) — you don't need to land files
  yourself, `orchestration_job` downloads whatever's missing before each run
* A SQL Warehouse ID (`warehouse_id` variable) if you want the data-quality
  alert in `resources/monitoring.yml` to deploy (see
  [Monitoring & Notifications](#-monitoring--notifications))

### Deploy Pipeline

```bash
# 1. Validate the bundle configuration
databricks bundle validate --target dev

# 2. Deploy to development environment
databricks bundle deploy --target dev

# 3a. Run the full orchestration job (ingest -> yellow_taxi -> quality_gate -> gold, in order)
databricks bundle run orchestration_job --target dev

# 3b. Or run one pipeline on its own (using tables already produced upstream)
databricks bundle run yellow_taxi_pipeline --target dev
databricks bundle run nyc_taxi_gold_pipeline --target dev
```

## 📥 Data Source

Real, public NYC TLC Yellow Taxi monthly files
(`yellow_tripdata_YYYY-MM.parquet`, e.g. from
`https://d37ci6vzurychx.cloudfront.net/trip-data/`) are expected to already be
landed in a Unity Catalog Volume at
`/Volumes/{catalog}/{landing_schema}/{landing_volume}/{year}/yellow_tripdata_{year}-{month}.parquet`
(configurable via the `landing_schema`/`landing_volume` bundle variables in
`databricks.yml`, default `landing` / `nyc-yellow-taxi-files`).

> ⚠️ If that Volume ever contains multiple years' worth of files, `yellow_taxi_bronze_layer.py`
> loads the Volume's root path recursively via Auto Loader — point
> `landing_volume`/`landing_schema` at a location scoped to what you actually
> want ingested, or you'll stream in every year on disk.

## 🎯 Environment Targets

The project supports three deployment targets configured in `databricks.yml`:

### Development (Default)
```yaml
Target: dev (default)
Catalog: biap_dev
Schemas: bronze, silver, gold
Mode: development
Pipeline Development Mode: true
```

### Staging
```yaml
Target: staging
Catalog: biap_staging
Schemas: bronze, silver, gold
Mode: production
Pipeline Development Mode: false
```

### Production
```yaml
Target: prod
Catalog: biap_prod
Schemas: bronze, silver, gold
Mode: production
Pipeline Development Mode: false
```

## 📊 Pipeline Architecture

### Bronze Layer
**Purpose:** Incremental ingestion of real NYC TLC Yellow Taxi files, no transformations

**Location:** `src/pipeline/yellow_taxi/bronze/`
- Auto Loader (`cloudFiles`) incrementally picks up new `yellow_tripdata_YYYY-MM.parquet`
  files from the landing Volume
- Adds an `_ingested_at` lineage timestamp (`trip_year`/`trip_month` are parsed
  from the file name in the silver layer, not here)
- Real TLC schema (`PULocationID`/`DOLocationID`, `VendorID`, etc.) - no transformations applied

**Output:** `{catalog}.bronze.bronze_yellow_tripdata`

### Silver Layer
**Purpose:** Data cleaning, feature engineering, and data quality enforcement

**Location:** `src/pipeline/yellow_taxi/silver/`

**Transformations:**
* Calculate trip duration (minutes)
* Calculate average speed (mph)
* Extract time features (hour, day of week)
* Apply DQX checks (`src/checks/silver_yellow_tripdata_checks.yml`), including
  business rules (fare/distance/duration > 0, dropoff after pickup) via
  `sql_expression` checks - every row keeps its `_errors`/`_warnings` result
  columns rather than being silently dropped or split into a separate table

**Utilities:**
- `calculations.py` - Metric calculation functions
- `transformations.py` - Data transformation functions

**Output:**
- `{catalog}.silver.silver_yellow_tripdata` - every transformed row, with
  `_errors`/`_warnings` columns from DQX (this is the "Flag Violations"
  pattern - see [Databricks: Data Quality Management](https://www.databricks.com/discover/pages/data-quality-management#data-quarantine) -
  rather than a physical valid/quarantine table split, so the DQX check pass
  runs once, not once per downstream table)
- `verified_trips` / `quarantined_trips` - `dp.temporary_view`s over
  `silver_yellow_tripdata` (no extra storage, only usable within
  `yellow_taxi_pipeline` itself), filtered to rows with no
  `_errors`/`_warnings` and at least one `_errors`/`_warnings` entry
  respectively. `nyc_taxi_gold_pipeline`, a separate pipeline, can't read a
  view from another pipeline, so it doesn't use these - it re-applies the
  same "verified" filter itself directly on `silver_yellow_tripdata` (see
  Gold Layer below) instead of reading a duplicated materialized copy.

### Gold Layer
**Purpose:** Business-ready aggregations for analytics and reporting

**Location:** `src/pipeline/gold/`

- **`monthly_trip_metrics`** - aggregated by `trip_year`/`trip_month`: rides, fare,
  distance, speed, tips. The timeline view for a quality/trend dashboard.
- **`pickup_zone_metrics`** - aggregated by `PULocationID`: ride demand by pickup zone.
- **`hourly_demand_heatmap`** - aggregated by pickup day-of-week x hour: a 7x24
  ride-demand grid with readable day names.
- **`data_quality_trend`** - bronze volume vs. silver valid/quarantine counts
  (derived from `_errors`/`_warnings` on `silver_yellow_tripdata`) by month,
  with a `quarantine_rate_pct`.

The first three tables above all read `silver_yellow_tripdata` directly and
filter it with `gold_transformations.filter_verified_trips` before
aggregating, rather than reading a separately materialized `verified_trips`
table - `silver_yellow_tripdata` is the only table both pipelines share, so
this avoids storing the same rows twice across pipelines.

**Utilities:**
- `gold_transformations.py` - `filter_verified_trips` plus the aggregation
  function for each table above

**Output:** `{catalog}.gold.monthly_trip_metrics`, `{catalog}.gold.pickup_zone_metrics`,
`{catalog}.gold.hourly_demand_heatmap`, `{catalog}.gold.data_quality_trend`

## 📥 Ingest

**Location:** `src/ingest/yellow_taxi_data_download.py`

Scans the landing Volume for `(year, month)` files already present, computes
every month missing between `START_YEAR_MONTH` and the current month, and
downloads each one from the public TLC CloudFront endpoint with a randomized
delay between requests. A month with no file published yet (HTTP 403/404) is
treated as an expected soft skip and logged, not a failure - but a real error
(timeout, unexpected HTTP status, network exception) causes `main()` to raise,
so the job task fails loudly and the retry/notification config below actually
triggers instead of silently completing.

Run standalone via `resources/orchestration_job.job.yml`'s `ingest_task`, or
as the first task of `orchestration_job`, ahead of `yellow_taxi_pipeline` /
`nyc_taxi_gold_pipeline`.

## 🔔 Monitoring & Notifications

* **`data_quality_trend`** (gold table, see above) is the monitoring source of
  truth: bronze ingestion volume vs. silver valid/quarantine counts and a
  `quarantine_rate_pct`, by `trip_year`/`trip_month`. It's an after-the-fact
  view - the pipeline doesn't wait for gold to compute it before deciding
  whether to keep going (see `quality_gate_task` below for the part that does).
* **`quality_gate_task`** (`src/monitoring/quality_gate.py`) runs in
  `orchestration_job` right after `yellow_taxi_silver_task`, before
  `gold_task`. It queries `silver_yellow_tripdata` directly (not
  `data_quality_trend` - that's a gold table, and gold hasn't run yet at this
  point in the job) for the DQX quarantine rate on the batch just processed,
  and **fails the job task** if it exceeds `quality_gate_threshold_pct`
  (default 20%). Since `gold_task` depends on it, a failed gate means gold
  never runs on a bad batch, and the job-level failure notification fires
  immediately - not just whenever the separate SQL alert's schedule next ticks.
* **`resources/monitoring.yml`** deploys a Databricks SQL Alert
  (`data_quality_alert`) that separately queries the latest month's
  `quarantine_rate_pct` from `data_quality_trend` on its own schedule and
  fires when it exceeds 5%. Requires the `warehouse_id` variable to be set to
  a real SQL Warehouse ID, and `alert_email_primary` for the recipient.
* **Pipeline failure alerts**: each of `resources/yellow_taxi_pipeline.pipeline.yml`
  and `nyc_taxi_gold_pipeline.pipeline.yml` independently sends email to
  `alert_email` on `on-update-failure` / `on-update-fatal-failure` /
  `on-flow-failure` - separately, the way a source-owning team and a
  cross-source analytics team in a larger org would each alert their own
  on-call, rather than one shared notification for everything.
  Note DQX's flag-violations pattern doesn't fail the flow on bad data (rows
  are tagged, not rejected), so `on-flow-failure` here only catches actual
  pipeline errors - `quality_gate_task` above is what catches bad data itself.
* **Job failure alerts**: `resources/orchestration_job.job.yml` sends email to
  `alert_email` on job failure (`email_notifications.on_failure`), and caps
  `max_concurrent_runs` at 1 so ingest -> yellow_taxi -> gate -> gold
  runs never overlap.
* **Variables to set** (in `databricks.yml`, per target, or via
  `--var`/`DATABRICKS_BUNDLE_VAR_*` at deploy time):
  * `alert_email` - list of emails for job/pipeline failure notifications
  * `alert_email_primary` - single email for the SQL alert subscription
  * `warehouse_id` - SQL Warehouse ID the alert runs against
  * `quality_gate_threshold_pct` - max acceptable quarantine rate before
    `quality_gate_task` blocks `gold_task` (default `"20"`)

## 📅 Mocking a Historical Timeline for Demos

Since all months are typically backfilled in one run, `current_timestamp()` stamps
every row with today's date regardless of which historical month it's from - not
useful for a quality dashboard that wants to show a timeline.
`explorations/mock_historical_timeline.py` is a one-off, run-it-yourself script
(deliberately **not** part of the pipeline) that backfills believable values after
the fact: `_ingested_at` becomes that row's trip month's last day plus a fixed lag
(default 5 days), and `_processed_at` becomes a further fixed lag (default 1 day)
after that.

> ⚠️ Run this only after the pipelines have finished backfilling, and don't
> re-run `yellow_taxi_pipeline` afterward without a full refresh: `silver_yellow_tripdata`
> reads `bronze_yellow_tripdata` as an append-only stream (with `skipChangeCommits`
> to tolerate the mock `UPDATE`), but re-running the pipeline can still overwrite
> `_processed_at` back to `current_timestamp()` unless the checkpoint has already
> passed that update.

## 🧪 Testing

### Test Structure

**Unit Tests** (`tests/unit/`) - one file per source module:
- `test_calculations.py`, `test_transformations.py` - the low-level utility
  functions, in isolation
- `test_bronze_transformations.py`, `test_silver_transformations.py`,
  `test_gold_transformations.py` - the composed `*_transformation()` functions
  that wire those utilities together
- Pure DataFrame-in/DataFrame-out functions; no Databricks workspace required
  beyond a Spark session

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=src.pipeline --cov-report=html

# Run tests matching a keyword
pytest tests/ -k "silver" -v
```

### Test Configuration

**Pytest Configuration:** `pyproject.toml`
- Test discovery patterns
- Pytest options

**Test Fixtures:**
- `tests/conftest.py` - Spark session setup, shared sample data

## 🔄 CI/CD Pipeline

The project uses GitHub Actions for automated deployment, delegating to a shared
reusable workflow (`WatcharawitAis/data-platform-devops`). That workflow's test/lint/
validate stages are defined outside this repo, so consult it directly for the exact
steps it runs.

### Workflow Triggers

* Push to `main`, `dev`, or `staging` branches
* Pull requests to these branches

### Required GitHub Secrets

Configure these secrets in your GitHub repository settings (Settings → Secrets → Actions):

```
DATABRICKS_HOST          # Your Databricks workspace URL
                         # Example: https://adb-1234567890123456.7.azuredatabricks.net

DATABRICKS_TOKEN         # Service principal token for bundle validation

DATABRICKS_DEV_TOKEN     # Token for deploying to development environment

DATABRICKS_STAGING_TOKEN # Token for deploying to staging environment

DATABRICKS_PROD_TOKEN    # Token for deploying to production environment
```

### Branch Deployment Strategy

* `dev` branch → Auto-deploy to **Development** (biap_dev catalog)
* `staging` branch → Auto-deploy to **Staging** (biap_staging catalog)
* `main` branch → Auto-deploy to **Production** (biap_prod catalog)

## 🛠️ Development Workflow

### 1. Local Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd nyc-taxi-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies (includes pytest, databricks-connect, databricks-sdk, dqx)
pip install -r requirements.txt

# Install this project itself in editable mode, so `import src.pipeline...`
# resolves in tests without rebuilding a wheel on every change
pip install -e .
```

### 2. Making Changes

```bash
# Create feature branch from dev
git checkout dev
git pull origin dev
git checkout -b feature/your-feature-name

# Make your changes to code in src/pipeline/

# Add tests for new functionality in tests/

# Run tests locally
pytest tests/ -v

# Lint (and auto-fix) with ruff
ruff check src/ tests/ --fix
```

### 3. Testing Locally

```bash
# Validate bundle before deploying
databricks bundle validate --target dev

# Deploy to your dev environment
databricks bundle deploy --target dev

# Run the full pipeline chain
databricks bundle run orchestration_job --target dev
```

### 4. Submit Changes

```bash
# Commit changes
git add .
git commit -m "Description of changes"

# Push to remote
git push origin feature/your-feature-name

# Create Pull Request on GitHub
# CI/CD will automatically run tests and validation
```

## 📝 Contributing Guidelines

1. **Branch from `dev`** - All feature branches should be created from the `dev` branch
2. **Write tests** - Add unit tests for new functions
3. **Follow code style** - Follow PEP 8 guidelines; run `ruff check` before pushing
4. **Test locally** - Run all tests and ensure they pass before pushing
5. **Update documentation** - Update README or code comments if adding new features
6. **Small PRs** - Keep pull requests focused on a single feature or fix
7. **Code review** - All PRs require review before merging

## ✅ Quality Gates

* ✅ **All pytest tests passing** - Unit tests
* ✅ **Linting** - `ruff check` passing
* ✅ **Bundle validation** - Databricks bundle validates successfully

Additional gates enforced by the shared CI/CD workflow are defined outside this
repo (see `.github/workflows/ci-cd.yml`).

## 📚 Additional Resources

### Databricks Documentation
* [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html)
* [Spark Declarative Pipelines (SDP)](https://docs.databricks.com/delta-live-tables/index.html)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

### Python Testing
* [pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)

### Code Quality Tools
* [Ruff Linter](https://docs.astral.sh/ruff/)

## 🏆 Project Features

* **Medallion Architecture** - Bronze → Silver → Gold data layers
* **Spark Declarative Pipelines** - Modern, declarative pipeline framework
* **Serverless Compute** - Automatic scaling, no cluster management
* **Unit Testing** - pytest coverage for utility and pipeline functions
* **CI/CD Automation** - GitHub Actions with multi-environment deployment
* **Infrastructure as Code** - Complete deployment automation with DABs
* **Multi-Environment Support** - Dev, Staging, and Production configurations
* **Data Quality** - DQX checks with per-row `_errors`/`_warnings` results
* **Automated Ingest** - monthly TLC file download with soft-skip vs. hard-failure
  handling
* **Monitoring & Notifications** - `data_quality_trend` gold table, a SQL alert on
  quarantine rate, and job/pipeline failure email notifications

## 📄 License

This project is intended for educational and demonstration purposes.

## 👥 Support

For questions or issues:
* Check the [Databricks Documentation](https://docs.databricks.com/)
* Review existing GitHub Issues
* Create a new issue with detailed description
