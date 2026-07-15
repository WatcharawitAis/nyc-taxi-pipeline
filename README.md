# NYC Taxi Pipeline

A production-ready Databricks data pipeline for processing real NYC TLC Yellow Taxi trip data using the medallion architecture (Bronze → Silver → Gold). Built with Spark Declarative Pipelines (SDP) and deployed via Databricks Asset Bundles (DABs).

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── src/
│   └── pipeline/
│       ├── bronze/
│       │   ├── bronze_layer.py       # Auto Loader ingestion from a Volume
│       │   └── bronze_pipelines.py   # Bronze transform logic
│       ├── silver/
│       │   ├── silver_layer.py       # Data cleaning + DQX checks (_errors/_warnings kept)
│       │   └── silver_pipelines.py   # Silver transform logic
│       ├── gold/
│       │   ├── gold_layer.py         # Business aggregations
│       │   └── gold_pipelines.py     # Gold aggregation logic
│       ├── utils/
│       │   ├── calculations.py       # Metric calculations
│       │   ├── transformations.py    # Data transformations
│       │   └── spark_session.py      # Spark session utilities
│       └── pipeline.py               # Main entry point (imports all layers)
├── tests/
│   ├── unit/                             # Pure function tests, one file per source module
│   │   ├── test_calculations.py
│   │   ├── test_transformations.py
│   │   ├── test_bronze_pipelines.py
│   │   ├── test_silver_pipelines.py
│   │   └── test_gold_pipelines.py
│   └── conftest.py                       # Pytest fixtures & test configuration
├── explorations/
│   └── mock_historical_timeline.py       # One-off script to backfill demo timestamps
├── resources/
│   ├── nyc_taxi_pipeline.pipeline.yml    # Pipeline resource definition
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
* NYC TLC Yellow Taxi monthly parquet files already landed in a Unity Catalog
  Volume (see [Data Source](#-data-source) below)

### Deploy Pipeline

```bash
# 1. Validate the bundle configuration
databricks bundle validate --target dev

# 2. Deploy to development environment
databricks bundle deploy --target dev

# 3. Run the pipeline
databricks bundle run nyc_taxi_pipeline --target dev
```

## 📥 Data Source

Real, public NYC TLC Yellow Taxi monthly files
(`yellow_tripdata_YYYY-MM.parquet`, e.g. from
`https://d37ci6vzurychx.cloudfront.net/trip-data/`) are expected to already be
landed in a Unity Catalog Volume at
`/Volumes/{catalog}/{landing_schema}/{landing_volume}/{year}/yellow_tripdata_{year}-{month}.parquet`
(configurable via the `landing_schema`/`landing_volume` bundle variables in
`databricks.yml`, default `landing` / `nyc-yellow-taxi-files`).

> ⚠️ If that Volume ever contains multiple years' worth of files, `bronze_layer.py`
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
Pipeline Development Mode: true
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

**Location:** `src/pipeline/bronze/`
- Auto Loader (`cloudFiles`) incrementally picks up new `yellow_tripdata_YYYY-MM.parquet`
  files from the landing Volume
- Adds `trip_year`/`trip_month` (parsed from the file name) and an `_ingested_at`
  lineage timestamp
- Real TLC schema (`PULocationID`/`DOLocationID`, `VendorID`, etc.) - no transformations applied

**Output:** `{catalog}.bronze.bronze_yellow_tripdata`

### Silver Layer
**Purpose:** Data cleaning, feature engineering, and data quality enforcement

**Location:** `src/pipeline/silver/`

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
- `{catalog}.silver.verified_trips` - a view over `silver_yellow_tripdata`
  filtered to rows with no `_errors`/`_warnings`, with those columns dropped.
  No extra storage; use this instead of querying `silver_yellow_tripdata`
  directly if you want clean data without knowing the filtering convention.
- `{catalog}.silver.quarantined_trips` - a view over `silver_yellow_tripdata`
  filtered to rows with at least one `_errors`/`_warnings` entry.

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

**Utilities:**
- `gold_pipelines.py` - Aggregation functions for each table above

**Output:** `{catalog}.gold.monthly_trip_metrics`, `{catalog}.gold.pickup_zone_metrics`,
`{catalog}.gold.hourly_demand_heatmap`, `{catalog}.gold.data_quality_trend`

## 📅 Mocking a Historical Timeline for Demos

Since all months are typically backfilled in one run, `current_timestamp()` stamps
every row with today's date regardless of which historical month it's from - not
useful for a quality dashboard that wants to show a timeline.
`explorations/mock_historical_timeline.py` is a one-off, run-it-yourself script
(deliberately **not** part of the pipeline) that backfills believable values after
the fact: `_ingested_at` becomes that row's trip month's last day plus a fixed lag
(default 5 days), and `_processed_at` becomes a further fixed lag (default 1 day)
after that.

> ⚠️ Run this only after the pipeline has finished backfilling, and don't
> re-run `nyc_taxi_pipeline` afterward without a full refresh: `silver_yellow_tripdata`
> reads `bronze_yellow_tripdata` as an append-only stream (with `skipChangeCommits`
> to tolerate the mock `UPDATE`), but re-running the pipeline can still overwrite
> `_processed_at` back to `current_timestamp()` unless the checkpoint has already
> passed that update.

## 🧪 Testing

### Test Structure

**Unit Tests** (`tests/unit/`) - one file per source module:
- `test_calculations.py`, `test_transformations.py` - the low-level utility
  functions, in isolation
- `test_bronze_pipelines.py`, `test_silver_pipelines.py`, `test_gold_pipelines.py` -
  the composed `*_pipeline()` functions that wire those utilities together
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

# Run the pipeline
databricks bundle run nyc_taxi_pipeline --target dev
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

## 📄 License

This project is intended for educational and demonstration purposes.

## 👥 Support

For questions or issues:
* Check the [Databricks Documentation](https://docs.databricks.com/)
* Review existing GitHub Issues
* Create a new issue with detailed description
