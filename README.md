# NYC Taxi Pipeline

A production-ready Databricks data pipeline for processing NYC Taxi Trip data using the medallion architecture (Bronze → Silver → Gold). Built with Spark Declarative Pipelines (SDP) and deployed via Databricks Asset Bundles (DABs).

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── src/
│   └── pipeline/
│       ├── bronze/
│       │   ├── bronze_layer.py       # Raw data ingestion
│       │   └── bronze_pipelines.py   # Bronze table definitions
│       ├── silver/
│       │   ├── silver_layer.py       # Data cleaning & validation
│       │   └── silver_pipelines.py   # Silver table definitions
│       ├── gold/
│       │   ├── gold_layer.py         # Business aggregations
│       │   └── gold_pipelines.py     # Gold table definitions
│       ├── utils/
│       │   ├── validations.py        # Data validation functions
│       │   ├── calculations.py       # Metric calculations
│       │   ├── transformations.py    # Data transformations
│       │   ├── aggregations.py       # Aggregation functions
│       │   ├── constraints.py        # Data quality constraints
│       │   └── spark_session.py      # Spark session utilities
│       └── pipeline.py               # Main entry point (imports all layers)
├── tests/
│   ├── unit/
│   │   ├── test_silver_layer_sample.py   # Silver layer unit tests
│   │   └── test_gold_layer_sample.py     # Gold layer unit tests
│   ├── integration/
│   │   └── test_pipeline_sample.py       # End-to-end integration tests
│   └── conftest.py                       # Pytest fixtures & test configuration
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

### Deploy Pipeline

```bash
# 1. Validate the bundle configuration
databricks bundle validate --target dev

# 2. Deploy to development environment
databricks bundle deploy --target dev

# 3. Run the pipeline
databricks bundle run nyc_taxi_pipeline --target dev
```

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
Mode: development
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
**Purpose:** Raw data ingestion from source without transformations

**Location:** `src/pipeline/bronze/`
- Ingests raw NYC taxi trip data
- Preserves original data format (ISO 8601 timestamps)
- No data quality filters applied

**Output:** `{catalog}.bronze.nyc_taxi_trips_raw`

### Silver Layer
**Purpose:** Data cleaning, validation, and feature engineering

**Location:** `src/pipeline/silver/`

**Transformations:**
* Clean and validate ZIP codes
* Parse and validate datetime columns (ISO 8601 format)
* Calculate trip duration (minutes)
* Calculate average speed (mph)
* Extract time features (hour, day of week)
* Apply data quality filters (remove invalid records)

**Utilities:**
- `validations.py` - Data validation functions
- `calculations.py` - Metric calculation functions
- `transformations.py` - Data transformation functions

**Output:** `{catalog}.silver.nyc_taxi_trips_cleaned`

### Gold Layer
**Purpose:** Business-ready aggregations for analytics and reporting

**Location:** `src/pipeline/gold/`

**Aggregations:**
* Group rides by day of week
* Calculate total rides, total fare
* Calculate average distance, fare, and speed
* Convert day numbers to readable names (Sunday, Monday, etc.)
* Round metrics for readability

**Utilities:**
- `aggregations.py` - Aggregation functions
- `constraints.py` - Data quality expectations

**Output:** `{catalog}.gold.nyc_taxi_daily_stats`

## 🧪 Testing

### Test Structure

The project includes comprehensive testing organized into two categories:

**Unit Tests** (`tests/unit/`)
- Test individual transformation functions in isolation
- Verify calculations, validations, and data transformations
- Fast execution with minimal dependencies

**Integration Tests** (`tests/integration/`)
- Test end-to-end pipeline execution
- Verify data flows through all layers
- Validate final output schema and data quality

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run only unit tests
pytest tests/unit/ -v

# Run only integration tests
pytest tests/integration/ -v

# Run with coverage report
pytest tests/ --cov=src.pipeline --cov-report=html

# Run tests matching a keyword
pytest tests/ -k "silver" -v
```

### Test Configuration

**Pytest Configuration:** `pyproject.toml`
- Test discovery patterns
- Coverage settings
- Pytest options

**Test Fixtures:** `tests/conftest.py`
- Spark session setup for testing
- Shared test data and utilities

## 🔄 CI/CD Pipeline

The project uses GitHub Actions for automated testing and deployment.

### Workflow Triggers

* Push to `main`, `dev`, or `staging` branches
* Pull requests to these branches

### Pipeline Stages

1. **Test** - Run pytest with coverage reporting
2. **Lint** - Code quality checks (Black, Flake8, isort, mypy)
3. **Validate** - Validate bundle configuration for target environment
4. **Deploy** - Automatically deploy to appropriate environment based on branch

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

# Install dependencies
pip install -r requirements.txt

# Install development tools
pip install pytest pytest-cov black flake8 isort mypy
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

# Format code
black src/ tests/
isort src/ tests/

# Check code quality
flake8 src/ tests/
mypy src/
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
2. **Write tests** - Add unit tests for new functions and integration tests for new features
3. **Follow code style** - Use Black for formatting, follow PEP 8 guidelines
4. **Test locally** - Run all tests and ensure they pass before pushing
5. **Update documentation** - Update README or code comments if adding new features
6. **Small PRs** - Keep pull requests focused on a single feature or fix
7. **Code review** - All PRs require review before merging

## ✅ Quality Gates

All checks must pass before deployment:

* ✅ **All pytest tests passing** - Unit and integration tests
* ✅ **Code formatting** - Black code formatter applied
* ✅ **Linting** - Flake8 checks passing
* ✅ **Import sorting** - isort applied correctly
* ✅ **Type checking** - mypy static type analysis passing
* ✅ **Bundle validation** - Databricks bundle validates successfully

The CI/CD pipeline automatically enforces these quality gates. Failed checks will block merging and deployment.

## 📚 Additional Resources

### Databricks Documentation
* [Databricks Asset Bundles (DABs)](https://docs.databricks.com/dev-tools/bundles/index.html)
* [Spark Declarative Pipelines (SDP)](https://docs.databricks.com/delta-live-tables/index.html)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

### Python Testing
* [pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)

### Code Quality Tools
* [Black Code Formatter](https://black.readthedocs.io/)
* [Flake8 Linter](https://flake8.pycqa.org/)
* [isort Import Sorter](https://pycqa.github.io/isort/)
* [mypy Type Checker](https://mypy.readthedocs.io/)

## 🏆 Project Features

* **Medallion Architecture** - Bronze → Silver → Gold data layers
* **Spark Declarative Pipelines** - Modern, declarative pipeline framework
* **Serverless Compute** - Automatic scaling, no cluster management
* **Comprehensive Testing** - Unit and integration test coverage
* **CI/CD Automation** - GitHub Actions with multi-environment deployment
* **Infrastructure as Code** - Complete deployment automation with DABs
* **Multi-Environment Support** - Dev, Staging, and Production configurations
* **Data Quality** - Built-in validation, constraints, and quality checks

## 📄 License

This project is intended for educational and demonstration purposes.

## 👥 Support

For questions or issues:
* Check the [Databricks Documentation](https://docs.databricks.com/)
* Review existing GitHub Issues
* Create a new issue with detailed description
