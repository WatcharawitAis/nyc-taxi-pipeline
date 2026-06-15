# NYC Taxi Pipeline

Databricks data pipeline สำหรับประมวลผล NYC Taxi Trip data โดยใช้ medallion architecture (Bronze → Silver → Gold)

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── pipeline/
│   └── transformations/
│       ├── bronze_layer.py      # Raw data ingestion
│       ├── silver_layer.py      # Data cleaning & enrichment
│       └── gold_layer.py        # Business aggregations
├── tests/
│   ├── conftest.py              # Pytest fixtures
│   ├── test_silver_layer.py     # Silver layer tests (21 tests)
│   └── test_gold_layer.py       # Gold layer tests (25 tests)
├── resources/
│   └── nyc_taxi_pipeline.pipeline.yml
├── .github/
│   └── workflows/
│       └── ci-cd.yml            # CI/CD pipeline
├── databricks.yml               # DABs configuration
├── requirements-dev.txt         # Development dependencies
└── pytest.ini                   # Pytest configuration
```

## 🧪 Testing

### Quick Start

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run all tests
pytest tests/ -v

# Run specific layer tests
pytest tests/test_silver_layer.py -v
pytest tests/test_gold_layer.py -v

# Run only integration tests
pytest tests/ -k "integration" -v

# Run with coverage report
pytest tests/ --cov=pipeline --cov-report=html
```

### Test Coverage

**Silver Layer (21 tests):**
- ✅ clean_and_validate_zip() - 3 tests
- ✅ validate_datetime_columns() - 3 tests (รองรับ ISO 8601)
- ✅ calculate_trip_duration() - 3 tests
- ✅ calculate_avg_speed() - 3 tests
- ✅ extract_time_features() - 1 test
- ✅ apply_data_quality_filters() - 3 tests
- ✅ Integration tests - 3 tests

**Gold Layer (25 tests):**
- ✅ convert_day_number_to_name() - 5 tests
- ✅ aggregate_by_day_of_week() - 4 tests
- ✅ round_metric_columns() - 4 tests
- ✅ sort_by_day_of_week() - 3 tests
- ✅ Integration tests - 6 tests

**Total: 46 test cases** covering unit tests และ integration tests

## 🚀 CI/CD Pipeline

### Automated Testing on Push/PR

GitHub Actions จะรันอัตโนมัติเมื่อ:
- Push ไป `main` หรือ `dev` branch
- สร้าง Pull Request

### CI Pipeline Stages

```yaml
1. 🧪 Tests          → pytest tests/ -v --cov=pipeline
2. 🔍 Lint           → flake8 + black
3. ✅ Validate       → databricks bundle validate
4. 🚀 Deploy (auto) → deploy to dev/prod based on branch
```

### Required GitHub Secrets

ตั้งค่า secrets ใน GitHub repository:

```
DATABRICKS_HOST          # Databricks workspace URL
DATABRICKS_TOKEN         # Service principal token (for validation)
DATABRICKS_DEV_TOKEN     # Dev environment token
DATABRICKS_PROD_TOKEN    # Production environment token
```

### Branch Strategy

* `dev` branch → Auto deploy to **Development**
* `main` branch → Auto deploy to **Production** (requires approval)

## 🏗️ Databricks Asset Bundles (DABs)

### Deploy Pipeline

```bash
# Validate bundle
databricks bundle validate --strict

# Deploy to development
databricks bundle deploy --target dev

# Deploy to production
databricks bundle deploy --target prod

# Run pipeline
databricks bundle run nyc_taxi_pipeline --target dev
```

## 📊 Data Schema

### Bronze Layer
Raw data ingested from source with ISO 8601 datetime format:
```
tpep_pickup_datetime: "2016-01-22T22:39:39.000+00:00"
tpep_dropoff_datetime: "2016-01-22T23:09:39.000+00:00"
pickup_zip: "10001.0"
trip_distance: 2.5
fare_amount: 15.0
...
```

### Silver Layer
Cleaned and enriched data:
```
tpep_pickup_datetime: timestamp
trip_duration_minutes: double
avg_speed_mph: double
pickup_hour: int
pickup_day_of_week: int (1=Sunday, 7=Saturday)
...
```

### Gold Layer
Business-ready aggregations:
```
day_of_week: int
day_name: string
total_rides: bigint
total_fare: double
avg_distance: double
avg_fare: double
avg_speed: double
```

## 🛠️ Development

### Local Development

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run tests during development
pytest tests/ -v --tb=short

# Check code quality
flake8 pipeline/transformations/ --max-line-length=120
black pipeline/transformations/

# Type checking
mypy pipeline/transformations/
```

### Adding New Tests

1. เพิ่ม test functions ใน `tests/test_*_layer.py`
2. ใช้ `spark` fixture จาก conftest.py
3. รัน tests locally ก่อน commit
4. Tests จะรันอัตโนมัติใน CI/CD

## 📝 Contributing

1. สร้าง feature branch จาก `dev`
2. เขียน tests สำหรับ code ใหม่
3. รัน `pytest tests/ -v` ให้ผ่านทั้งหมด
4. รัน `black` และ `flake8`
5. สร้าง Pull Request → CI จะรัน tests อัตโนมัติ
6. Merge เข้า `dev` → Auto deploy to Development
7. Merge `dev` → `main` → Auto deploy to Production

## 🎯 Quality Gates

Tests ต้องผ่านทั้งหมดก่อน deploy:
* ✅ All pytest tests passing
* ✅ Code formatting (black)
* ✅ Linting (flake8)
* ✅ Bundle validation (databricks)

## 📚 Documentation

* [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
* [pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)
