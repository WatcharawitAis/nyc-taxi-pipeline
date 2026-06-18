# NYC Taxi Pipeline Tests

This directory contains unit tests and integration tests for the NYC Taxi data pipeline.

## Test Structure

```
tests/
├── conftest.py                           # Shared fixtures and test configuration
├── unit/                                 # Unit tests (fast, local Spark)
│   ├── __init__.py
│   ├── test_silver_layer.py             # Silver layer unit tests
│   └── test_gold_layer.py               # Gold layer unit tests
├── integration/                          # Integration tests (Databricks cluster)
│   ├── __init__.py
│   └── test_pipeline.py                 # End-to-end pipeline tests
├── pytest.ini                           # Pytest configuration
└── README.md                            # This file
```

## Test Types

### Unit Tests (`tests/unit/`)
* **Location**: Tests in `tests/unit/` directory
* **Purpose**: Test individual functions in isolation
* **Speed**: Fast (< 1 second per test)
* **Dependencies**: Local Spark session only
* **When to run**: During development, before every commit

Unit tests verify:
* Individual transformation functions work correctly
* Edge cases are handled (nulls, zeros, negatives)
* Data type conversions are accurate
* Business logic is correctly implemented

### Integration Tests (`tests/integration/`)
* **Location**: Tests in `tests/integration/` directory
* **Purpose**: Test end-to-end pipeline flows
* **Speed**: Slower (requires cluster connection)
* **Dependencies**: Databricks cluster via databricks-connect
* **When to run**: Before deployment, as part of CI/CD

Integration tests verify:
* Multiple transformations work together correctly
* Data flows through bronze → silver → gold layers
* Data quality rules are enforced
* Final outputs match business requirements

## Running Tests

### Prerequisites

```bash
# Install test dependencies
pip install pytest pytest-cov databricks-connect

# Configure databricks-connect (for integration tests)
databricks-connect configure
```

### Run All Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=html
```

### Run Specific Test Types

```bash
# Run only unit tests (fast, no cluster needed)
pytest tests/unit/ -v

# Run only integration tests (requires cluster)
pytest tests/integration/ -v

# Run tests for specific layer
pytest tests/unit/test_silver_layer.py -v
pytest tests/unit/test_gold_layer.py -v
pytest tests/integration/test_pipeline.py -v
```

### Run Individual Test Classes or Functions

```bash
# Run a specific test class
pytest tests/unit/test_silver_layer.py::TestCleanAndValidateZip -v

# Run a specific test function
pytest tests/unit/test_silver_layer.py::TestCleanAndValidateZip::test_clean_valid_zip_code -v
```

### Using Markers

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run tests related to data quality
pytest -m data_quality

# Skip slow tests
pytest -m "not slow"
```

## Test Fixtures

Fixtures are defined in `conftest.py` and available to all tests:

### Spark Sessions
* **`local_spark`**: Local Spark session for unit tests
* **`databricks_spark`**: Databricks cluster connection for integration tests

### Sample Data
* **`sample_taxi_schema`**: Schema for NYC taxi data
* **`sample_taxi_data`**: Bronze layer sample data with valid and invalid records
* **`sample_silver_data`**: Silver layer sample data for gold layer tests
* **`expected_columns`**: Expected column names for each layer

## Writing New Tests

### Unit Test Template

```python
import pytest
from src.pipeline.silver.silver_layer import your_function

class TestYourFunction:
    """Test your_function behavior"""
    
    def test_normal_case(self, local_spark):
        """Test with normal valid input"""
        data = [("value1",), ("value2",)]
        df = local_spark.createDataFrame(data, ["col"])
        
        result = your_function(df)
        
        assert result.count() == 2
    
    def test_edge_case(self, local_spark):
        """Test with edge case input"""
        data = [(None,), ("",)]
        df = local_spark.createDataFrame(data, ["col"])
        
        result = your_function(df)
        
        assert result.count() == 0
```

### Integration Test Template

```python
import pytest

class TestYourIntegration:
    """Test end-to-end flow"""
    
    def test_full_transformation(self, databricks_spark, sample_taxi_data):
        """Test complete transformation pipeline"""
        # Apply transformations
        result = (
            sample_taxi_data
            .transform(step1)
            .transform(step2)
            .transform(step3)
        )
        
        # Verify results
        assert result.count() > 0
        assert "expected_column" in result.columns
```

## Best Practices

### Test Naming
* Use descriptive names: `test_clean_valid_zip_code` not `test1`
* Include expected behavior: `test_negative_fare_filtered`
* Use docstrings to explain what's being tested

### Test Independence
* Each test should be independent
* Don't rely on test execution order
* Use fixtures for shared setup
* Clean up after tests (fixtures handle this)

### Assertions
* Use descriptive assertion messages
* Test one thing per test function
* Include both positive and negative cases

### Coverage Goals
* Aim for >80% code coverage for transformation functions
* Test happy path, edge cases, and error conditions
* Don't test framework code (Spark, DLT)

## CI/CD Integration

Tests run automatically in GitHub Actions:

```yaml
# .github/workflows/ci-cd.yml
- name: Run Unit Tests
  run: pytest tests/unit/ -v
  
- name: Run Integration Tests
  run: pytest tests/integration/ -v
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

## Troubleshooting

### Common Issues

**ImportError: No module named 'src'**
* Make sure `PYTHONPATH` includes the project root
* Run pytest from the project root directory
* Check that `pytest.ini` has `pythonpath = .`

**Databricks connection failed (integration tests)**
* Verify `databricks-connect` is configured: `databricks-connect test`
* Check cluster is running
* Verify authentication credentials

**Tests pass locally but fail in CI**
* Check Python version matches CI environment
* Verify all dependencies are in `requirements.txt`
* Look for hardcoded paths or environment-specific code

## Resources

* [Pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/user_guide/testing.html)
* [Databricks Testing Best Practices](https://docs.databricks.com/dev-tools/testing.html)
