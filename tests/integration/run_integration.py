# tests/run_pytest.py

import pytest
import sys

sys.exit(
    pytest.main([
        "tests/integration",
        "-v"
    ])
)