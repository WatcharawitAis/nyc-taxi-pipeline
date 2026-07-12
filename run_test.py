"""This is an entry point for pytest"""

import sys
import os
import pytest

sys.dont_write_bytecode = True

if __name__ == "__main__":
    current_path = os.path.abspath("__file__")
    tests_dir = os.path.dirname(current_path) + "/tests"
    exit_code = pytest.main([
        tests_dir,
        "--verbose",
        "--cov=src",
        "--cov-report=xml",
    ])
    sys.exit(exit_code)