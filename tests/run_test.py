
import pytest
import sys
sys.dont_write_bytecode = True # Prevents Python from creating the __pycache__ folder.

sys.exit(
    pytest.main([
        "tests/",
        "-v",
        "-p", "no:cacheprovider"
    ])
)