import sys
import os
import pytest

sys.dont_write_bytecode = True

if __name__ == "__main__":
    dir_root = os.path.abspath('__file__')
    tests_dir = os.path.dirname(dir_root) + "/tests"
    exit_code = pytest.main([tests_dir, "--verbose"])
