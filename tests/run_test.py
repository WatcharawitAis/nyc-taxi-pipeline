import sys
import os
import pytest

# ปิดระบบ Cache ของ Python
sys.dont_write_bytecode = True

if __name__ == "__main__":
    try:
        current_file_path = os.path.abspath(__file__)
    except NameError:
        current_file_path = os.path.abspath(sys.argv[0])
        
    tests_dir = os.path.dirname(current_file_path)
    project_root = os.path.dirname(tests_dir)
    ini_file_path = os.path.join(project_root, "pytest.ini")

    # 1. สั่งรัน Pytest และเก็บรหัสผลลัพธ์ (Exit Code) เอาไว้
    exit_code = pytest.main([
        tests_dir,
        "-v",
        "-p", "no:cacheprovider",
        f"--rootdir={project_root}",
        f"-c={ini_file_path}"
    ])

    
    if exit_code != 0:
        raise RuntimeError(f"Pytest failed with exit code {exit_code}")
        
   