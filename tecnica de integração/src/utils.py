import os
from pathlib import Path

def ensure_dirs(*dirs):
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)

def safe_path(*parts):
    return os.path.join(*parts)
