# preprocessing/paths.py
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]   # .../car_price_pipeline
DATA_DIR  = REPO_ROOT / "data"
RAW_DIR   = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "cleaned"
PROC_DIR  = DATA_DIR / "processed"
META_DIR  = DATA_DIR / "meta"
LOG_DIR   = REPO_ROOT / "logs"
