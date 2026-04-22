from pathlib import Path

# The three months we want to process
MONTHS = ["2025-01", "2025-02", "2025-03"]

# Where the data lives online
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Resolve all paths relative to this file, so the project
# works no matter where you run it from
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # goes up: data_pipeline → src → root

RAW_DIR       = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def filename_for(month: str) -> str:
    """Return the parquet filename for a given month, e.g. '2025-01'."""
    return f"green_tripdata_{month}.parquet"


def url_for(month: str) -> str:
    """Return the full download URL for a given month."""
    return f"{BASE_URL}/{filename_for(month)}"