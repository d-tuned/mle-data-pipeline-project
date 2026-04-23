from pathlib import Path
import requests
from .config import MONTHS, RAW_DIR, filename_for, url_for


def download_file(month: str, raw_dir: Path = RAW_DIR) -> Path:
    """
    Download one month's parquet file into raw_dir.
    Skips the download if the file already exists.
    Returns the path to the local file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    destination = raw_dir / filename_for(month)

    if destination.exists():
        print(f"  already exists, skipping: {destination.name}")
        return destination

    url = url_for(month)
    print(f"  downloading {destination.name} ...")

    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    with destination.open("wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    print(f"  saved to: {destination}")
    return destination


def download_all(months: list = MONTHS, raw_dir: Path = RAW_DIR) -> list[Path]:
    """
    Download all months and return a list of local file paths.
    """
    print(f"Downloading {len(months)} file(s) into {raw_dir}")
    return [download_file(month, raw_dir) for month in months]