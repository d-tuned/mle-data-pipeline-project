from prefect import flow, task
from pathlib import Path

from .config import MONTHS, RAW_DIR, PROCESSED_DIR, filename_for
from .download import download_all
from .transform import run_pipeline


@task(name="Download Taxi Data", retries=2, retry_delay_seconds=30)
def download_task() -> None:
    """
    Download all monthly parquet files.
    Retries up to 2 times if the download fails —
    useful if the NYC TLC website is temporarily unavailable.
    """
    download_all()


@task(name="Transform Taxi Data")
def transform_task() -> dict:
    """
    Read raw parquet files and calculate daily revenue.
    Returns metadata about the run.
    """
    paths = [RAW_DIR / filename_for(m) for m in MONTHS]
    _, metadata = run_pipeline(paths, PROCESSED_DIR)
    return metadata


@flow(name="NYC Green Taxi Pipeline", log_prints=True)
def green_taxi_pipeline() -> None:
    """
    Full pipeline flow — download then transform.
    Prefect tracks each task individually and logs everything.
    """
    print("Starting NYC Green Taxi Pipeline...")

    download_task()
    metadata = transform_task()

    print(f"Pipeline complete!")
    print(f"  Days in output:  {metadata['days_in_output']}")
    print(f"  Trips processed: {metadata['trips_in_output']}")
    print(f"  Total revenue:   ${metadata['revenue_total']:,.2f}")


if __name__ == "__main__":
    green_taxi_pipeline()