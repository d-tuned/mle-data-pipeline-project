import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import PROCESSED_DIR, RAW_DIR, MONTHS, filename_for


# The column names we depend on
PICKUP_COL = "lpep_pickup_datetime"
REVENUE_COL = "total_amount"

@dataclass
class PipelineOutputs:
    """Holds the paths of everything the pipeline writes."""
    daily_revenue_csv: Path
    daily_revenue_parquet: Path
    metadata_json: Path

def prepare_trips(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw trip data down to just date and revenue.
    Drops rows with invalid dates or missing revenue.
    """
    if PICKUP_COL not in df.columns:
        raise ValueError(f"Missing expected column: {PICKUP_COL}")

    result = df.copy()

    # Parse dates — coerce means "turn bad values into NaT instead of crashing"
    result[PICKUP_COL] = pd.to_datetime(result[PICKUP_COL], errors="coerce")

    # Drop rows where the date is invalid
    result = result.dropna(subset=[PICKUP_COL])

    # Normalize timestamp to midnight so we can group by day
    # e.g. 2025-01-15 08:32:00 → 2025-01-15 00:00:00
    result["service_date"] = result[PICKUP_COL].dt.normalize()

    # Parse revenue — coerce bad values to NaN
    result[REVENUE_COL] = pd.to_numeric(result[REVENUE_COL], errors="coerce")

    # Drop rows with no revenue
    result = result.dropna(subset=[REVENUE_COL])

    return result[["service_date", REVENUE_COL]]

def calculate_daily_revenue(input_paths: list[Path]) -> tuple[pd.DataFrame, dict]:
    """
    Read parquet files, clean them, and aggregate revenue by day.
    Returns the daily revenue dataframe and a metadata dictionary.
    """
    if not input_paths:
        raise ValueError("No input files provided.")

    frames = []
    total_rows_read = 0

    for path in input_paths:
        df = pd.read_parquet(path)
        total_rows_read += len(df)

        cleaned = prepare_trips(df)

        daily = cleaned.groupby("service_date", as_index=False).agg(
            trip_count=(REVENUE_COL, "size"),
            daily_revenue=(REVENUE_COL, "sum"),
        )
        frames.append(daily)
        print(f"  processed {path.name}: {len(df)} rows → {len(cleaned)} valid trips")

    # Combine all months and re-aggregate in case dates overlap
    combined = (
        pd.concat(frames, ignore_index=True)
        .groupby("service_date", as_index=False)
        .agg(
            trip_count=("trip_count", "sum"),
            daily_revenue=("daily_revenue", "sum"),
        )
        .sort_values("service_date")
    )

    combined["daily_revenue"] = combined["daily_revenue"].round(2)
    combined["service_date"] = combined["service_date"].dt.strftime("%Y-%m-%d")

    metadata = {
        "days_in_output": len(combined),
        "total_rows_read": total_rows_read,
        "trips_in_output": int(combined["trip_count"].sum()),
        "revenue_total": float(combined["daily_revenue"].sum()),
    }

    return combined, metadata

def write_outputs(
    daily_revenue: pd.DataFrame,
    metadata: dict,
    output_dir: Path = PROCESSED_DIR,
) -> PipelineOutputs:
    """Write results to CSV, parquet and JSON metadata."""
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path      = output_dir / "daily_revenue.csv"
    parquet_path  = output_dir / "daily_revenue.parquet"
    metadata_path = output_dir / "pipeline_metadata.json"

    daily_revenue.to_csv(csv_path, index=False)
    daily_revenue.to_parquet(parquet_path, index=False)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    return PipelineOutputs(
        daily_revenue_csv=csv_path,
        daily_revenue_parquet=parquet_path,
        metadata_json=metadata_path,
    )


def run_pipeline(
    input_paths: list[Path],
    output_dir: Path = PROCESSED_DIR,
) -> tuple[PipelineOutputs, dict]:
    """Main entry point — runs the full transform and writes outputs."""
    print("Running transformation pipeline...")
    daily_revenue, metadata = calculate_daily_revenue(input_paths)
    outputs = write_outputs(daily_revenue, metadata, output_dir)
    print(f"  days in output:   {metadata['days_in_output']}")
    print(f"  trips processed:  {metadata['trips_in_output']}")
    print(f"  total revenue:    ${metadata['revenue_total']:,.2f}")
    return outputs, metadata