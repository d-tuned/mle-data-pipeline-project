from pathlib import Path
import pandas as pd
import pytest
from data_pipeline.transform import prepare_trips, calculate_daily_revenue


def test_prepare_trips_drops_invalid_dates(tmp_path: Path):
    """Rows with unparseable dates should be silently dropped."""
    df = pd.DataFrame({
        "lpep_pickup_datetime": ["2025-01-01 08:00:00", "not-a-date", None],
        "total_amount": [10.0, 20.0, 30.0],
    })

    result = prepare_trips(df)

    # Only the first row had a valid date — the other two should be gone
    assert len(result) == 1
    assert result["service_date"].iloc[0] == pd.Timestamp("2025-01-01")


def test_prepare_trips_drops_missing_revenue(tmp_path: Path):
    """Rows with no revenue should be dropped."""
    df = pd.DataFrame({
        "lpep_pickup_datetime": ["2025-01-01 08:00:00", "2025-01-01 09:00:00"],
        "total_amount": [15.0, None],
    })

    result = prepare_trips(df)

    assert len(result) == 1
    assert float(result["total_amount"].iloc[0]) == 15.0


def test_calculate_daily_revenue_aggregates_correctly(tmp_path: Path):
    """Two trips on the same day should be summed into one row."""
    # Create a fake parquet file with known values
    fake_file = tmp_path / "green_tripdata_2025-01.parquet"
    pd.DataFrame({
        "lpep_pickup_datetime": [
            "2025-01-01 08:00:00",
            "2025-01-01 09:30:00",  # same day as above
            "2025-01-02 14:00:00",  # different day
        ],
        "total_amount": [10.0, 5.0, 20.0],
    }).to_parquet(fake_file)

    daily, metadata = calculate_daily_revenue([fake_file])

    # Jan 1 should have 2 trips totalling $15, Jan 2 should have 1 trip of $20
    assert len(daily) == 2
    assert daily.iloc[0]["service_date"] == "2025-01-01"
    assert daily.iloc[0]["trip_count"] == 2
    assert daily.iloc[0]["daily_revenue"] == 15.0
    assert daily.iloc[1]["daily_revenue"] == 20.0
    assert metadata["trips_in_output"] == 3
    assert metadata["revenue_total"] == 35.0


def test_calculate_daily_revenue_raises_on_empty_input():
    """Passing no files should raise a clear error."""
    with pytest.raises(ValueError, match="No input files provided"):
        calculate_daily_revenue([])