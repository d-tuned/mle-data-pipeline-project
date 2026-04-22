# Reference Solution

This folder contains a complete local-first reference implementation for this project.

## What the solution does

1. Downloads the January, February and March 2025 Green Taxi parquet files into `data/raw/`.
2. Reads those parquet files locally.
3. Aggregates `total_amount` by pickup date to produce daily revenue.
4. Writes outputs to `data/processed/` as CSV, parquet and JSON metadata.
5. Includes an optional Prefect flow for orchestration.

## Project structure

- [src/data_pipeline/cli.py](src/data_pipeline/cli.py) exposes the `download`, `run` and `all` commands.
- [src/data_pipeline/download.py](src/data_pipeline/download.py) handles dataset downloads.
- [src/data_pipeline/prefect_flow.py](src/data_pipeline/prefect_flow.py) contains the optional Prefect flow.
- [src/data_pipeline/transform.py](src/data_pipeline/transform.py) calculates daily revenue and writes outputs.
- [tests/test_transform.py](tests/test_transform.py) covers the revenue aggregation logic.

## Run the reference solution

### macOS

```bash
cd solution
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python scripts/run_pipeline.py
pytest
```

### Windows

For `PowerShell` CLI:

```powershell
cd solution
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python scripts/run_pipeline.py
pytest
```

For `Git-Bash` CLI:

```bash
cd solution
py -3 -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python scripts/run_pipeline.py
pytest
```

[requirements.txt](./requirements.txt) pins the exact dependency versions that were used for this solution.

## Optional Prefect flow

Prefect is intentionally optional. Install it only if you want to try the orchestration version of the same local pipeline.
Run these commands from the activated `solution` virtual environment you created above.

This reference solution is meant to let Prefect start a temporary local server for the run.
If you previously ran another Prefect project, an old background server or a saved `PREFECT_API_URL` can cause errors.

Use the cleanup commands below before running the flow.
If Prefect says no server is running or the setting is not set, you can continue.

`macOS` / `Linux` / `Windows Git-Bash`:

```bash
python -m pip install prefect==3.6.25
prefect server stop
prefect config set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true
prefect config unset PREFECT_API_URL --yes
unset PREFECT_API_URL
python scripts/run_prefect_flow.py
```

`Windows PowerShell`:

```powershell
python -m pip install prefect==3.6.25
prefect server stop
prefect config set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true
prefect config unset PREFECT_API_URL --yes
Remove-Item Env:PREFECT_API_URL -ErrorAction SilentlyContinue
python scripts/run_prefect_flow.py
```

The helper scripts add the local `src/` directory to `PYTHONPATH` automatically, so you do not need an editable package install.

When you are done with the optional flow, you can stop any background Prefect server with `prefect server stop`.

If you intentionally want to use a dedicated Prefect server instead of the temporary server, start it first with `prefect server start --background`, check it with `prefect server status`, and then run the flow.

## Expected outputs

After a successful run, you should see:

- `data/raw/green_tripdata_2025-01.parquet`
- `data/raw/green_tripdata_2025-02.parquet`
- `data/raw/green_tripdata_2025-03.parquet`
- `data/processed/daily_revenue.csv`
- `data/processed/daily_revenue.parquet`
- `data/processed/pipeline_metadata.json`
