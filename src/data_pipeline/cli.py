import click
from pathlib import Path
from .config import MONTHS, RAW_DIR, PROCESSED_DIR, filename_for
from .download import download_all
from .transform import run_pipeline


def get_input_paths(raw_dir: Path, months: list) -> list[Path]:
    """Build input file paths and check they all exist."""
    paths = [raw_dir / filename_for(m) for m in months]
    missing = [p for p in paths if not p.exists()]
    if missing:
        missing_names = ", ".join(p.name for p in missing)
        raise click.ClickException(
            f"Missing files in {raw_dir}: {missing_names}. Run download first."
        )
    return paths


@click.group()
def cli():
    """NYC Green Taxi data pipeline."""


@cli.command("download")
def download_command():
    """Download raw parquet files for Jan, Feb, Mar 2025."""
    click.echo("Starting download...")
    download_all()
    click.echo("Download complete!")


@cli.command("run")
def run_command():
    """Transform raw files into daily revenue outputs."""
    paths = get_input_paths(RAW_DIR, MONTHS)
    click.echo("Starting transformation...")
    _, metadata = run_pipeline(paths, PROCESSED_DIR)
    click.echo(f"Done! {metadata['days_in_output']} days, "
               f"{metadata['trips_in_output']} trips, "
               f"${metadata['revenue_total']:,.2f} total revenue")


@cli.command("all")
def all_command():
    """Download and transform in one step."""
    click.echo("Step 1/2: Downloading...")
    download_all()
    click.echo("Step 2/2: Transforming...")
    paths = get_input_paths(RAW_DIR, MONTHS)
    _, metadata = run_pipeline(paths, PROCESSED_DIR)
    click.echo(f"Pipeline complete! {metadata['trips_in_output']} trips processed.")


def main():
    cli()


if __name__ == "__main__":
    main()