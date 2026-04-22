"""Test setup for importing the local solution package without installing it."""

import sys
from pathlib import Path

# Pytest runs from the solution folder, so add src/ to the import path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
