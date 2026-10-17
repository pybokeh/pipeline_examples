"""
pydoit example (closure style): download a CSV file and load it into a
PostgreSQL table using Polars.

Each task creator (task_*) defines its own action functions inside it. The
actions are closures: they can see the task creator's variables, and doit
passes `targets` / `dependencies` into them by parameter name.

Setup:
    pip install doit polars requests adbc-driver-postgresql pyarrow

Configure the database connection (defaults shown):
    export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/mydb"

Run:
    doit            # runs both tasks (download, then load)
    doit list       # show available tasks
    doit forget -a  # clear doit state to force everything to re-run
"""
import os
from pathlib import Path

import polars as pl
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CSV_URL = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "iris.csv"
TABLE_NAME = "iris"
# Plain libpq-style URI (used by the ADBC driver)
DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/mydb",
)

DOIT_CONFIG = {
    "default_tasks": ["load_csv_to_postgres"],  # `doit` with no args runs this (and its deps)
    "verbosity": 2,  # show print output while tasks run
}


# ---------------------------------------------------------------------------
# doit tasks (task creators); actions are closures defined inside each one
# ---------------------------------------------------------------------------
def task_download_csv():
    """Download the CSV file."""

    def download_csv(targets):
        # `targets` is injected by doit from the 'targets' list below.
        # CSV_URL is captured from the enclosing module scope.
        dest = Path(targets[0])
        dest.parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(CSV_URL, timeout=60)
        response.raise_for_status()
        dest.write_bytes(response.content)
        print(f"Downloaded {CSV_URL} -> {dest} ({len(response.content)} bytes)")

    return {
        "actions": [download_csv],
        "targets": [CSV_PATH],      # doit skips the task if the target exists
        "uptodate": [True],         # ...and won't re-download unless the target is missing
        "clean": True,              # `doit clean` deletes the target file
    }


def task_load_csv_to_postgres():
    """Load the downloaded CSV into a PostgreSQL table."""

    def load_to_postgres(dependencies):
        # `dependencies` is injected by doit from the 'file_dep' list below.
        df = pl.read_csv(dependencies[0])
        df.write_database(
            table_name=TABLE_NAME,
            connection=DB_URL,
            if_table_exists="replace",
            engine="adbc",
        )
        print(f"Loaded {df.height} rows into table '{TABLE_NAME}'")

    def table_exists():
        """uptodate check: True if the table already exists in the database."""
        try:
            result = pl.read_database_uri(
                query=f"SELECT to_regclass('public.{TABLE_NAME}') IS NOT NULL AS exists",
                uri=DB_URL,
                engine="adbc",
            )
            return bool(result["exists"][0])
        except Exception:
            return False  # DB unreachable -> treat as not up to date so the task runs/errors visibly

    return {
        "actions": [load_to_postgres],
        "file_dep": [CSV_PATH],     # re-run if the CSV content changes
        "task_dep": ["download_csv"],
        # also re-run if the table was dropped from the database
        "uptodate": [table_exists],
        "verbosity": 2,
    }