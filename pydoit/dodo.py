"""
pydoit example: download a CSV file and load it into a PostgreSQL table using Polars.

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
# Task actions (plain Python functions)
# ---------------------------------------------------------------------------
def download_csv(url, dest):
    """Download a file from `url` to `dest`."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    dest.write_bytes(response.content)
    print(f"Downloaded {url} -> {dest} ({len(response.content)} bytes)")


def load_to_postgres(csv_path, table_name, db_url):
    """Read a CSV with Polars and write it to a PostgreSQL table (replacing it)."""
    df = pl.read_csv(csv_path)
    df.write_database(
        table_name=table_name,
        connection=db_url,
        if_table_exists="replace",
        engine="adbc",
    )
    print(f"Loaded {df.height} rows into table '{table_name}'")


def table_exists(table_name, db_url):
    """uptodate check: True if the table already exists in the database."""
    try:
        result = pl.read_database_uri(
            query=f"SELECT to_regclass('public.{table_name}') IS NOT NULL AS exists",
            uri=db_url,
            engine="adbc",
        )
        return bool(result["exists"][0])
    except Exception:
        return False  # DB unreachable -> treat as not up to date so the task runs/errors visibly


# ---------------------------------------------------------------------------
# doit tasks (functions named task_*)
# ---------------------------------------------------------------------------
def task_download_csv():
    """Download the CSV file."""
    return {
        "actions": [(download_csv, [CSV_URL, CSV_PATH])],
        "targets": [CSV_PATH],      # doit skips the task if the target exists
        "uptodate": [True],         # ...and won't re-download unless the target is missing
        "clean": True,              # `doit clean` deletes the target file
    }


def task_load_csv_to_postgres():
    """Load the downloaded CSV into a PostgreSQL table."""
    return {
        "actions": [(load_to_postgres, [CSV_PATH, TABLE_NAME, DB_URL])],
        "file_dep": [CSV_PATH],     # re-run if the CSV content changes
        "task_dep": ["download_csv"],
        # also re-run if the table was dropped from the database
        "uptodate": [(table_exists, [TABLE_NAME, DB_URL])],
        "verbosity": 2,
    }
