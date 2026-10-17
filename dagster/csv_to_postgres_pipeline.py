"""
Dagster example: download a CSV file and load it into a PostgreSQL table using Polars.

This version makes the dependency explicit and decouples asset names from function names:

    function download_iris_csv   ->  asset key "raw_iris_csv"
    function load_iris_to_postgres -> asset key "iris_postgres_table"

Setup:
    pip install dagster dagster-webserver polars requests adbc-driver-postgresql pyarrow

Configure the database connection (defaults shown):
    export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/mydb"

Run in the Dagster UI (then click "Materialize all"):
    dagster dev -f dagster_pipeline.py

Or materialize from the command line (select by ASSET KEY, not function name):
    dagster asset materialize --select 'raw_iris_csv+' -f dagster_pipeline.py
    dagster asset materialize --select '*iris_postgres_table' -f dagster_pipeline.py

Or just run this file directly:
    python dagster_pipeline.py
"""
import os
from pathlib import Path

import dagster as dg
import polars as pl
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CSV_URL = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "iris.csv"
TABLE_NAME = "iris"
DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/mydb",
)

# Asset keys defined once, so names are never repeated as bare strings.
CSV_ASSET_KEY = dg.AssetKey("raw_iris_csv")
TABLE_ASSET_KEY = dg.AssetKey("iris_postgres_table")


# ---------------------------------------------------------------------------
# Resource: wraps the Postgres connection details and write logic
# ---------------------------------------------------------------------------
class PostgresResource(dg.ConfigurableResource):
    """Writes Polars DataFrames to PostgreSQL via the ADBC driver."""

    connection_uri: str

    def write_dataframe(self, df: pl.DataFrame, table_name: str) -> None:
        df.write_database(
            table_name=table_name,
            connection=self.connection_uri,
            if_table_exists="replace",
            engine="adbc",
        )


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------
@dg.asset(
    key=CSV_ASSET_KEY,  # <-- asset name comes from here, NOT from the function name
    group_name="csv_to_postgres",
    description="Raw CSV file downloaded from the web.",
)
def download_iris_csv(context: dg.AssetExecutionContext) -> str:
    """Download the CSV to disk and return its path."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    response = requests.get(CSV_URL, timeout=60)
    response.raise_for_status()
    CSV_PATH.write_bytes(response.content)
    context.log.info(f"Downloaded {CSV_URL} -> {CSV_PATH} ({len(response.content)} bytes)")
    context.add_output_metadata(
        {
            "source_url": CSV_URL,
            "path": str(CSV_PATH),
            "size_bytes": len(response.content),
        }
    )
    return str(CSV_PATH)


@dg.asset(
    key=TABLE_ASSET_KEY,
    group_name="csv_to_postgres",
    description="The CSV data loaded into a PostgreSQL table.",
    # EXPLICIT DEPENDENCY: this asset's `csv_path` argument is fed by the
    # output of the asset whose key is CSV_ASSET_KEY. The parameter name
    # (`csv_path`) does not need to match the upstream asset or function name.
    ins={"csv_path": dg.AssetIn(key=CSV_ASSET_KEY)},
)
def load_iris_to_postgres(
    context: dg.AssetExecutionContext,
    csv_path: str,  # value returned by download_iris_csv
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    """Read the CSV with Polars and write it to PostgreSQL (replacing the table)."""
    df = pl.read_csv(csv_path)
    postgres.write_dataframe(df, TABLE_NAME)
    context.log.info(f"Loaded {df.height} rows into table '{TABLE_NAME}'")
    return dg.MaterializeResult(
        metadata={
            "table": TABLE_NAME,
            "row_count": df.height,
            "columns": df.columns,
            "preview": dg.MetadataValue.text(str(df.head(5))),
        }
    )


# ---------------------------------------------------------------------------
# Alternative (not used above): dependency WITHOUT passing data
# ---------------------------------------------------------------------------
# If the downstream asset only needs the upstream to have run (e.g. it reads
# the file from a known path), declare the edge with `deps` instead of `ins`:
#
# @dg.asset(key=TABLE_ASSET_KEY, deps=[CSV_ASSET_KEY])
# def load_iris_to_postgres(postgres: PostgresResource) -> dg.MaterializeResult:
#     df = pl.read_csv(CSV_PATH)
#     ...


# ---------------------------------------------------------------------------
# Definitions: what Dagster loads
# ---------------------------------------------------------------------------
defs = dg.Definitions(
    assets=[download_iris_csv, load_iris_to_postgres],
    resources={"postgres": PostgresResource(connection_uri=DB_URL)},
)


if __name__ == "__main__":
    result = dg.materialize(
        [download_iris_csv, load_iris_to_postgres],
        resources={"postgres": PostgresResource(connection_uri=DB_URL)},
    )
    print("Success!" if result.success else "Run failed.")