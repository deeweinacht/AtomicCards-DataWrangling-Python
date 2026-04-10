"""
Register warehouse parquet outputs as core.* DuckDB views and 
build analytics.* semantic views from SQL definitions in sql/views.
"""

import duckdb
from pathlib import Path
import json

WAREHOUSE_DIR = Path("data/warehouse")
ANALYTICS_DB = Path("data/analytics/product_and_design_analytics.duckdb")
SCHEMAS_FILE = Path("data/schemas.json")
VIEWS_DIR = Path("sql/views")
MACROS_DIR = Path("sql/macros")
TABLE_SCHEMAS = {}


def load_warehouse_schema() -> None:
    with open(SCHEMAS_FILE, "r", encoding="utf-8") as f:
        global TABLE_SCHEMAS
        TABLE_SCHEMAS = json.load(f)


def create_analytics_schemas(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS core;")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")


def load_warehouse_tables(con: duckdb.DuckDBPyConnection):
    print("\nLoading warehouse tables into DuckDB...")
    for table_name in TABLE_SCHEMAS["warehouse_tables"].keys():
        parquet_path = WAREHOUSE_DIR / f"core_{table_name}.parquet"
        if not parquet_path.is_file():
            raise FileNotFoundError(f"Missing warehouse file {parquet_path}")
        try:
            con.execute(f"""
                CREATE OR REPLACE VIEW core.{table_name} AS
                SELECT *
                FROM read_parquet('{parquet_path.as_posix()}');
            """)
            print(f"Loaded {table_name} into DuckDB")
        except Exception as e:
            raise RuntimeError(f"Failed to load {table_name} into DuckDB: {e}") from e

def add_macro_functions(con: duckdb.DuckDBPyConnection):
    print("\nAdding macros to DB...")
    for macro_file in sorted(MACROS_DIR.glob("*.sql")):
        if not macro_file.is_file():
            raise FileNotFoundError(f"Missing view file: {macro_file}")
        try:
            view_sql = macro_file.read_text(encoding="utf-8")
            con.execute(view_sql)
        except Exception as e:
            raise RuntimeError(f"Failed to add {macro_file.name} macro into DuckDB: {e}") from e

def build_analytics_semantic_layer(con: duckdb.DuckDBPyConnection):
    print("\nBuilding DuckDB analytics layer as semantic views...")
    for view_file in sorted(VIEWS_DIR.glob("*.sql")):
        if not view_file.is_file():
            raise FileNotFoundError(f"Missing view file: {view_file}")
        try:
            view_sql = view_file.read_text(encoding="utf-8")
            con.execute(view_sql)
            print(f"Added {view_file.name} view into DuckDB")
        except Exception as e:
            raise RuntimeError(f"Failed to add {view_file.name} view into DuckDB: {e}") from e



def load_core_tables_into_duckdb():
    ANALYTICS_DB.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=ANALYTICS_DB, read_only=False)
    try:
        load_warehouse_schema()
        create_analytics_schemas(con)
        load_warehouse_tables(con)
        add_macro_functions(con)
        build_analytics_semantic_layer(con)
    finally:
        con.close()
    print("\nDuckDB analytics layer built successfully.")


if __name__ == "__main__":
    load_core_tables_into_duckdb()
