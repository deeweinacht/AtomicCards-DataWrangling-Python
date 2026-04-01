import duckdb
from pathlib import Path
import json

WAREHOUSE_DIR = Path("data/warehouse")
ANALYTICS_DB = Path("data/analytics/analytics.duckdb")
SCHEMAS_FILE = Path("docs/schemas.json")
TABLE_SCHEMAS = {}

con = duckdb.connect(database=ANALYTICS_DB, read_only=False)

def load_warehouse_schema() -> None:
    with open(SCHEMAS_FILE, "r", encoding="utf-8") as f:
        global TABLE_SCHEMAS
        TABLE_SCHEMAS = json.load(f) 

def create_analytics_schemas():
    con.execute("CREATE SCHEMA IF NOT EXISTS core;")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

def load_warehouse_tables():
    for table_name in TABLE_SCHEMAS["warehouse_tables"].keys():
        parquet_path = WAREHOUSE_DIR / f"core_{table_name}.parquet"
        con.execute(f"""
            CREATE OR REPLACE VIEW core.{table_name} AS
            SELECT *
            FROM read_parquet('{parquet_path.as_posix()}');
        """)

def load_warehouse_into_duckdb():
    load_warehouse_schema()
    create_analytics_schemas()
    load_warehouse_tables()

if __name__ == "__main__":
    load_warehouse_into_duckdb()