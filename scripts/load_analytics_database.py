import duckdb
from pathlib import Path

from pyarrow import json

WAREHOUSE_DIR = Path("data/warehouse")
SCHEMAS_FILE = Path("docs/schemas.json")
TABLE_SCHEMAS = {}

con = duckdb.connect(database=WAREHOUSE_DIR.joinpath("analytics.duckdb"), read_only=False)

def load_analytics_tables():
    for table_name in TABLE_SCHEMAS["analytics_tables"].keys():
        con.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                        SELECT *
                        FROM read_parquet('{WAREHOUSE_DIR}/analytics_{table_name}.parquet');
                    """)
    
def load_table_schemas() -> None:
    with open(SCHEMAS_FILE, "r", encoding="utf-8") as f:
        global TABLE_SCHEMAS
        TABLE_SCHEMAS = json.load(f) 

if __name__ == "__main__":
    load_table_schemas()
    load_analytics_tables()