"""
This script runs the entire data pipeline:
 1. Extract the dataset from MTGJSON and select only the paper constructed-legal cards.
 2. Build staging tables from the selected data.
 3. Build analytics tables from the staging tables.
 4. Load the analytics tables into a DuckDB database for querying and analysis."""

from extract_mtgjson_data import extract_mtgjson_datasets
from select_paper_constructed_cards import select_paper_constructed
from build_staging_tables import build_staging_tables
from build_core_tables import build_warehouse_tables
from load_into_analytics_database import load_warehouse_into_duckdb

if __name__ == "__main__":
    # extract_mtgjson_datasets()
    select_paper_constructed()
    build_staging_tables()
    build_warehouse_tables()
    load_warehouse_into_duckdb()
