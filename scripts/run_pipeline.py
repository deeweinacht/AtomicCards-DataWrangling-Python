"""
Run the ETL pipeline from source extraction through analytics-layer loading.

Steps:
1. Extract MTGJSON source datasets.
2. Filter the source data to the paper-constructed project scope.
3. Build normalized staging datasets.
4. Build curated warehouse-layer datasets.
5. Load warehouse outputs into the DuckDB analytics layer for semantic querying.
"""

from extract_mtgjson_data import extract_mtgjson_datasets
from filter_paper_constructed_cards import filter_for_paper_constructed
from build_staging_tables import build_staging_tables
from build_core_tables import build_warehouse_core_tables
from load_into_analytics_database import load_core_tables_into_duckdb


def main():
    print("Running ETL pipeline...")

    print("Step 1/5: Extracting MTGJSON datasets...")
    extract_mtgjson_datasets()

    print("\nStep 2/5: Filtering cards and sets by paper constructed scope...")
    filter_for_paper_constructed()

    print("\nStep 3/5: Building staging tables...")
    build_staging_tables()

    print("\nStep 4/5: Building warehouse core tables...")
    build_warehouse_core_tables()

    print("\nStep 5/5: Loading core tables into DuckDB analytics layer...")
    load_core_tables_into_duckdb()

    print("\nETL pipeline complete.")


if __name__ == "__main__":
    main()
