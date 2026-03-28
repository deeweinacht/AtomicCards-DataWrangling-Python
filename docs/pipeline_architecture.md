# Pipeline Architecture

## Extract

**[extract_mtgjson_atomic_cards.py](../scripts/extract_mtgjson_atomic_cards.py)** 
Downloads the MTGJSON AtomicCards dataset, verifies download integrity using the published SHA256 hash, and extracts the archive.

The script also records metadata about the source dataset for reproducibility and provenance.

Outputs to /data/raw

## Transform

**[select_paper_constructed_cards.py](../scripts/select_paper_constructed_cards.py)**
Filters the AtomicCards dataset to include only cards relevant to paper constructed formats.

Excludes cards that:
- are not legal in any supported paper format
- have non-playable layouts (tokens, planes, schemes, etc.)
- are marked as joke/unset cards

This step reduces the full dataset to the subset relevant for deckbuilding and gameplay analysis.

Outputs to /data/selected

**[created_staging_tables.py]**
Normalizes the curated JSON dataset into structured pandas DataFrames then saves into Parquet files.

This step flattens the nested AtomicCards structure and prepares tables representing cards, faces, types, keywords, and sets.

Outputs to /data/staging.


**[build_analytics_tables.py]**
Creates analytics-ready warehouse tables by adding derived features, flags, and analysis-friendly fields.

Outputs both CSV and Parquet versions for compatibility with common analytics tools.

Outputs to /data/warehouse

## Load

**[load_sqlite_database.py]**
Loads the warehouse tables into a SQLite database to confirm relational integrity and SQL compatibility.

This step verifies that the final dataset can be used directly in a relational analytics environment.

## Example Queries
**[product_analytic_queries.sql]**

