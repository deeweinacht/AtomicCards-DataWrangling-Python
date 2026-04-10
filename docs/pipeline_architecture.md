# Pipeline Architecture

## Data Folder Structure

data/
  raw/        Original downloaded MTGJSON source files
  selected/   Filtered source extracts restricted to paper-constructed scope
  staging/    Intermediate normalized datasets used during transformation
  warehouse/  Curated analytics-ready parquet outputs at stable business grains
  analytics/  DuckDB database file used to expose the semantic/query layer
  output/     Final query result exports and execution metadata

## Naming Conventions

Naming conventions:
- stg_*: staging-layer datasets used for normalized intermediate transformations
- core_*: warehouse-layer datasets at stable analytical grain
- analytics.*: DuckDB semantic views built on top of core parquet-backed outputs
- numbered SQL files: final business queries and semantic view definitions in deterministic review order

## Pipeline Orchestration

**[run_pipeline.py](../scripts/run_pipeline.py)
The main orchestration entrypoint for the ETL workflow. 

It sequentially executes extraction, project-scope filtering, staging transformations, warehouse modeling, and DuckDB analytics-layer loading.
Final business queries are executed separately through [run_queries](../scripts/run_queries.py).

## Extract

**[extract_mtgjson_data.py](../scripts/extract_mtgjson_data.py)** 
This step establishes a reproducible, integrity-checked raw data foundation before project-specific filtering and transformation begin.

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

**[build_staging_tables.py]**
Normalizes the curated JSON dataset into structured pandas DataFrames then saves into Parquet files.

This step flattens the nested AtomicCards structure and prepares tables representing cards, faces, types, keywords, and sets.

Outputs to /data/staging.


**[build_core_tables.py]**
This warehouse layer derives stable analytical features from staging outputs while preserving selected nested source attributes needed for downstream semantic modeling.

Canonical warehouse outputs are written as parquet, with preview CSVs included for quick inspection.

Outputs to /data/warehouse

## Load

**[load_into_analytics_database.py]**
The load step initializes a local DuckDB analytics database, registers warehouse parquet outputs as core.* views, and builds business-facing analytics.* semantic views from version-controlled SQL files.

Adds 4 semantic layer analytics views on top of the warehouse tables.

- analytics.v_card_design_features
    Grain: one row per card.
    Purpose: reusable card-level design attributes for trend and complexity analysis.
- analytics.v_creature_design_features
    Grain: one row per valid creature face.
    Purpose: creature-only efficiency and stat analysis.
- analytics.v_color_mechanics
    Grain: one row per card-mechanic observation.
    Purpose: color identity and mechanic distribution analysis.
- analytics.v_set_design_profile
    Grain: one row per set.
    Purpose: compact release-level fingerprint for consistency and design analysis.
    Note: Set-level profiles are based on card membership in any printed set, not only original printings.
    Note: "_share" columns do not total to 100%, because cards can fit multiple categories (e.g. creature type and artifact type)

## Query

**[run_queries.py]**
Querying the DuckDB views to answer product and design questions.

