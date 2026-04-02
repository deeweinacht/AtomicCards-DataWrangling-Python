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


**[build_core_tables.py]**
Creates core analytics-ready warehouse tables by adding derived features, flags, and analysis-friendly fields.

Outputs both CSV and Parquet versions for compatibility with common analytics tools.

Outputs to /data/warehouse

## Load

**[load_into_analytics_database.py]**
The warehouse tables are exposed in DuckDB by registering Parquet-backed views.

This provides a SQL query layer without duplicating storage, enabling efficient analytical queries.

**[add_semantic_layer.py]**

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

Querying the DuckDB views to answer product and design questions.

