# Decisions Log

## Project Scope

**In scope**

- source selection and filtering
- staged transformations
- warehouse-layer modeling
- semantic SQL views
- representative business queries and CSV outputs

**Out of scope**

- gameplay outcomes
- tournament or win-rate analysis
- sales or market performance
- BI/dashboard delivery

## Dataset Filtering

This project narrows the full MTGJSON dataset to cards that are relevant to real-world deckbuilding in paper-constructed play. The goal is to analyze card design, not art variation, print treatment, or other release details that do not change the underlying design questions being studied.

The filtering layer reduces the MTGJSON source universe to a paper-constructed analytical scope that is suitable for card-design analysis. It excludes digital-only content, non-constructed game objects, selected experimental or joke-only card types, and cards printed exclusively in un-set products.


### Card Selection

The following inclusion and exclusion rules were applied to the MTGJSON AtomicCards dataset to produce the filtered dataset atomiccards_paper_constructed.json.

**Included**

This project filters MTGJSON AtomicCards to a paper-constructed analytical scope rather than retaining the full source universe. Cards are included when they are legal or restricted in at least one selected paper-constructed format and are represented as fully released physical cards suitable for deck construction.


**Excluded**

Exclusions remove records that would distort card-design analysis or add non-comparable entities to the modeled dataset, including humor/acorn cards, non-constructed game objects, draft-only conspiracy-style content, reversible duplicate layouts, and cards printed exclusively in un-set products.

### Set Selection

The following exclusion rules were applied to the MTGJSON Sets dataset to produce `setlist_paper_constructed.json`.

Excluded sets were limited to those that are definitively not relevant to paper-constructed play:

- sets flagged as `Online Only`
- set types such as `memorabilia` (art cards), `tokens` (temporary indicators), and `minigames` (separate products from MTG)
- `UNSET` set codes, because cards printed exclusively in un-set products represent intentionally experimental designs that do not belong in mainstream card-design analysis
- `PMEI` set code, because it behaves like a long-running promotional grouping rather than a standard comparable set release

## Staging Tables

Core challenge - the product has had many layout variations of cards over its 30-year history, including cards with multiple faces.

The cleanest way to handle this was to create separate tables at card grain and face grain. Each card is related to one or more faces.
This allows clean analysis of features that are inherent to the entire physical card, as well as features that are face-specific.

data/schemas.json defines the expected structure of both staging and warehouse outputs. The staging pipeline uses it as the source of truth for required columns, target dtypes, and validation checks before parquet outputs are written.

Some fields (e.g. power, toughness) are generally numeric, but sometimes are variable (e.g. "*+1), therefore these fields are maintained as strings in the staging tables.

Staging null-handling is applied before schema coercion so that fields representing “not applicable” can remain null where meaningful, while fields used as normalized analytical attributes are filled to stable defaults.

## Warehouse Tables

Card-grain rollups aggregate face, type, keyword, legality, and printing data into stable `core_cards` tables. This creates reusable analytical entities at a consistent card-level grain that supports repeated business analysis without re-deriving from staging tables each time.

Face-grain stat derivation in `core_card_faces` computes numeric versions of power, toughness, loyalty, and defense alongside variable-value flags. This preserves printed text values for auditability while enabling numeric analysis that distinguishes ordinary stats from symbolic or variable cases.

Schema validation is a hard build gate that fails the warehouse build if outputs do not match `schemas.json`. This ensures data contract stability across pipeline runs and prevents downstream analysis from consuming malformed warehouse tables.

Preview CSVs are generated alongside canonical Parquet outputs for reviewer inspection* This supports portfolio auditability by making warehouse table contents human-readable without requiring a query engine or Parquet reader.
## Analytics Layer

After loading curated warehouse tables into DuckDB, a semantic view layer was added to support stakeholder-facing analysis without changing the warehouse source-of-truth tables. This layer exists to keep the warehouse tables stable and reusable, while expressing analysis-specific business logic such as design complexity proxies, creature stat efficiency, color-mechanic mappings, and set-level design profiles in a form that is easier to query and interpret.

The semantic views are not 1:1 copies of warehouse tables. Instead, they encapsulate repeated joins, derived metrics, and standardized inclusion logic so that final SQL analyses remain concise, consistent, and easier to audit. This follows a common layered analytics pattern in which staging models standardize raw data, warehouse or core models preserve cleaned entities, and downstream semantic or mart models expose business-ready concepts for reporting and analysis.

For this project, the semantic layer was introduced specifically to answer product/design questions about card design evolution, color identity stability, and set composition. Metrics such as complexity stand-ins or creature stat efficiency are interpretive analytical features rather than canonical source data, so they are defined in views rather than persisted back into the warehouse tables.

### Semantic Metric Placement

Interpretive analytical metrics such as `design_complexity_score`, `stat_total`, `stat_efficiency`, and set-level development profiles were defined in semantic views rather than persisted into warehouse tables.

This keeps the warehouse layer closer to reusable modeled source truth while reserving analysis-specific logic and proxy metrics for the business-facing semantic layer.

### Set-Level Profiling Logic

Set-level design profiles are based on card membership across all recorded printings rather than only original printings.

This decision was made so that set analysis reflects the composition of a set as printed, which is more useful for release-level design profiling, while acknowledging that this differs from a “newly introduced cards only” view of set design.

## Storage and Query Engine

### DuckDB as the Local Analytics Layer

DuckDB was used as the local analytics engine to expose warehouse outputs through a lightweight semantic layer and support version-controlled SQL analysis without requiring an external database service.

This choice keeps the project easy to run locally while still demonstrating warehouse-to-semantic-layer modeling patterns.

### Parquet as the Warehouse Storage Format

Staging and warehouse outputs are written as Parquet files rather than being persisted only inside DuckDB.

This keeps intermediate and modeled datasets engine-agnostic, enables efficient columnar reads for analytical workloads, and separates transformation outputs from the query-serving layer.

