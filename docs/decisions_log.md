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

This project focuses on cards relevant to real-world deckbuilding in paper-constructed formats. The MTG AtomicCards dataset was selected as the scope of this project is considering mechanically distinct products, where differences in art and printing are not relevant to the design questions and final analysis.

The filtering layer narrows MTGJSON source data to a paper-constructed analytical scope suitable for card-design analysis. It excludes digital-only content, non-constructed game objects, selected experimental/joke-only card types, and cards printed exclusively in defined un-set products.


### Card Selection

The following inclusion and exclusion rules were applied to the MTGJSON AtomicCards dataset to produce the filtered dataset atomiccards_paper_constructed.json.

**Included**

This project filters MTGJSON AtomicCards to a paper-constructed analytical scope rather than retaining the full source universe. Cards are included when they are legal or restricted in at least one selected paper-constructed format and are represented as fully released physical cards suitable for deck construction.


**Excluded**

Exclusions remove records that would distort card-design analysis or add non-comparable entities to the modeled dataset, including humor/acorn cards, non-constructed game objects, draft-only conspiracy-style content, reversible duplicate layouts, and cards printed exclusively in un-set products.

### Set Selection

The following exclusion rules were applied to the MTGJSON Sets dataset to produce the filtered dataset setlist_paper_constructed.json.

Excluded only sets that are definitively not for paper constructed play:
- flagged "Online Only"
- set types "memorabilia" (art cards), "tokens" (temporary indicators), or "minigames" (separate product from MTG)
- UNSET set codes. Cards printed exclusively in defined un-set products are excluded to avoid mixing intentionally experimental designs into analyses of mainstream card-design patterns.
- PMEI set code. The PMEI set is excluded because its release metadata behaves as a long-running promotional grouping rather than a standard comparable set release, which would otherwise distort time-based set analysis.

## Staging Tables

Core challenge - the product has had many layout variations of cards over it's 30-year history, including cards with multiple faces.

The cleanest way to handle this was to create separate tables for a grain of card and a grain of face. Each card is related to one or more faces.
The allows clean analysis of features that are inherent to the entire physical card, as well as features that are face-specific.

data/schemas.json defines the expected structure of both staging and warehouse outputs. The staging pipeline uses it as the source of truth for required columns, target dtypes, and validation checks before parquet outputs are written.

Some fields (e.g. power, toughness) are generally numeric, but sometimes are variable (e.g. "*+1), therefore these fields are maintained as strings in the staging tables.

Staging null-handling is applied before schema coercion so that fields representing “not applicable” can remain null where meaningful, while fields used as normalized analytical attributes are filled to stable defaults.

## Warehouse Tables

core_card_faces keeps printed stat fields in their original text form while adding numeric versions and variable-value flags, allowing later analysis to distinguish numeric creature stats from special or symbolic values.

### Semantive View Layer

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

