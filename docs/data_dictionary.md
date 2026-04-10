# Data Dictionary

## Purpose

This document defines the main datasets and analytical views used in the MTGJSON Card Design Analytics project. It focuses on the structure, meaning, and intended use of the modeled data rather than reproducing full source-system documentation.

The project follows a layered design:

- **Staging layer** — normalized, source-aligned intermediate tables
- **Warehouse layer** — curated analytical entities with reusable derived features
- **Semantic view layer** — business-facing views used to answer the project’s design and product questions

This dictionary uses a selective documentation scope:
- brief summaries for staging tables
- fuller field documentation for warehouse tables
- fuller field documentation for semantic views

## Scope notes

- The project is restricted to a **filtered paper-constructed analytical scope**, not the full MTGJSON dataset.
- Cards and sets excluded during filtering include digital-only content, non-constructed game objects, humor/acorn content, selected non-comparable layouts, and cards or sets that would distort mainstream card-design analysis.
- Set-level analyses are based on the project’s filtered paper-constructed card universe rather than every printed object in MTGJSON.

## Layer guide

### Staging layer

The staging layer preserves source fidelity while flattening the nested MTGJSON AtomicCards structure into explicit analytical grains. It is designed to normalize records, standardize dtypes, and preserve ambiguous or semi-structured source fields for later modeling.

### Warehouse layer

The warehouse layer adds reusable analytical features while preserving stable entity grains. It is the main modeled source of truth for downstream analysis.

### Semantic view layer

The semantic view layer expresses analysis-specific logic in reusable DuckDB views. These views standardize repeated joins, define project-specific proxy metrics, and expose business-facing concepts used in the final SQL analyses.

# Staging Tables

## `stg_cards`

**Grain:** one row per card  
**Purpose:** preserves card-level attributes that apply to the physical card as a whole rather than to an individual face

| Column               | Type       | Definition                                                      | Notes                                                        |
| -------------------- | ---------- | --------------------------------------------------------------- | ------------------------------------------------------------ |
| `id`                 | `str`      | Stable card identifier derived from the MTGJSON Atomic card key | Uses a slugified Atomic card key; treated as unique          |
| `color_identity`     | `object`   | Card color identity                                             | Stored as a string array such as `["B"]` or `["G", "R"]`     |
| `edh_rec_rank`       | `Int64`    | EDHREC popularity rank from source                              | Nullable; primarily relevant for Commander-oriented analysis |
| `has_alt_deck_limit` | `bool`     | Indicates whether the card uses an alternative deck-limit rule  | Source-preserved boolean                                     |
| `is_game_changer`    | `bool`     | Indicates whether the card is flagged as a Game Changer         | Source-preserved boolean                                     |
| `is_reserved`        | `bool`     | Indicates whether the card is on the Reserved List              | Source-preserved boolean                                     |
| `layout`             | `category` | Card layout classification                                      | Used for filtering and layout-aware modeling                 |
| `legalities`         | `object`   | Per-format legality mapping                                     | Preserved as an object for downstream legality summarization |
| `mana_value`         | `Int64`    | Card mana value                                                 | Card-level mana value                                        |
| `name`               | `str`      | Card name                                                       | Card-level display name                                      |
| `printings`          | `object`   | Set printings associated with the card                          | Preserved for first-printing and set profile analysis        |
| `rulings`            | `object`   | Source rulings collection                                       | Preserved for complexity-style proxy analysis                |

## `stg_faces`

**Grain:** one row per card face  
**Purpose:** stores face-specific attributes needed to model multi-face cards cleanly and support printed-stat analysis

| Column       | Type       | Definition                    | Notes                                                         |
| ------------ | ---------- | ----------------------------- | ------------------------------------------------------------- |
| `card_id`    | `str`      | Foreign key to `stg_cards.id` | Card-level identifier                                         |
| `face_index` | `Int64`    | Face position within the card | Stable face-level key with `card_id`                          |
| `name`       | `str`      | Face name                     | For single-face cards this may match the card name            |
| `side`       | `category` | Face side identifier          | Often used for multi-face cards such as `a` / `b`             |
| `colors`     | `object`   | Face color representation     | Preserved as object because it is multi-valued                |
| `defense`    | `str`      | Printed defense value         | Stored as string in staging; never variable in practice       |
| `mana_value` | `Int64`    | Face mana value               | Face-level mana value                                         |
| `loyalty`    | `str`      | Printed loyalty value         | Stored as string to preserve non-numeric printed values       |
| `mana_cost`  | `str`      | Printed mana cost             | Stored as strings such as `"{1}{B}"`                          |
| `power`      | `str`      | Printed power value           | Stored as string because some values are symbolic or variable |
| `text`       | `str`      | Printed rules text            | Used later for text-density metrics                           |
| `toughness`  | `str`      | Printed toughness value       | Stored as string because some values are symbolic or variable |
| `type`       | `str`      | Printed type line             | Face-level printed type string                                |

## `stg_keywords`

**Grain:** one row per card-keyword pair  
**Purpose:** normalizes keyword mechanics into a reusable bridge-style table

| Column    | Type  | Definition                    | Notes                                |
| --------- | ----- | ----------------------------- | ------------------------------------ |
| `card_id` | `str` | Foreign key to `stg_cards.id` | Card-level identifier                |
| `keyword` | `str` | Keyword mechanic              | One row per card-keyword combination |

## `stg_types`

**Grain:** one row per face-type pair  
**Purpose:** normalizes type-line components into reusable analytical records

| Column       | Type       | Definition                    | Notes                                                                        |
| ------------ | ---------- | ----------------------------- | ---------------------------------------------------------------------------- |
| `card_id`    | `str`      | Foreign key to `stg_cards.id` | Card-level identifier                                                        |
| `face_index` | `Int64`    | Foreign key to face grain     | Used with `card_id` to identify the face                                     |
| `type_kind`  | `category` | Type category                 | Values represent normalized type classes such as type, supertype, or subtype |
| `type`       | `str`      | Type value                    | One normalized type token per row                                            |

## `stg_sets`

**Grain:** one row per set  
**Purpose:** preserves set-level release metadata used for first-printing and set-profile analysis

| Column         | Type             | Definition              | Notes                                                  |
| -------------- | ---------------- | ----------------------- | ------------------------------------------------------ |
| `id`           | `str`            | Set code                | Stable set identifier                                  |
| `parent_set`   | `str`            | Parent set code         | Nullable; used where source models set hierarchies     |
| `name`         | `str`            | Set name                | Human-readable set name                                |
| `release_date` | `datetime64[ns]` | Set release date        | Used for time-based and first-printing analysis        |
| `block`        | `str`            | Set block               | Nullable for sets outside block structures             |
| `type`         | `category`       | Set type                | Used in downstream filtering and interpretation        |
| `total_cards`  | `Int64`          | Source total card count | Represents source set size, not filtered project scope |

# Warehouse Tables

## `core_cards`

**Grain:** one row per card  
**Purpose:** primary card-level analytical entity combining preserved source attributes with reusable derived design features

| Column                | Type             | Definition                                                | Notes                                                                |
| --------------------- | ---------------- | --------------------------------------------------------- | -------------------------------------------------------------------- |
| `id`                  | `str`            | Card identifier                                           | Stable warehouse card key derived from the slugified Atomic card key |
| `name`                | `str`            | Card name                                                 | Card-level display name                                              |
| `mana_value`          | `Int64`          | Card mana value                                           | Preserved from source/staging                                        |
| `color_identity`      | `object`         | Card color identity                                       | Stored as a string array such as `["W"]` or `["U", "B"]`             |
| `edh_rec_rank`        | `Int64`          | EDHREC rank                                               | Nullable source-derived popularity field                             |
| `has_alt_deck_limit`  | `bool`           | Alternative deck limit flag                               | Source-preserved boolean                                             |
| `is_game_changer`     | `bool`           | Game Changer flag                                         | Source-preserved boolean                                             |
| `is_reserved`         | `bool`           | Reserved List flag                                        | Source-preserved boolean                                             |
| `layout`              | `category`       | Layout classification                                     | Preserved from staging                                               |
| `color_count`         | `Int64`          | Number of represented colors across faces                 | Derived from face-level color flags                                  |
| `is_colorless`        | `bool`           | Indicates no represented colors                           | Derived from color count                                             |
| `is_mono`             | `bool`           | Indicates exactly one represented color                   | Derived from color count                                             |
| `is_multi`            | `bool`           | Indicates more than one represented color                 | Derived from color count                                             |
| `is_white`            | `bool`           | White color flag                                          | Derived from face-level colors                                       |
| `is_blue`             | `bool`           | Blue color flag                                           | Derived from face-level colors                                       |
| `is_black`            | `bool`           | Black color flag                                          | Derived from face-level colors                                       |
| `is_red`              | `bool`           | Red color flag                                            | Derived from face-level colors                                       |
| `is_green`            | `bool`           | Green color flag                                          | Derived from face-level colors                                       |
| `face_count`          | `Int64`          | Number of distinct faces                                  | Derived from `core_card_faces`                                       |
| `text_length`         | `Int64`          | Total printed text length across faces                    | Character-count style measure                                        |
| `text_tokens`         | `Int64`          | Total printed text token count across faces               | Used in downstream text-density and complexity proxies               |
| `is_legendary`        | `bool`           | Indicates whether any face has the `legendary` type token | Derived from normalized types                                        |
| `type_count`          | `Int64`          | Count of distinct normalized type values                  | Derived from `core.types`                                            |
| `keyword_count`       | `Int64`          | Count of distinct keyword mechanics                       | Derived from `core.keywords`                                         |
| `first_printing_date` | `datetime64[ns]` | Earliest release date across known printings              | Derived by exploding printings and joining to sets                   |
| `first_printing_set`  | `str`            | Set code of earliest known printing                       | Derived alongside `first_printing_date`                              |
| `legalities_count`    | `Int64`          | Count of formats where the card is legal or restricted    | Derived from `legalities` object                                     |
| `rulings_count`       | `Int64`          | Count of source rulings entries                           | Used in complexity-style proxy logic                                 |
| `printings_count`     | `Int64`          | Count of known set printings                              | Derived from `printings` object                                      |
| `is_legal_commander`  | `bool`           | Commander legality flag                                   | Derived from `legalities`                                            |
| `is_legal_legacy`     | `bool`           | Legacy legality flag                                      | Derived from `legalities`                                            |
| `is_legal_modern`     | `bool`           | Modern legality flag                                      | Derived from `legalities`                                            |
| `is_legal_pauper`     | `bool`           | Pauper legality flag                                      | Derived from `legalities`                                            |
| `legalities`          | `object`         | Raw legality mapping                                      | Retained for semantic-layer reuse                                    |
| `printings`           | `object`         | Raw printings collection                                  | Retained for semantic-layer reuse                                    |
| `rulings`             | `object`         | Raw rulings collection                                    | Retained for semantic-layer reuse                                    |

## `core_card_faces`

**Grain:** one row per card face  
**Purpose:** preserves face-level printed attributes while adding numeric stat fields and flags that distinguish numeric values from symbolic or variable printed values

| Column                  | Type       | Definition                                                                  | Notes                                                              |
| ----------------------- | ---------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `card_id`               | `str`      | Foreign key to `core_cards.id`                                              | Card-level identifier                                              |
| `face_index`            | `Int64`    | Face position within the card                                               | Stable face-level key with `card_id`                               |
| `name`                  | `str`      | Face name                                                                   | Face-level display name                                            |
| `side`                  | `category` | Face side identifier                                                        | Useful for multi-face card interpretation                          |
| `colors`                | `object`   | Face color representation                                                   | Preserved as object                                                |
| `power`                 | `str`      | Printed power                                                               | Preserved as text                                                  |
| `toughness`             | `str`      | Printed toughness                                                           | Preserved as text                                                  |
| `mana_value`            | `Int64`    | Face mana value                                                             | Face-level mana value                                              |
| `mana_cost`             | `str`      | Printed mana cost                                                           | Preserved as strings such as `"{1}{B}"`                            |
| `loyalty`               | `str`      | Printed loyalty                                                             | Preserved as text                                                  |
| `defense`               | `str`      | Printed defense                                                             | Preserved as text; never variable in practice                      |
| `power_num`             | `Float64`  | Numeric power where coercible                                               | Null when printed value is symbolic or non-numeric                 |
| `toughness_num`         | `Float64`  | Numeric toughness where coercible                                           | Null when printed value is symbolic or non-numeric                 |
| `loyalty_num`           | `Float64`  | Numeric loyalty where coercible                                             | Null when printed value is non-numeric or absent                   |
| `defense_num`           | `Float64`  | Numeric defense where coercible                                             | Defense is not variable, but remains nullable where not applicable |
| `power_is_variable`     | `bool`     | Indicates non-null printed power that could not be converted to numeric     | Used to distinguish symbolic printed stats                         |
| `toughness_is_variable` | `bool`     | Indicates non-null printed toughness that could not be converted to numeric | Used to distinguish symbolic printed stats                         |
| `loyalty_is_variable`   | `bool`     | Indicates non-null printed loyalty that could not be converted to numeric   | Used to distinguish symbolic printed stats                         |
| `mana_cost_is_variable` | `bool`     | Indicates variable mana-cost symbols such as `X`, `Y`, `Z`, or `*`          | Derived from printed mana cost text                                |
| `is_colorless`          | `bool`     | Indicates no represented colors on the face                                 | Derived from face colors                                           |
| `is_white`              | `bool`     | White color flag                                                            | Derived from face colors                                           |
| `is_blue`               | `bool`     | Blue color flag                                                             | Derived from face colors                                           |
| `is_black`              | `bool`     | Black color flag                                                            | Derived from face colors                                           |
| `is_red`                | `bool`     | Red color flag                                                              | Derived from face colors                                           |
| `is_green`              | `bool`     | Green color flag                                                            | Derived from face colors                                           |
| `pt_ratio`              | `Float64`  | Power-to-toughness ratio where calculable                                   | Null when toughness is missing, zero, or not usable                |
| `type`                  | `str`      | Printed type line                                                           | Face-level printed type string                                     |
| `text`                  | `str`      | Printed rules text                                                          | Used in downstream semantic analysis                               |

## `core_keywords`

**Grain:** one row per card-keyword pair  
**Purpose:** preserves normalized keyword mechanics for reusable joins and mechanic analysis

| Column    | Type  | Definition                     | Notes                                |
| --------- | ----- | ------------------------------ | ------------------------------------ |
| `card_id` | `str` | Foreign key to `core_cards.id` | Card-level identifier                |
| `keyword` | `str` | Keyword mechanic               | Normalized card-mechanic observation |

## `core_types`

**Grain:** one row per face-type pair  
**Purpose:** preserves normalized type-line tokens for reusable joins and type-based analytical logic

| Column       | Type       | Definition                     | Notes                                    |
| ------------ | ---------- | ------------------------------ | ---------------------------------------- |
| `card_id`    | `str`      | Foreign key to `core_cards.id` | Card-level identifier                    |
| `face_index` | `Int64`    | Face key component             | Used with `card_id` to identify the face |
| `type_kind`  | `category` | Normalized type category       | For example: type, supertype, subtype    |
| `type`       | `str`      | Type token                     | One normalized type token per row        |

## `core_sets`

**Grain:** one row per set  
**Purpose:** preserves set-level release metadata used in first-printing logic and set-level analytical profiling

| Column         | Type             | Definition              | Notes                                               |
| -------------- | ---------------- | ----------------------- | --------------------------------------------------- |
| `id`           | `str`            | Set code                | Stable set identifier                               |
| `parent_set`   | `str`            | Parent set code         | Nullable hierarchy field                            |
| `name`         | `str`            | Set name                | Human-readable set label                            |
| `release_date` | `datetime64[ns]` | Set release date        | Used in time-based analysis                         |
| `block`        | `str`            | Set block               | Nullable for non-block-era products                 |
| `type`         | `category`       | Set type                | Used in semantic-layer filtering and interpretation |
| `total_cards`  | `Int64`          | Source total card count | Represents full set size from source metadata       |

# Semantic Views

## `analytics.v_card_design_features`

**Grain:** one row per card  
**Purpose:** reusable card-level design view for trend, complexity, color-structure, and type-profile analysis

| Column                    | Definition                                  | Notes                                              |
| ------------------------- | ------------------------------------------- | -------------------------------------------------- |
| `card_id`                 | Card identifier                             | Derived from `core_cards.id`                       |
| `card_name`               | Card name                                   | Card-level display name                            |
| `first_printing_date`     | Earliest known printing date                | Cast to date in the semantic layer                 |
| `first_printing_year`     | Year of earliest known printing             | Derived from `first_printing_date`                 |
| `mana_value`              | Card mana value                             | Card-level value                                   |
| `color_identity`          | Card color identity                         | Preserved from warehouse table as a string array   |
| `color_count`             | Count of represented colors                 | Coalesced to `0` where needed                      |
| `is_colorless`            | Colorless flag                              | Boolean convenience field                          |
| `is_mono`                 | Monocolored flag                            | Boolean convenience field                          |
| `is_multi`                | Multicolored flag                           | Boolean convenience field                          |
| `is_white`                | White flag                                  | Boolean convenience field                          |
| `is_blue`                 | Blue flag                                   | Boolean convenience field                          |
| `is_black`                | Black flag                                  | Boolean convenience field                          |
| `is_red`                  | Red flag                                    | Boolean convenience field                          |
| `is_green`                | Green flag                                  | Boolean convenience field                          |
| `face_count`              | Number of faces                             | Coalesced to `0` where needed                      |
| `text_length`             | Total printed text length across faces      | Coalesced to `0` where needed                      |
| `text_tokens`             | Total printed text token count across faces | Coalesced to `0` where needed                      |
| `rulings_count`           | Count of rulings entries                    | Coalesced to `0` where needed                      |
| `printings_count`         | Count of known printings                    | Coalesced to `0` where needed                      |
| `design_complexity_score` | Project-defined design complexity proxy     | Calculated as `text_tokens + (rulings_count * 10)` |
| `is_creature`             | Creature type flag                          | Derived from normalized types                      |
| `is_artifact`             | Artifact type flag                          | Derived from normalized types                      |
| `is_enchantment`          | Enchantment type flag                       | Derived from normalized types                      |
| `is_instant`              | Instant type flag                           | Derived from normalized types                      |
| `is_sorcery`              | Sorcery type flag                           | Derived from normalized types                      |
| `is_land`                 | Land type flag                              | Derived from normalized types                      |
| `is_planeswalker`         | Planeswalker type flag                      | Derived from normalized types                      |

### Metric note

`design_complexity_score` is a heuristic analytical proxy. It is intended to approximate card-design complexity by combining printed text density with rulings volume, not to represent formal rules burden or official complexity.

Rulings are weighted more heavily because a single ruling often represents multiple lines of interpretive text. The multiplier of `10` is a project-defined weighting choice intended to balance rulings against text-token counts rather than a formally validated coefficient.

## `analytics.v_creature_design_features`

**Grain:** one row per creature face with numeric power, toughness, and positive mana value  
**Purpose:** creature-design trend analysis using numeric printed stats and simple efficiency-style proxies

| Column                   | Definition                                   | Notes                                        |
| ------------------------ | -------------------------------------------- | -------------------------------------------- |
| `card_id`                | Card identifier                              | Foreign key to card-level analysis           |
| `face_index`             | Face position within the card                | Face-level key component                     |
| `face_name`              | Face name                                    | Creature-face display name                   |
| `first_printing_year`    | Year of earliest known printing for the card | Joined from `v_card_design_features`         |
| `mana_value`             | Face mana value                              | Filtered to positive values in this view     |
| `power`                  | Numeric power                                | Derived from `core_card_faces.power_num`     |
| `toughness`              | Numeric toughness                            | Derived from `core_card_faces.toughness_num` |
| `pt_ratio`               | Power-to-toughness ratio                     | Preserved from warehouse layer               |
| `stat_total`             | Sum of power and toughness                   | Printed-stat proxy metric                    |
| `stat_efficiency`        | `stat_total / mana_value`                    | Printed-stat efficiency proxy                |
| `has_variable_mana_cost` | Variable mana-cost flag                      | Derived from warehouse layer                 |
| `is_colorless`           | Colorless flag                               | Face-level color flag                        |
| `is_white`               | White flag                                   | Face-level color flag                        |
| `is_blue`                | Blue flag                                    | Face-level color flag                        |
| `is_black`               | Black flag                                   | Face-level color flag                        |
| `is_red`                 | Red flag                                     | Face-level color flag                        |
| `is_green`               | Green flag                                   | Face-level color flag                        |

### Metric note

`stat_total` and `stat_efficiency` are printed-stat proxy metrics intended for design trend analysis. They should not be interpreted as formal gameplay balance metrics or as measures of creature overall effectiveness.

## `analytics.v_color_mechanics`

**Grain:** one row per card-keyword observation  
**Purpose:** supports mechanic prevalence and color-concentration analysis across cards

| Column                | Definition                      | Notes                                                           |
| --------------------- | ------------------------------- | --------------------------------------------------------------- |
| `card_id`             | Card identifier                 | Joined from keywords to card-level design features              |
| `mechanic`            | Keyword mechanic                | Derived from `core_keywords.keyword`                            |
| `first_printing_year` | Year of earliest known printing | Used for time-based mechanic analysis                           |
| `color_count`         | Count of represented colors     | Card-level color structure                                      |
| `is_white`            | White flag                      | Card-level color flag                                           |
| `is_blue`             | Blue flag                       | Card-level color flag                                           |
| `is_black`            | Black flag                      | Card-level color flag                                           |
| `is_red`              | Red flag                        | Card-level color flag                                           |
| `is_green`            | Green flag                      | Card-level color flag                                           |
| `color_category`      | High-level color bucket         | One of `Colorless`, `Monocolored`, `Multicolored`, or `Unknown` |

### Usage note

This view is best used for mechanic distribution, mechanic prevalence, and mono-color concentration analyses. Because the grain is card-mechanic, cards with multiple mechanics contribute multiple rows.

## `analytics.v_set_design_profile`

**Grain:** one row per set  
**Purpose:** compact set-level design fingerprint used for release comparison and set deviation analysis

| Column                           | Definition                                                    | Notes                                               |
| -------------------------------- | ------------------------------------------------------------- | --------------------------------------------------- |
| `set_id`                         | Set code                                                      | Stable set identifier                               |
| `set_name`                       | Set name                                                      | Human-readable set label                            |
| `release_date`                   | Set release date                                              | Cast to date in the semantic layer                  |
| `release_year`                   | Release year                                                  | Derived from `release_date`                         |
| `set_type`                       | Set type                                                      | Preserved from `core_sets.type`                     |
| `all_cards_in_set`               | Source total set size                                         | From set metadata, not filtered project scope       |
| `paper_constructed_cards_in_set` | Count of filtered project-scope cards mapped to the set       | Used as the denominator for set-profile shares      |
| `avg_mana_value`                 | Average mana value of filtered project-scope cards in the set | Set-level average                                   |
| `avg_color_count`                | Average color count                                           | Set-level average across filtered cards             |
| `avg_text_tokens`                | Average text token count                                      | Set-level text-density measure                      |
| `avg_rulings_count`              | Average rulings count                                         | Set-level rules-density measure                     |
| `avg_complexity_score`           | Average project-defined complexity proxy                      | Based on card-level `design_complexity_score`       |
| `creature_type_share`            | Share of filtered cards flagged as creatures                  | Based on the set’s filtered project-scope card pool |
| `artifact_type_share`            | Share of filtered cards flagged as artifacts                  | Same denominator as above                           |
| `enchantment_type_share`         | Share of filtered cards flagged as enchantments               | Same denominator as above                           |
| `instant_type_share`             | Share of filtered cards flagged as instants                   | Same denominator as above                           |
| `sorcery_type_share`             | Share of filtered cards flagged as sorceries                  | Same denominator as above                           |
| `planeswalker_type_share`        | Share of filtered cards flagged as planeswalkers              | Same denominator as above                           |
| `land_type_share`                | Share of filtered cards flagged as lands                      | Same denominator as above                           |
| `white_color_share`              | Share of filtered cards flagged as white                      | Same denominator as above                           |
| `blue_color_share`               | Share of filtered cards flagged as blue                       | Same denominator as above                           |
| `black_color_share`              | Share of filtered cards flagged as black                      | Same denominator as above                           |
| `red_color_share`                | Share of filtered cards flagged as red                        | Same denominator as above                           |
| `green_color_share`              | Share of filtered cards flagged as green                      | Same denominator as above                           |
| `colorless_proportion`           | Share of filtered cards flagged as colorless                  | Based on filtered set card pool                     |
| `mono_card_proportion`           | Share of filtered cards flagged as monocolored                | Based on filtered set card pool                     |
| `multi_card_proportion`          | Share of filtered cards flagged as multicolored               | Based on filtered set card pool                     |

### Usage notes

- Set-level shares are calculated over the project’s filtered paper-constructed card pool, not over all source cards in MTGJSON.
- Type-share columns do not sum to `1.0` because a card can contribute to multiple type categories.
- Color-share columns may also overlap because a card can contribute to more than one represented color.

# Metric and Modeling Notes

## Card vs. face grain

The project explicitly separates card-level and face-level modeling because MTG contains many layout variations, including cards with multiple faces. Card-level fields capture properties of the physical card as a whole, while face-level fields preserve printed attributes that vary by face.

## Variable printed stats

Fields such as `power`, `toughness`, and `loyalty` are preserved as strings in earlier layers because some printed values are symbolic or variable rather than strictly numeric. The warehouse layer adds numeric versions and variable-value flags so analysis can distinguish numeric cards from special printed values.

## Null handling

Null handling is applied before schema coercion in staging so that fields representing "not applicable" can remain null where meaningful, while fields intended to behave as stable analytical attributes can be filled to consistent defaults.

## First-printing logic

`first_printing_date` and `first_printing_set` are derived by exploding each card’s `printings` list and joining to set release metadata, then selecting the earliest available release date. These fields support time-based design analysis but depend on the quality and completeness of the source printings metadata.