# MTGJSON Card Design Analytics Pipeline

A layered analytics ETL pipeline that transforms semi-structured MTGJSON source data into query-ready analytical datasets for product and design analysis.

This project was built for a theoretical Magic: The Gathering product/design analytics team that needs a reliable way to track how card design evolves over time, assess whether color identities remain differentiated, and identify sets whose design patterns differ from broader release norms.

## Why this project

Magic card data is rich, messy, and highly nested. Answering product and design questions using data from overy thirty years of product releases requires more than ad hoc SQL or notebook analysis. It requires a repeatable data pipeline with clear grains, stable modeled outputs, and a business-facing analytical layer.

This project builds that system end to end:

- ingests MTGJSON source data
- filters it to a paper-constructed analytical scope
- normalizes nested JSON into structured staging tables
- models warehouse-layer analytical entities
- exposes a reusable DuckDB semantic layer
- answers stakeholder questions through interpretable SQL outputs

## Business problem

The product/design analytics team needs a reliable card-design analytics layer to:

- monitor how card characteristics evolve across releases
- evaluate whether color identities remain distinct
- identify sets that deviate from expected design patterns


## What this pipeline does

This project extracts and filters MTGJSON card and set data, transforms semi-structured and nested source fields into curated analytical datasets, and exposes a DuckDB semantic layer for business-oriented SQL analysis.


## Example business questions answered

### Card design evolution

- How has card design complexity changed over time?
- Are newer cards accumulating more text density and rulings burden than earlier designs?
- How have multicolored and multi-face card structures changed across release eras?

### Color identity stability

- Which mechanics remain strongly associated with specific mono-colors?
- Which mechanics are common enough within a color to act as practical identity signals?
- Which mechanics are broadly shared across colors rather than strongly owned by one color?

### Set design deviation

- Which sets differ most from broader release norms?
- Do the biggest outliers reflect noise, or intentionally pushed design environments?
- Which releases stand out because of artifact concentration, color structure, or composition shifts?

## Example findings

### 1) Complexity creep is real

The analysis shows a clear long-term increase in card complexity. Compared with the first decade of Magic design, recent cards carry substantially more text and significantly more rulings burden.

Across the earliest release periods, cards averaged roughly 19 to 23 text tokens and under 1.1 rulings per card. In the 2021-present period, the number if text tokens is nearly double that and the number of rulings has tripled.

This suggests that complexity creep is not just anecdotal. It is visible in the modeled data and may represent a meaningful design concern around player mental load and fatigue.

| Release period | Avg text tokens per card | Avg rulings count per card | Avg design complexity score | % multicolored cards | % multi-face cards |
| -------------- | -----------------------: | -------------------------: | --------------------------: | -------------------: | -----------------: |
| 1993-1995      |                       22 |                       1.05 |                          33 |                    6 |                  0 |
| 1996-2000      |                       19 |                       0.81 |                          27 |                    4 |                  0 |
| 2001-2005      |                       23 |                       0.97 |                          32 |                    5 |                  1 |
| 2006-2010      |                       24 |                       2.14 |                          45 |                   17 |                  0 |
| 2011-2015      |                       24 |                       2.46 |                          49 |                   13 |                  1 |
| 2016-2020      |                       28 |                       3.03 |                          58 |                   15 |                  3 |
| 2021-Present   |                       37 |                       3.10 |                          68 |                   19 |                  5 |

### 2) Color identity is still visible, but not equally exclusive

The analysis suggests that color identity remains meaningful, especially through signature mechanics, but exclusivity varies a lot by mechanic.

Some mechanics still function as strong mono-color signals:
- **Haste** is overwhelmingly concentrated in red
- **Reach** and **Fight** are strongly concentrated in green
- **Vigilance** remains strongly associated with white
- **Deathtouch** remains strongly associated with black

At the same time, other mechanics are much less exclusive than they may appear:
- **Flying** is common in both blue and white
- **Enchant** is broadly distributed
- **Cycling**, **Transform**, and **Equip** do not show strong single-color ownership

The takeaway is not that color identity disappeared. It is that color identity is strong in some mechanics and much more diffuse in others.

### 3) Set outliers usually reflect intentional design environments

The set deviation analysis identifies releases that differ most from broader design norms using a composite score built from mana value, complexity, type share, and color-structure metrics.

The strongest outliers are not random anomalies. They are usually sets with intentionally pushed design identities, such as artifact-heavy environments or unusually extreme color structures.

Examples include:
- **Alara Reborn**, which stands out heavily on color structure
- **Mirrodin**, **Darksteel**, **Fifth Dawn**, and **Antiquities**, which align with unusually artifact-centric environments
- **Legions**, which reflects an unusually creature-focused design profile

This suggests the outlier logic is surfacing deliberate product and design choices rather than meaningless statistical noise.

## Architecture overview

The project follows a layered ETL-style workflow:

1. Raw MTGJSON files are ingested into `data/raw`
2. Source data is filtered to the project’s paper-constructed analytical scope
3. Nested JSON is normalized into staging parquet datasets
4. Curated warehouse-layer tables are derived at stable analytical grains
5. Warehouse outputs are exposed through DuckDB semantic views
6. Final business queries are exported as CSV outputs with execution metadata

![Pipeline diagram](docs/images/pipeline_diagram.png)

For more detail, see [`docs/pipeline_architecture.md`](docs/pipeline_architecture.md).

## Primary outputs

- **Staging layer** — normalized parquet datasets for cards, faces, keywords, types, and sets
- **Warehouse layer** — curated card-, face-, keyword-, type-, and set-level analytical tables
- **Semantic layer** — DuckDB views that expose business-facing analytical concepts
- **Query outputs** — CSV exports of final business-query results
- **Execution metadata** — JSON run logs for pipeline and query review

## Tech stack

- Python
- pandas
- DuckDB
- SQL
- Parquet

## Repository structure

- `data/` — pipeline data artifacts across raw, filtered, staging, warehouse, analytics, and output layers
- `docs/` — project documentation, including architecture, data dictionary, and decisions log
- `scripts/` — Python ETL and orchestration scripts
- `setup/` — one-command environment setup scripts
- `sql/` — semantic view definitions and business query files

Large raw source files are excluded from version control. Curated staging, warehouse, semantic, and final-query artifacts are included so reviewers can inspect modeled outputs without rerunning the entire pipeline.

## Quickstart
```bash
# === Windows ===
setup\bootstrap.ps1  # setup dependencies and virtual environment
.\.venv\Scripts\activate  # activate virtual environment

# === macOS/Linux ===
setup/mac_linux.sh  # setup dependencies and virtual environment
source .venv/bin/activate # activate virtual environment
```

Run the full pipeline:

```bash
python scripts/run_pipeline.py
```

Run the final business queries (after the pipeline):

```bash
python scripts/run_queries.py
```

## Documentation

- [`docs/pipeline_architecture.md`](docs/pipeline_architecture.md) — process, structure, and data flow
- [`docs/data_dictionary.md`](docs/data_dictionary.md) — table and semantic-view definitions
- [`docs/decisions_log.md`](docs/decisions_log.md) — key project design and modeling decisions
- [`docs/SOURCES.md`](docs/SOURCES.md) — source attribution and licensing

### Data Source and Licensing

This project uses data from [MTGJSON](https://mtgjson.com/) (MIT License).  
Full attribution and license text can be found in [`docs/SOURCES.md`](docs/SOURCES.md).

Magic: The Gathering® and all related properties are owned by Wizards of the Coast LLC.  
This project is not affiliated with or endorsed by Wizards of the Coast.
