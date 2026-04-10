# MTGJSON Card Design Analytics Pipeline

A reproducible ETL pipeline that ingests MTGJSON AtomicCards data and exports normalized relational tables suitable for product analytics.

Taking the full MTGJson dataset and preparing it for product analysis for MTG design teams.

- ingested semi-structured MTGJSON data.
- modeled it into analytics-ready relational tables.
- exposed a reusable DuckDB semantic layer through views.
- answered stakeholder questions with SQL that is interpretable and defensible.

The project follows an ETL-style pipeline: raw MTGJSON files are extracted, filtered to the project’s analytical scope, transformed into curated staging and warehouse datasets, and then loaded into a DuckDB analytics layer implemented through views over parquet-backed outputs.


## Problem Statement:
The game design / product analytics team needs a reliable card-design analytics layer to monitor how card characteristics evolve across releases, evaluate whether color identities remain distinct, and identify sets that deviate from expected design patterns.


### Card Design Consistency


### Color Identity Stability


### Set Design Consistency





## Quickstart
```bash
# === Windows ===
setup\bootstrap.ps1  # setup dependencies and virtual environment
.\.venv\Scripts\activate  # activate virtual environment

# === macOS/Linux ===
setup/mac_linux.sh  # setup dependencies and virtual environment
source .venv/bin/activate # activate virtual environment
```


### Project Structure

**data** organizes the data in all stages of the pipeline.
**docs** contains project documentation.
**scripts** contains the python files that execute the pipeline.
**setup** contains scripts for single-command setup of the environment.
**sql** contains the sql files that structure the database and database queries.

Large raw source files are excluded from version control. 
Curated staging, warehouse, semantic, and final-query artifacts are included so reviewers can inspect outputs without rerunning the pipeline.

### Data Source and Licensing

This project uses data from [MTGJSON](https://mtgjson.com/) (MIT License).  
Full attribution and license text can be found in [`docs/SOURCES.md`](docs/SOURCES.md).

Magic: The Gathering® and all related properties are owned by Wizards of the Coast LLC.  
This project is not affiliated with or endorsed by Wizards of the Coast.
