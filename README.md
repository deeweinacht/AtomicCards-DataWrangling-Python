# AtomicCards-DataWrangling-Python
A reproducible ETL pipeline that ingests MTGJSON AtomicCards data and exports normalized relational tables suitable for product analytics.

Taking the full MTGJson dataset and preparing it for product analysis for MTG design teams.

- ingested semi-structured MTGJSON data.
- modeled it into analytics-ready relational tables.
- exposed a reusable DuckDB semantic layer through views.
- answered stakeholder questions with SQL that is interpretable and defensible.

# WORK IN PROGRESS


## Problem Statement:
The game design / product analytics team needs a reliable card-design analytics layer to monitor how card characteristics evolve across releases, evaluate whether color identities remain distinct, and identify sets that deviate from expected design patterns.


### Card Power Creep
Has the power of cards increased over the history of the game?
Are some sets, colors, card types more powerful than others?

Example metrics:
A. Stat Efficiency (creatures only)
(power + toughness) / mana_cost
B. Keyword Density
number of keywords (flying, haste, etc.)
proxy for complexity / strength
C. Text Length
crude, but correlates with complexity/power creep
D. Mana Efficiency Buckets
low-cost vs high-impact cards

### Color Identity Stability
Do colors maintain consistent identities?
Are mechanics drifting between colors?
Metrics:
A. Mechanic frequency by color
% of blue cards with flying, draw, etc.
B. Color entropy (optional but impressive)
how “spread out” mechanics are within a color
C. Cross-color overlap
how often mechanics appear outside their “home” color

### Set Design Consistency
Are sets internally consistent?
Do some sets deviate significantly?

Metrics:
A. Avg CMC per set
B. Avg keyword count
C. Creature vs non-creature ratio
D. Mechanic distribution




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

### Data Source and Licensing

This project uses data from [MTGJSON](https://mtgjson.com/) (MIT License).  
Full attribution and license text can be found in [`docs/SOURCES.md`](docs/SOURCES.md).

Magic: The Gathering® and all related properties are owned by Wizards of the Coast LLC.  
This project is not affiliated with or endorsed by Wizards of the Coast.
