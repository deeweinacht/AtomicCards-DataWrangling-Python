# AtomicCards-DataWrangling-Python
A reproducible ETL pipeline that ingests MTGJSON AtomicCards data and exports normalized relational tables suitable for analytics workflows.

## WORK IN PROGRESS

## Quickstart
```bash
# === Windows ===
setup\bootstrap.ps1  # setup dependencies and virtual environment
.\.venv\Scripts\activate  # activate virtual environment

# === macOS/Linux ===
setup/mac_linux.sh  # setup dependencies and virtual environment
source .venv/bin/activate # activate virtual environment
```

### Card Selection

This project focuses on cards relevant to real-world deckbuilding in paper-constructed formats.
The following inclusion and exclusion rules were applied to the MTGJSON AtomicCards dataset to produce the filtered subset atomiccards_legal.json.


**Included**

Cards are retained if they meet all of the following:

Playable in at least one paper-constructed format
(Commander, Oathbreaker, Standard, Pioneer, Modern, Legacy, Vintage, Pauper, Penny Dreadful, Premodern, or Old School)
with legality status Legal or Restricted.

Fully released, physical cards that can be used in a playable deck — not tokens, emblems, schemes, or other digital-only content.

**Excluded**

Cards were excluded if any of these conditions applied:

- Humor/Acorn cards (isFunny=True) from Un-sets or other joke products.
- Non-constructed layouts, including tokens, emblems, schemes, vanguards, planes, phenomena, attractions, stickers, or contraptions.
- Conspiracy-type cards, which are draft-only mechanics and not used in constructed play.
- Cards only printed in “Un-sets” (UGL, UNH, UNST, UND, UNF), even if technically legal in Commander or Legacy.
    - Rationale: While some Un-set cards (e.g. from Unfinity) are Commander-legal, these sets were designed as experimental products that intentionally break normal mechanical and color-pie conventions.
    - Excluding them improves dataset consistency for downstream analyses focused on color identity and core design patterns.

### Project Structure

**data** organizes the data in all stages of the pipeline.
**docs** contains project documentation.
**scripts** contains the python files that execute the pipeline.
**setup** contains scripts for single-command setup of the environment.

### Data Source and Licensing

This project uses data from [MTGJSON](https://mtgjson.com/) (MIT License).  
Full attribution and license text can be found in [`docs/SOURCES.md`](docs/SOURCES.md).

Magic: The Gathering® and all related properties are owned by Wizards of the Coast LLC.  
This project is not affiliated with or endorsed by Wizards of the Coast.
