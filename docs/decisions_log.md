## Card Selection

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
- "reversible" layouts - which are reprints of existing cards, with the same card on both faces, but with different art. These would result in duplicates in the dataset.
- Conspiracy-type cards, which are draft-only mechanics and not used in constructed play.
- Cards only printed in “Un-sets” (UGL, UNH, UNST, UND, UNF), even if technically legal in Commander or Legacy.
    - Rationale: While some Un-set cards (e.g. from Unfinity) are Commander-legal, these sets were designed as experimental products that intentionally break normal mechanical and color-pie conventions.
    - Excluding them improves dataset consistency for downstream analyses focused on color identity and core design patterns.

## Set Selection

Excluded only sets that are definitively not for paper constructed play:
- flagged "Online Only"
- set types "memorabilia" (art cards), "tokens" (temporary indicators), or "minigames" (separate product from MTG)

## Staging Tables

Core challenge - the product has had many layout variations of cards over it's 30-year history, including cards with multiple faces.

The cleanest way to handle this was to create separate tables for a grain of card and a grain of face. Each card is related to one or more faces.
The allows clean analysis of features that are inherent to the entire physical card, as well as features that are face-specific.

Some fields (e.g. power, toughness) are generally numeric, but sometimes are variable (e.g. "*+1), therefore these fields are maintained as strings in the staging tables

## Warehouse Tables

Added numeric columns for fields (e.g. power, toughness) that are generally numeric and added flag columns to indicate where the values are not numeric and are instead variable.

### Semantive View Layer

After loading curated warehouse tables into DuckDB, a semantic view layer was added to support stakeholder-facing analysis without changing the warehouse source-of-truth tables. This layer exists to keep the warehouse tables stable and reusable, while expressing analysis-specific business logic such as design complexity proxies, creature stat efficiency, color-mechanic mappings, and set-level design profiles in a form that is easier to query and interpret.

The semantic views are not 1:1 copies of warehouse tables. Instead, they encapsulate repeated joins, derived metrics, and standardized inclusion logic so that final SQL analyses remain concise, consistent, and easier to audit. This follows a common layered analytics pattern in which staging models standardize raw data, warehouse or core models preserve cleaned entities, and downstream semantic or mart models expose business-ready concepts for reporting and analysis.

For this project, the semantic layer was introduced specifically to answer product/design questions about card design evolution, color identity stability, and set composition. Metrics such as complexity stand-ins or creature stat efficiency are interpretive analytical features rather than canonical source data, so they are defined in views rather than persisted back into the warehouse tables.