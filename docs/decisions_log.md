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
- Conspiracy-type cards, which are draft-only mechanics and not used in constructed play.
- Cards only printed in “Un-sets” (UGL, UNH, UNST, UND, UNF), even if technically legal in Commander or Legacy.
    - Rationale: While some Un-set cards (e.g. from Unfinity) are Commander-legal, these sets were designed as experimental products that intentionally break normal mechanical and color-pie conventions.
    - Excluding them improves dataset consistency for downstream analyses focused on color identity and core design patterns.

## Staging Tables

Core challenge - the product has had many layout variations of cards over it's 30-year history, cards with multiple faces.

The cleanest way to handle this was to create separate tables for a grain of card and a grain of face. Each card is related to one or more faces.
The allows clean analysis of features that are inherent to the entire physical card, as well as features that are face-specific.

Some numeric fields (e.g. power, toughness) are generally numeric, but sometimes are variable (e.g. "*+1), therefore these fields are maintained as strings in the staging tables

## Warehouse Tables