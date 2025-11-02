# AtomicCards-DataWrangling-Python
End-to-end data preparation pipeline for the MTGJSON AtomicCards dataset. Parsing nested JSON, feature engineering, and exploratory profiling. Output published as a Kaggle dataset.


### Card Selection

This project focuses on cards relevant to real-world deckbuilding in paper-constructed formats.
The following inclusion and exclusion rules were applied to the MTGJSON AtomicCards dataset to produce the filtered subset atomiccards_legal.json.

**Included**

Cards are retained if they meet all of the following:

Playable in at least one paper-constructed format
(Commander, Oathbreaker, Standard, Pioneer, Modern, Legacy, Vintage, Pauper, Penny Dreadful, Premodern, or Old School)
with legality status Legal or Restricted.

Fully released, physical cards — not tokens, emblems, schemes, or other digital-only content.

**Excluded**

Cards were excluded if any of these conditions applied:

- Humor/Acorn cards (isFunny=True) from Un-sets or other joke products.
- Non-constructed layouts, including tokens, emblems, schemes, vanguards, planes, phenomena, attractions, stickers, or contraptions.
- Conspiracy-type cards, which are draft-only mechanics and not used in constructed play.
- Cards first printed in “Un-sets” (UGL, UNH, UNST, UND, UNF), even if technically legal in Commander or Legacy.
    - Rationale: While some Un-set cards (e.g. from Unfinity) are Commander-legal, these sets were designed as experimental products that intentionally break normal mechanical and color-pie conventions.
    - Excluding them improves dataset consistency for downstream analyses focused on color identity and core design patterns.



### Data Source and Licensing

This project uses data from [MTGJSON](https://mtgjson.com/) (MIT License).  
Full attribution and license text can be found in [`docs/SOURCES.md`](docs/SOURCES.md).

Magic: The Gathering® and all related properties are owned by Wizards of the Coast LLC.  
This project is not affiliated with or endorsed by Wizards of the Coast.