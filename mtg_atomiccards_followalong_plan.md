# MTG AtomicCards Wrangling — Follow-Along Plan (Part 1)

## Week 1: Build the clean dataset (AtomicCards → CSV/Parquet)

### 0) Prep & repo scaffolding (30–45 min)
1. **Create GitHub repo**
   - Name: `mtg-atomiccards-wrangling`
   - Initialize with **MIT License** (MTGJSON is MIT; you’ll attribute them in README and keep your repo MIT as well).
   - Add a short **README** placeholder.

2. **Clone locally**
   - `git clone <repo-url>`
   - `cd mtg-atomiccards-wrangling`

3. **Create basic structure**
   ```
   .
   ├─ data/
   │  ├─ raw/        # put AtomicCards.json here
   │  └─ processed/  # outputs: CSV/Parquet and long tables
   ├─ notebooks/
   ├─ scripts/
   ├─ docs/
   ├─ .gitignore
   ├─ LICENSE
   └─ README.md
   ```
   - In `.gitignore`: add `data/raw/`, `*.ipynb_checkpoints`, `__pycache__/`, `.venv/`, `.DS_Store`

4. **Create environment**
   - Use `uv`, `pipenv`, or `venv`. Example:
     ```bash
     python -m venv .venv
     source .venv/bin/activate  # Windows: .venv\Scripts\activate
     pip install pandas pyarrow numpy matplotlib plotly scipy scikit-learn python-slugify
     pip freeze > requirements.txt
     ```

### 1) Acquire the data (10–20 min)
5. **Download** `AtomicCards.json` (zipped if needed) from MTGJSON and place in `data/raw/`.
6. **Add attribution note file**
   - `docs/SOURCES.md` with:
     - “Data source: MTGJSON (MIT License).”
     - Link to MTGJSON site + license notice text.
     - “Non-affiliation” statement with Wizards of the Coast.

### 2) Define target schemas (30–45 min)
7. **Lock the output tables**
   - **`cards_clean` (wide, one row per card)**
     - Identifiers/basics: `name`, `ascii_name`, `layout`, `first_printing`, `is_funny`, `is_reserved`, `has_alt_deck_limit`
     - Colors: `color_identity` (list), `colors` (list), `color_count`, `is_colorless`, `is_mono`, `is_multi`, one-hots `ci_W/U/B/R/G`
     - Mana/stats: `mana_value`, `mana_cost_len`, `power`, `toughness`, `loyalty`, `defense`, `life`, `hand`, `pt_ratio`, `pt_symbolic`
     - Text: `text`, `text_length`, `text_tokens`, `num_keywords`, `num_rulings`, `avg_ruling_len`
     - Types: `types` (list), `supertypes` (list), `subtypes` (list), one-hots for primary card types, `type_count`, `has_subtypes`
     - Community/meta: `edhrec_rank`, `edhrec_saltiness` (nullable)
     - Legalities snapshot: `legal_commander`, `legal_modern`, `legal_legacy`, `legal_pioneer`, `legal_vintage`, `legal_standard`
     - Hygiene flags: `has_foreign_data`, `parse_notes`
   - **`cards_keywords` (long)**: `name`, `keyword`
   - **`cards_types` (long)**: `name`, `kind` (`type|supertype|subtype`), `value`

8. **Create a living data dictionary**
   - File: `docs/data_dictionary.md`
   - Start a table: column | dtype | description | source/logic.

### 3) Implement the build script (2–3 hrs)
9. **Create script skeleton**
   - `scripts/build_clean_dataset.py`
   - CLI interface (argparse) with `--input data/raw/AtomicCards.json --outdir data/processed`

10. **Parsing & normalization**
   - Load JSON (one object keyed by `name` → list of face objects)
   - Normalize top-level fields safely (handle `Optional`)
   - **Coercions & flags**
     - Parse numeric/string mix safely
     - Derive `text_length`, `num_keywords`, `num_rulings`, color booleans, etc.
     - Map `legalities` into booleans

11. **Assemble tables**
   - `cards_clean`: list of dicts → pandas DataFrame
   - `cards_keywords`: explode `keywords` to long
   - `cards_types`: build three long frames and concat with `kind` label

12. **Validation checks**
   - Row count equals distinct `name`
   - No duplicate `name`s
   - Sanity asserts for color logic, P/T ratios, keyword counts

13. **Persist outputs**
   - Write:
     - `data/processed/cards_clean.csv`
     - `data/processed/cards_clean.parquet`
     - `data/processed/cards_keywords.csv`
     - `data/processed/cards_types.csv`
   - Add `data/processed/sample_rows.csv` (head(100))

14. **Re-run reproducibly**
   - Add optional `Makefile`
   - Commit: `git add . && git commit -m "ETL: build clean AtomicCards dataset"`

### 4) Quick EDA (90 min)
15. **Notebook** `notebooks/01_explore_and_parse.ipynb`
   - Load `cards_clean.parquet`
   - Produce 3–4 visuals (PNG in `docs/` or `visuals/`)
     - Dist: `color_count` and primary `types`
     - Scatter: `text_length` vs `mana_value`
     - Bar: mean `pt_ratio` by mono color (filter `is_creature`)
     - Heatmap: `% type` by mono color

16. **Capture insights bullets**
   - Add 5–8 bullets in README summary.

### 5) Documentation & licensing polish (30–45 min)
17. **README.md**
   - Project purpose (data wrangling focus)
   - MTGJSON attribution and MIT license snippet
   - How to reproduce
   - Output file list
   - Example plot(s)

18. **docs/data_dictionary.md**
   - Fill column descriptions
   - Note coercion rules for symbolic stats

19. **LICENSE**
   - Keep MIT; clarify source attribution.

### 6) Versioning & housekeeping (15–30 min)
20. **Version stamp**
   - Write `data/processed/VERSION.json` with MTGJSON build date + pipeline version.

21. **Tag the commit**
   - `git tag v0.1.0-initial-clean`
   - `git push --tags`

---

## Week 2 (start) — Color identity add-on (setup only)
22. **Create notebook** `notebooks/02_color_identity_analysis.ipynb`
   - Load `cards_clean.parquet`
   - Filter to mono-color identities
   - Prepare features (`text_length`, `num_keywords`, `mana_value`, `pt_ratio`)
   - Add helper for Kruskal–Wallis + Dunn’s post-hoc

---

### Final pre-Kaggle checklist
- ✅ Clean CSV + Parquet
- ✅ Long tables
- ✅ Sample rows
- ✅ Data dictionary
- ✅ Source attribution
- ⬜ Kaggle readme (to do next)
- ⬜ Example code snippet for Kaggle description
