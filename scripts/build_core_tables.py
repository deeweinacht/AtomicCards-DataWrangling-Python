"""
Build warehouse-layer core tables from staging tables.
This step derives analysis-ready card- and face-level features, preserves selected nested source attributes,
and writes canonical parquet outputs plus reviewer-friendly preview CSVs.

Tables: cards, card_faces, keywords, types, sets.

core_cards contains one row per card with aggregated face, type, legality, and printing features.
core_card_faces contains one row per face with numeric stat derivations and variable-value flags.
core_keywords, core_types, and core_sets preserve normalized supporting dimensions at stable warehouse grain.
"""

from pathlib import Path
import json
import pandas as pd
from pandas.api import types as pd_types
from typing import Dict, Any

INPUT_DIR = Path("data/staging")
OUTPUT_DIR = Path("data/warehouse")
PREVIEW_DIR = Path("data/output/previews")
SCHEMAS_FILE = Path("data/schemas.json")
TABLE_SCHEMAS = {}

report = {"read_errors": []}


def load_table_schemas() -> None:
    with open(SCHEMAS_FILE, "r", encoding="utf-8") as f:
        global TABLE_SCHEMAS
        TABLE_SCHEMAS = json.load(f)


def read_staging_tables(input_dir: Path) -> Dict[str, pd.DataFrame]:
    """
    Read full parquet tables from staging.
    Returns dict of DataFrames.
    """
    dfs = {}

    print("Reading staging tables...")
    for table in TABLE_SCHEMAS["staging_tables"].keys():
        parquet_path = input_dir / f"stg_{table}.parquet"
        if parquet_path.exists():
            dfs[table] = pd.read_parquet(parquet_path)
        else:
            report["read_errors"].append(f"Could not read staging file for {table}")

    if report["read_errors"]:
        save_warehouse_report()
        raise FileNotFoundError(
            "Missing staging file(s) for build, aborting. See warehouse_report.json for details."
        )
    return dfs


def build_tables(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Build core warehouse tables from staging.
    """
    core_tables = {}

    print("Building core tables using staging data...")
    core_tables["card_faces"] = build_faces_core(dfs["faces"])
    core_tables["cards"] = build_cards_core(
        dfs["cards"],
        core_tables["card_faces"],
        dfs["types"],
        dfs["keywords"],
        dfs["sets"],
    )
    core_tables["keywords"] = build_keywords_core(dfs["keywords"])
    core_tables["types"] = build_types_core(dfs["types"])
    core_tables["sets"] = build_sets_core(dfs["sets"])
    return core_tables


def build_faces_core(staging_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive analytic features for card faces: power, toughness, loyalty, defense, mana_cost, pt_ratio.
    """
    core_df = staging_df.copy()
    core_df["power_num"] = pd.to_numeric(core_df["power"], errors="coerce").astype(
        "Float64"
    )
    core_df["toughness_num"] = pd.to_numeric(
        core_df["toughness"], errors="coerce"
    ).astype("Float64")
    core_df["loyalty_num"] = pd.to_numeric(core_df["loyalty"], errors="coerce").astype(
        "Float64"
    )
    core_df["defense_num"] = pd.to_numeric(core_df["defense"], errors="coerce").astype(
        "Float64"
    )  # defense is never variable, so the variable flag is not added for defense
    core_df["power_is_variable"] = (
        core_df["power"].notnull() & core_df["power_num"].isnull()
    )
    core_df["toughness_is_variable"] = (
        core_df["toughness"].notnull() & core_df["toughness_num"].isnull()
    )
    core_df["loyalty_is_variable"] = (
        core_df["loyalty"].notnull() & core_df["loyalty_num"].isnull()
    )
    core_df["mana_cost_is_variable"] = core_df["mana_cost"].str.contains(
        r"[XYZ\*]", case=False, na=False
    )
    core_df["pt_ratio"] = core_df["power_num"] / core_df["toughness_num"].astype(
        "Float64"
    )

    temp_extracted_colors = core_df["colors"].apply(
        lambda x: set(list(x) if x.any() else set())
    )
    core_df["is_colorless"] = temp_extracted_colors.apply(
        lambda colors: len(colors) == 0
    ).astype("bool")
    core_df["is_white"] = temp_extracted_colors.apply(
        lambda colors: "W" in colors
    ).astype("bool")
    core_df["is_blue"] = temp_extracted_colors.apply(
        lambda colors: "U" in colors
    ).astype("bool")
    core_df["is_black"] = temp_extracted_colors.apply(
        lambda colors: "B" in colors
    ).astype("bool")
    core_df["is_red"] = temp_extracted_colors.apply(
        lambda colors: "R" in colors
    ).astype("bool")
    core_df["is_green"] = temp_extracted_colors.apply(
        lambda colors: "G" in colors
    ).astype("bool")

    return core_df.sort_values(["card_id", "face_index"]).reset_index(drop=True)


def build_cards_core(
    cards_df: pd.DataFrame,
    faces_df: pd.DataFrame,
    types_df: pd.DataFrame,
    keywords_df: pd.DataFrame,
    sets_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Derive analytic features for cards: colors, text, types, printings from faces/joins.
    """
    # Temporarily rename 'id' to 'card_id' so it matches the results of our groupbys and joins from other tables
    core_df = cards_df.copy().rename(columns={"id": "card_id"}).set_index("card_id")

    core_df["face_count"] = (
        faces_df.groupby("card_id")["face_index"].nunique().astype("Int64")
    )
    core_df["text_length"] = (
        faces_df["text"].str.len().groupby(faces_df["card_id"]).sum().astype("Int64")
    )
    core_df["text_tokens"] = (
        faces_df["text"]
        .str.split()
        .apply(len)
        .groupby(faces_df["card_id"])
        .sum()
        .astype("Int64")
    )
    core_df["type_count"] = (
        types_df.groupby("card_id")["type"].nunique().astype("Int64")
    )
    core_df["keyword_count"] = keywords_df.groupby("card_id")["keyword"].nunique()
    core_df["keyword_count"] = core_df["keyword_count"].fillna(0).astype("Int64")
    core_df["legalities_count"] = (
        core_df["legalities"]
        .apply(
            lambda x: (
                len([v for v in x.values() if v in ("Legal", "Restricted")])
                if x is not None
                else 0
            )
        )
        .astype("Int64")
    )
    core_df["rulings_count"] = (
        core_df["rulings"]
        .apply(lambda x: len(list(x)) if x is not None else 0)
        .astype("Int64")
    )
    core_df["printings_count"] = (
        core_df["printings"]
        .apply(lambda x: len(list(x)) if x is not None else 0)
        .astype("Int64")
    )
    core_df["is_legal_commander"] = (
        core_df["legalities"]
        .apply(
            lambda x: (
                x.get("commander") in ("Legal", "Restricted")
                if x is not None
                else False
            )
        )
        .astype("bool")
    )
    core_df["is_legal_modern"] = (
        core_df["legalities"]
        .apply(
            lambda x: (
                x.get("modern") in ("Legal", "Restricted") if x is not None else False
            )
        )
        .astype("bool")
    )
    core_df["is_legal_pauper"] = (
        core_df["legalities"]
        .apply(
            lambda x: (
                x.get("pauper") in ("Legal", "Restricted") if x is not None else False
            )
        )
        .astype("bool")
    )
    core_df["is_legal_legacy"] = (
        core_df["legalities"]
        .apply(
            lambda x: (
                x.get("legacy") in ("Legal", "Restricted") if x is not None else False
            )
        )
        .astype("bool")
    )

    # determine legendary status by checking if any type for the card contains "legendary" (case-insensitive)
    temp_legendary_id_map = types_df[types_df["type"].str.lower() == "legendary"][
        "card_id"
    ].unique()
    core_df["is_legendary"] = core_df.index.isin(temp_legendary_id_map)

    # determine card colors by summarizing color flags from faces
    temp_color_cols = ["is_white", "is_blue", "is_black", "is_red", "is_green"]
    temp_color_summary = faces_df.groupby("card_id")[temp_color_cols].any()
    core_df = core_df.join(temp_color_summary)

    # derive color metrics from color flags
    core_df["color_count"] = core_df[temp_color_cols].sum(axis=1).astype("Int64")
    core_df["is_colorless"] = (core_df["color_count"] == 0).astype("bool")
    core_df["is_mono"] = (core_df["color_count"] == 1).astype("bool")
    core_df["is_multi"] = (core_df["color_count"] > 1).astype("bool")

    # determine first printing set and date by exploding printings and joining with sets to find earliest release date
    temp_exploded_printings = core_df["printings"].explode().rename("set_id")
    temp_printings_with_dates = pd.DataFrame(temp_exploded_printings).join(
        sets_df.set_index("id")[["release_date"]], on="set_id"
    )
    temp_first_printings = (
        temp_printings_with_dates.dropna(subset=["release_date"])
        .sort_values(["card_id", "release_date"])
        .groupby(level=0)  # Groups by the 'card_id' index
        .first()  # Grabs the earliest record for each ID
    )
    core_df["first_printing_set"] = temp_first_printings["set_id"]
    core_df["first_printing_date"] = temp_first_printings["release_date"]

    return core_df.reset_index().rename(columns={"card_id": "id"}).sort_values("id")


def build_keywords_core(keywords_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with dedupe/sort.
    """
    return (
        keywords_df.drop_duplicates()
        .sort_values(["card_id", "keyword"])
        .reset_index(drop=True)
    )


def build_types_core(types_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with dedupe/sort.
    """

    return (
        types_df.drop_duplicates()
        .sort_values(["card_id", "face_index", "type_kind", "type"])
        .reset_index(drop=True)
    )


def build_sets_core(sets_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with sort.
    """
    return sets_df.drop_duplicates().sort_values("id").reset_index(drop=True)


def column_matches_dtype(column: pd.Series, target_dtype: str) -> bool:
    if target_dtype == "Int64":
        return pd_types.is_integer_dtype(column) and str(column.dtype) == "Int64"
    if target_dtype in ("Float64", "float64"):
        return pd_types.is_float_dtype(column) and str(column.dtype) in (
            "Float64",
            "float64",
        )
    if target_dtype == "datetime64[ns]":
        return pd_types.is_datetime64_any_dtype(column)
    if target_dtype == "category":
        return str(column.dtype) == "category"
    if target_dtype == "bool":
        return pd_types.is_bool_dtype(column)
    if target_dtype == "str":
        return pd_types.is_string_dtype(column)
    if target_dtype == "object":
        return pd_types.is_object_dtype(column)
    raise ValueError(f"Unsupported dtype: {target_dtype}")


def validate_warehouse_schema(core_tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Validate warehouse tables against schema.
    """
    validation_passed = True
    validation_failures = []

    print("Validating warehouse tables against schema...")
    for table_type, table_definition in TABLE_SCHEMAS["warehouse_tables"].items():
        if table_type not in core_tables.keys():
            validation_failures.append(f"Missing warehouse table: {table_type}")
            continue  # if the table is missing, we can't run any further validations on it, so we skip to the next table
        df = core_tables[table_type]
        for column, dtype in table_definition.items():
            if column not in df.columns:
                validation_failures.append(
                    f'Expected column "{column}" is missing from {table_type} table.'
                )
                continue  # if the column is missing, we can't run any further validations on it, so we skip to the next column
            if not column_matches_dtype(df[column], dtype):
                validation_failures.append(
                    f'Column "{column}" in {table_type} table has dtype {df[column].dtype} but expected {dtype}.'
                )

    if validation_failures:
        validation_passed = False

    report["schema_validation_passed"] = validation_passed
    report["schema_validation_failures"] = validation_failures

    if not validation_passed:
        print(f"Schema validation failed: {validation_failures}")
        save_warehouse_report()
        raise ValueError("Schema validation failed. Aborting build.")
    else:
        print("Schema validation passed.")

    return {
        "schema_validation_passed": validation_passed,
        "schema_validation_failures": validation_failures,
    }


def save_warehouse_tables(core_tables: Dict[str, pd.DataFrame]) -> None:
    """
    Save warehouse tables to Parquet and output preview CSVs.
    """
    print("Saving warehouse tables...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    row_counts = {}
    for table, df in core_tables.items():
        # CSV: stringify objects
        csv_df = df.copy()
        for col in df.select_dtypes(include=["object"], exclude=["string"]).columns:
            csv_df[col] = csv_df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )
        csv_df.head(100).to_csv(
            PREVIEW_DIR.joinpath(f"preview_core_{table}.csv"), index=False
        )

        # Parquet: keep objects
        df.to_parquet(OUTPUT_DIR.joinpath(f"core_{table}.parquet"), index=False)

        row_counts[table] = len(df)
    report["row_counts"] = row_counts


def save_warehouse_report() -> None:
    """
    Save report to JSON.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR.joinpath("warehouse_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)


def build_warehouse_core_tables():
    load_table_schemas()
    staging_dfs = read_staging_tables(INPUT_DIR)
    warehouse_dfs = build_tables(staging_dfs)
    schema_validation_results = validate_warehouse_schema(warehouse_dfs)

    print(
        "Warehouse tables schema validation results:",
        json.dumps(schema_validation_results, indent=2),
    )

    save_warehouse_tables(warehouse_dfs)
    save_warehouse_report()
    print("Warehouse tables built successfully. See warehouse_report.json for details.")


if __name__ == "__main__":
    build_warehouse_core_tables()
