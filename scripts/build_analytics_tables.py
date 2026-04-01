"""
Build analytics tables from staging data.

Transforms staging tables into analytics-ready warehouse tables by adding derived features, flags, and analysis-friendly fields.
Outputs both CSV and Parquet versions for compatibility with common analytics tools.
Outputs to /data/warehouse.

Tables: cards, card_faces, keywords, types, sets.
"""

from pathlib import Path
import json
import pandas as pd
from typing import Dict, Any
from build_staging_tables import validate_staging_dataframes

INPUT_DIR = Path("data/staging")
OUTPUT_DIR = Path("data/warehouse")
SCHEMAS_FILE = Path("docs/schemas.json")
TABLE_SCHEMAS = {}

report = {}


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
    for table in TABLE_SCHEMAS["staging_tables"].keys():
        parquet_path = input_dir / f"stg_{table}.parquet"
        if parquet_path.exists():
            dfs[table] = pd.read_parquet(parquet_path)
        else:
            report["read_errors"].append(f"Missing staging file for {table}")
    return dfs

def build_analytics_tables(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Build analytics tables from staging.
    """
    analytics = {}
    analytics["card_faces"] = build_analytics_faces(dfs["faces"])
    analytics["cards"] = build_analytics_cards(dfs["cards"], analytics["card_faces"], dfs["types"], dfs["keywords"], dfs["sets"])
    analytics["keywords"] = build_analytics_keywords(dfs["keywords"])
    analytics["types"] = build_analytics_types(dfs["types"])
    analytics["sets"] = build_analytics_sets(dfs["sets"])
    return analytics

def build_analytics_faces(staging_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive analytics for card faces: power, toughness, loyalty, defense, mana_cost, pt_ratio.
    """
    analytics_df = staging_df.copy()
    analytics_df["power_num"] = pd.to_numeric(analytics_df["power"], errors="coerce").astype("Float64")
    analytics_df["toughness_num"] = pd.to_numeric(analytics_df["toughness"], errors="coerce").astype("Float64")
    analytics_df["loyalty_num"] = pd.to_numeric(analytics_df["loyalty"], errors="coerce").astype("Float64")
    analytics_df["defense_num"] = pd.to_numeric(analytics_df["defense"], errors="coerce").astype("Float64")
    analytics_df["power_is_variable"] = (analytics_df["power"].notnull() & analytics_df["power_num"].isnull())
    analytics_df["toughness_is_variable"] = (analytics_df["toughness"].notnull() & analytics_df["toughness_num"].isnull())
    analytics_df["loyalty_is_variable"] = (analytics_df["loyalty"].notnull() & analytics_df["loyalty_num"].isnull())
    analytics_df["defense_is_variable"] = (analytics_df["defense"].notnull() & analytics_df["defense_num"].isnull())
    analytics_df["mana_cost_is_variable"] = analytics_df["mana_cost"].str.contains(r"[XYZ\*]", case=False, na=False)
    analytics_df["pt_ratio"] = analytics_df["power_num"] / analytics_df["toughness_num"].astype("Float64")

    temp_extracted_colors = analytics_df["colors"].apply(lambda x: set(list(x)))
    analytics_df["is_colorless"] = temp_extracted_colors.apply(lambda colors: len(colors) == 0).astype("bool")
    analytics_df["is_white"] = temp_extracted_colors.apply(lambda colors: "W" in colors).astype("bool")
    analytics_df["is_blue"] = temp_extracted_colors.apply(lambda colors: "U" in colors).astype("bool")
    analytics_df["is_black"] = temp_extracted_colors.apply(lambda colors: "B" in colors).astype("bool")
    analytics_df["is_red"] = temp_extracted_colors.apply(lambda colors: "R" in colors).astype("bool")
    analytics_df["is_green"] = temp_extracted_colors.apply(lambda colors: "G" in colors).astype("bool")

    return analytics_df.sort_values(["card_id", "face_index"]).reset_index(drop=True)

def build_analytics_cards(cards_df: pd.DataFrame, faces_df: pd.DataFrame, types_df: pd.DataFrame, keywords_df: pd.DataFrame, sets_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive analytics for cards: colors, text, types, printings from faces/joins.
    """
    cards = {
            "id": "str",
            "color_identity": "object",
            "edh_rec_rank": "Int64",
            "has_alt_deck_limit": "bool",
            "is_game_changer": "bool",
            "is_reserved": "bool",
            "layout": "category",
            "legalities": "object",
            "mana_value": "Int64",
            "name": "str",
            "printings": "object",
            "rulings": "object"
        }
    analytics_df = cards_df.copy()

    analytics_df["face_count"] = faces_df.groupby("card_id")["face_index"].nunique().astype("Int64")
    analytics_df["text_length"] = faces_df.groupby("card_id")["text"].apply(lambda texts: sum(len(t) for t in texts if isinstance(t, str))).astype("Int64")
    analytics_df["text_tokens"] = faces_df.groupby("card_id")["text"].apply(lambda texts: sum(len(t.split()) for t in texts if isinstance(t, str))).astype("Int64")
    analytics_df["type_count"] = types_df.groupby("card_id")["type"].nunique().astype("Int64")
    analytics_df["keyword_count"] = keywords_df.groupby("card_id")["keyword"].nunique().astype("Int64")
    analytics_df["legalities_count"] = analytics_df["legalities"].apply(lambda x: len([v for v in x.values() if v in ("Legal", "Restricted")]) if x is not None else 0).astype("Int64")
    analytics_df["rulings_count"] = analytics_df["rulings"].apply(lambda x: len(list(x)) if x is not None else 0).astype("Int64")
    analytics_df["printings_count"] = analytics_df["printings"].apply(lambda x: len(list(x)) if x is not None else 0).astype("Int64")
    analytics_df["is_legal_commander"] = analytics_df["legalities"].apply(lambda x: x.get("commander") in ("Legal", "Restricted") if x is not None else False).astype("bool")
    analytics_df["is_legal_modern"] = analytics_df["legalities"].apply(lambda x: x.get("modern") in ("Legal", "Restricted") if x is not None else False).astype("bool")
    analytics_df["is_legal_pauper"] = analytics_df["legalities"].apply(lambda x: x.get("pauper") in ("Legal", "Restricted") if x is not None else False).astype("bool")
    analytics_df["is_legal_legacy"] = analytics_df["legalities"].apply(lambda x: x.get("legacy") in ("Legal", "Restricted") if x is not None else False).astype("bool")
    analytics_df["is_legendary"] = types_df.groupby("card_id")["type"].apply(lambda types: any(t.lower() == "legendary" for t in types) if types is not None else False).astype("bool")

    # determine legendary status by checking if any type for the card contains "legendary" (case-insensitive)
    types_df["temp_legendary_flag"] = types_df["type"].str.lower() == "legendary"
    temp_legendary_map = types_df.groupby("card_id")["temp_legendary_flag"].any()
    analytics_df["is_legendary"] = analytics_df["id"].map(temp_legendary_map).fillna(False).astype("bool")

    # determine card colors by summarizing color flags from faces
    temp_color_summary = faces_df.groupby("card_id").agg({
        "is_white": "any",
        "is_blue": "any",
        "is_black": "any",
        "is_red": "any",
        "is_green": "any",
        "is_colorless": "all"
    })
    analytics_df = analytics_df.merge(
        temp_color_summary, 
        left_on="id", 
        right_index=True, 
        how="left"
    )
    analytics_df["is_mono"] = (analytics_df[["is_white", "is_blue", "is_black", "is_red", "is_green"]].sum(axis=1) == 1).astype("bool")
    analytics_df["is_multi"] = (analytics_df[["is_white", "is_blue", "is_black", "is_red", "is_green"]].sum(axis=1) > 1).astype("bool")
    analytics_df["color_count"] = analytics_df[["is_white", "is_blue", "is_black", "is_red", "is_green"]].sum(axis=1).astype("Int64")

    # determine first printing set and date by exploding printings and joining with sets to find earliest release date
    analytics_df["printings"] = analytics_df["printings"].apply(lambda x: x.tolist() if x is not None else [])
    temp_printings = analytics_df[["id", "printings"]].explode("printings").rename(columns={"id":"card_id", "printings":"set_id"})
    temp_printings = temp_printings.merge(sets_df[["id", "release_date"]], left_on="set_id", right_on="id", how="left")
    temp_mapping = temp_printings.dropna(subset=["release_date"]).sort_values(["card_id", "release_date"]).drop_duplicates("card_id", keep="first").set_index("card_id")
    analytics_df["first_printing_set"] = analytics_df["id"].map(temp_mapping["set_id"])
    analytics_df["first_printing_date"] = analytics_df["id"].map(temp_mapping["release_date"])

    return analytics_df.sort_values("id").reset_index(drop=True)

def build_analytics_keywords(keywords_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with dedupe/sort.
    """
    return keywords_df.drop_duplicates().sort_values(["card_id", "keyword"]).reset_index(drop=True)

def build_analytics_types(types_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with dedupe/sort.
    """
    
    return types_df.drop_duplicates().sort_values(["card_id", "face_index", "type_kind", "type"]).reset_index(drop=True)

def build_analytics_sets(sets_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with sort.
    """
    return sets_df.drop_duplicates().sort_values("id").reset_index(drop=True)

def validate_analytics_schema(analytics: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Validate analytics tables against schema (non-fatal).
    """
    validation_passed = True
    validation_failures = []
    for table_type, table_definition in TABLE_SCHEMAS["analytics_tables"].items():
        if table_type not in analytics.keys():
            validation_failures.append(f'Missing analytics table: {table_type}')
            continue # if the table is missing, we can't run any further validations on it, so we skip to the next table
        df = analytics[table_type]
        for column, dtype in table_definition.items():
                if column not in df.columns:
                    validation_failures.append(f'Expected column "{column}" is missing from {table_type} analytics table.')
                    continue # if the column is missing, we can't run any further validations on it, so we skip to the next column
                if df[column].dtype != dtype:
                    validation_failures.append(f'Column "{column}" in {table_type} analytics table has dtype {df[column].dtype} but expected {dtype}.')
    
    if validation_failures:
        validation_passed = False

    report["schema_validation_passed"] = validation_passed
    report["schema_validation_failures"] = validation_failures
    return {"schema_validation_passed": validation_passed, "schema_validation_failures": validation_failures}

def save_analytics_tables(analytics: Dict[str, pd.DataFrame], output_dir: Path) -> None:
    """
    Save analytics tables to CSV and Parquet.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    row_counts = {}
    for table, df in analytics.items():
        # CSV: stringify objects
        csv_df = df.copy()
        for col in df.select_dtypes(include=["object"], exclude=["string"]).columns:
            csv_df[col] = csv_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        csv_df.to_csv(output_dir / f"analytics_{table}.csv", index=False)
        
        # Parquet: keep objects
        df.to_parquet(output_dir / f"analytics_{table}.parquet", index=False)
        
        row_counts[table] = len(df)
    report["row_counts"] = row_counts

def save_warehouse_report(report: Dict[str, Any], output_dir: Path) -> None:
    """
    Save report to JSON.
    """
    report_path = output_dir / "warehouse_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

if __name__ == "__main__":
    load_table_schemas()
    staging_dfs = read_staging_tables(INPUT_DIR)
    validate_staging_dataframes(staging_dfs)
    analytics_dfs = build_analytics_tables(staging_dfs)
    schema_validation_results = validate_analytics_schema(analytics_dfs)

    print(
        "Analytics tables schema validation results:",
        json.dumps(schema_validation_results, indent=2),
        )

    save_analytics_tables(analytics_dfs, OUTPUT_DIR)
    save_warehouse_report(report, OUTPUT_DIR)
    print("Analytics tables built successfully. See warehouse_report.json for details.")
    