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
from scripts.build_staging_tables import validate_analytics_tables

INPUT_DIR = Path("data/staging")
OUTPUT_DIR = Path("data/warehouse")
SCHEMAS_FILE = Path("docs/schemas.json")

report = {}

# Analytics schema (add derived fields)
ANALYTICS_TABLE_SCHEMA = {
    "cards": {
        "id": "str",
        "name": "str",
        "mana_value": "Int64",
        "color_count": "Int64",
        "is_colorless": "bool",
        "is_mono": "bool",
        "is_multi": "bool",
        "color_W": "bool",
        "color_U": "bool",
        "color_B": "bool",
        "color_R": "bool",
        "color_G": "bool",
        "face_count": "Int64",
        "text_length": "Int64",
        "text_tokens": "Int64",
        "type_count": "Int64",
        "has_subtypes": "bool",
        "first_printing": "datetime64[ns]",
        "legalities_count": "Int64",
        "rulings_count": "Int64",
        "printings_count": "Int64",
        "layout": "object",
        "legalities": "object",
        "printings": "object",
        "rulings": "object",
    },
    "card_faces": {
        "card_id": "str",
        "face_index": "Int64",
        "name": "str",
        "side": "category",
        "power": "str",
        "toughness": "str",
        "loyalty": "str",
        "defense": "str",
        "power_num": "Float64",
        "toughness_num": "Float64",
        "loyalty_num": "Float64",
        "defense_num": "Float64",
        "power_is_variable": "bool",
        "toughness_is_variable": "bool",
        "loyalty_is_variable": "bool",
        "defense_is_variable": "bool",
        "pt_ratio": "Float64",
        "is_creature": "bool",
        "is_planeswalker": "bool",
        "is_artifact": "bool",
        "type": "str",
        "text": "str",
    },
    "keywords": {"card_id": "str", "face_index": "Int64", "keyword": "str"},
    "types": {
        "card_id": "str",
        "face_index": "Int64",
        "type_kind": "category",
        "type": "str",
    },
    "sets": {
        "id": "str",
        "parent_set": "str",
        "name": "str",
        "release_date": "datetime64[ns]",
        "block": "str",
        "type": "category",
        "total_cards": "Int64",
    },
}

def load_table_schema() -> None:
    with open(SCHEMAS_FILE, "r", encoding="utf-8") as f:
        global table_schemas
        table_schemas = json.load(f) 

def read_staging_tables(input_dir: Path) -> Dict[str, pd.DataFrame]:
    """
    Read full parquet tables from staging.
    Returns dict of DataFrames.
    """
    dfs = {}
    for table in table_schemas["staging_tables"].keys():
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
    analytics["card_faces"] = build_analytics_faces(dfs["faces"], dfs["types"])
    analytics["cards"] = build_analytics_cards(dfs["cards"], analytics["card_faces"], dfs["types"], dfs["sets"])
    analytics["keywords"] = build_analytics_keywords(dfs["keywords"])
    analytics["types"] = build_analytics_types(dfs["types"])
    analytics["sets"] = build_analytics_sets(dfs["sets"])
    return analytics

def build_analytics_faces(faces_df: pd.DataFrame, types_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive analytics for faces: numeric stats, flags, ratios.
    """
    # TODO: implement parsing and derivation

    return faces_df.copy()

def build_analytics_cards(cards_df: pd.DataFrame, faces_df: pd.DataFrame, types_df: pd.DataFrame, sets_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive analytics for cards: colors, text, types, printings from faces/joins.
    """
    # TODO: implement aggregation and derivation
    return cards_df.copy()

def build_analytics_keywords(keywords_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with dedupe/sort.
    """
    return keywords_df.drop_duplicates().sort_values(["card_id", "face_index", "keyword"]).reset_index(drop=True)

def build_analytics_types(types_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with dedupe/sort.
    """
    return types_df.drop_duplicates().sort_values(["card_id", "face_index", "type_kind", "type"]).reset_index(drop=True)

def build_analytics_sets(sets_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass-through with sort.
    """
    return sets_df.sort_values("id").reset_index(drop=True)

def validate_analytics_tables(analytics: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Validate analytics tables against schema (non-fatal).
    """
    # TODO: implement schema checks and collect warnings
    return {"analytics_validations": []}

def save_analytics_tables(analytics: Dict[str, pd.DataFrame], output_dir: Path) -> None:
    """
    Save analytics tables to CSV and Parquet.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    row_counts = {}
    for table, df in analytics.items():
        # CSV: stringify objects
        csv_df = df.copy()
        for col in df.select_dtypes(include=["object"]).columns:
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
    dfs = read_staging_tables(INPUT_DIR)
    analytics = build_analytics_tables(dfs)
    report = validate_analytics_tables(analytics)
    save_analytics_tables(analytics, OUTPUT_DIR)
    save_warehouse_report(report, OUTPUT_DIR)
    print("Analytics tables built successfully. See warehouse_report.json for details.")