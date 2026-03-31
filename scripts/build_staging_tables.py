"""Iterates through the paper selected cards JSON and transforms into staging tables"""

from slugify import slugify
import json
import pandas as pd
from pathlib import Path


CARDS_PAPER_CONSTRUCTED_DATA = Path("data/selected/atomiccards_paper_constructed.json")
SETS_PAPER_CONSTRUCTED_DATA = Path("data/selected/setlist_paper_constructed.json")
OUT_DIR = Path("data/staging")
REPORT = {}

SCHEMAS_FILE = Path("docs/schemas.json")
STAGING_SCHEMA = {}


# Map for converting the different type categories in the source data to a consistent "type_kind" in the staging tables
KIND_MAP = {"types": "type", "supertypes": "supertype", "subtypes": "subtype"}

def load_staging_schema() -> None:
    global STAGING_SCHEMA
    with open(SCHEMAS_FILE, "r", encoding="utf-8") as f:
        table_schemas = json.load(f)
    STAGING_SCHEMA = table_schemas["staging_tables"]

def build_card_row(card_name: str, faces: list) -> dict:
    # For simplicity, card-level fields are taken from the first face, as these should be consistent across faces.
    card_row = {}
    front_face = faces[0] if len(faces) > 0 else {}

    card_row["id"] = slugify(card_name)  # Slugify card name to create a unique ID
    card_row["color_identity"] = front_face.get(
        "colorIdentity"
    )  # Used in commander format, which is important for paper constructed
    card_row["edh_rec_rank"] = front_face.get(
        "edhrecRank"
    )  # Used in commander format, which is important for paper constructed
    card_row["has_alt_deck_limit"] = front_face.get("hasAlternativeDeckLimit", False)
    card_row["is_game_changer"] = front_face.get(
        "isGameChanger", False
    )  # Used in commander format, which is important for paper constructed
    card_row["is_reserved"] = front_face.get("isReserved", False)
    card_row["layout"] = front_face.get("layout")
    card_row["legalities"] = front_face.get("legalities")
    card_row["mana_value"] = front_face.get("manaValue")
    card_row["name"] = front_face.get("name")
    card_row["printings"] = front_face.get("printings")
    card_row["rulings"] = front_face.get("rulings")
    return card_row


def build_face_row(card_id: str, index: int, face: dict) -> dict:
    face_row = {"card_id": card_id}  # Foreign key to card table
    face_row["face_index"] = index
    face_row["name"] = (
        face.get("faceName") or face.get("name")
    )  # some cards have "faceName" for individual faces, but single-face cards only have the card-level name
    face_row["side"] = face.get(
        "side"
    )  # for cards with multiple faces, e.g. "a" or "b"
    face_row["colors"] = face.get("colors")
    face_row["defense"] = face.get("defense")  # only present on battle card types
    face_row["mana_value"] = (
        face.get("faceManaValue") or face.get("manaValue")
    )  # some cards have "faceManaValue" for individual faces, but single-face cards only have the card-level manaValue
    face_row["loyalty"] = face.get("loyalty")  # only present on planeswalker card types
    face_row["mana_cost"] = face.get(
        "manaCost"
    )  # list of mana symbols in the cost, e.g. ["{1}", "{U}"]
    face_row["power"] = face.get("power")
    face_row["text"] = face.get("text")
    face_row["toughness"] = face.get("toughness")
    face_row["type"] = face.get("type")

    return face_row


def build_keyword_rows(card_id: str, faces: list) -> list:
    # For simplicity, keywords are taken from the first face, as these are consistent across faces.
    front_face = faces[0] if len(faces) > 0 else {}
    keyword_rows = []
    keywords = front_face.get("keywords") or []
    for keyword in keywords:
        keyword_row = {
            "card_id": card_id,  # Foreign key to card table
            "keyword": keyword,
        }
        keyword_rows.append(keyword_row)
    return keyword_rows


def build_types_rows(card_id: str, face_index: int, face: dict) -> list:

    type_rows = []
    for kind in KIND_MAP.keys():
        types = face.get(kind) or []
        for t in types:
            type_row = build_type_row(card_id, face_index, kind, t)
            type_rows.append(type_row)
    return type_rows


def build_type_row(card_id: str, face_index: int, kind: str, type_value: str) -> dict:
    return {
        "card_id": card_id,  # Foreign key to card table
        "face_index": face_index,
        "type_kind": KIND_MAP[kind],  # remove plural "s" for clarity
        "type": type_value,
    }

def build_set_row(set: dict) -> dict:
    return {
        "id": set.get("code"),  # Use set code as unique ID
        "parent_set": set.get("parentCode"),  # For sets that are part of a larger group
        "name": set.get("name"),
        "release_date": set.get("releaseDate"),
        "block": set.get("block"),
        "type": set.get("type"),
        "total_cards": set.get("totalSetSize"),
    }

def create_staging_dataframes():
    """Load card data from JSON and create initial DataFrames for staging tables."""
    card_rows = []
    faces_rows = []
    card_keyword_rows = []
    card_type_rows = []
    set_rows = []

    with open(CARDS_PAPER_CONSTRUCTED_DATA, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data = raw.get("data", raw)  # actual data nests under "data"
    for card_name, faces in data.items():
        card_row: dict | None = build_card_row(card_name, faces)
        if card_row is not None:
            card_rows.append(card_row)
            keyword_rows = build_keyword_rows(card_row["id"], faces)
            card_keyword_rows.extend(keyword_rows)
            for i, face in enumerate(faces):
                face_row = build_face_row(card_row["id"], i, face)
                faces_rows.append(face_row)

                face_type_rows = build_types_rows(card_row["id"], i, face)
                card_type_rows.extend(face_type_rows)



    with open(SETS_PAPER_CONSTRUCTED_DATA, "r", encoding="utf-8") as f:
        raw_sets = json.load(f)
    
    for set in raw_sets:
        set_rows.append(build_set_row(set))

    return {
        "cards": pd.DataFrame(card_rows),
        "faces": pd.DataFrame(faces_rows),
        "keywords": pd.DataFrame(card_keyword_rows).drop_duplicates(),
        "types": pd.DataFrame(card_type_rows).drop_duplicates(),
        "sets": pd.DataFrame(set_rows),
    }


def handle_missing_values(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Handle missing values in staging tables according to business rules."""
    missing_value_errors = []
    for table_type, table_definition in STAGING_SCHEMA.items():
        df = dfs.get(table_type)
        if df is None:
            missing_value_errors.append(
                f'Expected staging table "{table_type}" is missing.'
            )
            continue
        for column, dtype in table_definition.items():
            if column not in df.columns:
                missing_value_errors.append(
                    f'Expected column "{column}" is missing from {table_type} staging table.'
                )
                continue
            if dtype == "str" and column not in [
                "side",
                "defense",
                "loyalty",
                "mana_cost",
                "power",
                "toughness",
                "parent_set",
            ]:
                # For string fields, we fill missing values with an empty string, except for certain fields where null likely indicates "not applicable" rather than a true empty value
                dfs[table_type][column] = df[column].fillna("")
            elif dtype in ["Int64", "float64"] and column not in ["edh_rec_rank"]:
                # For numeric fields, we fill missing values with 0, except for "edh_rec_rank" where null likely indicates "not applicable" rather than a true zero value
                dfs[table_type][column] = df[column].fillna(0)
            elif dtype == "bool":
                dfs[table_type][column] = df[column].fillna(False)
            # For other field types (e.g., object), we leave nulls as is, as they generally indicate "not applicable" rather than a true missing value
    REPORT["missing_value_errors"] = missing_value_errors
    return dfs


def apply_staging_schema(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Apply type conversions to match the staging schema."""
    schema_errors = []
    for table_type, table_definition in STAGING_SCHEMA.items():
        for column, dtype in table_definition.items():
            df = dfs.get(table_type)
            if df is None:
                schema_errors.append(
                    f'Expected staging table "{table_type}" is missing.'
                )
                continue
            if column not in df.columns:
                schema_errors.append(
                    f'Expected column "{column}" is missing from {table_type} staging table.'
                )
                pass
            else:
                if dtype in ["Int64", "float64", "bool"] and df[column].dtype == "str":
                    try:
                        df[column] = pd.to_numeric(df[column], errors="coerce")
                    except Exception as e:
                        schema_errors.append(
                            f'Could not convert column "{column}" in {table_type} staging table to numeric type {dtype}: {e}'
                        )
                try:
                    dfs[table_type][column] = dfs[table_type][column].astype(dtype)
                except Exception as e:
                    schema_errors.append(
                        f'Could not convert column "{column}" in {table_type} staging table to type {dtype}: {e}'
                    )
    REPORT["schema_errors"] = schema_errors
    return dfs


def sort_staging_dataframes(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Sort staging tables for consistency."""

    if "cards" in dfs:
        dfs["cards"] = dfs["cards"].sort_values("id").reset_index(drop=True)
    if "faces" in dfs:
        dfs["faces"] = (
            dfs["faces"].sort_values(["card_id", "face_index"]).reset_index(drop=True)
        )
    if "keywords" in dfs:
        dfs["keywords"] = (
            dfs["keywords"]
            .sort_values(["card_id", "keyword"])
            .reset_index(drop=True)
        )
    if "types" in dfs:
        dfs["types"] = (
            dfs["types"]
            .sort_values(["card_id", "face_index", "type_kind", "type"])
            .reset_index(drop=True)
        )
    if "sets" in dfs:
        dfs["sets"] = dfs["sets"].sort_values("id").reset_index(drop=True)
    return dfs


def validate_staging_dataframes(dfs: dict[str, pd.DataFrame]) -> dict:
    """Validate staging tables against schema requirements."""

    validation_passed = True
    validation_failures = []
    if "cards" in dfs:
        if "id" not in dfs["cards"].columns:
            validation_failures.append("Cards table must have an 'id' column.")
        if dfs["cards"]["id"].isnull().any():
            validation_failures.append("Cards table must not have null 'id' values.")
        if not dfs["cards"]["id"].is_unique:
            validation_failures.append("Card IDs must be unique.")
    if "faces" in dfs:
        if "card_id" not in dfs["faces"].columns:
            validation_failures.append("Faces table must have a 'card_id' column.")
        if "face_index" not in dfs["faces"].columns:
            validation_failures.append("Faces table must have a 'face_index' column.")
        if dfs["faces"]["card_id"].isnull().any():
            validation_failures.append(
                "Faces table must not have null 'card_id' values."
            )
        if dfs["faces"]["face_index"].isnull().any():
            validation_failures.append(
                "Faces table must not have null 'face_index' values."
            )
        if dfs["faces"][["card_id", "face_index"]].duplicated().any():
            validation_failures.append(
                "Combination of card_id and face_index in faces table must be unique."
            )
    if "keywords" in dfs:
        if "card_id" not in dfs["keywords"].columns:
            validation_failures.append("Keywords table must have a 'card_id' column.")
        if "keyword" not in dfs["keywords"].columns:
            validation_failures.append("Keywords table must have a 'keyword' column.")
        if dfs["keywords"]["card_id"].isnull().any():
            validation_failures.append(
                "Keywords table must not have null 'card_id' values."
            )
        if dfs["keywords"]["keyword"].isnull().any():
            validation_failures.append(
                "Keywords table must not have null 'keyword' values."
            )
        if dfs["keywords"][["card_id", "keyword"]].duplicated().any():
            validation_failures.append(
                "Combination of card_id and keyword in keywords table must be unique."
            )
    if "types" in dfs:
        if "card_id" not in dfs["types"].columns:
            validation_failures.append("Types table must have a 'card_id' column.")
        if "face_index" not in dfs["types"].columns:
            validation_failures.append("Types table must have a 'face_index' column.")
        if "type_kind" not in dfs["types"].columns:
            validation_failures.append("Types table must have a 'type_kind' column.")
        if "type" not in dfs["types"].columns:
            validation_failures.append("Types table must have a 'type' column.")
        if dfs["types"]["card_id"].isnull().any():
            validation_failures.append(
                "Types table must not have null 'card_id' values."
            )
        if dfs["types"]["face_index"].isnull().any():
            validation_failures.append(
                "Types table must not have null 'face_index' values."
            )
        if dfs["types"]["type_kind"].isnull().any():
            validation_failures.append(
                "Types table must not have null 'type_kind' values."
            )
        if dfs["types"]["type"].isnull().any():
            validation_failures.append("Types table must not have null 'type' values.")
        if (
            dfs["types"][["card_id", "face_index", "type_kind", "type"]]
            .duplicated()
            .any()
        ):
            validation_failures.append(
                "Combination of card_id, face_index, type_kind, and type in types table must be unique."
            )
    if "sets" in dfs:
        if "id" not in dfs["sets"].columns:
            validation_failures.append("Sets table must have an 'id' column.")
        if dfs["sets"]["id"].isnull().any():
            validation_failures.append("Sets table must not have null 'id' values.")
        if not dfs["sets"]["id"].is_unique:
            validation_failures.append("Set IDs must be unique.")

    if validation_failures:
        validation_passed = False
    REPORT["validation_passed"] = validation_passed
    REPORT["validation_failures"] = validation_failures
    return {
        "validation_passed": validation_passed,
        "validation_failures": validation_failures,
    }


def save_staging_tables(dfs: dict[str, pd.DataFrame]) -> None:
    row_counts = {}
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for table_type, df in dfs.items():
        preview_path = OUT_DIR.joinpath(f"preview_stg_{table_type}.csv")
        df.head(100).to_csv(preview_path, index=False)

        table_path = OUT_DIR.joinpath(f"stg_{table_type}.parquet")
        df.to_parquet(table_path, index=False)

        row_counts[table_type] = len(df)
    REPORT["row_counts"] = row_counts


def save_staging_report():
    error_count = 0
    for values in REPORT.values():
        error_count += len(values) if isinstance(values, list) else 0
    REPORT["total_errors"] = error_count

    report_path = OUT_DIR.joinpath("staging_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(REPORT, f, indent=2)
    print(f"Staging report saved to {report_path}")


if __name__ == "__main__":
    # create and validate staging tables

    load_staging_schema()

    staging_dfs = create_staging_dataframes()

    staging_dfs = handle_missing_values(staging_dfs)

    staging_dfs = apply_staging_schema(staging_dfs)

    staging_dfs = sort_staging_dataframes(staging_dfs)

    validation_results = validate_staging_dataframes(staging_dfs)

    save_staging_tables(staging_dfs)

    save_staging_report()

    print(
        "Staging tables created successfully. See staging_report.json for details about any issues encountered during processing."
    )
