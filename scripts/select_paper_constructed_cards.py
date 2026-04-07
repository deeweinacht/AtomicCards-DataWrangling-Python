"""
Select script to identify cards that are legal in paper constructed formats, excluding those that are only digital,
have non-standard layouts or types, or are exclusively from unsets.
The script reads the raw MTGJSON data, applies filtering criteria, and outputs a JSON file in the same format
containing only the cards that meet the paper constructed legality requirements.
After processing, it prints counts of how many cards were kept and how many were excluded for each reason.
"""

import json
from pathlib import Path

RAW_ATOMIC_CARD_DATA_FILE = Path("data/raw/atomiccards.json")
RAW_SET_LIST_DATA_FILE = Path("data/raw/setlist.json")
OUT_DIR = Path("data/selected")
ATOMIC_CARD_OUTPUT_FILE = OUT_DIR.joinpath("atomiccards_paper_constructed.json")
SET_LIST_OUTPUT_FILE = OUT_DIR.joinpath("setlist_paper_constructed.json")

PAPER_FORMATS = {
    "commander",
    "oathbreaker",
    "standard",
    "pioneer",
    "modern",
    "legacy",
    "vintage",
    "pauper",
    "penny",
    "premodern",
    "oldschool",
}
PLAYABLE_STATUSES = {"Legal", "Restricted"}
NON_CONSTRUCTED_LAYOUTS = {
    "token",
    "emblem",
    "scheme",
    "vanguard",
    "planar",
    "host",
    "augment",
    "reversible_card" #  technically a constructed-legal layout, but these are reprints of existing cards and would result in duplicates in the database
}
NON_CONSTRUCTED_SUPERTYPES = {"host"}

NON_CONSTRUCTED_TYPES = {
    "conspiracy",
    "eaturecray",
    "ever",
    "phenome-nom",
    "phenomenon",
    "plane",
    "scariest",
    "scheme",
    "see",
    "stickers",
    "universewalker",
    "you'll",
}

NON_CONSTRUCTED_SUBTYPES = {"attraction", "contraption", "dungeon"}


UNSETS = {
    "UGL",
    "UNH",
    "PUNH",
    "UST",
    "PUST",
    "UND",
    "UNF",
    "SUNF",
    "ULST",
    "UNK",
    "PUNK",
}


def card_is_funny(faces) -> bool:
    return any(bool(face.get("isFunny")) for face in faces)


def card_legal_any_paper_constructed(faces) -> bool:
    for face in faces:
        leg = face.get("legalities") or {}
        for fmt, status in leg.items():
            if fmt.lower() in PAPER_FORMATS and status in PLAYABLE_STATUSES:
                return True
    return False


def card_has_non_constructed_layout(faces) -> bool:
    return any(
        (face.get("layout", "") or "").lower() in NON_CONSTRUCTED_LAYOUTS
        for face in faces
    )


def card_has_non_standard_type(faces) -> bool:
    # MTGJSON has multiple ways to indicate types, so we check all relevant fields
    for face in faces:
        supertypes_list = face.get("supertypes") or []
        if any(st.lower() in NON_CONSTRUCTED_SUPERTYPES for st in supertypes_list):
            return True

        types_list = face.get("types") or []
        if any(st.lower() in NON_CONSTRUCTED_TYPES for st in types_list):
            return True

        subtypes_list = face.get("subtypes") or []
        if any(st.lower() in NON_CONSTRUCTED_SUBTYPES for st in subtypes_list):
            return True

        t = face.get("type") or ""
        split_t = t.split(" ")
        if any(st.lower() in NON_CONSTRUCTED_TYPES for st in split_t):
            print(f"Card '{face}' has non-standard type: {t}")
        if any(st.lower() in NON_CONSTRUCTED_SUPERTYPES for st in split_t):
            print(f"Card '{face}' has non-standard supertype: {t}")
        if any(st.lower() in NON_CONSTRUCTED_SUBTYPES for st in split_t):
            print(f"Card '{face}' has non-standard subtype: {t}")
    return False


def card_is_from_unset(faces) -> bool:
    for face in faces:
        printings = face.get("printings") or []
        if printings and len(printings) == 1:
            if printings[0] in UNSETS:
                return True
    return False


def print_card_counts(counts: dict):
    print(f"Total cards in source: {counts['total_cards']}")
    print(f"Kept (paper constructed): {counts['kept']}")
    print(
        "Excluded — funny:",
        counts["funny"],
        "nonstandard_layout:",
        counts["nonstandard_layout"],
        "nonstandard_type:",
        counts["nonstandard_type"],
        "not_paper_legal:",
        counts["not_paper_legal"],
        "from_unset:",
        counts["from_unset"],
    )


def process_cards(data: dict) -> dict:
    legal_cards = {}
    counts = {
        "total_cards": len(data),
        "kept": 0,
        "funny": 0,
        "nonstandard_layout": 0,
        "nonstandard_type": 0,
        "not_paper_legal": 0,
        "from_unset": 0,
    }

    for card_name, faces in data.items():
        if card_is_funny(faces):
            counts["funny"] += 1
            continue
        if card_has_non_constructed_layout(faces):
            counts["nonstandard_layout"] += 1
            continue
        if card_has_non_standard_type(faces):
            counts["nonstandard_type"] += 1
            continue
        if not card_legal_any_paper_constructed(faces):
            counts["not_paper_legal"] += 1
            continue
        if card_is_from_unset(faces):
            counts["from_unset"] += 1
            continue

        legal_cards[card_name] = faces
        counts["kept"] += 1

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(ATOMIC_CARD_OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(legal_cards, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error writing output file: {e}")

    return counts


def select_cards():
    with open(RAW_ATOMIC_CARD_DATA_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data = raw.get("data", raw)  # card data nests under "data"
    counts = process_cards(data)
    print_card_counts(counts)


def print_set_counts(counts: dict):
    print(f"Total sets in source: {counts['total_sets']}")
    print(f"Kept (paper legal): {counts['kept']}")
    print(f"Excluded — not paper legal: {counts['not_paper_legal']}")


def process_sets(data: list) -> list:
    legal_sets = []
    counts = {"total_sets": len(data), "kept": 0, "not_paper_legal": 0}

    for set in data:
        if set.get("code") == "PMEI":  # exclude the 'Media and Collaboration Promos' set because it causes issues with card release date analysis
            continue
        if not set.get("isOnlineOnly", False) and set.get("type", "").lower() not in {"memorabilia", "token", "minigame"}:
            legal_sets.append(set)
            counts["kept"] += 1
        else:
            counts["not_paper_legal"] += 1

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(SET_LIST_OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(legal_sets, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error writing output file: {e}")

    return counts


def select_sets():
    with open(RAW_SET_LIST_DATA_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data = raw.get("data", raw)  # set data nests under "data"
    counts = process_sets(data)
    print_set_counts(counts)

def select_paper_constructed():
    select_cards()
    select_sets()

if __name__ == "__main__":
    select_paper_constructed()
