"""
Filter MTGJSON card and set data to the project's paper-constructed analytical scope.

The script excludes digital-only content, non-constructed game objects, selected nonstandard
card types/layouts, and cards printed exclusively in defined un-set products. It writes filtered
card and set JSON outputs along with a summary report of filtering counts and applied rules.
"""

import json
from pathlib import Path

RAW_ATOMIC_CARD_DATA_FILE = Path("data/raw/AtomicCards.json")
RAW_SET_LIST_DATA_FILE = Path("data/raw/SetList.json")
OUT_DIR = Path("data/filtered")
ATOMIC_CARD_OUTPUT_FILE = OUT_DIR.joinpath("atomiccards_paper_constructed.json")
SET_LIST_OUTPUT_FILE = OUT_DIR.joinpath("setlist_paper_constructed.json")
REPORT_OUTPUT_FILE = OUT_DIR.joinpath("paper_constructed_filtering_report.json")

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
PLAYABLE_FORMAT_STATUSES = {"Legal", "Restricted"}
NON_CONSTRUCTED_LAYOUTS = {
    "token",
    "emblem",
    "scheme",
    "vanguard",
    "planar",
    "host",
    "augment",
    "reversible_card",  #  technically a constructed-legal layout, but these are reprints of existing cards and would result in duplicates in the database
}
NON_CONSTRUCTED_SUPERTYPES = {"host"}

NON_CONSTRUCTED_TYPES = {
    "conspiracy",
    "eaturecray",  # A one-off joke type from an unset, excluded for clarity
    "ever",  # A one-off joke type from an unset, excluded for clarity
    "phenome-nom",  # A one-off joke type from an unset, excluded for clarity
    "phenomenon",
    "plane",
    "scariest",  # A one-off joke type from an unset, excluded for clarity
    "scheme",
    "see",  # A one-off joke type from an unset, excluded for clarity
    "stickers",
    "universewalker",
    "you'll",  # A one-off joke type from an unset, excluded for clarity
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

MANUALLY_EXCLUDED_SET_CODES = {
    "PMEI"
}  # PMEI is a long-running promotional grouping rather than a standard comparable set release, which distorts time-based set analysis

NON_CONSTRUCTED_SET_TYPES = {
    "memorabilia",
    "token",
    "minigame",
}


def card_is_funny(faces) -> bool:
    return any(bool(face.get("isFunny")) for face in faces)


def card_legal_any_paper_constructed(faces) -> bool:
    for face in faces:
        leg = face.get("legalities") or {}
        for fmt, status in leg.items():
            if fmt.lower() in PAPER_FORMATS and status in PLAYABLE_FORMAT_STATUSES:
                return True
    return False


def card_has_non_constructed_layout(faces) -> bool:
    return any(
        (face.get("layout", "") or "").lower() in NON_CONSTRUCTED_LAYOUTS
        for face in faces
    )


def card_has_non_standard_type(faces) -> bool:
    # Cards have multiple categories of type, so we check each for invalid types
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
        "Excluded -\nfunny:",
        counts["funny"],
        "nonstandard layout:",
        counts["nonstandard_layout"],
        "nonstandard type:",
        counts["nonstandard_type"],
        "not paper playable:",
        counts["not_paper_playable"],
        "from unset:",
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
        "not_paper_playable": 0,
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
            counts["not_paper_playable"] += 1
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
        print(f"Error writing {ATOMIC_CARD_OUTPUT_FILE} output file: {e}")
        raise

    return counts


def select_cards() -> dict:
    with open(RAW_ATOMIC_CARD_DATA_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data = raw.get("data", raw)  # card data nests under "data"
    counts = process_cards(data)
    print_card_counts(counts)
    return counts


def print_set_counts(counts: dict):
    print(f"Total sets in source: {counts['total_sets']}")
    print(f"Kept (paper constructed): {counts['kept']}")
    print(f"Excluded (not paper playable): {counts['not_paper_playable']}")
    print(f"Excluded (manually excluded): {counts['manually_excluded']}")


def process_sets(data: list) -> dict:
    legal_sets = []
    counts = {
        "total_sets": len(data),
        "kept": 0,
        "not_paper_playable": 0,
        "manually_excluded": 0,
    }

    for set_record in data:
        if set_record.get("code") in MANUALLY_EXCLUDED_SET_CODES:
            counts["manually_excluded"] += 1
            continue
        if (
            not set_record.get("isOnlineOnly", False)
            and set_record.get("type", "").lower() not in NON_CONSTRUCTED_SET_TYPES
            and set_record.get("code") not in UNSETS
        ):
            legal_sets.append(set_record)
            counts["kept"] += 1
        else:
            counts["not_paper_playable"] += 1

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(SET_LIST_OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(legal_sets, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error writing {SET_LIST_OUTPUT_FILE} output file: {e}")
        raise

    return counts


def select_sets() -> dict:
    with open(RAW_SET_LIST_DATA_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data = raw.get("data", raw)  # set data nests under "data"
    counts = process_sets(data)
    print_set_counts(counts)
    return counts


def save_report(cards: dict, sets: dict):
    rules = {
        "formats_included": sorted(PAPER_FORMATS),
        "supertypes_excluded": sorted(NON_CONSTRUCTED_SUPERTYPES),
        "types_excluded": sorted(NON_CONSTRUCTED_TYPES),
        "subtypes_excluded": sorted(NON_CONSTRUCTED_SUBTYPES),
        "sets_excluded": sorted(UNSETS | MANUALLY_EXCLUDED_SET_CODES),
    }
    report = {"card_counts": cards, "set_counts": sets, "filtering_rules": rules}
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(REPORT_OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error writing {REPORT_OUTPUT_FILE} output file: {e}")
        raise


def filter_for_paper_constructed():
    print("Filtering cards...")
    card_counts = select_cards()
    print("Filtering sets...")
    set_counts = select_sets()
    save_report(card_counts, set_counts)


if __name__ == "__main__":
    filter_for_paper_constructed()
