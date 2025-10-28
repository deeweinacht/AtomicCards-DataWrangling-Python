import json

with open("data/raw/atomiccards.json", "r", encoding="utf-8") as f:
    raw = json.load(f)

data = raw.get("data", raw)  # some MTGJSON files nest under "data"

PAPER_FORMATS = {
    "commander", "oathbreaker", "standard", "pioneer", "modern",
    "legacy", "vintage", "pauper", "penny", "premodern", "oldschool"
}
PLAYABLE_STATUSES = {"Legal", "Restricted"}

NON_CONSTRUCTED_LAYOUTS = {
    "token", "emblem", "scheme", "vanguard", "plane", "phenomenon",
    "attraction", "sticker", "contraption"
}

def card_is_funny(faces) -> bool:
    return any(bool(face.get("isFunny")) for face in faces)

def card_has_ephemera_layout(faces) -> bool:
    return any((face.get("layout", "") or "").lower() in NON_CONSTRUCTED_LAYOUTS for face in faces)

def card_has_conspiracy(faces) -> bool:
    # MTGJSON has "types" list and "type" string; check both, case-sensitive token
    for face in faces:
        types_list = face.get("types") or []
        if "Conspiracy" in types_list:
            return True
        # fallback: scan the string if present
        t = face.get("type") or ""
        if "Conspiracy" in t.split(" "):
            return True
    return False

def card_legal_any_paper_constructed(faces) -> bool:
    for face in faces:
        leg = face.get("legalities") or {}
        for fmt, status in leg.items():
            if fmt.lower() in PAPER_FORMATS and status in PLAYABLE_STATUSES:
                return True
    return False

legal_cards = {}
counts = {"total_cards": len(data), "kept": 0, "funny": 0, "ephemera": 0, "conspiracy": 0, "not_paper_playable": 0}

for card_name, faces in data.items():
    if card_is_funny(faces):
        counts["funny"] += 1
        continue
    if card_has_ephemera_layout(faces):
        counts["ephemera"] += 1
        continue
    if card_has_conspiracy(faces):
        counts["conspiracy"] += 1
        continue
    if not card_legal_any_paper_constructed(faces):
        counts["not_paper_playable"] += 1
        continue

    legal_cards[card_name] = faces
    counts["kept"] += 1

print(f"Total cards in source: {counts['total_cards']}")
print(f"Kept (paper constructed): {counts['kept']}")
print("Excluded — funny:", counts["funny"],
      "ephemera:", counts["ephemera"],
      "conspiracy:", counts["conspiracy"],
      "not_paper_playable:", counts["not_paper_playable"])
