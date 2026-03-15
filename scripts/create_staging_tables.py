'''Iterates through the paper selected cards JSON and tranforms into staging tables for further transformation'''

import json
from typing import TypedDict

import pandas as pd
from pathlib import Path
from slugify import slugify

SELECTED_DATA_FILE = Path("data/selected/atomiccards_paper_constructed.json")

def get_card_id(faces: list) -> str:
    slugged_names = [slugify(face["name"]) for face in faces if face.get("name")]
    if len(set(slugged_names)) == 1:
        return slugged_names[0]
    elif len(set(slugged_names)) > 1:
        print(f"Warning: Multiple names found: {slugged_names}.")
        return slugged_names[0]  # Arbitrarily return the first one, but this should be investigated further.
    else:
        print("Warning: No name found for card.")
        return None



def get_card_field(faces: list, field_name: str):

    pass

def get_face_field(faces: list, field_name: str):
    pass

def create_staging_tables():
    card_rows = []
    faces_rows = []

    with open(SELECTED_DATA_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)
    
    data = raw.get("data", raw)  # actual data nests under "data"
    for card_name, faces in data.items():
        card_row = {}
        face_row = {}
        card_id = get_card_id(faces)

        card_rows["card_id"] = card_id
        face_row["card_id"] = card_id


if __name__ == "__main__":
    create_staging_tables()
