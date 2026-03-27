"""
Downloads and extracts the MTGJSON AtomicCards and SetList datasets, including verifying the download integrity and saving metadata about the extraction process.
"""

import hashlib
import json
import lzma
import requests
from pathlib import Path
from datetime import datetime, timezone

BASE_URL = "https://mtgjson.com/api/v5/"

DATASETS = [
    {
        "name": "AtomicCards",
        "file": "AtomicCards.json.xz",
        "hash": "AtomicCards.json.xz.sha256",
        "json": "AtomicCards.json",
    },
    {
        "name": "SetList",
        "file": "SetList.json.xz",
        "hash": "SetList.json.xz.sha256",
        "json": "SetList.json",
    },
]

OUT_DIR = Path("data/raw")


def download_file(url: str, path: Path):
    request = requests.get(url, stream=True)
    request.raise_for_status()

    with open(path, "wb") as f:
        for chunk in request.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


def compute_sha256(path: Path):
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def extract_xz(archive_path: Path, out_path: Path):
    with lzma.open(archive_path) as f_in:
        with open(out_path, "wb") as f_out:
            f_out.write(f_in.read())


def extract(outdir: Path, dataset: dict):
    raw_dir = outdir
    raw_dir.mkdir(parents=True, exist_ok=True)

    archive_path = raw_dir.joinpath(dataset["file"])
    hash_path = raw_dir.joinpath(dataset["hash"])
    json_path = raw_dir.joinpath(dataset["json"])

    archive_url = BASE_URL + dataset["file"]
    hash_url = BASE_URL + dataset["hash"]

    print(f"Downloading {dataset['name']} archive...")
    download_file(archive_url, archive_path)

    print(f"Downloading {dataset['name']} checksum...")
    download_file(hash_url, hash_path)

    expected_hash = hash_path.read_text().split()[0]
    actual_hash = compute_sha256(archive_path)

    if actual_hash != expected_hash:
        raise ValueError(f"SHA256 verification failed for {dataset['name']}!")

    print(f"{dataset['name']} checksum verified")

    print(f"Extracting {dataset['name']} archive...")
    extract_xz(archive_path, json_path)

    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
        date_str = raw.get("meta", {}).get("date", "unknown")
        ver_str = raw.get("meta", {}).get("version", "unknown")

    metadata = {
        "source": "MTGJSON",
        "dataset": dataset["name"],
        "download_url": archive_url,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "sha256_verified": True,
        "archive_name": dataset["file"],
        "data_date": date_str,
        "data_version": ver_str,
    }

    meta_path = raw_dir.joinpath(f"{dataset['name']}Source.json")
    meta_path.write_text(json.dumps(metadata, indent=2))

    print(f"{dataset['name']} extraction complete")
    print(f"JSON saved to {json_path}")


if __name__ == "__main__":
    print("Extracting MTGJSON datasets...")
    for dataset in DATASETS:
        print(f"\nProcessing {dataset['name']}...")
        extract(OUT_DIR, dataset)
    print("\nAll extractions complete.")
