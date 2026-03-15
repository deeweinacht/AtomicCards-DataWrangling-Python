"""
Downloads and extracts the MTGJSON AtomicCards dataset, including verifying the download integrity and saving metadata about the extraction process.
"""
import hashlib
import json
import lzma
import requests
from pathlib import Path
from datetime import datetime, timezone

BASE_URL = "https://mtgjson.com/api/v5/"
FILE_NAME = "AtomicCards.json.xz"
HASH_NAME = "AtomicCards.json.xz.sha256"
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


def extract(outdir: Path):
    raw_dir = outdir
    raw_dir.mkdir(parents=True, exist_ok=True)

    archive_path = raw_dir.joinpath(FILE_NAME)
    hash_path = raw_dir.joinpath(HASH_NAME)
    json_path = raw_dir.joinpath("AtomicCards.json")

    archive_url = BASE_URL + FILE_NAME
    hash_url = BASE_URL + HASH_NAME

    print("Downloading archive...")
    download_file(archive_url, archive_path)

    print("Downloading checksum...")
    download_file(hash_url, hash_path)

    expected_hash = hash_path.read_text().split()[0]
    actual_hash = compute_sha256(archive_path)

    if actual_hash != expected_hash:
        raise ValueError("SHA256 verification failed!")

    print("Checksum verified")

    print("Extracting archive...")
    extract_xz(archive_path, json_path)

    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
        date_str = raw.get("meta", {}).get("date", "unknown")
        ver_str = raw.get("meta", {}).get("version", "unknown")

    metadata = {
        "source": "MTGJSON",
        "dataset": "AtomicCards",
        "download_url": archive_url,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "sha256_verified": True,
        "archive_name": FILE_NAME,
        "data_date": date_str,
        "data_version": ver_str,
    }

    meta_path = raw_dir.joinpath("AtomicCardsSource.json")
    meta_path.write_text(json.dumps(metadata, indent=2))

    print("Extraction complete")
    print(f"JSON saved to {json_path}")


if __name__ == "__main__":
    print("Extracting MTGJSON dataset...")
    extract(OUT_DIR)
