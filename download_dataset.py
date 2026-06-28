#!/usr/bin/env python3
"""
download_dataset.py
───────────────────
Downloads the IMDb dataset from Kaggle using kagglehub and copies the
required files into data/raw/, replacing any existing files.

Dataset: https://www.kaggle.com/datasets/ashirwadsangwan/imdb-dataset

Prerequisites:
  1. Install kagglehub:  pip install kagglehub
  2. Authenticate once — kagglehub will prompt on first run, or you can:
       export KAGGLE_USERNAME=your_username
       export KAGGLE_KEY=your_api_key
     Or place kaggle.json at ~/.kaggle/kaggle.json (chmod 600).

Usage:
  python3 download_dataset.py
  # or via make:
  make download
"""

import gzip
import shutil
import sys
from pathlib import Path

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import kagglehub
except ImportError:
    print(
        "ERROR: kagglehub not installed.\n"
        "  pip install kagglehub",
        file=sys.stderr,
    )
    sys.exit(1)

DATASET_SLUG = "ashirwadsangwan/imdb-dataset"
RAW_DIR = Path(__file__).parent / "data" / "raw"

REQUIRED_FILES = (
    "title.basics.tsv",      # title metadata (type, year, genre, runtime)
    "title.ratings.tsv",     # weighted avg rating + vote count
    "name.basics.tsv",       # people — actors, directors, writers
    "title.principals.tsv",  # cast/crew per title (links titles → people)
    "title.akas.tsv",        # alternate titles by region/language
)

def download_dataset() -> Path:
    """Download the dataset and return kagglehub's local cache path."""
    return Path(kagglehub.dataset_download(DATASET_SLUG))


def _decompress_gz(src: Path, dst: Path) -> None:
    """Decompress a gzip file to dst, streaming to avoid loading 2 GB into RAM."""
    with gzip.open(src, "rb") as f_in, open(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def copy_to_raw(source_dir: Path) -> None:
    """Copy required TSV files into data/raw/, decompressing .gz files if needed."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    available = {path.name: path for path in source_dir.rglob("*") if path.is_file()}

    for filename in REQUIRED_FILES:
        dst = RAW_DIR / filename
        src_plain = available.get(filename)
        src_gz = available.get(f"{filename}.gz")

        if src_plain is not None:
            shutil.copy2(src_plain, dst)
        elif src_gz is not None:
            _decompress_gz(src_gz, dst)
        else:
            print(f"WARNING: {filename} (or {filename}.gz) not found.", file=sys.stderr)


def verify_files() -> None:
    """Confirm all required files landed in data/raw/."""
    missing = [f for f in REQUIRED_FILES if not (RAW_DIR / f).exists()]
    if missing:
        print(
            "WARNING: Missing required files: " + ", ".join(missing),
            file=sys.stderr,
        )
    else:
        print(f"Ready: {len(REQUIRED_FILES)} files in {RAW_DIR}")

def main() -> None:
    print("Downloading IMDb dataset...")
    cached_path = download_dataset()
    copy_to_raw(cached_path)
    verify_files()


if __name__ == "__main__":
    main()
