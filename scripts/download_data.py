"""Download trends.db from the Kaggle dataset into data/.

Usage:
    uv run python scripts/download_data.py

Auth (pick one):
    - KAGGLE_API_TOKEN in .env  (KGAT_... token)
    - KAGGLE_USERNAME + KAGGLE_KEY env vars
    - ~/.kaggle/kaggle.json
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_HERE = Path(__file__).parent
_ROOT = _HERE.parent

load_dotenv(_ROOT / ".env")

# Map KAGGLE_API_TOKEN → KAGGLE_TOKEN (used by the kaggle package for KGAT_ tokens)
if os.environ.get("KAGGLE_API_TOKEN") and not os.environ.get("KAGGLE_TOKEN"):
    os.environ["KAGGLE_TOKEN"] = os.environ["KAGGLE_API_TOKEN"]

# Import after env vars are set so the kaggle package picks them up
import kaggle  # noqa: E402

DATASET = "tiagoadrianunes/last-fm-global-trends"
FILE = "trends.db"
DEST = _ROOT / "data"

DEST.mkdir(parents=True, exist_ok=True)


def main() -> None:
    print(f"Downloading {FILE} from {DATASET} → {DEST}/")
    kaggle.api.authenticate()
    kaggle.api.dataset_download_file(
        DATASET,
        file_name=FILE,
        path=str(DEST),
        force=True,
        quiet=False,
    )
    db_path = DEST / FILE
    if not db_path.exists():
        print(f"ERROR: {db_path} not found after download.", file=sys.stderr)
        sys.exit(1)

    size_mb = db_path.stat().st_size / 1024 / 1024
    print(f"Done — {db_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
