"""Deploy the app to shinyapps.io, updating the existing app in place.

Usage:
    uv run python scripts/deploy.py --account NAME --app-id ID   # deploy
    uv run python scripts/deploy.py --dry-run                    # list files to upload

The account name and app ID can also come from the SHINYAPPS_ACCOUNT and
SHINYAPPS_APP_ID environment variables (e.g. in .env). A deploy fails if either
is missing, so it never creates a new app by accident.

One-time setup: save your shinyapps.io account with `uv run rsconnect add ...`
(see README → Deploying to shinyapps.io). --account must match that saved name.

What it does:
    1. Exports requirements.txt from uv.lock (no dev dependencies).
    2. Runs `rsconnect deploy shiny` against the existing app, excluding the local
       database (data/) and files the app doesn't need. .env is uploaded on
       purpose: the app reads KAGGLE_API_TOKEN from it to download trends.db.
    3. Removes requirements.txt / manifest.json afterwards, unless they already
       existed before the script ran.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).parent.parent

load_dotenv(_ROOT / ".env")

TITLE = "lastfm-global-trends"

# Plain names only: on Windows rsconnect's CLI expands glob patterns such as
# "data/**" itself, which breaks the command.
EXCLUDES = [
    "data",  # local trends.db; the deployed app downloads its own from Kaggle
    ".github",
    ".pytest_cache",
    ".ruff_cache",
    ".vscode",
    "fetch_countries.py",
    "modules/__pycache__",
    "scripts",
    "skills",
    "skills-lock.json",
    "tests",
]

# Refuse to upload if any of these end up in the bundle.
FORBIDDEN = ["data/"]

REQUIREMENTS = _ROOT / "requirements.txt"
MANIFEST = _ROOT / "manifest.json"


def _run(cmd: list[str]) -> None:
    print("$", " ".join(cmd))
    subprocess.run(cmd, cwd=_ROOT, check=True)


def _rsconnect(*args: str) -> list[str]:
    exclude_args = [arg for name in EXCLUDES for arg in ("-x", name)]
    return [sys.executable, "-m", "rsconnect.main", *args, ".", *exclude_args]


def _export_requirements() -> None:
    cmd = ["uv", "export", "--no-dev", "--no-hashes", "--no-annotate", "-o", str(REQUIREMENTS)]
    print("$", " ".join(cmd))
    # uv also echoes the file to stdout; keep the output readable.
    subprocess.run(cmd, cwd=_ROOT, check=True, stdout=subprocess.DEVNULL)


def _check_bundle() -> list[str]:
    """Write manifest.json, fail if it contains forbidden files, return the file list."""
    _run(_rsconnect("write-manifest", "shiny", "--overwrite"))
    files = sorted(json.loads(MANIFEST.read_text(encoding="utf-8"))["files"])
    leaked = [f for f in files if any(f == p or f.startswith(p) for p in FORBIDDEN)]
    if leaked:
        sys.exit(f"Refusing to deploy: these files would be uploaded: {leaked}")
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--account",
        default=os.environ.get("SHINYAPPS_ACCOUNT"),
        help="rsconnect account nickname (default: $SHINYAPPS_ACCOUNT).",
    )
    parser.add_argument(
        "--app-id",
        default=os.environ.get("SHINYAPPS_APP_ID"),
        help="ID of the existing shinyapps.io app to update (default: $SHINYAPPS_APP_ID).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list the files that would be uploaded; don't deploy.",
    )
    args = parser.parse_args()

    if not args.dry_run:
        missing = [
            flag
            for flag, value in (("--account", args.account), ("--app-id", args.app_id))
            if not value
        ]
        if missing:
            parser.error(
                f"missing {' and '.join(missing)}: pass them or set SHINYAPPS_ACCOUNT / "
                "SHINYAPPS_APP_ID. Refusing to deploy without an app ID, since that "
                "would create a new app instead of updating the existing one."
            )

    # Both files are regenerated below; only clean up the ones this run created.
    generated = [path for path in (REQUIREMENTS, MANIFEST) if not path.exists()]

    try:
        _export_requirements()
        files = _check_bundle()
        print(f"\nBundle: {len(files)} files")
        for f in files:
            print("  ", f)

        if args.dry_run:
            print("\nDry run: nothing was deployed.")
            return

        # Let `rsconnect deploy` build its own manifest from the same excludes.
        MANIFEST.unlink()
        _run(
            _rsconnect(
                "deploy", "shiny",
                "--name", args.account,
                "--app-id", args.app_id,
                "--title", TITLE,
            )
        )
    finally:
        for path in generated:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
