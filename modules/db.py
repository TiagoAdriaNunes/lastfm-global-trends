import logging
import os
from functools import cache
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

DB_PATH = Path(__file__).parent.parent / "data" / "trends.db"

log = logging.getLogger(__name__)


def ensure_db() -> None:
    """Download trends.db from Kaggle if it doesn't exist locally."""
    if DB_PATH.exists():
        return

    load_dotenv(DB_PATH.parent.parent / ".env")
    if os.environ.get("KAGGLE_API_TOKEN") and not os.environ.get("KAGGLE_TOKEN"):
        os.environ["KAGGLE_TOKEN"] = os.environ["KAGGLE_API_TOKEN"]

    import kaggle

    log.info("trends.db not found — downloading from Kaggle...")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    kaggle.api.authenticate()
    kaggle.api.dataset_download_file(
        "tiagoadrianunes/last-fm-global-trends",
        file_name="trends.db",
        path=str(DB_PATH.parent),
        force=True,
        quiet=False,
    )
    if not DB_PATH.exists():
        raise RuntimeError(f"Download failed — {DB_PATH} not found after download.")
    log.info("Downloaded trends.db (%.1f MB)", DB_PATH.stat().st_size / 1024 / 1024)


def _connect() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            "Run scripts/download_data.py to download it."
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


@cache
def get_global_top_artists() -> pd.DataFrame:
    with _connect() as con:
        return con.execute(
            "SELECT rank AS \"Rank\", artist AS \"Artist\", "
            "listeners AS \"Listeners\", playcount AS \"Scrobbles\" "
            "FROM global_top_artists ORDER BY rank"
        ).df()


@cache
def get_global_top_tracks() -> pd.DataFrame:
    with _connect() as con:
        cols = {r[0] for r in con.execute("DESCRIBE global_top_tracks").fetchall()}
        listeners_expr = 'listeners AS "Listeners"' if "listeners" in cols else '0 AS "Listeners"'
        return con.execute(
            f"SELECT rank AS \"Rank\", track AS \"Track\", "
            f"artist AS \"Artist\", {listeners_expr}, playcount AS \"Scrobbles\" "
            f"FROM global_top_tracks ORDER BY rank"
        ).df()


@cache
def get_global_top_tags() -> pd.DataFrame:
    try:
        with _connect() as con:
            return con.execute(
                "SELECT rank AS \"Rank\", tag AS \"Tag\", "
                "reach AS \"Reach\", taggings AS \"Taggings\" "
                "FROM global_top_tags ORDER BY rank"
            ).df()
    except Exception:
        return pd.DataFrame(columns=["Rank", "Tag", "Reach", "Taggings"])


@cache
def get_geo_top_artists(country: str) -> pd.DataFrame:
    with _connect() as con:
        return con.execute(
            "SELECT rank AS \"Rank\", artist AS \"Artist\", listeners AS \"Listeners\" "
            "FROM geo_top_artists WHERE country = ? ORDER BY rank",
            [country],
        ).df()


@cache
def get_geo_top_tracks(country: str) -> pd.DataFrame:
    with _connect() as con:
        return con.execute(
            "SELECT rank AS \"Rank\", track AS \"Track\", "
            "artist AS \"Artist\", listeners AS \"Listeners\" "
            "FROM geo_top_tracks WHERE country = ? ORDER BY rank",
            [country],
        ).df()


@cache
def get_available_countries() -> list[str]:
    try:
        with _connect() as con:
            rows = con.execute(
                "SELECT DISTINCT country FROM geo_top_artists ORDER BY country"
            ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []
