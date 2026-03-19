import logging
import os
from functools import cache
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

DB_PATH = Path(__file__).parent.parent / "data" / "trends.db"

log = logging.getLogger(__name__)


def _clear_db_caches() -> None:
    get_global_top_artists.cache_clear()
    get_global_top_tracks.cache_clear()
    get_global_top_tags.cache_clear()
    get_geo_top_artists.cache_clear()
    get_geo_top_tracks.cache_clear()
    get_available_countries.cache_clear()


def ensure_db(on_progress=None, force_check: bool = False) -> None:
    """Download trends.db from Kaggle if it doesn't exist.

    If the database already exists locally, it is used as-is unless
    ``force_check=True`` is passed, in which case the remote file size is
    compared and a re-download is triggered on mismatch.

    Args:
        on_progress: optional callable(fraction: float) called with values 0–1
                     as bytes are received. Called with 1.0 on completion.
        force_check: when True, always contact Kaggle to verify file size even
                     if the DB already exists locally.
    """
    if DB_PATH.exists() and not force_check:
        log.info("trends.db already present (%.1f MB), skipping download.", DB_PATH.stat().st_size / 1024 / 1024)
        return

    load_dotenv(DB_PATH.parent.parent / ".env")
    if os.environ.get("KAGGLE_API_TOKEN") and not os.environ.get("KAGGLE_TOKEN"):
        os.environ["KAGGLE_TOKEN"] = os.environ["KAGGLE_API_TOKEN"]

    import kaggle
    import requests
    from kagglesdk.datasets.types.dataset_api_service import ApiDownloadDatasetRequest

    kaggle.api.authenticate()

    # Resolve the pre-signed download URL and remote file size.
    with kaggle.api.build_kaggle_client() as kc:
        req = ApiDownloadDatasetRequest()
        req.owner_slug = "tiagoadrianunes"
        req.dataset_slug = "last-fm-global-trends"
        req.file_name = "trends.db"
        response = kc.datasets.dataset_api_client.download_dataset(req)

    remote_size = int(response.headers.get("Content-Length", 0))

    if remote_size == 0:
        log.warning("Kaggle returned Content-Length: 0 — skipping download to avoid corrupting existing DB.")
        return

    if DB_PATH.exists():
        local_size = DB_PATH.stat().st_size
        if local_size == remote_size:
            log.info(
                "trends.db is up to date (%.1f MB), skipping download.",
                remote_size / 1024 / 1024,
            )
            return
        log.info(
            "trends.db size mismatch (local=%.1f MB, remote=%.1f MB) — re-downloading.",
            local_size / 1024 / 1024,
            remote_size / 1024 / 1024,
        )

    log.info("Downloading trends.db (%.1f MB) from Kaggle...", remote_size / 1024 / 1024)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Pre-signed URL requires no auth headers.
    downloaded = 0
    with requests.get(response.url, stream=True) as r:
        r.raise_for_status()
        with open(DB_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                f.write(chunk)
                downloaded += len(chunk)
                if on_progress and remote_size:
                    on_progress(downloaded / remote_size)

    if not DB_PATH.exists():
        raise RuntimeError(f"Download failed — {DB_PATH} not found after download.")
    if on_progress:
        on_progress(1.0)
    log.info("Downloaded trends.db (%.1f MB)", DB_PATH.stat().st_size / 1024 / 1024)
    _clear_db_caches()


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
            "artist_url AS \"ArtistUrl\", "
            "listeners AS \"Listeners\", playcount AS \"Scrobbles\" "
            "FROM global_top_artists ORDER BY rank"
        ).df()


@cache
def get_global_top_tracks() -> pd.DataFrame:
    with _connect() as con:
        cols = {r[0] for r in con.execute("DESCRIBE global_top_tracks").fetchall()}
        listeners_expr = 'listeners AS "Listeners"' if "listeners" in cols else '0 AS "Listeners"'
        return con.execute(
            f"SELECT rank AS \"Rank\", track AS \"Track\", track_url AS \"TrackUrl\", "
            f"artist AS \"Artist\", artist_url AS \"ArtistUrl\", "
            f"{listeners_expr}, playcount AS \"Scrobbles\" "
            f"FROM global_top_tracks ORDER BY rank"
        ).df()


@cache
def get_global_top_tags() -> pd.DataFrame:
    try:
        with _connect() as con:
            return con.execute(
                "SELECT rank AS \"Rank\", tag AS \"Tag\", tag_url AS \"TagUrl\", "
                "reach AS \"Reach\", taggings AS \"Taggings\" "
                "FROM global_top_tags ORDER BY rank"
            ).df()
    except Exception:
        log.warning("global_top_tags table unavailable", exc_info=True)
        return pd.DataFrame(columns=["Rank", "Tag", "TagUrl", "Reach", "Taggings"])


@cache
def get_geo_top_artists(country: str) -> pd.DataFrame:
    with _connect() as con:
        return con.execute(
            "SELECT rank AS \"Rank\", artist AS \"Artist\", "
            "artist_url AS \"ArtistUrl\", listeners AS \"Listeners\" "
            "FROM geo_top_artists WHERE country = ? ORDER BY rank",
            [country],
        ).df()


@cache
def get_geo_top_tracks(country: str) -> pd.DataFrame:
    with _connect() as con:
        return con.execute(
            "SELECT rank AS \"Rank\", track AS \"Track\", track_url AS \"TrackUrl\", "
            "artist AS \"Artist\", artist_url AS \"ArtistUrl\", listeners AS \"Listeners\" "
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
        log.warning("geo_top_artists table unavailable", exc_info=True)
        return []
