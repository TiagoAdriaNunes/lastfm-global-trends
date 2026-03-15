"""Fetch top artists and tracks for all countries and save to DuckDB.

Usage:
    uv run python fetch_countries.py

Output:
    data/trends.db  — DuckDB file with tables geo_top_artists and geo_top_tracks
"""

import argparse
import contextlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pylast
from dotenv import load_dotenv

from countries import COUNTRY_CODES, LASTFM_COUNTRY_NAME_MAP
from modules.utils import text

load_dotenv()

_HERE = Path(__file__).parent
DB_PATH = _HERE / "data" / "trends.db"
LOG_DIR = _HERE / "data" / "logs"
FETCH_LIMIT = 1000  # results per page (API max)
REQUEST_DELAY = 1  # seconds between API calls
MAX_RETRIES = 3  # retries for transient errors (500s, network issues)
RETRY_BACKOFF = 5  # seconds to wait between retries
DEFAULT_MAX_AGE_HOURS = 168  # 7 days

log = logging.getLogger(__name__)

ALL_COUNTRIES = list(COUNTRY_CODES.keys())


def setup_db(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS geo_top_artists (
            country     VARCHAR,
            rank        INTEGER,
            artist      VARCHAR,
            listeners   INTEGER,
            fetched_at  TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (country, rank)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS geo_top_tracks (
            country     VARCHAR,
            rank        INTEGER,
            track       VARCHAR,
            artist      VARCHAR,
            listeners   INTEGER,
            fetched_at  TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (country, rank)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS global_top_artists (
            rank        INTEGER PRIMARY KEY,
            artist      VARCHAR,
            listeners   BIGINT,
            playcount   BIGINT,
            fetched_at  TIMESTAMP DEFAULT current_timestamp
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS global_top_tracks (
            rank        INTEGER PRIMARY KEY,
            track       VARCHAR,
            artist      VARCHAR,
            playcount   BIGINT,
            fetched_at  TIMESTAMP DEFAULT current_timestamp
        )
    """)
    # migrate existing tables if columns were created with wrong types
    for tbl, col in [
        ("global_top_artists", "listeners"),
        ("global_top_artists", "playcount"),
        ("global_top_tracks", "playcount"),
    ]:
        with contextlib.suppress(duckdb.CatalogException):
            con.execute(f"ALTER TABLE {tbl} ALTER COLUMN {col} TYPE BIGINT")


def _last_fetched(
    con: duckdb.DuckDBPyConnection, table: str, country: str | None
) -> datetime | None:
    """Return the most recent fetched_at timestamp for a country (or globally if None)."""
    if country is None:
        result = con.execute(f"SELECT MAX(fetched_at) FROM {table}").fetchone()
    else:
        result = con.execute(
            f"SELECT MAX(fetched_at) FROM {table} WHERE country = ?", [country]
        ).fetchone()
    return result[0] if result and result[0] is not None else None


def _is_stale(
    con: duckdb.DuckDBPyConnection, table: str, country: str | None, max_age_hours: float
) -> bool:
    """Return True if data is missing or older than max_age_hours.

    Age is computed entirely in DuckDB using its own now() so the result is
    independent of Python's local timezone.
    """
    where = "WHERE country = ?" if country is not None else ""
    params: list = [max_age_hours] + ([country] if country is not None else [])
    row = con.execute(
        f"SELECT MAX(fetched_at) IS NULL OR "
        f"(epoch(now()) - epoch(MAX(fetched_at))) / 3600.0 > ? "
        f"FROM {table} {where}",
        params,
    ).fetchone()
    return bool(row[0])


def _raw_request(
    network: pylast.LastFMNetwork,
    method: str,
    country: str,
    run_errors: list[str],
    page: int = 1,
) -> object | None:
    # pylast._Request is a private class; pin the pylast version in pyproject.toml
    # in case it is renamed or removed in a future release.
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            request = pylast._Request(
                network,
                method,
                {"country": country, "limit": FETCH_LIMIT, "page": page},
            )
            return request.execute(cacheable=False)
        except pylast.WSError as e:
            msg = f"{method} for {country} (page {page}): {e}"
            log.warning(msg)
            run_errors.append(msg)
            return None
        except (pylast.MalformedResponseError, pylast.NetworkError) as e:
            if attempt < MAX_RETRIES:
                log.warning(
                    "%s for %s (page %d) failed (attempt %d/%d): %s — retrying in %ds",
                    method,
                    country,
                    page,
                    attempt,
                    MAX_RETRIES,
                    e,
                    RETRY_BACKOFF,
                )
                time.sleep(RETRY_BACKOFF)
            else:
                msg = f"{method} for {country} (page {page}) failed after {MAX_RETRIES} attempts: {e}"
                log.error(msg)
                run_errors.append(msg)
                return None


def _total_pages(doc: object, wrapper_tag: str) -> int:
    nodes = doc.getElementsByTagName(wrapper_tag)
    if nodes and "totalPages" in nodes[0].attributes:
        return int(nodes[0].attributes["totalPages"].value)
    return 1



def _db_count(con: duckdb.DuckDBPyConnection, table: str, country: str | None) -> int:
    if country is None:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE country = ?", [country]
    ).fetchone()[0]


def _parse_page1_artists(doc: object, country: str) -> list[dict]:
    return [
        {
            "country": country,
            "rank": i,
            "artist": text(el, "name"),
            "listeners": int(text(el, "listeners") or 0),
        }
        for i, el in enumerate(doc.getElementsByTagName("artist"), start=1)
    ]


def _parse_page1_tracks(doc: object, country: str) -> list[dict]:
    rows = []
    for i, el in enumerate(doc.getElementsByTagName("track"), start=1):
        artist_nodes = el.getElementsByTagName("artist")
        rows.append(
            {
                "country": country,
                "rank": i,
                "track": text(el, "name"),
                "artist": text(artist_nodes[0], "name") if artist_nodes else "",
                "listeners": int(text(el, "listeners") or 0),
            }
        )
    return rows


def fetch_geo_artists(
    network: pylast.LastFMNetwork,
    country: str,
    con: duckdb.DuckDBPyConnection,
    run_errors: list[str],
    max_age_hours: float,
) -> tuple[list[dict], bool]:
    """Fetch top artists for a country.

    Returns (rows, force_upsert). force_upsert=True when data was stale or the
    API total didn't match the DB — callers should bypass the unchanged check.
    Returns ([], False) when data is fresh and counts match (skip upsert entirely).
    """
    stale = _is_stale(con, "geo_top_artists", country, max_age_hours)
    log.info("Fetching geo.getTopArtists — %s%s", country, " (stale)" if stale else "")

    doc = _raw_request(network, "geo.getTopArtists", country, run_errors, page=1)
    if doc is None:
        return [], False

    total_pages = _total_pages(doc, "topartists")
    db_count = _db_count(con, "geo_top_artists", country)
    # api_total is a listener-count metric, not a row count — use total_pages instead.
    # db_count is complete if it falls in ((total_pages-1)×limit, total_pages×limit].
    count_ok = (total_pages - 1) * FETCH_LIMIT < db_count <= total_pages * FETCH_LIMIT

    if not stale and count_ok:
        log.info("  artists up to date (%d rows), skipping", db_count)
        return [], False

    if not stale and not count_ok:
        log.info(
            "  count mismatch (db=%d expected %d–%d) — re-fetching despite fresh data",
            db_count,
            (total_pages - 1) * FETCH_LIMIT + 1,
            total_pages * FETCH_LIMIT,
        )

    rows = _parse_page1_artists(doc, country)
    for page in range(2, total_pages + 1):
        time.sleep(REQUEST_DELAY)
        doc = _raw_request(network, "geo.getTopArtists", country, run_errors, page=page)
        if doc is None:
            break
        offset = (page - 1) * FETCH_LIMIT
        for i, el in enumerate(doc.getElementsByTagName("artist"), start=1):
            rows.append(
                {
                    "country": country,
                    "rank": offset + i,
                    "artist": text(el, "name"),
                    "listeners": int(text(el, "listeners") or 0),
                }
            )
    log.info("  → %d rows (%d pages)", len(rows), total_pages)
    return rows, True  # force upsert — data was stale or count mismatched


def fetch_geo_tracks(
    network: pylast.LastFMNetwork,
    country: str,
    con: duckdb.DuckDBPyConnection,
    run_errors: list[str],
    max_age_hours: float,
) -> tuple[list[dict], bool]:
    """Fetch top tracks for a country.

    Returns (rows, force_upsert). See fetch_geo_artists for semantics.
    """
    stale = _is_stale(con, "geo_top_tracks", country, max_age_hours)
    log.info("Fetching geo.getTopTracks — %s%s", country, " (stale)" if stale else "")

    doc = _raw_request(network, "geo.getTopTracks", country, run_errors, page=1)
    if doc is None:
        return [], False

    total_pages = _total_pages(doc, "tracks")
    db_count = _db_count(con, "geo_top_tracks", country)
    count_ok = (total_pages - 1) * FETCH_LIMIT < db_count <= total_pages * FETCH_LIMIT

    if not stale and count_ok:
        log.info("  tracks up to date (%d rows), skipping", db_count)
        return [], False

    if not stale and not count_ok:
        log.info(
            "  count mismatch (db=%d expected %d–%d) — re-fetching despite fresh data",
            db_count,
            (total_pages - 1) * FETCH_LIMIT + 1,
            total_pages * FETCH_LIMIT,
        )

    rows = _parse_page1_tracks(doc, country)
    for page in range(2, total_pages + 1):
        time.sleep(REQUEST_DELAY)
        doc = _raw_request(network, "geo.getTopTracks", country, run_errors, page=page)
        if doc is None:
            break
        offset = (page - 1) * FETCH_LIMIT
        for i, el in enumerate(doc.getElementsByTagName("track"), start=1):
            artist_nodes = el.getElementsByTagName("artist")
            rows.append(
                {
                    "country": country,
                    "rank": offset + i,
                    "track": text(el, "name"),
                    "artist": text(artist_nodes[0], "name") if artist_nodes else "",
                    "listeners": int(text(el, "listeners") or 0),
                }
            )
    log.info("  → %d rows (%d pages)", len(rows), total_pages)
    return rows, True


def _is_unchanged(
    con: duckdb.DuckDBPyConnection, table: str, country: str, rows: list[dict]
) -> bool:
    """Return True if the DB already has the same row count and total listeners."""
    result = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(listeners), 0) FROM {table} WHERE country = ?",
        [country],
    ).fetchone()
    db_count, db_sum = result
    return db_count == len(rows) and db_sum == sum(r["listeners"] for r in rows)


def upsert_artists(
    con: duckdb.DuckDBPyConnection, rows: list[dict], force: bool, run_errors: list[str]
) -> bool:
    """Replace artist rows for a country. Returns True if skipped.

    Deletes all existing rows for the country before inserting fresh data so
    stale ranks never linger (e.g. when the API returns fewer results than before).
    When force=False and data is unchanged the write is skipped entirely.
    """
    if not rows:
        return True
    if not force and _is_unchanged(con, "geo_top_artists", rows[0]["country"], rows):
        log.info("  artists unchanged, skipping")
        return True
    country = rows[0]["country"]
    df = pd.DataFrame(rows)
    con.register("_staging_artists", df)
    con.execute("DELETE FROM geo_top_artists WHERE country = ?", [country])
    con.execute("""
        INSERT INTO geo_top_artists (country, rank, artist, listeners)
        SELECT country, rank, artist, listeners FROM _staging_artists
    """)
    con.unregister("_staging_artists")
    saved = _db_count(con, "geo_top_artists", country)
    if saved != len(rows):
        msg = f"{country} artists: fetched {len(rows)} but db has {saved}"
        log.warning("  save mismatch: %s", msg)
        run_errors.append(f"save mismatch — {msg}")
    return False


def upsert_tracks(
    con: duckdb.DuckDBPyConnection, rows: list[dict], force: bool, run_errors: list[str]
) -> bool:
    """Replace track rows for a country. Returns True if skipped.

    Deletes all existing rows for the country before inserting fresh data so
    stale ranks never linger (e.g. when the API returns fewer results than before).
    When force=False and data is unchanged the write is skipped entirely.
    """
    if not rows:
        return True
    if not force and _is_unchanged(con, "geo_top_tracks", rows[0]["country"], rows):
        log.info("  tracks unchanged, skipping")
        return True
    country = rows[0]["country"]
    df = pd.DataFrame(rows)
    con.register("_staging_tracks", df)
    con.execute("DELETE FROM geo_top_tracks WHERE country = ?", [country])
    con.execute("""
        INSERT INTO geo_top_tracks (country, rank, track, artist, listeners)
        SELECT country, rank, track, artist, listeners FROM _staging_tracks
    """)
    con.unregister("_staging_tracks")
    saved = _db_count(con, "geo_top_tracks", country)
    if saved != len(rows):
        msg = f"{country} tracks: fetched {len(rows)} but db has {saved}"
        log.warning("  save mismatch: %s", msg)
        run_errors.append(f"save mismatch — {msg}")
    return False


def _global_request(
    network: pylast.LastFMNetwork,
    method: str,
    run_errors: list[str],
    page: int = 1,
) -> object | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            request = pylast._Request(network, method, {"limit": FETCH_LIMIT, "page": page})
            return request.execute(cacheable=False)
        except pylast.WSError as e:
            msg = f"{method} (page {page}): {e}"
            log.warning(msg)
            run_errors.append(msg)
            return None
        except (pylast.MalformedResponseError, pylast.NetworkError) as e:
            if attempt < MAX_RETRIES:
                log.warning(
                    "%s (page %d) failed (attempt %d/%d): %s — retrying in %ds",
                    method,
                    page,
                    attempt,
                    MAX_RETRIES,
                    e,
                    RETRY_BACKOFF,
                )
                time.sleep(RETRY_BACKOFF)
            else:
                msg = f"{method} (page {page}) failed after {MAX_RETRIES} attempts: {e}"
                log.error(msg)
                run_errors.append(msg)
                return None


def fetch_global_artists(
    network: pylast.LastFMNetwork,
    con: duckdb.DuckDBPyConnection,
    run_errors: list[str],
    max_age_hours: float,
) -> list[dict]:
    """Fetch global top artists. Returns rows, or [] if data is fresh and count matches."""
    stale = _is_stale(con, "global_top_artists", None, max_age_hours)
    log.info("Fetching chart.getTopArtists — global%s", " (stale)" if stale else "")

    doc = _global_request(network, "chart.getTopArtists", run_errors, page=1)
    if doc is None:
        return [], False

    total_pages = _total_pages(doc, "artists")
    db_count = _db_count(con, "global_top_artists", None)
    count_ok = (total_pages - 1) * FETCH_LIMIT < db_count <= total_pages * FETCH_LIMIT

    if not stale and count_ok:
        log.info("  global artists up to date (%d rows), skipping", db_count)
        return []

    if not stale and not count_ok:
        log.info(
            "  count mismatch (db=%d expected %d–%d) — re-fetching despite fresh data",
            db_count,
            (total_pages - 1) * FETCH_LIMIT + 1,
            total_pages * FETCH_LIMIT,
        )
    rows = []
    for page in range(1, total_pages + 1):
        if page > 1:
            time.sleep(REQUEST_DELAY)
            doc = _global_request(network, "chart.getTopArtists", run_errors, page=page)
            if doc is None:
                break
        offset = (page - 1) * FETCH_LIMIT
        for i, el in enumerate(doc.getElementsByTagName("artist"), start=1):
            rows.append(
                {
                    "rank": offset + i,
                    "artist": text(el, "name"),
                    "listeners": int(text(el, "listeners") or 0),
                    "playcount": int(text(el, "playcount") or 0),
                }
            )
    log.info("  → %d rows (%d pages)", len(rows), total_pages)
    return rows


def fetch_global_tracks(
    network: pylast.LastFMNetwork,
    con: duckdb.DuckDBPyConnection,
    run_errors: list[str],
    max_age_hours: float,
) -> list[dict]:
    """Fetch global top tracks. Returns rows, or [] if data is fresh and count matches."""
    stale = _is_stale(con, "global_top_tracks", None, max_age_hours)
    log.info("Fetching chart.getTopTracks — global%s", " (stale)" if stale else "")

    doc = _global_request(network, "chart.getTopTracks", run_errors, page=1)
    if doc is None:
        return [], False

    total_pages = _total_pages(doc, "tracks")
    db_count = _db_count(con, "global_top_tracks", None)
    count_ok = (total_pages - 1) * FETCH_LIMIT < db_count <= total_pages * FETCH_LIMIT

    if not stale and count_ok:
        log.info("  global tracks up to date (%d rows), skipping", db_count)
        return []

    if not stale and not count_ok:
        log.info(
            "  count mismatch (db=%d expected %d–%d) — re-fetching despite fresh data",
            db_count,
            (total_pages - 1) * FETCH_LIMIT + 1,
            total_pages * FETCH_LIMIT,
        )
    rows = []
    for page in range(1, total_pages + 1):
        if page > 1:
            time.sleep(REQUEST_DELAY)
            doc = _global_request(network, "chart.getTopTracks", run_errors, page=page)
            if doc is None:
                break
        offset = (page - 1) * FETCH_LIMIT
        for i, el in enumerate(doc.getElementsByTagName("track"), start=1):
            artist_nodes = el.getElementsByTagName("artist")
            rows.append(
                {
                    "rank": offset + i,
                    "track": text(el, "name"),
                    "artist": text(artist_nodes[0], "name") if artist_nodes else "",
                    "playcount": int(text(el, "playcount") or 0),
                }
            )
    log.info("  → %d rows (%d pages)", len(rows), total_pages)
    return rows


def upsert_global_artists(
    con: duckdb.DuckDBPyConnection, rows: list[dict], run_errors: list[str]
) -> bool:
    """Replace all global artist rows. Returns True if skipped (empty)."""
    if not rows:
        return True
    df = pd.DataFrame(rows)
    con.register("_staging_global_artists", df)
    con.execute("DELETE FROM global_top_artists")
    con.execute("""
        INSERT INTO global_top_artists (rank, artist, listeners, playcount)
        SELECT rank, artist, listeners, playcount FROM _staging_global_artists
    """)
    con.unregister("_staging_global_artists")
    saved = _db_count(con, "global_top_artists", None)
    if saved != len(rows):
        msg = f"global artists: fetched {len(rows)} but db has {saved}"
        log.warning("  save mismatch: %s", msg)
        run_errors.append(f"save mismatch — {msg}")
    return False


def upsert_global_tracks(
    con: duckdb.DuckDBPyConnection, rows: list[dict], run_errors: list[str]
) -> bool:
    """Replace all global track rows. Returns True if skipped (empty)."""
    if not rows:
        return True
    df = pd.DataFrame(rows)
    con.register("_staging_global_tracks", df)
    con.execute("DELETE FROM global_top_tracks")
    con.execute("""
        INSERT INTO global_top_tracks (rank, track, artist, playcount)
        SELECT rank, track, artist, playcount FROM _staging_global_tracks
    """)
    con.unregister("_staging_global_tracks")
    saved = _db_count(con, "global_top_tracks", None)
    if saved != len(rows):
        msg = f"global tracks: fetched {len(rows)} but db has {saved}"
        log.warning("  save mismatch: %s", msg)
        run_errors.append(f"save mismatch — {msg}")
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only",
        metavar="COUNTRIES",
        help="Comma-separated display names to fetch (e.g. \"Côte d'Ivoire,Libya\"). Skips global charts.",
    )
    parser.add_argument(
        "--max-age",
        metavar="HOURS",
        type=float,
        default=DEFAULT_MAX_AGE_HOURS,
        help=f"Re-fetch data older than this many hours (default: {DEFAULT_MAX_AGE_HOURS} = 7 days). "
             "Data with a count mismatch vs the API is always re-fetched regardless of age.",
    )
    args = parser.parse_args()

    api_key = os.environ.get("LASTFM_API_KEY")
    api_secret = os.environ.get("LASTFM_API_SECRET")
    if not api_key or not api_secret:
        log.error("LASTFM_API_KEY and LASTFM_API_SECRET must be set")
        sys.exit(1)

    only_countries = [c.strip() for c in args.only.split(",") if c.strip()] if args.only else []
    if only_countries:
        unknown = [c for c in only_countries if c not in COUNTRY_CODES]
        if unknown:
            log.error("Unknown country names: %s", ", ".join(unknown))
            sys.exit(1)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    file_handler = logging.FileHandler(LOG_DIR / f"run_{run_ts}.log")
    file_handler.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=[console, file_handler])

    run_errors: list[str] = []
    max_age_hours: float = args.max_age

    network = pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)

    try:
        con_ctx = duckdb.connect(str(DB_PATH))
    except duckdb.IOException as e:
        log.error(
            "Cannot open %s — the database is locked by another process.\n"
            "  Close any other connections to the DB and retry.\n"
            "  Details: %s",
            DB_PATH,
            e,
        )
        sys.exit(1)

    with con_ctx as con:
        setup_db(con)

        if not only_countries:
            log.info("--- Global charts (max-age %.0fh) ---", max_age_hours)
            t0 = time.monotonic()
            if not upsert_global_artists(con, fetch_global_artists(network, con, run_errors, max_age_hours), run_errors):
                log.info("  saved global artists in %.2fs", time.monotonic() - t0)
            time.sleep(REQUEST_DELAY)

            t0 = time.monotonic()
            if not upsert_global_tracks(con, fetch_global_tracks(network, con, run_errors, max_age_hours), run_errors):
                log.info("  saved global tracks in %.2fs", time.monotonic() - t0)
            time.sleep(REQUEST_DELAY)

        run_countries = only_countries if only_countries else ALL_COUNTRIES
        log.info("--- Country charts (%d, max-age %.0fh) ---", len(run_countries), max_age_hours)
        total = len(run_countries)
        country_stats = []
        for i, country in enumerate(run_countries, start=1):
            api_country = LASTFM_COUNTRY_NAME_MAP.get(country, country)
            if api_country is None:
                log.info(
                    "[%d/%d] %s — skipped (not supported by Last.fm)", i, total, country
                )
                country_stats.append({"country": country, "artists": 0, "tracks": 0, "status": "unsupported"})
                continue

            log.info("[%d/%d] %s", i, total, country)
            stat = {"country": country, "artists": 0, "tracks": 0, "status": "ok"}

            artists, force = fetch_geo_artists(network, api_country, con, run_errors, max_age_hours)
            t0 = time.monotonic()
            skipped = upsert_artists(con, artists, force, run_errors)
            if not skipped:
                log.info("  saved artists in %.2fs", time.monotonic() - t0)
                stat["artists"] = len(artists)
            elif not artists:
                stat["status"] = (
                    "skipped"
                    if not any(api_country in e for e in run_errors)
                    else "failed"
                )
            time.sleep(REQUEST_DELAY)

            tracks, force = fetch_geo_tracks(network, api_country, con, run_errors, max_age_hours)
            t0 = time.monotonic()
            skipped = upsert_tracks(con, tracks, force, run_errors)
            if not skipped:
                log.info("  saved tracks in %.2fs", time.monotonic() - t0)
                stat["tracks"] = len(tracks)
            time.sleep(REQUEST_DELAY)

            country_stats.append(stat)

        artist_count = con.execute("SELECT COUNT(*) FROM geo_top_artists").fetchone()[0]
        track_count = con.execute("SELECT COUNT(*) FROM geo_top_tracks").fetchone()[0]
        global_artist_count = con.execute(
            "SELECT COUNT(*) FROM global_top_artists"
        ).fetchone()[0]
        global_track_count = con.execute(
            "SELECT COUNT(*) FROM global_top_tracks"
        ).fetchone()[0]
        log.info(
            "Done. global: %d artists, %d tracks | geo: %d artists, %d tracks | saved to %s",
            global_artist_count,
            global_track_count,
            artist_count,
            track_count,
            DB_PATH,
        )

        summary = {
            "run_at": run_ts,
            "db": str(DB_PATH),
            "max_age_hours": max_age_hours,
            "global": {"artists": global_artist_count, "tracks": global_track_count},
            "geo": {"artists": artist_count, "tracks": track_count},
            "countries": country_stats,
            "errors": run_errors,
        }
        summary_path = LOG_DIR / f"summary_{run_ts}.json"
        summary_path.write_text(json.dumps(summary, indent=2))
        log.info("Summary written to %s", summary_path)


if __name__ == "__main__":
    main()
