import logging
import time

import pandas as pd
import pylast
from cachetools import TTLCache, cached
from shiny import module, reactive, render, ui

from countries import COUNTRY_CODES
from modules.utils import (
    _ARTISTS_COL_DEFS,
    _FETCH_LIMIT,
    _FETCH_PAGES,
    _REQUEST_DELAY,
    _TRACKS_COL_DEFS,
    build_network,
    dt,
    fmt,
    raw_request,
    text,
)

log = logging.getLogger(__name__)

_geo_artists_cache: TTLCache = TTLCache(maxsize=50, ttl=6 * 3600)
_geo_tracks_cache: TTLCache = TTLCache(maxsize=50, ttl=6 * 3600)

# {value: label} — value sent to Last.fm API is the name, label shows code + name
COUNTRY_CHOICES = {name: f"({code}) {name}" for name, code in COUNTRY_CODES.items()}


@cached(_geo_artists_cache)
def _fetch_geo_top_artists(network: pylast.LastFMNetwork, country: str) -> pd.DataFrame:
    rows = []
    for page in range(1, _FETCH_PAGES + 1):
        if page > 1:
            time.sleep(_REQUEST_DELAY)
        try:
            doc = raw_request(
                network,
                "geo.getTopArtists",
                {"country": country, "limit": _FETCH_LIMIT, "page": page},
            )
        except pylast.WSError as e:
            log.warning("geo.getTopArtists failed for %r page %d: %s", country, page, e)
            break
        offset = (page - 1) * _FETCH_LIMIT
        for i, artist in enumerate(doc.getElementsByTagName("artist")):
            rows.append(
                {
                    "Rank": offset + i + 1,
                    "Artist": text(artist, "name"),
                    "Listeners": int(text(artist, "listeners") or 0),
                }
            )
    log.info("Fetched %d geo artists for %r", len(rows), country)
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["Rank", "Artist", "Listeners"])


@cached(_geo_tracks_cache)
def _fetch_geo_top_tracks(network: pylast.LastFMNetwork, country: str) -> pd.DataFrame:
    rows = []
    for page in range(1, _FETCH_PAGES + 1):
        if page > 1:
            time.sleep(_REQUEST_DELAY)
        try:
            doc = raw_request(
                network,
                "geo.getTopTracks",
                {"country": country, "limit": _FETCH_LIMIT, "page": page},
            )
        except pylast.WSError as e:
            log.warning("geo.getTopTracks failed for %r page %d: %s", country, page, e)
            break
        offset = (page - 1) * _FETCH_LIMIT
        for i, track in enumerate(doc.getElementsByTagName("track")):
            artist_nodes = track.getElementsByTagName("artist")
            artist_name = text(artist_nodes[0], "name") if artist_nodes else ""
            rows.append(
                {
                    "Rank": offset + i + 1,
                    "Track": text(track, "name"),
                    "Artist": artist_name,
                    "Listeners": int(text(track, "listeners") or 0),
                }
            )
    log.info("Fetched %d geo tracks for %r", len(rows), country)
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["Rank", "Track", "Artist", "Listeners"])


@module.ui
def geo_ui():
    return ui.div(
        ui.input_selectize(
            "country",
            "Country",
            choices=COUNTRY_CHOICES,
            selected="United States",
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header(f"Top Artists (Top {_FETCH_LIMIT * _FETCH_PAGES})"),
                ui.output_ui("geo_artists_table"),
            ),
            ui.card(
                ui.card_header(f"Top Tracks (Top {_FETCH_LIMIT * _FETCH_PAGES})"),
                ui.output_ui("geo_tracks_table"),
            ),
            col_widths=[6, 6],
        ),
    )


@module.server
def geo_server(input, output, session, api_key: str, api_secret: str):
    network = build_network(api_key, api_secret)

    @reactive.calc
    def geo_artists():
        if not input.country():
            return pd.DataFrame(columns=["Rank", "Artist", "Listeners"])
        return fmt(_fetch_geo_top_artists(network, input.country()), ["Listeners"])

    @reactive.calc
    def geo_tracks():
        if not input.country():
            return pd.DataFrame(columns=["Rank", "Track", "Artist", "Listeners"])
        return fmt(_fetch_geo_top_tracks(network, input.country()), ["Listeners"])

    @render.ui
    def geo_artists_table():
        return dt(geo_artists(), _ARTISTS_COL_DEFS)

    @render.ui
    def geo_tracks_table():
        return dt(geo_tracks(), _TRACKS_COL_DEFS)
