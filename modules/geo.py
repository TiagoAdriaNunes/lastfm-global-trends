import logging

import pandas as pd
import pylast
from cachetools import TTLCache, cached
from shiny import module, reactive, render, ui

from countries import COUNTRY_CODES, LASTFM_COUNTRY_NAME_MAP
from modules.utils import (
    ARTISTS_COL_DEFS,
    FETCH_LIMIT,
    FETCH_PAGES,
    TRACKS_COL_DEFS,
    build_network,
    dt,
    fetch_paginated,
    fmt,
    text,
)

log = logging.getLogger(__name__)

_geo_artists_cache: TTLCache = TTLCache(maxsize=50, ttl=6 * 3600)
_geo_tracks_cache: TTLCache = TTLCache(maxsize=50, ttl=6 * 3600)

# {value: label} — value sent to Last.fm API is the name, label shows code + name
# Countries mapped to None in LASTFM_COUNTRY_NAME_MAP are excluded (not supported).
COUNTRY_CHOICES = {
    name: f"({code}) {name}"
    for name, code in COUNTRY_CODES.items()
    if LASTFM_COUNTRY_NAME_MAP.get(name, name) is not None
}


def _lastfm_country_name(display_name: str) -> str:
    """Translate a display country name to the name accepted by the Last.fm API."""
    return LASTFM_COUNTRY_NAME_MAP.get(display_name, display_name)


@cached(_geo_artists_cache)
def _fetch_geo_top_artists(network: pylast.LastFMNetwork, country: str) -> pd.DataFrame:
    """Fetch top artists for a country from the Last.fm geo API (cached 6 h).

    Args:
        network: Authenticated Last.fm network instance.
        country: Country name as accepted by the Last.fm geo.getTopArtists method.

    Returns:
        DataFrame with columns: Rank, Artist, Listeners.
    """
    return fetch_paginated(
        network,
        "geo.getTopArtists",
        {"country": _lastfm_country_name(country)},
        "artist",
        lambda el, rank: {
            "Rank": rank,
            "Artist": text(el, "name"),
            "Listeners": int(text(el, "listeners") or 0),
        },
        ["Rank", "Artist", "Listeners"],
    )


@cached(_geo_tracks_cache)
def _fetch_geo_top_tracks(network: pylast.LastFMNetwork, country: str) -> pd.DataFrame:
    """Fetch top tracks for a country from the Last.fm geo API (cached 6 h).

    Args:
        network: Authenticated Last.fm network instance.
        country: Country name as accepted by the Last.fm geo.getTopTracks method.

    Returns:
        DataFrame with columns: Rank, Track, Artist, Listeners.
    """
    def _row(el, rank):
        artist_nodes = el.getElementsByTagName("artist")
        return {
            "Rank": rank,
            "Track": text(el, "name"),
            "Artist": text(artist_nodes[0], "name") if artist_nodes else "",
            "Listeners": int(text(el, "listeners") or 0),
        }

    return fetch_paginated(
        network,
        "geo.getTopTracks",
        {"country": _lastfm_country_name(country)},
        "track",
        _row,
        ["Rank", "Track", "Artist", "Listeners"],
    )


@module.ui
def geo_ui():
    """Render the By Country module UI.

    Returns:
        A Shiny div containing a country selector and side-by-side artist
        and track DataTable cards.
    """
    return ui.div(
        ui.input_selectize(
            "country",
            "Country",
            choices=COUNTRY_CHOICES,
            selected="United States",
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header(f"Top Artists (Top {FETCH_LIMIT * FETCH_PAGES})"),
                ui.output_ui("geo_artists_table"),
            ),
            ui.card(
                ui.card_header(f"Top Tracks (Top {FETCH_LIMIT * FETCH_PAGES})"),
                ui.output_ui("geo_tracks_table"),
            ),
            col_widths=[6, 6],
        ),
    )


@module.server
def geo_server(input, output, session, api_key: str, api_secret: str):
    """Run the By Country module server logic.

    Builds a Last.fm network connection and registers reactive calculations
    and renderers for the top artists and top tracks tables, filtered by the
    selected country.

    Args:
        input: Shiny input object (reads: country).
        output: Shiny output object (writes: geo_artists_table, geo_tracks_table).
        session: Shiny session object.
        api_key: Last.fm API key.
        api_secret: Last.fm API secret.
    """
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
        return dt(geo_artists(), ARTISTS_COL_DEFS)

    @render.ui
    def geo_tracks_table():
        return dt(geo_tracks(), TRACKS_COL_DEFS)
