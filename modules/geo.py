import logging

import pandas as pd
import pylast
from cachetools import TTLCache, cached
from shiny import module, reactive, render, ui

from countries import COUNTRY_CODES
from modules.utils import (
    _ARTISTS_COL_DEFS,
    _FETCH_LIMIT,
    _FETCH_PAGES,
    _TRACKS_COL_DEFS,
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
COUNTRY_CHOICES = {name: f"({code}) {name}" for name, code in COUNTRY_CODES.items()}


@cached(_geo_artists_cache)
def _fetch_geo_top_artists(network: pylast.LastFMNetwork, country: str) -> pd.DataFrame:
    return fetch_paginated(
        network,
        "geo.getTopArtists",
        {"country": country},
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
        {"country": country},
        "track",
        _row,
        ["Rank", "Track", "Artist", "Listeners"],
    )


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
