import logging
import time

import pandas as pd
import pylast
from cachetools import TTLCache, cached
from itables.shiny import DT
from shiny import module, reactive, render, ui

from countries import COUNTRY_CODES

log = logging.getLogger(__name__)

_geo_artists_cache: TTLCache = TTLCache(maxsize=50, ttl=6 * 3600)
_geo_tracks_cache: TTLCache = TTLCache(maxsize=50, ttl=6 * 3600)

_FETCH_LIMIT = 500
_FETCH_PAGES = 2
_REQUEST_DELAY = 1


# {value: label} — value sent to Last.fm API is the name, label shows code + name
COUNTRY_CHOICES = {name: f"({code}) {name}" for name, code in COUNTRY_CODES.items()}


def _build_network(api_key: str, api_secret: str) -> pylast.LastFMNetwork:
    return pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)


def _raw_request(network: pylast.LastFMNetwork, method: str, params: dict):
    log.info("Last.fm request: %s %s", method, params)
    request = pylast._Request(network, method, params)
    return request.execute(cacheable=True)


def _text(node, tag: str) -> str:
    els = node.getElementsByTagName(tag)
    if els and els[0].firstChild:
        return els[0].firstChild.nodeValue or ""
    return ""


@cached(_geo_artists_cache)
def _fetch_geo_top_artists(
    network: pylast.LastFMNetwork, country: str
) -> pd.DataFrame:
    rows = []
    for page in range(1, _FETCH_PAGES + 1):
        if page > 1:
            time.sleep(_REQUEST_DELAY)
        try:
            doc = _raw_request(
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
                    "Artist": _text(artist, "name"),
                    "Listeners": int(_text(artist, "listeners") or 0),
                }
            )
    log.info("Fetched %d geo artists for %r", len(rows), country)
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["Rank", "Artist", "Listeners"])


@cached(_geo_tracks_cache)
def _fetch_geo_top_tracks(
    network: pylast.LastFMNetwork, country: str
) -> pd.DataFrame:
    rows = []
    for page in range(1, _FETCH_PAGES + 1):
        if page > 1:
            time.sleep(_REQUEST_DELAY)
        try:
            doc = _raw_request(
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
            artist_name = _text(artist_nodes[0], "name") if artist_nodes else ""
            rows.append(
                {
                    "Rank": offset + i + 1,
                    "Track": _text(track, "name"),
                    "Artist": artist_name,
                    "Listeners": int(_text(track, "listeners") or 0),
                }
            )
    log.info("Fetched %d geo tracks for %r", len(rows), country)
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=["Rank", "Track", "Artist", "Listeners"])


def _fmt(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        df[col] = df[col].map("{:,}".format)
    return df


def _dt(df: pd.DataFrame) -> ui.HTML:
    return ui.HTML(DT(df, pageLength=10, style="width:100%;margin:0"))


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
    network = _build_network(api_key, api_secret)

    @reactive.calc
    def geo_artists():
        if not input.country():
            return pd.DataFrame(columns=["Rank", "Artist", "Listeners"])
        return _fmt(_fetch_geo_top_artists(network, input.country()), ["Listeners"])

    @reactive.calc
    def geo_tracks():
        if not input.country():
            return pd.DataFrame(columns=["Rank", "Track", "Artist", "Listeners"])
        return _fmt(_fetch_geo_top_tracks(network, input.country()), ["Listeners"])

    @render.ui
    def geo_artists_table():
        return _dt(geo_artists())

    @render.ui
    def geo_tracks_table():
        return _dt(geo_tracks())
