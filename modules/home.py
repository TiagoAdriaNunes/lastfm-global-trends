import logging

import pandas as pd
import pylast
from cachetools import TTLCache, cached
from itables.shiny import DT
from shiny import module, reactive, render, ui

log = logging.getLogger(__name__)

_artists_cache: TTLCache = TTLCache(maxsize=5, ttl=6 * 3600)
_tracks_cache: TTLCache = TTLCache(maxsize=5, ttl=6 * 3600)
_tags_cache: TTLCache = TTLCache(maxsize=5, ttl=6 * 3600)

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


@cached(_artists_cache)
def _fetch_top_artists(network: pylast.LastFMNetwork, limit: int = 50) -> pd.DataFrame:
    doc = _raw_request(network, "chart.getTopArtists", {"limit": limit})
    rows = []
    for i, artist in enumerate(doc.getElementsByTagName("artist")):
        rows.append({
            "Rank": i + 1,
            "Artist": _text(artist, "name"),
            "Listeners": int(_text(artist, "listeners") or 0),
            "Scrobbles": int(_text(artist, "playcount") or 0),
        })
    return pd.DataFrame(rows)


@cached(_tracks_cache)
def _fetch_top_tracks(network: pylast.LastFMNetwork, limit: int = 50) -> pd.DataFrame:
    doc = _raw_request(network, "chart.getTopTracks", {"limit": limit})
    rows = []
    for i, track in enumerate(doc.getElementsByTagName("track")):
        artist_nodes = track.getElementsByTagName("artist")
        artist_name = _text(artist_nodes[0], "name") if artist_nodes else ""
        rows.append({
            "Rank": i + 1,
            "Track": _text(track, "name"),
            "Artist": artist_name,
            "Listeners": int(_text(track, "listeners") or 0),
            "Scrobbles": int(_text(track, "playcount") or 0),
        })
    return pd.DataFrame(rows)


@cached(_tags_cache)
def _fetch_top_tags(network: pylast.LastFMNetwork, limit: int = 50) -> pd.DataFrame:
    doc = _raw_request(network, "chart.getTopTags", {"limit": limit})
    rows = []
    for i, tag in enumerate(doc.getElementsByTagName("tag")):
        rows.append({
            "Rank": i + 1,
            "Tag": _text(tag, "name"),
            "Reach": int(_text(tag, "reach") or 0),
            "Taggings": int(_text(tag, "taggings") or 0),
        })
    return pd.DataFrame(rows)



def _fmt(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        df[col] = df[col].map("{:,}".format)
    return df


def _dt(df: pd.DataFrame) -> ui.HTML:
    return ui.HTML(DT(df, pageLength=10, style="width:100%;margin:0"))


@module.ui
def home_ui():
    return ui.div(
        ui.layout_columns(
            ui.card(
                ui.card_header("Top Artists"),
                ui.output_ui("top_artists_table"),
            ),
            ui.card(
                ui.card_header("Top Tracks"),
                ui.output_ui("top_tracks_table"),
            ),
            col_widths=[6, 6],
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Top Tags"),
                ui.output_ui("top_tags_table"),
            ),
            col_widths=[6],
        ),
    )


@module.server
def home_server(input, output, session, api_key: str, api_secret: str):
    network = _build_network(api_key, api_secret)

    @reactive.calc
    def top_artists():
        return _fmt(_fetch_top_artists(network), ["Listeners", "Scrobbles"])

    @reactive.calc
    def top_tracks():
        return _fmt(_fetch_top_tracks(network), ["Listeners", "Scrobbles"])

    @reactive.calc
    def top_tags():
        return _fmt(_fetch_top_tags(network), ["Reach", "Taggings"])

    @render.ui
    def top_artists_table():
        return _dt(top_artists())

    @render.ui
    def top_tracks_table():
        return _dt(top_tracks())

    @render.ui
    def top_tags_table():
        return _dt(top_tags())

