import pandas as pd
import pylast
from cachetools import TTLCache, cached
from itables.shiny import DT
from shiny import module, reactive, render, ui

_artists_cache: TTLCache = TTLCache(maxsize=5, ttl=6 * 3600)
_tracks_cache: TTLCache = TTLCache(maxsize=5, ttl=6 * 3600)


def _build_network(api_key: str, api_secret: str) -> pylast.LastFMNetwork:
    return pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)


@cached(_artists_cache)
def _fetch_top_artists(network: pylast.LastFMNetwork, limit: int = 50) -> pd.DataFrame:
    items = network.get_top_artists(limit=limit)
    return pd.DataFrame(
        [{"Rank": i + 1, "Artist": t.item.name, "Playcount": int(t.weight)} for i, t in enumerate(items)]
    )


@cached(_tracks_cache)
def _fetch_top_tracks(network: pylast.LastFMNetwork, limit: int = 50) -> pd.DataFrame:
    items = network.get_top_tracks(limit=limit)
    return pd.DataFrame(
        [
            {
                "Rank": i + 1,
                "Track": t.item.title,
                "Artist": t.item.artist.name,
                "Playcount": int(t.weight),
            }
            for i, t in enumerate(items)
        ]
    )


@module.ui
def home_ui():
    return ui.layout_columns(
        ui.card(
            ui.card_header("Top Artists"),
            ui.output_ui("top_artists_table"),
        ),
        ui.card(
            ui.card_header("Top Tracks"),
            ui.output_ui("top_tracks_table"),
        ),
        col_widths=[6, 6],
    )


@module.server
def home_server(input, output, session, api_key: str, api_secret: str):
    network = _build_network(api_key, api_secret)

    @reactive.calc
    def top_artists():
        df = _fetch_top_artists(network).copy()
        df["Playcount"] = df["Playcount"].map("{:,}".format)
        return df

    @reactive.calc
    def top_tracks():
        df = _fetch_top_tracks(network).copy()
        df["Playcount"] = df["Playcount"].map("{:,}".format)
        return df

    @render.ui
    def top_artists_table():
        return ui.HTML(DT(top_artists(), pageLength=10, style="width:100%;margin:0"))

    @render.ui
    def top_tracks_table():
        return ui.HTML(DT(top_tracks(), pageLength=10, style="width:100%;margin:0"))
