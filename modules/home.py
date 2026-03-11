import pandas as pd
import pylast
from cachetools import TTLCache, cached
from shiny import module, reactive, render, ui

# Cache shared across all sessions, refreshed every hour
_cache: TTLCache = TTLCache(maxsize=10, ttl=6 * 3600)


def _build_network(api_key: str, api_secret: str) -> pylast.LastFMNetwork:
    return pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)


@cached(_cache)
def _fetch_top_artists(network: pylast.LastFMNetwork, limit: int = 10) -> pd.DataFrame:
    items = network.get_top_artists(limit=limit)
    return pd.DataFrame(
        [{"#": i + 1, "Artist": t.item.name, "Playcount": int(t.weight)} for i, t in enumerate(items)]
    )


@cached(_cache)
def _fetch_top_tracks(network: pylast.LastFMNetwork, limit: int = 10) -> pd.DataFrame:
    items = network.get_top_tracks(limit=limit)
    return pd.DataFrame(
        [
            {
                "#": i + 1,
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
            ui.output_data_frame("top_artists_table"),
        ),
        ui.card(
            ui.card_header("Top Tracks"),
            ui.output_data_frame("top_tracks_table"),
        ),
        col_widths=[6, 6],
    )


@module.server
def home_server(input, output, session, api_key: str, api_secret: str):
    network = _build_network(api_key, api_secret)

    @reactive.calc
    def top_artists():
        return _fetch_top_artists(network)

    @reactive.calc
    def top_tracks():
        return _fetch_top_tracks(network)

    @render.data_frame
    def top_artists_table():
        df = top_artists().copy()
        df["Playcount"] = df["Playcount"].map("{:,}".format)
        return render.DataGrid(df, width="100%")

    @render.data_frame
    def top_tracks_table():
        df = top_tracks().copy()
        df["Playcount"] = df["Playcount"].map("{:,}".format)
        return render.DataGrid(df, width="100%")
