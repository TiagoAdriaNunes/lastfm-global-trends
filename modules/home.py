import pandas as pd
import pylast
from shiny import module, reactive, render, ui


def _build_network(api_key: str, api_secret: str) -> pylast.LastFMNetwork:
    return pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)


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
        items = network.get_top_artists(limit=10)
        return pd.DataFrame(
            [
                {"#": i + 1, "Artist": t.item.name, "Playcount": int(t.weight)}
                for i, t in enumerate(items)
            ]
        )

    @reactive.calc
    def top_tracks():
        items = network.get_top_tracks(limit=10)
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
