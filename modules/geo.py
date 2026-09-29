import logging

import pandas as pd
from itables.widget import ITable
from shiny import module, reactive, ui
from shinywidgets import output_widget, render_widget

from countries import COUNTRY_CODES, LASTFM_COUNTRY_NAME_MAP
from modules.db import get_available_countries, get_geo_top_artists, get_geo_top_tracks
from modules.utils import ARTISTS_COL_DEFS, TRACKS_COL_DEFS, dt_options, fmt, linkify

log = logging.getLogger(__name__)


def _build_country_choices() -> dict[str, str]:
    """Build {name: label} dict from DB countries, falling back to COUNTRY_CODES."""
    countries = get_available_countries()
    if countries:
        return {
            name: f"({COUNTRY_CODES.get(name, '?')}) {name}" for name in countries
        }
    # fallback if DB not yet available
    return {
        name: f"({code}) {name}"
        for name, code in COUNTRY_CODES.items()
        if LASTFM_COUNTRY_NAME_MAP.get(name, name) is not None
    }


@module.ui
def geo_ui():
    # Built on render, not at import: the app only renders this panel once the DB
    # is downloaded, so the choices come from the DB instead of the fallback list.
    return ui.div(
        ui.input_selectize(
            "country",
            "Country",
            choices=_build_country_choices(),
            selected="United States",
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Top Artists"),
                output_widget("geo_artists_table"),
            ),
            ui.card(
                ui.card_header("Top Tracks"),
                output_widget("geo_tracks_table"),
            ),
            col_widths=[6, 6],
        ),
    )


@module.server
def geo_server(input, output, session):
    @reactive.calc
    def geo_artists():
        if not input.country():
            return pd.DataFrame(columns=["Rank", "Artist", "ArtistUrl", "Listeners"])
        return fmt(get_geo_top_artists(input.country()), ["Listeners"])

    @reactive.calc
    def geo_tracks():
        if not input.country():
            return pd.DataFrame(columns=["Rank", "Track", "TrackUrl", "Artist", "ArtistUrl", "Listeners"])
        return fmt(get_geo_top_tracks(input.country()), ["Listeners"])

    # The table widgets are created once and then updated in place when the
    # country changes. Re-rendering them would rebuild the whole table and flicker.
    @render_widget
    def geo_artists_table():
        with reactive.isolate():
            return ITable(_artists_df(), **dt_options(ARTISTS_COL_DEFS))

    @render_widget
    def geo_tracks_table():
        with reactive.isolate():
            return ITable(_tracks_df(), **dt_options(TRACKS_COL_DEFS))

    def _artists_df() -> pd.DataFrame:
        return linkify(geo_artists(), "Artist", "ArtistUrl")

    def _tracks_df() -> pd.DataFrame:
        df = linkify(geo_tracks(), "Track", "TrackUrl")
        return linkify(df, "Artist", "ArtistUrl")

    @reactive.effect
    def _update_artists_table():
        df = _artists_df()
        widget = geo_artists_table.widget
        if widget is not None:
            widget.update(df, **dt_options(ARTISTS_COL_DEFS))

    @reactive.effect
    def _update_tracks_table():
        df = _tracks_df()
        widget = geo_tracks_table.widget
        if widget is not None:
            widget.update(df, **dt_options(TRACKS_COL_DEFS))
