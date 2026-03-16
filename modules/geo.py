import logging

import pandas as pd
from shiny import module, reactive, render, ui

from countries import COUNTRY_CODES, LASTFM_COUNTRY_NAME_MAP
from modules.db import get_available_countries, get_geo_top_artists, get_geo_top_tracks
from modules.utils import ARTISTS_COL_DEFS, TRACKS_COL_DEFS, dt, fmt

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


COUNTRY_CHOICES = _build_country_choices()


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
                ui.card_header("Top Artists"),
                ui.output_ui("geo_artists_table"),
            ),
            ui.card(
                ui.card_header("Top Tracks"),
                ui.output_ui("geo_tracks_table"),
            ),
            col_widths=[6, 6],
        ),
    )


@module.server
def geo_server(input, output, session):
    @reactive.calc
    def geo_artists():
        if not input.country():
            return pd.DataFrame(columns=["Rank", "Artist", "Listeners"])
        return fmt(get_geo_top_artists(input.country()), ["Listeners"])

    @reactive.calc
    def geo_tracks():
        if not input.country():
            return pd.DataFrame(columns=["Rank", "Track", "Artist", "Listeners"])
        return fmt(get_geo_top_tracks(input.country()), ["Listeners"])

    @render.ui
    def geo_artists_table():
        return dt(geo_artists(), ARTISTS_COL_DEFS)

    @render.ui
    def geo_tracks_table():
        return dt(geo_tracks(), TRACKS_COL_DEFS)
