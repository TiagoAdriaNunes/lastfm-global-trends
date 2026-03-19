import logging
from math import ceil

import pandas as pd
import plotly.express as px
from shiny import module, reactive, render, ui
from shinywidgets import output_widget, render_plotly

from modules.db import (
    get_global_top_artists,
    get_global_top_tags,
    get_global_top_tracks,
)
from modules.utils import ARTISTS_COL_DEFS, TRACKS_COL_DEFS, dt, fmt, linkify

log = logging.getLogger(__name__)

_ARTIST_TRACKS_PAGE_SIZE = 20
_CHART_PAGE_SIZE = 20


def _top_artists_plot(
    artists_df: pd.DataFrame,
    metric: str = "scrobbles",
    top_n: int = _CHART_PAGE_SIZE,
):
    if artists_df.empty:
        return px.bar(title="No artist data available")

    metric_col = "Listeners" if metric == "listeners" else "Scrobbles"
    top = artists_df.nlargest(top_n, metric_col).copy()
    top = top.sort_values(metric_col, ascending=True)

    fig = px.bar(
        top,
        x=metric_col,
        y="Artist",
        orientation="h",
        color_discrete_sequence=["#1b6ef3" if metric_col == "Listeners" else "#16a34a"],
    )
    fig.update_layout(
        height=520,
        margin={"l": 8, "r": 8, "t": 8, "b": 16},
        showlegend=False,
        xaxis_title=metric_col,
        yaxis_title="",
    )
    fig.update_xaxes(tickformat=",d")
    return fig


def _artist_track_counts(
    tracks_df: pd.DataFrame, top_n: int | None = None
) -> pd.DataFrame:
    if tracks_df.empty or "Artist" not in tracks_df.columns:
        return pd.DataFrame(columns=["Artist", "Tracks"])
    counts = (
        tracks_df["Artist"]
        .fillna("")
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
        .value_counts()
        .rename_axis("Artist")
    )
    if top_n is not None:
        counts = counts.head(top_n)
    counts = counts.reset_index(name="Tracks")
    return counts


def _artist_count_bars(df: pd.DataFrame, max_count: int | None = None) -> ui.Tag:
    if df.empty:
        return ui.p("No data available.")

    if max_count is None:
        max_count = int(df["Tracks"].max()) or 1
    rows: list[ui.Tag] = []
    for row in df.itertuples(index=False):
        artist = row.Artist
        count = int(row.Tracks)
        width_pct = max(1.0, (count / max_count) * 100)
        rows.append(
            ui.div(
                {"class": "artist-count-row"},
                ui.div({"class": "artist-count-label"}, artist),
                ui.div(
                    {"class": "artist-count-bar-wrap"},
                    ui.div(
                        {
                            "class": "artist-count-bar",
                            "style": f"width: {width_pct:.2f}%;",
                        }
                    ),
                ),
                ui.div({"class": "artist-count-value"}, f"{count}"),
            )
        )
    return ui.div({"class": "artist-count-chart"}, rows)


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
                ui.card_header("Top Artists Chart (Listeners / Scrobbles)"),
                output_widget("top_artists_chart"),
                ui.div(
                    {"style": "display:flex; justify-content:center;"},
                    ui.input_radio_buttons(
                        "artist_metric",
                        "Chart metric",
                        choices={
                            "listeners": "Listeners",
                            "scrobbles": "Scrobbles",
                        },
                        selected="scrobbles",
                        inline=True,
                    ),
                ),
            ),
            ui.card(
                ui.card_header("Artists With Most Tracks in Top Tracks"),
                ui.output_ui("top_track_artists_chart"),
                ui.div(
                    {"class": "artist-count-controls"},
                    ui.input_action_button(
                        "track_artists_prev",
                        "Previous",
                        class_="btn btn-outline-secondary",
                    ),
                    ui.output_text("top_track_artists_page_info"),
                    ui.input_action_button(
                        "track_artists_next",
                        "Next",
                        class_="btn btn-outline-secondary",
                    ),
                ),
            ),
            col_widths=[6, 6],
        ),
        ui.layout_columns(
            ui.card(
                ui.card_header("Top Tags (Top 10,000)"),
                ui.output_ui("top_tags_table"),
            ),
            col_widths=[12],
        ),
    )


@module.server
def home_server(input, output, session):
    @reactive.calc
    def top_artists_raw():
        return get_global_top_artists()

    @reactive.calc
    def top_tracks_raw():
        return get_global_top_tracks()

    @reactive.calc
    def top_track_artist_counts():
        return _artist_track_counts(top_tracks_raw())

    track_artists_page = reactive.value(1)

    @reactive.calc
    def top_track_artist_total_pages():
        total_rows = len(top_track_artist_counts())
        return max(1, ceil(total_rows / _ARTIST_TRACKS_PAGE_SIZE))

    @reactive.effect
    def _clamp_track_artists_page():
        current = track_artists_page()
        max_page = top_track_artist_total_pages()
        if current > max_page:
            track_artists_page.set(max_page)
        elif current < 1:
            track_artists_page.set(1)

    @reactive.effect
    @reactive.event(input.track_artists_prev)
    def _track_artists_prev():
        track_artists_page.set(max(1, track_artists_page() - 1))

    @reactive.effect
    @reactive.event(input.track_artists_next)
    def _track_artists_next():
        track_artists_page.set(
            min(top_track_artist_total_pages(), track_artists_page() + 1)
        )

    @reactive.calc
    def top_track_artist_counts_page():
        page = track_artists_page()
        start = (page - 1) * _ARTIST_TRACKS_PAGE_SIZE
        stop = start + _ARTIST_TRACKS_PAGE_SIZE
        return top_track_artist_counts().iloc[start:stop]

    @reactive.calc
    def top_track_artist_max_tracks():
        counts = top_track_artist_counts()
        if counts.empty:
            return 1
        return int(counts["Tracks"].max()) or 1

    @render_plotly
    def top_artists_chart():
        return _top_artists_plot(
            top_artists_raw(), input.artist_metric() or "scrobbles"
        )

    @render.ui
    def top_artists_table():
        df = fmt(top_artists_raw(), ["Listeners", "Scrobbles"])
        df = linkify(df, "Artist", "ArtistUrl")
        return dt(df, ARTISTS_COL_DEFS)

    @render.ui
    def top_tracks_table():
        df = fmt(top_tracks_raw(), ["Listeners", "Scrobbles"])
        df = linkify(df, "Track", "TrackUrl")
        df = linkify(df, "Artist", "ArtistUrl")
        return dt(df, TRACKS_COL_DEFS)

    @render.text
    def top_track_artists_page_info():
        return (
            f"Page {track_artists_page()} of {top_track_artist_total_pages()} "
            f"({len(top_track_artist_counts())} artists)"
        )

    @render.ui
    def top_track_artists_chart():
        return _artist_count_bars(
            top_track_artist_counts_page(),
            max_count=top_track_artist_max_tracks(),
        )

    @render.ui
    def top_tags_table():
        return dt(fmt(get_global_top_tags(), ["Reach", "Taggings"]))
