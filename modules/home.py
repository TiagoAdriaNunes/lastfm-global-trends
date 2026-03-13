import logging
import time
from math import ceil

import pandas as pd
import plotly.express as px
import pylast
from cachetools import TTLCache, cached
from itables.shiny import DT
from shiny import module, reactive, render, ui
from shinywidgets import output_widget, render_plotly

log = logging.getLogger(__name__)

_artists_cache: TTLCache = TTLCache(maxsize=5, ttl=6 * 3600)
_tracks_cache: TTLCache = TTLCache(maxsize=5, ttl=6 * 3600)
_tags_cache: TTLCache = TTLCache(maxsize=5, ttl=6 * 3600)

_FETCH_LIMIT = 1000  # results per API request (API max)
_FETCH_PAGES = 1  # single request of 1000
_REQUEST_DELAY = 1  # seconds between requests
_ARTIST_TRACKS_PAGE_SIZE = 20
_CHART_PAGE_SIZE = 20

# chart.getTopTags pagination is broken on the Last.fm API — always returns the
# same data regardless of page. Fetch all tags in a single request instead.
_TAGS_LIMIT = 1000


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
def _fetch_top_artists(network: pylast.LastFMNetwork) -> pd.DataFrame:
    rows = []
    for page in range(1, _FETCH_PAGES + 1):
        if page > 1:
            time.sleep(_REQUEST_DELAY)
        doc = _raw_request(
            network, "chart.getTopArtists", {"limit": _FETCH_LIMIT, "page": page}
        )
        offset = (page - 1) * _FETCH_LIMIT
        for i, artist in enumerate(doc.getElementsByTagName("artist")):
            rows.append(
                {
                    "Rank": offset + i + 1,
                    "Artist": _text(artist, "name"),
                    "Listeners": int(_text(artist, "listeners") or 0),
                    "Scrobbles": int(_text(artist, "playcount") or 0),
                }
            )
    log.info("Fetched %d artists", len(rows))
    return pd.DataFrame(rows)


@cached(_tracks_cache)
def _fetch_top_tracks(network: pylast.LastFMNetwork) -> pd.DataFrame:
    rows = []
    for page in range(1, _FETCH_PAGES + 1):
        if page > 1:
            time.sleep(_REQUEST_DELAY)
        doc = _raw_request(
            network, "chart.getTopTracks", {"limit": _FETCH_LIMIT, "page": page}
        )
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
                    "Scrobbles": int(_text(track, "playcount") or 0),
                }
            )
    log.info("Fetched %d tracks", len(rows))
    return pd.DataFrame(rows)


@cached(_tags_cache)
def _fetch_top_tags(network: pylast.LastFMNetwork) -> pd.DataFrame:
    doc = _raw_request(network, "chart.getTopTags", {"limit": _TAGS_LIMIT})
    rows = [
        {
            "Rank": i + 1,
            "Tag": _text(tag, "name"),
            "Reach": int(_text(tag, "reach") or 0),
            "Taggings": int(_text(tag, "taggings") or 0),
        }
        for i, tag in enumerate(doc.getElementsByTagName("tag"))
    ]
    return pd.DataFrame(rows)


def _fmt(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        df[col] = df[col].map("{:,}".format)
    return df


_ARTISTS_COL_DEFS = [{"targets": 0, "width": "8%"}]
_TRACKS_COL_DEFS = [{"targets": 0, "width": "8%"}, {"targets": 1, "width": "35%"}]


def _dt(df: pd.DataFrame, column_defs: list | None = None) -> ui.HTML:
    return ui.HTML(
        DT(df, pageLength=10, style="width:100%;margin:0", columnDefs=column_defs or [])
    )


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
        margin=dict(l=8, r=8, t=8, b=16),
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
        artist = row[0]
        count = int(row[1])
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
                ui.card_header(f"Top Artists (Top {_FETCH_LIMIT * _FETCH_PAGES})"),
                ui.output_ui("top_artists_table"),
            ),
            ui.card(
                ui.card_header(f"Top Tracks (Top {_FETCH_LIMIT * _FETCH_PAGES})"),
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
                ui.card_header(
                    "Artists With Most Tracks in Top Tracks "
                    f"(Top {_FETCH_LIMIT * _FETCH_PAGES})"
                ),
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
                ui.card_header(f"Top Tags (Top {_TAGS_LIMIT:,})"),
                ui.output_ui("top_tags_table"),
            ),
            col_widths=[12],
        ),
    )


@module.server
def home_server(input, output, session, api_key: str, api_secret: str):
    network = _build_network(api_key, api_secret)

    @reactive.calc
    def top_artists_raw():
        return _fetch_top_artists(network)

    @reactive.calc
    def top_tracks_raw():
        return _fetch_top_tracks(network)

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
        return _dt(
            _fmt(top_artists_raw(), ["Listeners", "Scrobbles"]), _ARTISTS_COL_DEFS
        )

    @render.ui
    def top_tracks_table():
        return _dt(_fmt(top_tracks_raw(), ["Listeners", "Scrobbles"]), _TRACKS_COL_DEFS)

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
        return _dt(_fmt(_fetch_top_tags(network), ["Reach", "Taggings"]))
