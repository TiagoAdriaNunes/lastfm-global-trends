import pandas as pd

from modules import home
from modules.home import _artist_count_bars, _artist_track_counts, _top_artists_plot


def test_artist_track_counts_counts_and_sorts():
    tracks = pd.DataFrame(
        {
            "Artist": [
                "Taylor Swift",
                "BTS",
                "Taylor Swift",
                " ",
                "",
                None,
                "BTS",
                "Radiohead",
                "Taylor Swift",
            ]
        }
    )

    result = _artist_track_counts(tracks)

    assert result.to_dict("records") == [
        {"Artist": "Taylor Swift", "Tracks": 3},
        {"Artist": "BTS", "Tracks": 2},
        {"Artist": "Radiohead", "Tracks": 1},
    ]


def test_artist_track_counts_top_n_limit():
    tracks = pd.DataFrame({"Artist": ["A", "A", "B", "C", "C", "C"]})

    result = _artist_track_counts(tracks, top_n=2)

    assert result.to_dict("records") == [
        {"Artist": "C", "Tracks": 3},
        {"Artist": "A", "Tracks": 2},
    ]


def test_artist_track_counts_missing_artist_column():
    tracks = pd.DataFrame({"Track": ["Song 1"]})

    result = _artist_track_counts(tracks)

    assert result.empty
    assert list(result.columns) == ["Artist", "Tracks"]


def test_artist_count_bars_empty_data_message():
    empty = pd.DataFrame(columns=["Artist", "Tracks"])

    tag = _artist_count_bars(empty)

    assert "No data available." in str(tag)


def test_artist_count_bars_uses_global_max_for_width():
    data = pd.DataFrame([{"Artist": "Artist A", "Tracks": 5}])

    tag = _artist_count_bars(data, max_count=20)

    assert "width: 25.00%;" in str(tag)


def test_top_artists_plot_scrobbles_mode_uses_scrobbles_axis():
    artists = pd.DataFrame(
        [
            {"Artist": "A", "Listeners": 10, "Scrobbles": 100},
            {"Artist": "B", "Listeners": 20, "Scrobbles": 300},
            {"Artist": "C", "Listeners": 30, "Scrobbles": 200},
        ]
    )

    fig = _top_artists_plot(artists, metric="scrobbles", top_n=2)

    assert fig.layout.xaxis.title.text == "Scrobbles"
    assert list(fig.data[0].y) == ["C", "B"]
    assert list(fig.data[0].x) == [200, 300]


def test_top_artists_plot_listeners_mode_uses_listeners_axis():
    artists = pd.DataFrame(
        [
            {"Artist": "A", "Listeners": 10, "Scrobbles": 100},
            {"Artist": "B", "Listeners": 20, "Scrobbles": 300},
            {"Artist": "C", "Listeners": 30, "Scrobbles": 200},
        ]
    )

    fig = _top_artists_plot(artists, metric="listeners", top_n=2)

    assert fig.layout.xaxis.title.text == "Listeners"
    assert list(fig.data[0].y) == ["B", "C"]
    assert list(fig.data[0].x) == [20, 30]


def test_top_artists_plot_empty_df_returns_figure():
    fig = _top_artists_plot(pd.DataFrame())

    assert fig is not None
    assert "No artist data available" in fig.layout.title.text


def test_artist_count_bars_auto_max_count():
    data = pd.DataFrame([{"Artist": "A", "Tracks": 10}, {"Artist": "B", "Tracks": 5}])

    tag = _artist_count_bars(data)

    # A is max (10), so A = 100%, B = 50%
    assert "width: 100.00%;" in str(tag)
    assert "width: 50.00%;" in str(tag)


def test_artist_count_bars_min_width_floor():
    # count=1, max_count=1000 → raw = 0.1%, should be clamped to 1%
    data = pd.DataFrame([{"Artist": "Rare", "Tracks": 1}])

    tag = _artist_count_bars(data, max_count=1000)

    assert "width: 1.00%;" in str(tag)

