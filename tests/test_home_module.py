import xml.dom.minidom

import pandas as pd

from modules import home, utils
from modules.home import _artist_count_bars, _artist_track_counts, _top_artists_plot


def _doc(xml_str: str):
    return xml.dom.minidom.parseString(xml_str).documentElement


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


def test_fetch_top_artists_parses_response(monkeypatch):
    home._artists_cache.clear()
    monkeypatch.setattr(utils, "_FETCH_PAGES", 1)

    doc = _doc(
        """
        <artists>
          <artist><name>Radiohead</name><listeners>500</listeners><playcount>9000</playcount></artist>
          <artist><name>Portishead</name><listeners>200</listeners><playcount>3000</playcount></artist>
        </artists>
        """
    )
    monkeypatch.setattr(utils, "raw_request", lambda *_a, **_kw: doc)

    result = home._fetch_top_artists(object())

    assert result.to_dict("records") == [
        {"Rank": 1, "Artist": "Radiohead", "Listeners": 500, "Scrobbles": 9000},
        {"Rank": 2, "Artist": "Portishead", "Listeners": 200, "Scrobbles": 3000},
    ]


def test_fetch_top_tracks_parses_response(monkeypatch):
    home._tracks_cache.clear()
    monkeypatch.setattr(utils, "_FETCH_PAGES", 1)

    doc = _doc(
        """
        <tracks>
          <track>
            <name>Creep</name>
            <artist><name>Radiohead</name></artist>
            <listeners>800</listeners>
            <playcount>12000</playcount>
          </track>
        </tracks>
        """
    )
    monkeypatch.setattr(utils, "raw_request", lambda *_a, **_kw: doc)

    result = home._fetch_top_tracks(object())

    assert result.to_dict("records") == [
        {
            "Rank": 1, "Track": "Creep", "Artist": "Radiohead",
            "Listeners": 800, "Scrobbles": 12000,
        },
    ]


def test_fetch_top_tags_parses_response(monkeypatch):
    home._tags_cache.clear()

    doc = _doc(
        """
        <tags>
          <tag><name>rock</name><reach>1000</reach><taggings>5000</taggings></tag>
          <tag><name>pop</name><reach>800</reach><taggings>3000</taggings></tag>
        </tags>
        """
    )
    monkeypatch.setattr(home, "raw_request", lambda *_a, **_kw: doc)

    result = home._fetch_top_tags(object())

    assert result.to_dict("records") == [
        {"Rank": 1, "Tag": "rock", "Reach": 1000, "Taggings": 5000},
        {"Rank": 2, "Tag": "pop", "Reach": 800, "Taggings": 3000},
    ]
