import xml.dom.minidom

import pylast

from modules import geo, utils


def _doc(xml_str: str):
    return xml.dom.minidom.parseString(xml_str).documentElement


def test_country_choices_uses_code_plus_name_format():
    assert geo.COUNTRY_CHOICES["United States"] == "(US) United States"


def test_fetch_geo_top_artists_parses_response(monkeypatch):
    geo._geo_artists_cache.clear()
    monkeypatch.setattr(utils, "FETCH_PAGES", 1)

    doc = _doc(
        """
        <topartists>
          <artist><name>Artist A</name><listeners>100</listeners></artist>
          <artist><name>Artist B</name><listeners>55</listeners></artist>
        </topartists>
        """
    )

    monkeypatch.setattr(utils, "raw_request", lambda *_args, **_kwargs: doc)

    result = geo._fetch_geo_top_artists(object(), "Brazil")

    assert result.to_dict("records") == [
        {"Rank": 1, "Artist": "Artist A", "Listeners": 100},
        {"Rank": 2, "Artist": "Artist B", "Listeners": 55},
    ]


def test_fetch_geo_top_tracks_parses_response(monkeypatch):
    geo._geo_tracks_cache.clear()
    monkeypatch.setattr(utils, "FETCH_PAGES", 1)

    doc = _doc(
        """
        <toptracks>
          <track>
            <name>Track A</name>
            <artist><name>Artist A</name></artist>
            <listeners>90</listeners>
          </track>
          <track>
            <name>Track B</name>
            <artist><name>Artist B</name></artist>
            <listeners>45</listeners>
          </track>
        </toptracks>
        """
    )

    monkeypatch.setattr(utils, "raw_request", lambda *_args, **_kwargs: doc)

    result = geo._fetch_geo_top_tracks(object(), "Brazil")

    assert result.to_dict("records") == [
        {"Rank": 1, "Track": "Track A", "Artist": "Artist A", "Listeners": 90},
        {"Rank": 2, "Track": "Track B", "Artist": "Artist B", "Listeners": 45},
    ]


def test_fetch_geo_top_artists_returns_empty_on_ws_error(monkeypatch):
    geo._geo_artists_cache.clear()

    def _raise_error(*_args, **_kwargs):
        raise pylast.WSError(None, 11, "Service Offline")

    monkeypatch.setattr(utils, "raw_request", _raise_error)

    result = geo._fetch_geo_top_artists(object(), "Brazil")

    assert result.empty
    assert list(result.columns) == ["Rank", "Artist", "Listeners"]


def test_fetch_geo_top_tracks_returns_empty_on_ws_error(monkeypatch):
    geo._geo_tracks_cache.clear()

    def _raise_error(*_args, **_kwargs):
        raise pylast.WSError(None, 11, "Service Offline")

    monkeypatch.setattr(utils, "raw_request", _raise_error)

    result = geo._fetch_geo_top_tracks(object(), "Brazil")

    assert result.empty
    assert list(result.columns) == ["Rank", "Track", "Artist", "Listeners"]


def test_fetch_geo_top_artists_empty_xml_returns_empty(monkeypatch):
    geo._geo_artists_cache.clear()

    doc = _doc("<topartists></topartists>")
    monkeypatch.setattr(utils, "raw_request", lambda *_a, **_kw: doc)

    result = geo._fetch_geo_top_artists(object(), "Brazil")

    assert result.empty
    assert list(result.columns) == ["Rank", "Artist", "Listeners"]
