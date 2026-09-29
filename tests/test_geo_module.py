import re

from modules.geo import _build_country_choices


def test_country_choices_uses_code_plus_name_format():
    assert _build_country_choices()["United States"] == "(US) United States"


def test_country_choices_is_non_empty():
    assert len(_build_country_choices()) > 0


def test_country_choices_all_values_match_format():
    pattern = re.compile(r"^\([A-Z?]{1,2}\) .+$")
    for name, label in _build_country_choices().items():
        assert pattern.match(label), f"Bad label for {name!r}: {label!r}"


def test_country_choices_keys_match_label_suffix():
    for name, label in _build_country_choices().items():
        # label is "(XX) <name>"
        assert label.endswith(name), f"Label {label!r} does not end with key {name!r}"


def test_country_choices_known_countries_present():
    choices = _build_country_choices()
    for country in ("United Kingdom", "Brazil", "Germany", "Japan"):
        assert country in choices, f"{country!r} missing from country choices"


def test_build_country_choices_uses_db_countries_when_available(monkeypatch):
    monkeypatch.setattr("modules.geo.get_available_countries", lambda: ["Brazil", "Germany"])
    choices = _build_country_choices()
    assert set(choices.keys()) == {"Brazil", "Germany"}
    assert choices["Brazil"] == "(BR) Brazil"
    assert choices["Germany"] == "(DE) Germany"


def test_build_country_choices_falls_back_when_db_empty(monkeypatch):
    monkeypatch.setattr("modules.geo.get_available_countries", lambda: [])
    choices = _build_country_choices()
    assert len(choices) > 0
    assert "United States" in choices


def test_build_country_choices_unknown_country_gets_question_mark(monkeypatch):
    monkeypatch.setattr("modules.geo.get_available_countries", lambda: ["Neverland"])
    choices = _build_country_choices()
    assert choices["Neverland"] == "(?) Neverland"
