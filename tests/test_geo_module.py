import re

from modules import geo
from modules.geo import _build_country_choices


def test_country_choices_uses_code_plus_name_format():
    assert geo.COUNTRY_CHOICES["United States"] == "(US) United States"


def test_country_choices_is_non_empty():
    assert len(geo.COUNTRY_CHOICES) > 0


def test_country_choices_all_values_match_format():
    pattern = re.compile(r"^\([A-Z?]{1,2}\) .+$")
    for name, label in geo.COUNTRY_CHOICES.items():
        assert pattern.match(label), f"Bad label for {name!r}: {label!r}"


def test_country_choices_keys_match_label_suffix():
    for name, label in geo.COUNTRY_CHOICES.items():
        # label is "(XX) <name>"
        assert label.endswith(name), f"Label {label!r} does not end with key {name!r}"


def test_country_choices_known_countries_present():
    for country in ("United Kingdom", "Brazil", "Germany", "Japan"):
        assert country in geo.COUNTRY_CHOICES, f"{country!r} missing from COUNTRY_CHOICES"


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
