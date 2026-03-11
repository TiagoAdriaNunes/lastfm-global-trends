from countries import COUNTRY_CODES


def test_country_codes_not_empty():
    assert len(COUNTRY_CODES) > 0


def test_known_countries():
    assert COUNTRY_CODES["United States"] == "US"
    assert COUNTRY_CODES["United Kingdom"] == "GB"
    assert COUNTRY_CODES["Brazil"] == "BR"
    assert COUNTRY_CODES["Germany"] == "DE"


def test_all_codes_are_two_letters():
    for name, code in COUNTRY_CODES.items():
        assert len(code) == 2, f"{name!r} has invalid code {code!r}"
        assert code.isupper(), f"{name!r} code {code!r} is not uppercase"


def test_no_duplicate_codes():
    codes = list(COUNTRY_CODES.values())
    assert len(codes) == len(set(codes)), "Duplicate country codes found"
