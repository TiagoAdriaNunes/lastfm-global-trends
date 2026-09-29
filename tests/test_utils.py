import xml.dom.minidom

import pandas as pd

from modules.utils import dt_options
from modules.utils import fmt as _fmt
from modules.utils import text as _text


def _make_node(xml_str: str):
    return xml.dom.minidom.parseString(xml_str).documentElement


def test_text_returns_value():
    node = _make_node("<artist><name>Radiohead</name></artist>")
    assert _text(node, "name") == "Radiohead"


def test_text_missing_tag_returns_empty():
    node = _make_node("<artist><name>Radiohead</name></artist>")
    assert _text(node, "listeners") == ""


def test_text_empty_tag_returns_empty():
    node = _make_node("<artist><name></name></artist>")
    assert _text(node, "name") == ""


def test_fmt_formats_numbers():
    df = pd.DataFrame({"Listeners": [1000000, 500], "Scrobbles": [2000000, 100]})
    result = _fmt(df, ["Listeners", "Scrobbles"])
    assert result["Listeners"].tolist() == ["1,000,000", "500"]
    assert result["Scrobbles"].tolist() == ["2,000,000", "100"]


def test_fmt_does_not_mutate_original():
    df = pd.DataFrame({"Listeners": [1000]})
    _fmt(df, ["Listeners"])
    assert df["Listeners"].tolist() == [1000]


def test_fmt_untouched_columns_unchanged():
    df = pd.DataFrame({"Rank": [1, 2], "Listeners": [1000, 500]})
    result = _fmt(df, ["Listeners"])
    assert result["Rank"].tolist() == [1, 2]


def test_dt_options_accepted_by_itables():
    # ITable widgets can only be built inside a Shiny session, so validate the
    # options with itables' HTML renderer, which takes the same arguments.
    from itables import to_html_datatable

    df = pd.DataFrame({"Artist": ["Radiohead"], "Listeners": [1000]})
    html = to_html_datatable(df, **dt_options([{"targets": 0, "width": "8%"}]))
    assert "<table" in html


def test_dt_options_defaults_to_no_column_defs():
    assert dt_options()["columnDefs"] == []
