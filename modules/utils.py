import xml.dom.minidom

import pandas as pd
from shiny import ui
from shinywidgets import output_widget

ARTISTS_COL_DEFS = [{"targets": 0, "width": "8%"}]
TRACKS_COL_DEFS = [{"targets": 0, "width": "8%"}, {"targets": 1, "width": "35%"}]


def text(node: xml.dom.minidom.Element, tag: str) -> str:
    """Extract text content of the first matching child tag, or '' if missing/empty."""
    elements = node.getElementsByTagName(tag)
    if not elements:
        return ""
    child = elements[0].firstChild
    return child.nodeValue if child else ""


def fmt(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Format numeric columns with thousands-separator commas."""
    df = df.copy()
    for col in cols:
        df[col] = df[col].map("{:,}".format)
    return df


def linkify(df: pd.DataFrame, col: str, url_col: str) -> pd.DataFrame:
    """Wrap col values with <a> tags using url_col, then drop url_col."""
    df = df.copy()
    urls = df[url_col].astype(str)
    safe = urls.str.startswith("https://") | urls.str.startswith("http://")
    mask = df[url_col].notna() & (urls != "") & safe
    df.loc[mask, col] = (
        '<a href="' + df.loc[mask, url_col] + '" target="_blank">' + df.loc[mask, col] + "</a>"
    )
    return df.drop(columns=[url_col])


def table_output(id: str) -> ui.Tag:
    """Output placeholder for an ITable widget, sized to the table's content.

    Without an explicit height, output_widget() is a fill item capped at 400px
    with overflow hidden, which clips the table's info line and pagination.
    """
    return output_widget(id, height="auto")


def dt_options(column_defs: list | None = None) -> dict:
    """itables options shared by all ITable widgets in the app."""
    return {
        "pageLength": 10,
        "style": "width:100%;margin:0",
        "columnDefs": column_defs or [],
        "maxBytes": 0,
        "allow_html": True,
    }
