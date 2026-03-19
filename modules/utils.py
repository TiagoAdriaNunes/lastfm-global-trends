import xml.dom.minidom

import pandas as pd
from itables.shiny import DT
from shiny import ui

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


def dt(df: pd.DataFrame, column_defs: list | None = None) -> ui.HTML:
    """Render a DataFrame as an interactive itables DataTable widget."""
    return ui.HTML(
        DT(df, pageLength=10, style="width:100%;margin:0", columnDefs=column_defs or [], maxBytes=0)
    )
