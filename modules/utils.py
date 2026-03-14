import logging
import time
from collections.abc import Callable
from typing import Any

import pandas as pd
import pylast
from itables.shiny import DT
from shiny import ui

log = logging.getLogger(__name__)

FETCH_LIMIT = 1000  # results per API request (API max)
FETCH_PAGES = 1  # single request of 1000
_REQUEST_DELAY = 1  # seconds between requests (internal only)

# chart.getTopTags pagination is broken on the Last.fm API — always returns the
# same data regardless of page. Fetch all tags in a single request instead.
TAGS_LIMIT = 1000

ARTISTS_COL_DEFS = [{"targets": 0, "width": "8%"}]
TRACKS_COL_DEFS = [{"targets": 0, "width": "8%"}, {"targets": 1, "width": "35%"}]


def build_network(api_key: str, api_secret: str) -> pylast.LastFMNetwork:
    """Create an authenticated Last.fm network connection.

    Args:
        api_key: Last.fm API key.
        api_secret: Last.fm API secret.

    Returns:
        An authenticated pylast.LastFMNetwork instance.
    """
    return pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)


def raw_request(network: pylast.LastFMNetwork, method: str, params: dict):
    """Execute a raw cacheable request against the Last.fm API.

    Args:
        network: Authenticated Last.fm network instance.
        method: Last.fm API method name (e.g. "chart.getTopArtists").
        params: Query parameters to include in the request.

    Returns:
        Parsed XML document response from the API.
    """
    log.info("Last.fm request: %s %s", method, params)
    request = pylast._Request(network, method, params)
    return request.execute(cacheable=True)


def fetch_paginated(
    network: pylast.LastFMNetwork,
    method: str,
    extra_params: dict,
    element_tag: str,
    row_builder: Callable[[Any, int], dict],
    empty_columns: list[str],
) -> pd.DataFrame:
    """Fetch one or more pages from a Last.fm API method and build a DataFrame.

    Iterates up to FETCH_PAGES pages, sleeping _REQUEST_DELAY seconds between
    requests. Stops early if the API returns an error on any page.

    Args:
        network: Authenticated Last.fm network instance.
        method: Last.fm API method name (e.g. "geo.getTopArtists").
        extra_params: Additional query parameters merged into each request.
        element_tag: XML tag name used to identify each result element.
        row_builder: Callable(element: Any, rank: int) -> dict that maps an XML
            element to a row dictionary.
        empty_columns: Column names used to construct an empty DataFrame when
            no results are returned.

    Returns:
        DataFrame containing all collected rows, or an empty DataFrame with
        empty_columns if no data was retrieved.
    """
    rows = []
    for page in range(1, FETCH_PAGES + 1):
        if page > 1:
            time.sleep(_REQUEST_DELAY)
        try:
            doc = raw_request(
                network,
                method,
                {"limit": FETCH_LIMIT, "page": page, **extra_params},
            )
        except pylast.WSError as e:
            log.warning("%s failed page %d: %s", method, page, e)
            break
        offset = (page - 1) * FETCH_LIMIT
        for i, el in enumerate(doc.getElementsByTagName(element_tag)):
            rows.append(row_builder(el, offset + i + 1))
    log.info("%s returned %d rows", method, len(rows))
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=empty_columns)


def text(node, tag: str) -> str:
    """Extract the text content of the first matching child element.

    Args:
        node: XML DOM node to search within.
        tag: Tag name of the child element to extract text from.

    Returns:
        Text content of the first matching element, or an empty string if
        the element is missing or has no text node.
    """
    els = node.getElementsByTagName(tag)
    if els and els[0].firstChild:
        return els[0].firstChild.nodeValue or ""
    return ""


def fmt(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Format numeric columns with thousands-separator commas.

    Args:
        df: DataFrame to format.
        cols: Column names to apply comma formatting to.

    Returns:
        A copy of the DataFrame with the specified columns formatted as strings.
    """
    df = df.copy()
    for col in cols:
        df[col] = df[col].map("{:,}".format)
    return df


def dt(df: pd.DataFrame, column_defs: list | None = None) -> ui.HTML:
    """Render a DataFrame as an interactive itables DataTable widget.

    Args:
        df: DataFrame to display.
        column_defs: Optional list of DataTables columnDefs objects for
            column-level formatting (e.g. fixed widths).

    Returns:
        A Shiny ui.HTML component containing the rendered DataTable.
    """
    return ui.HTML(
        DT(df, pageLength=10, style="width:100%;margin:0", columnDefs=column_defs or [])
    )
