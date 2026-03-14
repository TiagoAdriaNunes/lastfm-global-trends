import logging
import time

import pandas as pd
import pylast
from itables.shiny import DT
from shiny import ui

log = logging.getLogger(__name__)

_FETCH_LIMIT = 1000  # results per API request (API max)
_FETCH_PAGES = 1  # single request of 1000
_REQUEST_DELAY = 1  # seconds between requests

# chart.getTopTags pagination is broken on the Last.fm API — always returns the
# same data regardless of page. Fetch all tags in a single request instead.
_TAGS_LIMIT = 1000

_ARTISTS_COL_DEFS = [{"targets": 0, "width": "8%"}]
_TRACKS_COL_DEFS = [{"targets": 0, "width": "8%"}, {"targets": 1, "width": "35%"}]


def build_network(api_key: str, api_secret: str) -> pylast.LastFMNetwork:
    return pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)


def raw_request(network: pylast.LastFMNetwork, method: str, params: dict):
    log.info("Last.fm request: %s %s", method, params)
    request = pylast._Request(network, method, params)
    return request.execute(cacheable=True)


def fetch_paginated(
    network: pylast.LastFMNetwork,
    method: str,
    extra_params: dict,
    element_tag: str,
    row_builder,
    empty_columns: list[str],
) -> pd.DataFrame:
    rows = []
    for page in range(1, _FETCH_PAGES + 1):
        if page > 1:
            time.sleep(_REQUEST_DELAY)
        try:
            doc = raw_request(
                network,
                method,
                {"limit": _FETCH_LIMIT, "page": page, **extra_params},
            )
        except pylast.WSError as e:
            log.warning("%s failed page %d: %s", method, page, e)
            break
        offset = (page - 1) * _FETCH_LIMIT
        for i, el in enumerate(doc.getElementsByTagName(element_tag)):
            rows.append(row_builder(el, offset + i + 1))
    log.info("%s returned %d rows", method, len(rows))
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(columns=empty_columns)


def text(node, tag: str) -> str:
    els = node.getElementsByTagName(tag)
    if els and els[0].firstChild:
        return els[0].firstChild.nodeValue or ""
    return ""


def fmt(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        df[col] = df[col].map("{:,}".format)
    return df


def dt(df: pd.DataFrame, column_defs: list | None = None) -> ui.HTML:
    return ui.HTML(
        DT(df, pageLength=10, style="width:100%;margin:0", columnDefs=column_defs or [])
    )
