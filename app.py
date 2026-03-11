import logging
import os

from itables.shiny import init_itables
from shiny import App, ui

from modules.geo import geo_server, geo_ui
from modules.home import home_server, home_ui

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

API_KEY = os.environ["LASTFM_API_KEY"]
API_SECRET = os.environ["LASTFM_API_SECRET"]

app_ui = ui.page_navbar(
    ui.nav_panel("Global", home_ui("home")),
    ui.nav_panel("By Country", geo_ui("geo")),
    header=ui.tags.head(
        ui.HTML(init_itables()),
        ui.include_css("www/styles.css"),
    ),
    title="Last.fm Global Trends",
)


def server(input, output, session):
    home_server("home", api_key=API_KEY, api_secret=API_SECRET)
    geo_server("geo", api_key=API_KEY, api_secret=API_SECRET)


app = App(app_ui, server)
