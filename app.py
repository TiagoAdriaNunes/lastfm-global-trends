import os

from shiny import App, ui

from modules.home import home_server, home_ui

API_KEY = os.environ["LASTFM_API_KEY"]
API_SECRET = os.environ["LASTFM_API_SECRET"]

app_ui = ui.page_fluid(
    ui.h1("Last.fm Global Trends"),
    home_ui("home"),
)


def server(input, output, session):
    home_server("home", api_key=API_KEY, api_secret=API_SECRET)


app = App(app_ui, server)
