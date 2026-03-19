import asyncio
import logging
import queue

from itables.shiny import init_itables
from shiny import App, reactive, render, ui

from modules.db import ensure_db
from modules.geo import geo_server, geo_ui
from modules.home import home_server, home_ui

_theme = ui.Theme(preset="shiny").add_defaults(
    primary="#005399",
    **{
        "link-color": "#005399",
        "link-hover-color": "#003d73",
    },
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _loading_ui(pct: float) -> ui.Tag:
    bar_pct = max(5, int(pct * 100))  # keep bar visible even at 0%
    label = f"{bar_pct}%" if pct > 0 else "Connecting…"
    return ui.div(
        {
            "class": "d-flex flex-column align-items-center justify-content-center",
            "style": "min-height: 60vh;",
        },
        ui.tags.h5("Downloading database…", **{"class": "mb-2"}),
        ui.tags.p(
            {"class": "text-muted mb-4"},
            "Fetching Last.fm trends from Kaggle. Subsequent startups are instant.",
        ),
        ui.div(
            {"class": "w-50", "style": "max-width: 420px;"},
            ui.div(
                {"class": "progress", "style": "height: 22px; border-radius: 6px;"},
                ui.div(
                    {
                        "class": "progress-bar progress-bar-striped progress-bar-animated",
                        "role": "progressbar",
                        "style": f"width: {bar_pct}%; font-size: .85rem;",
                        "aria-valuenow": str(bar_pct),
                        "aria-valuemin": "0",
                        "aria-valuemax": "100",
                    },
                    label,
                ),
            ),
        ),
    )


app_ui = ui.page_navbar(
    ui.nav_panel("Global", ui.output_ui("home_panel")),
    ui.nav_panel("By Country", ui.output_ui("geo_panel")),
    theme=_theme,
    navbar_options=ui.navbar_options(inverse=True),
    header=ui.tags.head(
        ui.tags.link(rel="preconnect", href="https://fonts.googleapis.com"),
        ui.tags.link(
            rel="stylesheet",
            href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap",
        ),
        ui.HTML(init_itables()),
        ui.include_css("www/styles.css"),
    ),
    title="Last.fm Global Trends",
)


def server(input, output, session):
    db_ready = reactive.value(False)
    download_pct = reactive.value(0.0)
    _pct_queue: queue.SimpleQueue[float] = queue.SimpleQueue()
    _modules_started = False

    @reactive.extended_task
    async def _download():
        loop = asyncio.get_running_loop()

        def _cb(pct: float) -> None:
            _pct_queue.put_nowait(pct)

        await loop.run_in_executor(None, lambda: ensure_db(on_progress=_cb))

    @reactive.effect
    def _init():
        if not db_ready() and _download.status() == "initial":
            _download()

    @reactive.effect
    def _poll():
        if db_ready():
            return
        reactive.invalidate_later(0.3)
        pct = None
        while not _pct_queue.empty():
            pct = _pct_queue.get_nowait()
        if pct is not None:
            download_pct.set(pct)

    @reactive.effect
    def _on_complete():
        status = _download.status()
        if status == "success":
            db_ready.set(True)
        elif status == "error":
            try:
                _download.result()
            except Exception as exc:
                log.error("DB download failed: %s", exc, exc_info=True)
                ui.notification_show(
                    f"Download failed: {exc}",
                    type="error",
                    duration=None,
                )

    @reactive.effect
    def _start_modules():
        nonlocal _modules_started
        if db_ready() and not _modules_started:
            home_server("home")
            geo_server("geo")
            _modules_started = True

    @render.ui
    def home_panel():
        if not db_ready():
            return _loading_ui(download_pct())
        return home_ui("home")

    @render.ui
    def geo_panel():
        if not db_ready():
            return ui.div()
        return geo_ui("geo")


app = App(app_ui, server)
