"""Optional Windows/macOS/Linux system-tray launcher."""

from __future__ import annotations

import threading
import webbrowser

from easybackup.app import create_app
from easybackup.config import Settings


def _icon_image():
    from PIL import Image, ImageDraw

    image = Image.new("RGBA", (64, 64), (18, 26, 43, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle(
        (8, 9, 56, 55), radius=12, fill=(36, 49, 75, 255)
    )
    draw.polygon(
        [(19, 32), (28, 41), (47, 21), (52, 27), (28, 49), (14, 37)],
        fill=(70, 220, 162, 255),
    )
    return image


def main() -> None:
    try:
        import pystray
        import uvicorn
    except ImportError as exc:
        raise SystemExit(
            "托盘模式需要可选依赖：pip install 'easybackup[tray]'"
        ) from exc

    settings = Settings.from_env()
    settings.validate_network_binding()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings),
            host=settings.host,
            port=settings.port,
            log_level=settings.log_level.lower(),
        )
    )
    server_thread = threading.Thread(
        target=server.run, name="easybackup-server", daemon=True
    )
    server_thread.start()
    url_host = (
        "127.0.0.1"
        if settings.host in {"0.0.0.0", "::"}
        else settings.host
    )
    url = f"http://{url_host}:{settings.port}/"

    def open_console(icon=None, item=None):
        del icon, item
        webbrowser.open(url)

    def quit_app(icon, item):
        del item
        server.should_exit = True
        icon.stop()

    menu = pystray.Menu(
        pystray.MenuItem("打开控制台", open_console, default=True),
        pystray.MenuItem("退出 EasyBackup", quit_app),
    )
    icon = pystray.Icon("EasyBackup", _icon_image(), "EasyBackup", menu)
    if settings.open_browser:
        threading.Timer(1.2, open_console).start()
    try:
        icon.run()
    finally:
        server.should_exit = True
        server_thread.join(timeout=settings.shutdown_timeout_seconds + 5)


if __name__ == "__main__":
    main()

