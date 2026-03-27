from __future__ import annotations

import sys

from DrissionPage import Chromium, ChromiumOptions

from .context import RunContext


class BrowserManager:
    def __init__(self, context: RunContext):
        self.context = context
        self.browser = None

    def init_browser(self, user_agent: str | None = None):
        options = ChromiumOptions()
        if self.context.browser_path:
            options.set_paths(browser_path=self.context.browser_path)
        options.set_pref("credentials_enable_service", False)
        options.set_argument("--hide-crash-restore-bubble")
        if self.context.proxy:
            options.set_proxy(self.context.proxy)
        options.auto_port()
        if user_agent:
            options.set_user_agent(user_agent)
        options.headless(self.context.headless)
        if sys.platform == "darwin":
            options.set_argument("--no-sandbox")
            options.set_argument("--disable-gpu")
        self.browser = Chromium(options)
        return self.browser

    def get_user_agent(self) -> str | None:
        browser = self.init_browser()
        try:
            return browser.latest_tab.run_js("return navigator.userAgent")
        finally:
            self.quit()

    def quit(self) -> None:
        if self.browser:
            try:
                self.browser.quit()
            except Exception:
                pass
            self.browser = None
