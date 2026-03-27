from __future__ import annotations

import time
from typing import Iterable


def _as_list(value: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value if str(item).strip()]


def find_element(tab, selectors: str | list[str] | tuple[str, ...], timeout: float = 5):
    for selector in _as_list(selectors):
        try:
            element = tab.ele(selector, timeout=timeout)
            if element:
                return element
        except Exception:
            continue
    return None


def click_element(tab, selectors: str | list[str] | tuple[str, ...], timeout: float = 5) -> bool:
    element = find_element(tab, selectors, timeout=timeout)
    if not element:
        return False
    try:
        try:
            element.scroll.to_see()
            time.sleep(0.3)
        except Exception:
            pass
        element.click()
        return True
    except Exception:
        try:
            tab.run_js("arguments[0].click();", element)
            return True
        except Exception:
            return False


def input_text(tab, selectors: str | list[str] | tuple[str, ...], value: str, timeout: float = 5) -> bool:
    element = find_element(tab, selectors, timeout=timeout)
    if not element:
        return False
    element.input(value)
    return True


def wait_for_url_contains(tab, fragments: str | Iterable[str], timeout: float = 20, interval: float = 1.0) -> str:
    targets = _as_list(fragments)
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = str(tab.url or "")
        if any(fragment in current for fragment in targets):
            return current
        time.sleep(interval)
    return str(tab.url or "")
