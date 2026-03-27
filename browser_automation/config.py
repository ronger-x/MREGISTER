from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .context import RunContext


def _as_list(value: Any, fallback: list[str]) -> list[str]:
    if value is None:
        return list(fallback)
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return list(fallback)


def _as_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except Exception:
        return fallback


def _as_float(value: Any, fallback: float) -> float:
    try:
        return float(value)
    except Exception:
        return fallback


def _as_bool(value: Any, fallback: bool) -> bool:
    if value is None:
        return fallback
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class GenericWorkflowConfig:
    start_url: str | None
    cookie_accept_selectors: list[str]
    step1_button_selectors: list[str]
    step2_button_selectors: list[str]
    email_input_selectors: list[str]
    continue_button_selectors: list[str]
    password_input_selectors: list[str]
    password_button_selectors: list[str]
    code_input_selectors: list[str]
    verify_button_selectors: list[str]
    display_name_input_selectors: list[str]
    display_name_button_selectors: list[str]
    success_url_contains: list[str]
    code_timeout: int
    page_wait_seconds: float
    capture_screenshots: bool


def load_workflow_config(context: RunContext) -> GenericWorkflowConfig:
    payload = context.extra_env
    return GenericWorkflowConfig(
        start_url=str(payload.get("start_url") or "").strip() or None,
        cookie_accept_selectors=_as_list(payload.get("cookie_accept_selectors"), []),
        step1_button_selectors=_as_list(payload.get("step1_button_selectors"), []),
        step2_button_selectors=_as_list(payload.get("step2_button_selectors"), []),
        email_input_selectors=_as_list(payload.get("email_input_selectors"), ["@name=email", "@type=email", "tag:input"]),
        continue_button_selectors=_as_list(payload.get("continue_button_selectors"), ["使用邮箱继续", "继续", "Continue", "@type=submit"]),
        password_input_selectors=_as_list(payload.get("password_input_selectors"), ["@name=password", "@type=password"]),
        password_button_selectors=_as_list(payload.get("password_button_selectors"), []),
        code_input_selectors=_as_list(payload.get("code_input_selectors"), ["@name=code", "@type=text", "tag:input"]),
        verify_button_selectors=_as_list(payload.get("verify_button_selectors"), ["验证", "提交", "继续", "下一步", "Verify", "Continue", "@type=submit"]),
        display_name_input_selectors=_as_list(payload.get("display_name_input_selectors"), []),
        display_name_button_selectors=_as_list(payload.get("display_name_button_selectors"), []),
        success_url_contains=_as_list(payload.get("success_url_contains"), []),
        code_timeout=_as_int(payload.get("code_timeout"), 180),
        page_wait_seconds=_as_float(payload.get("page_wait_seconds"), 2.0),
        capture_screenshots=_as_bool(payload.get("capture_screenshots"), True),
    )
