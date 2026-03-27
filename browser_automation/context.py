from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RunContext:
    project_dir: Path
    task_dir: Path
    output_dir: Path
    run_dir: Path
    run_index: int
    results_file: Path
    proxy: str | None
    headless: bool
    browser_path: str | None
    extra_args: list[str]
    extra_env: dict[str, str]
    mail_provider: str | None
    gptmail_api_key: str | None
    gptmail_base_url: str | None
    gptmail_prefix: str | None
    gptmail_domain: str | None


def _parse_bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_context() -> RunContext:
    project_dir = Path(os.environ.get("MREGISTER_PROJECT_DIR", ".")).resolve()
    task_dir = Path(os.environ.get("MREGISTER_TASK_DIR", ".")).resolve()
    output_dir = Path(os.environ.get("MREGISTER_OUTPUT_DIR", task_dir / "output")).resolve()
    run_dir = Path(os.environ.get("MREGISTER_RUN_DIR", task_dir / "runs" / "run_0001")).resolve()
    results_file = Path(os.environ.get("MREGISTER_RESULTS_FILE", output_dir / "results.txt")).resolve()
    run_index = int(os.environ.get("MREGISTER_RUN_INDEX", "1") or "1")
    proxy = os.environ.get("MREGISTER_PROXY", "").strip() or None
    headless = _parse_bool(os.environ.get("MREGISTER_HEADLESS"), default=True)
    browser_path = os.environ.get("MREGISTER_BROWSER_PATH", "").strip() or None
    extra_args = [item for item in os.environ.get("MREGISTER_EXTRA_ARGS", "").splitlines() if item.strip()]
    extra_env_raw = os.environ.get("MREGISTER_EXTRA_ENV_JSON", "{}").strip() or "{}"
    try:
        extra_env_payload = json.loads(extra_env_raw)
    except json.JSONDecodeError:
        extra_env_payload = {}
    extra_env = {str(key): str(value) for key, value in extra_env_payload.items()} if isinstance(extra_env_payload, dict) else {}
    return RunContext(
        project_dir=project_dir,
        task_dir=task_dir,
        output_dir=output_dir,
        run_dir=run_dir,
        run_index=run_index,
        results_file=results_file,
        proxy=proxy,
        headless=headless,
        browser_path=browser_path,
        extra_args=extra_args,
        extra_env=extra_env,
        mail_provider=os.environ.get("MAIL_PROVIDER", "").strip() or None,
        gptmail_api_key=os.environ.get("GPTMAIL_API_KEY", "").strip() or None,
        gptmail_base_url=os.environ.get("GPTMAIL_BASE_URL", "").strip() or None,
        gptmail_prefix=os.environ.get("GPTMAIL_PREFIX", "").strip() or None,
        gptmail_domain=os.environ.get("GPTMAIL_DOMAIN", "").strip() or None,
    )
