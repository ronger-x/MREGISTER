from __future__ import annotations

import time
from pathlib import Path


def screenshot_dir(run_dir: Path) -> Path:
    path = run_dir / "screenshots"
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_screenshot(tab, run_dir: Path, stage: str, timestamp: bool = True) -> Path:
    target_dir = screenshot_dir(run_dir)
    filename = f"{stage}_{int(time.time())}.png" if timestamp else f"{stage}.png"
    output = target_dir / filename
    tab.get_screenshot(str(output))
    return output
