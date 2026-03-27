from __future__ import annotations

from .context import load_context
from .workflow import run_task


def main() -> int:
    context = load_context()
    result = run_task(context)
    result_path = context.run_dir / "metadata.json"
    result.write(result_path)
    print(f"[browser-automation] prepared run directory: {context.run_dir}")
    print(f"[browser-automation] wrote metadata: {result_path}")
    print(f"[browser-automation] status={result.status} message={result.message}")
    return 0 if result.success else 1


__all__ = ["main", "run_task"]
